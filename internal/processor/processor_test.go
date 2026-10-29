package processor

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/Mikewattoski/atrium/internal/model"
)

type fakeMsg struct {
	body    []byte
	acked   bool
	nacked  bool
	requeue bool
}

func (m *fakeMsg) Body() []byte { return m.body }
func (m *fakeMsg) Ack() error   { m.acked = true; return nil }
func (m *fakeMsg) Nack(requeue bool) error {
	m.nacked, m.requeue = true, requeue
	return nil
}

func goodMsg(dev string, v float64) *fakeMsg {
	b, _ := json.Marshal(ev(dev, "temperature", v, 0))
	return &fakeMsg{body: b}
}

type fakeStore struct {
	batchErr   error
	oneErr     map[string]error
	alertErr   error
	batchCalls int
	inserted   []model.ReadingEvent
	decisions  []AlertDecision
}

func (f *fakeStore) InsertReadings(_ context.Context, evs []model.ReadingEvent) error {
	f.batchCalls++
	if f.batchErr != nil {
		return f.batchErr
	}
	f.inserted = append(f.inserted, evs...)
	return nil
}

func (f *fakeStore) InsertReading(_ context.Context, e model.ReadingEvent) error {
	if err := f.oneErr[e.DeviceID]; err != nil {
		return err
	}
	f.inserted = append(f.inserted, e)
	return nil
}

func (f *fakeStore) ApplyAlerts(_ context.Context, ds []AlertDecision) error {
	if f.alertErr != nil {
		return f.alertErr
	}
	f.decisions = append(f.decisions, ds...)
	return nil
}

var (
	errFK   = &pgconn.PgError{Code: "23503"} // foreign key violation: unknown device
	errConn = errors.New("connection refused")
)

func handle(store *fakeStore, msgs ...*fakeMsg) {
	p := New(store, Options{Threshold: 30})
	in := make([]Msg, len(msgs))
	for i, m := range msgs {
		in[i] = m
	}
	p.HandleBatch(context.Background(), in)
}

func TestHandleBatchHappyPath(t *testing.T) {
	store := &fakeStore{}
	cool, hot := goodMsg("dev-001", 22), goodMsg("dev-002", 35)

	handle(store, cool, hot)

	if store.batchCalls != 1 || len(store.inserted) != 2 {
		t.Fatalf("batchCalls=%d inserted=%d, want 1 and 2", store.batchCalls, len(store.inserted))
	}
	if !cool.acked || !hot.acked {
		t.Error("all messages should be acked")
	}
	if len(store.decisions) != 2 ||
		store.decisions[0].Action != ResolveAlert || store.decisions[1].Action != OpenAlert {
		t.Errorf("unexpected alert decisions: %+v", store.decisions)
	}
}

func TestHandleBatchMalformedGoesToDLQ(t *testing.T) {
	store := &fakeStore{}
	bad, good := &fakeMsg{body: []byte("not json")}, goodMsg("dev-001", 22)

	handle(store, bad, good)

	if !bad.nacked || bad.requeue {
		t.Errorf("malformed: nacked=%v requeue=%v, want nacked without requeue", bad.nacked, bad.requeue)
	}
	if !good.acked {
		t.Error("good message should still be acked")
	}
}

func TestHandleBatchUnknownDeviceFallsBack(t *testing.T) {
	store := &fakeStore{batchErr: errFK, oneErr: map[string]error{"dev-999": errFK}}
	good, unknown := goodMsg("dev-001", 22), goodMsg("dev-999", 22)

	handle(store, good, unknown)

	if !good.acked {
		t.Error("known device should be acked after fallback")
	}
	if !unknown.nacked || unknown.requeue {
		t.Errorf("unknown device: nacked=%v requeue=%v, want dead-letter", unknown.nacked, unknown.requeue)
	}
	if len(store.inserted) != 1 || store.inserted[0].DeviceID != "dev-001" {
		t.Errorf("inserted = %+v, want only dev-001", store.inserted)
	}
}

func TestHandleBatchTransientErrorRequeues(t *testing.T) {
	store := &fakeStore{batchErr: errConn}
	a, b := goodMsg("dev-001", 22), goodMsg("dev-002", 22)

	handle(store, a, b)

	for _, m := range []*fakeMsg{a, b} {
		if m.acked || !m.nacked || !m.requeue {
			t.Errorf("want nack with requeue, got acked=%v nacked=%v requeue=%v", m.acked, m.nacked, m.requeue)
		}
	}
	if len(store.decisions) != 0 {
		t.Error("no alerts should be applied when the insert failed")
	}
}

func TestHandleBatchAlertFailureRequeues(t *testing.T) {
	store := &fakeStore{alertErr: errConn}
	m := goodMsg("dev-001", 35)

	handle(store, m)

	if m.acked || !m.nacked || !m.requeue {
		t.Errorf("want nack with requeue, got acked=%v nacked=%v requeue=%v", m.acked, m.nacked, m.requeue)
	}
}
