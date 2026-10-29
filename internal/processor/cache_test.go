package processor

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Mikewattoski/atrium/internal/model"
)

type fakeLatest struct {
	got []model.Reading
	err error
}

func (f *fakeLatest) SetLatestBatch(_ context.Context, rs []model.Reading) error {
	f.got = rs
	return f.err
}

func msgAt(dev string, v float64, offset time.Duration) *fakeMsg {
	b, _ := json.Marshal(ev(dev, "temperature", v, offset))
	return &fakeMsg{body: b}
}

func TestCacheFailureDoesNotAffectAcks(t *testing.T) {
	latest := &fakeLatest{err: errConn}
	m := goodMsg("dev-001", 22)

	New(&fakeStore{}, Options{Threshold: 30}).WithCache(latest).
		HandleBatch(context.Background(), []Msg{m})

	if !m.acked || m.nacked {
		t.Errorf("acked=%v nacked=%v, want acked even though the cache failed", m.acked, m.nacked)
	}
	if len(latest.got) != 1 {
		t.Errorf("cache writes = %d, want 1", len(latest.got))
	}
}

func TestCacheReceivesNewestReadingPerDevice(t *testing.T) {
	latest := &fakeLatest{}
	older := msgAt("dev-001", 20, 0)
	newer := msgAt("dev-001", 25, time.Second)
	other := msgAt("dev-002", 18, 0)

	New(&fakeStore{}, Options{Threshold: 30}).WithCache(latest).
		HandleBatch(context.Background(), []Msg{newer, older, other})

	if len(latest.got) != 2 {
		t.Fatalf("cache got %d readings, want 2 (one per device)", len(latest.got))
	}
	for _, r := range latest.got {
		if r.DeviceID == "dev-001" && r.Value != 25 {
			t.Errorf("dev-001 cached value = %v, want the newest (25)", r.Value)
		}
	}
}

func TestNothingCachedWhenInsertFails(t *testing.T) {
	latest := &fakeLatest{}
	m := goodMsg("dev-001", 22)

	New(&fakeStore{batchErr: errConn}, Options{Threshold: 30}).WithCache(latest).
		HandleBatch(context.Background(), []Msg{m})

	if len(latest.got) != 0 {
		t.Errorf("cache writes = %d, want 0 because nothing was stored", len(latest.got))
	}
}
