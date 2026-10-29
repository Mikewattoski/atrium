package cache

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/Mikewattoski/atrium/internal/model"
)

var t0 = time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)

func newTestCache(t *testing.T) (*Cache, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	c := New(mr.Addr(), time.Minute)
	t.Cleanup(func() { _ = c.Close() })
	return c, mr
}

func reading(v float64, offset time.Duration) model.Reading {
	return model.Reading{DeviceID: "dev-001", Metric: "temperature", Value: v, TS: t0.Add(offset)}
}

func TestMissReturnsNil(t *testing.T) {
	c, _ := newTestCache(t)
	got, err := c.GetLatest(context.Background(), "dev-001", "temperature")
	if err != nil || got != nil {
		t.Fatalf("got (%v, %v), want (nil, nil)", got, err)
	}
}

func TestSetAndGetPerMetricAndAny(t *testing.T) {
	c, _ := newTestCache(t)
	ctx := context.Background()

	if err := c.SetLatestBatch(ctx, []model.Reading{reading(21, 0)}); err != nil {
		t.Fatal(err)
	}
	for _, metric := range []string{"temperature", ""} {
		got, err := c.GetLatest(ctx, "dev-001", metric)
		if err != nil || got == nil || got.Value != 21 {
			t.Errorf("metric %q: got (%v, %v), want value 21", metric, got, err)
		}
	}
}

func TestOlderReadingNeverOverwritesNewer(t *testing.T) {
	c, _ := newTestCache(t)
	ctx := context.Background()

	_ = c.SetLatestBatch(ctx, []model.Reading{reading(2, time.Second)})
	_ = c.SetLatestBatch(ctx, []model.Reading{reading(1, 0)}) // stale write

	got, _ := c.GetLatest(ctx, "dev-001", "temperature")
	if got == nil || got.Value != 2 {
		t.Fatalf("got %v, want the newer value 2", got)
	}
}

func TestEntriesExpire(t *testing.T) {
	c, mr := newTestCache(t)
	ctx := context.Background()

	_ = c.SetLatestBatch(ctx, []model.Reading{reading(21, 0)})
	mr.FastForward(2 * time.Minute)

	got, err := c.GetLatest(ctx, "dev-001", "temperature")
	if err != nil || got != nil {
		t.Fatalf("got (%v, %v), want a miss after the TTL", got, err)
	}
}

func TestAllowLimitsPerWindow(t *testing.T) {
	c, _ := newTestCache(t)
	c.now = func() time.Time { return t0 }
	ctx := context.Background()

	for i := 1; i <= 3; i++ {
		ok, _, err := c.Allow(ctx, "1.2.3.4", 3, time.Minute)
		if err != nil || !ok {
			t.Fatalf("call %d: ok=%v err=%v, want allowed", i, ok, err)
		}
	}
	ok, retry, err := c.Allow(ctx, "1.2.3.4", 3, time.Minute)
	if err != nil || ok || retry <= 0 {
		t.Fatalf("4th call: ok=%v retry=%v err=%v, want blocked with retry > 0", ok, retry, err)
	}

	// a different client has its own budget
	if ok, _, _ := c.Allow(ctx, "5.6.7.8", 3, time.Minute); !ok {
		t.Error("other client should not be limited")
	}
}
