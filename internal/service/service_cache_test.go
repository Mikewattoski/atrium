package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Mikewattoski/atrium/internal/model"
)

type latestRepo struct {
	fakeRepo
	reading *model.Reading
	calls   int
}

func (r *latestRepo) LatestReading(context.Context, string, string) (*model.Reading, error) {
	r.calls++
	if r.reading == nil {
		return nil, model.ErrNotFound
	}
	cp := *r.reading
	return &cp, nil
}

type fakeCache struct {
	stored map[string]*model.Reading
	getErr error
	sets   int
}

func (f *fakeCache) GetLatest(_ context.Context, dev, metric string) (*model.Reading, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	return f.stored[dev+"|"+metric], nil
}

func (f *fakeCache) SetLatest(_ context.Context, dev, metric string, r model.Reading) error {
	f.sets++
	if f.stored == nil {
		f.stored = map[string]*model.Reading{}
	}
	f.stored[dev+"|"+metric] = &r
	return nil
}

func sample() *model.Reading {
	return &model.Reading{ID: 7, DeviceID: "d1", Metric: "temperature", Value: 21.5, TS: time.Now()}
}

func TestGetLatestCacheAside(t *testing.T) {
	repo := &latestRepo{reading: sample()}
	cache := &fakeCache{}
	svc := New(repo).WithCache(cache)
	ctx := context.Background()

	r, hit, err := svc.GetLatest(ctx, "d1", "temperature")
	if err != nil || hit || r.Value != 21.5 {
		t.Fatalf("first call: r=%v hit=%v err=%v, want a miss served from the database", r, hit, err)
	}
	if cache.sets != 1 {
		t.Errorf("cache refills = %d, want 1", cache.sets)
	}

	_, hit, err = svc.GetLatest(ctx, "d1", "temperature")
	if err != nil || !hit {
		t.Fatalf("second call: hit=%v err=%v, want a cache hit", hit, err)
	}
	if repo.calls != 1 {
		t.Errorf("database calls = %d, want 1 (second call must not reach the database)", repo.calls)
	}
}

func TestGetLatestCacheErrorFallsBackToDatabase(t *testing.T) {
	repo := &latestRepo{reading: sample()}
	svc := New(repo).WithCache(&fakeCache{getErr: errors.New("redis down")})

	r, hit, err := svc.GetLatest(context.Background(), "d1", "temperature")
	if err != nil || hit || r == nil {
		t.Fatalf("r=%v hit=%v err=%v, want a database answer with no error", r, hit, err)
	}
}

func TestGetLatestWithoutCache(t *testing.T) {
	svc := New(&latestRepo{reading: sample()})

	r, hit, err := svc.GetLatest(context.Background(), "d1", "temperature")
	if err != nil || hit || r == nil {
		t.Fatalf("r=%v hit=%v err=%v, want a plain database answer", r, hit, err)
	}
}

func TestGetLatestNotFoundIsNotCached(t *testing.T) {
	cache := &fakeCache{}
	svc := New(&latestRepo{}).WithCache(cache)

	_, _, err := svc.GetLatest(context.Background(), "nope", "")
	if !errors.Is(err, model.ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
	if cache.sets != 0 {
		t.Errorf("cache sets = %d, want 0", cache.sets)
	}
}
