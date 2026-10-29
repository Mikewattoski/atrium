package service

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Mikewattoski/atrium/internal/model"
	"github.com/Mikewattoski/atrium/internal/repository"
)

const (
	defaultLimit = 100
	maxLimit     = 1000
)

// LatestCache is an optional cache for the latest reading (cache-aside).
// GetLatest returns (nil, nil) on a miss. An empty metric means "any metric".
type LatestCache interface {
	GetLatest(ctx context.Context, deviceID, metric string) (*model.Reading, error)
	SetLatest(ctx context.Context, deviceID, metric string, r model.Reading) error
}

type Service struct {
	repo  repository.Repository
	cache LatestCache
}

// WithCache enables cache-aside reads for GetLatest.
func (s *Service) WithCache(c LatestCache) *Service {
	s.cache = c
	return s
}

func New(repo repository.Repository) *Service {
	return &Service{repo: repo}
}

type ReadingsQuery struct {
	Metric string
	From   time.Time
	To     time.Time
	Limit  int
	Cursor *time.Time // return readings strictly older than this
}

type ReadingsPage struct {
	Items      []model.Reading `json:"items"`
	NextCursor *string         `json:"next_cursor,omitempty"`
}

func (s *Service) ListDevices(ctx context.Context) ([]model.Device, error) {
	return s.repo.ListDevices(ctx)
}

func (s *Service) GetReadings(ctx context.Context, deviceID string, q ReadingsQuery) (*ReadingsPage, error) {
	if q.Limit <= 0 {
		q.Limit = defaultLimit
	}
	if q.Limit > maxLimit {
		q.Limit = maxLimit
	}
	if q.To.IsZero() {
		q.To = time.Now().UTC().Add(time.Minute)
	}
	if q.From.IsZero() {
		q.From = q.To.Add(-24 * time.Hour)
	}
	if !q.From.Before(q.To) {
		return nil, fmt.Errorf("%w: from must be before to", model.ErrInvalid)
	}
	if q.Cursor != nil && q.Cursor.Before(q.To) {
		q.To = *q.Cursor
	}

	exists, err := s.repo.DeviceExists(ctx, deviceID)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("%w: device %q", model.ErrNotFound, deviceID)
	}

	// fetch one extra row to know whether another page exists
	items, err := s.repo.ListReadings(ctx, repository.ReadingFilter{
		DeviceID: deviceID,
		Metric:   q.Metric,
		From:     q.From,
		To:       q.To,
		Limit:    q.Limit + 1,
	})
	if err != nil {
		return nil, err
	}

	page := &ReadingsPage{Items: items}
	if len(items) > q.Limit {
		page.Items = items[:q.Limit]
		cur := page.Items[q.Limit-1].TS.UTC().Format(time.RFC3339Nano)
		page.NextCursor = &cur
	}
	if page.Items == nil {
		page.Items = []model.Reading{}
	}
	return page, nil
}

// GetLatest is cache-aside: Redis first, PostgreSQL on a miss, then refill.
// The bool reports whether the answer came from the cache. Cache errors are
// logged and never fail the request.
func (s *Service) GetLatest(ctx context.Context, deviceID, metric string) (*model.Reading, bool, error) {
	if s.cache != nil {
		r, err := s.cache.GetLatest(ctx, deviceID, metric)
		if err != nil {
			log.Printf("cache get failed, falling back to database: %v", err)
		} else if r != nil {
			return r, true, nil
		}
	}

	r, err := s.repo.LatestReading(ctx, deviceID, metric)
	if err != nil {
		return nil, false, err
	}

	if s.cache != nil {
		if err := s.cache.SetLatest(ctx, deviceID, metric, *r); err != nil {
			log.Printf("cache refill failed: %v", err)
		}
	}
	return r, false, nil
}

func (s *Service) ListAlerts(ctx context.Context, status string, limit int) ([]model.Alert, error) {
	if status != "" && status != "open" && status != "resolved" {
		return nil, fmt.Errorf("%w: status must be open or resolved", model.ErrInvalid)
	}
	if limit <= 0 {
		limit = defaultLimit
	}
	if limit > maxLimit {
		limit = maxLimit
	}
	return s.repo.ListAlerts(ctx, status, limit)
}
