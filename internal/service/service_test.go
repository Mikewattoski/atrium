package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Mikewattoski/atrium/internal/model"
	"github.com/Mikewattoski/atrium/internal/repository"
)

type fakeRepo struct {
	deviceExists bool
	readings     []model.Reading
	lastFilter   repository.ReadingFilter
	lastLimit    int
}

func (f *fakeRepo) ListDevices(context.Context) ([]model.Device, error) { return nil, nil }
func (f *fakeRepo) DeviceExists(context.Context, string) (bool, error)  { return f.deviceExists, nil }
func (f *fakeRepo) ListReadings(_ context.Context, flt repository.ReadingFilter) ([]model.Reading, error) {
	f.lastFilter = flt
	if len(f.readings) > flt.Limit {
		return f.readings[:flt.Limit], nil
	}
	return f.readings, nil
}
func (f *fakeRepo) LatestReading(context.Context, string, string) (*model.Reading, error) {
	return nil, model.ErrNotFound
}
func (f *fakeRepo) ListAlerts(_ context.Context, _ string, limit int) ([]model.Alert, error) {
	f.lastLimit = limit
	return []model.Alert{}, nil
}

func makeReadings(n int) []model.Reading {
	base := time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)
	out := make([]model.Reading, n)
	for i := range out {
		out[i] = model.Reading{ID: int64(i), DeviceID: "d1", Metric: "temperature", TS: base.Add(-time.Duration(i) * time.Minute)}
	}
	return out
}

func TestGetReadings(t *testing.T) {
	tests := []struct {
		name       string
		exists     bool
		readings   []model.Reading
		query      ReadingsQuery
		wantErr    error
		wantItems  int
		wantCursor bool
		wantLimit  int // limit passed to repo
	}{
		{name: "device not found", exists: false, wantErr: model.ErrNotFound},
		{
			name: "from after to is invalid", exists: true,
			query:   ReadingsQuery{From: time.Now(), To: time.Now().Add(-time.Hour)},
			wantErr: model.ErrInvalid,
		},
		{
			name: "default limit applied", exists: true, readings: makeReadings(3),
			query: ReadingsQuery{}, wantItems: 3, wantLimit: defaultLimit + 1,
		},
		{
			name: "limit capped at max", exists: true, readings: makeReadings(1),
			query: ReadingsQuery{Limit: 999999}, wantItems: 1, wantLimit: maxLimit + 1,
		},
		{
			name: "next cursor when more rows exist", exists: true, readings: makeReadings(3),
			query: ReadingsQuery{Limit: 2}, wantItems: 2, wantCursor: true, wantLimit: 3,
		},
		{
			name: "no cursor on last page", exists: true, readings: makeReadings(2),
			query: ReadingsQuery{Limit: 2}, wantItems: 2, wantCursor: false, wantLimit: 3,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			repo := &fakeRepo{deviceExists: tc.exists, readings: tc.readings}
			svc := New(repo)

			page, err := svc.GetReadings(context.Background(), "d1", tc.query)

			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("want error %v, got %v", tc.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(page.Items) != tc.wantItems {
				t.Errorf("items = %d, want %d", len(page.Items), tc.wantItems)
			}
			if (page.NextCursor != nil) != tc.wantCursor {
				t.Errorf("next cursor present = %v, want %v", page.NextCursor != nil, tc.wantCursor)
			}
			if repo.lastFilter.Limit != tc.wantLimit {
				t.Errorf("repo limit = %d, want %d", repo.lastFilter.Limit, tc.wantLimit)
			}
		})
	}
}

func TestListAlertsValidation(t *testing.T) {
	tests := []struct {
		name    string
		status  string
		wantErr bool
	}{
		{"empty ok", "", false},
		{"open ok", "open", false},
		{"resolved ok", "resolved", false},
		{"bad status", "banana", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := New(&fakeRepo{}).ListAlerts(context.Background(), tc.status, 0)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantErr && !errors.Is(err, model.ErrInvalid) {
				t.Errorf("want ErrInvalid, got %v", err)
			}
		})
	}
}
