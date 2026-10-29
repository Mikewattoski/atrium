package processor

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Mikewattoski/atrium/internal/model"
)

type Store interface {
	InsertReadings(ctx context.Context, evs []model.ReadingEvent) error
	InsertReading(ctx context.Context, ev model.ReadingEvent) error
	ApplyAlerts(ctx context.Context, ds []AlertDecision) error
}

// IsDataError reports whether err is caused by the data itself (SQLSTATE
// class 22 data exception or 23 integrity violation, e.g. unknown device),
// as opposed to a transient problem such as a lost connection.
func IsDataError(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return strings.HasPrefix(pgErr.Code, "22") || strings.HasPrefix(pgErr.Code, "23")
	}
	return false
}

const (
	insertBatchSQL = `
		INSERT INTO readings (device_id, metric, value, ts)
		SELECT * FROM unnest($1::text[], $2::text[], $3::float8[], $4::timestamptz[])
		ON CONFLICT (device_id, metric, ts) DO NOTHING`

	insertOneSQL = `
		INSERT INTO readings (device_id, metric, value, ts)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (device_id, metric, ts) DO NOTHING`

	openAlertSQL = `
		INSERT INTO alerts (device_id, metric, value, threshold)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (device_id, metric) WHERE status = 'open' DO NOTHING`

	resolveAlertSQL = `
		UPDATE alerts SET status = 'resolved', resolved_at = now()
		WHERE device_id = $1 AND metric = $2 AND status = 'open'`
)

type PgStore struct {
	pool *pgxpool.Pool
}

func NewPgStore(pool *pgxpool.Pool) *PgStore {
	return &PgStore{pool: pool}
}

// InsertReadings writes the whole batch in a single statement.
func (s *PgStore) InsertReadings(ctx context.Context, evs []model.ReadingEvent) error {
	ids := make([]string, len(evs))
	metrics := make([]string, len(evs))
	values := make([]float64, len(evs))
	tss := make([]time.Time, len(evs))
	for i, ev := range evs {
		ids[i], metrics[i], values[i], tss[i] = ev.DeviceID, ev.Metric, ev.Value, ev.TS
	}
	_, err := s.pool.Exec(ctx, insertBatchSQL, ids, metrics, values, tss)
	return err
}

func (s *PgStore) InsertReading(ctx context.Context, ev model.ReadingEvent) error {
	_, err := s.pool.Exec(ctx, insertOneSQL, ev.DeviceID, ev.Metric, ev.Value, ev.TS)
	return err
}

// ApplyAlerts sends all alert statements in one network round trip.
func (s *PgStore) ApplyAlerts(ctx context.Context, ds []AlertDecision) error {
	b := &pgx.Batch{}
	for _, d := range ds {
		switch d.Action {
		case OpenAlert:
			b.Queue(openAlertSQL, d.DeviceID, d.Metric, d.Value, d.Threshold)
		case ResolveAlert:
			b.Queue(resolveAlertSQL, d.DeviceID, d.Metric)
		}
	}
	if b.Len() == 0 {
		return nil
	}
	br := s.pool.SendBatch(ctx, b)
	defer br.Close()
	for i := 0; i < b.Len(); i++ {
		if _, err := br.Exec(); err != nil {
			return err
		}
	}
	return nil
}
