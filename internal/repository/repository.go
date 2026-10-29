package repository

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Mikewattoski/atrium/internal/model"
)

type ReadingFilter struct {
	DeviceID string
	Metric   string // empty = any metric
	From     time.Time
	To       time.Time // exclusive
	Limit    int
}

type Repository interface {
	ListDevices(ctx context.Context) ([]model.Device, error)
	DeviceExists(ctx context.Context, id string) (bool, error)
	ListReadings(ctx context.Context, f ReadingFilter) ([]model.Reading, error)
	LatestReading(ctx context.Context, deviceID, metric string) (*model.Reading, error)
	ListAlerts(ctx context.Context, status string, limit int) ([]model.Alert, error)
}

type Postgres struct {
	pool *pgxpool.Pool
}

func NewPostgres(pool *pgxpool.Pool) *Postgres {
	return &Postgres{pool: pool}
}

func (p *Postgres) ListDevices(ctx context.Context) ([]model.Device, error) {
	rows, err := p.pool.Query(ctx,
		`SELECT id, name, type, location, created_at FROM devices ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []model.Device{}
	for rows.Next() {
		var d model.Device
		if err := rows.Scan(&d.ID, &d.Name, &d.Type, &d.Location, &d.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func (p *Postgres) DeviceExists(ctx context.Context, id string) (bool, error) {
	var ok bool
	err := p.pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM devices WHERE id = $1)`, id).Scan(&ok)
	return ok, err
}

func (p *Postgres) ListReadings(ctx context.Context, f ReadingFilter) ([]model.Reading, error) {
	rows, err := p.pool.Query(ctx, `
		SELECT id, device_id, metric, value, ts
		FROM readings
		WHERE device_id = $1
		  AND ($2::text = '' OR metric = $2)
		  AND ts >= $3 AND ts < $4
		ORDER BY ts DESC
		LIMIT $5`,
		f.DeviceID, f.Metric, f.From, f.To, f.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []model.Reading{}
	for rows.Next() {
		var r model.Reading
		if err := rows.Scan(&r.ID, &r.DeviceID, &r.Metric, &r.Value, &r.TS); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (p *Postgres) LatestReading(ctx context.Context, deviceID, metric string) (*model.Reading, error) {
	var r model.Reading
	err := p.pool.QueryRow(ctx, `
		SELECT id, device_id, metric, value, ts
		FROM readings
		WHERE device_id = $1 AND ($2::text = '' OR metric = $2)
		ORDER BY ts DESC
		LIMIT 1`, deviceID, metric).
		Scan(&r.ID, &r.DeviceID, &r.Metric, &r.Value, &r.TS)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, model.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &r, nil
}

func (p *Postgres) ListAlerts(ctx context.Context, status string, limit int) ([]model.Alert, error) {
	rows, err := p.pool.Query(ctx, `
		SELECT id, device_id, metric, value, threshold, status, created_at, resolved_at
		FROM alerts
		WHERE ($1::text = '' OR status = $1)
		ORDER BY created_at DESC
		LIMIT $2`, status, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []model.Alert{}
	for rows.Next() {
		var a model.Alert
		if err := rows.Scan(&a.ID, &a.DeviceID, &a.Metric, &a.Value, &a.Threshold,
			&a.Status, &a.CreatedAt, &a.ResolvedAt); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}
