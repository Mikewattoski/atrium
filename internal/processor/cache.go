package processor

import (
	"context"
	"log"

	"github.com/Mikewattoski/atrium/internal/model"
)

// LatestWriter caches the newest reading per device. It is optional and
// best-effort: PostgreSQL stays the source of truth, so a cache failure
// never fails, requeues or delays a batch.
type LatestWriter interface {
	SetLatestBatch(ctx context.Context, rs []model.Reading) error
}

func (p *Processor) WithCache(c LatestWriter) *Processor {
	p.cache = c
	return p
}

func (p *Processor) updateCache(ctx context.Context, items []item) {
	if p.cache == nil || len(items) == 0 {
		return
	}

	// keep only the newest reading per device+metric to cut Redis commands
	newest := make(map[deviceMetric]model.Reading, len(items))
	for _, it := range items {
		k := deviceMetric{it.ev.DeviceID, it.ev.Metric}
		if cur, ok := newest[k]; !ok || !it.ev.TS.Before(cur.TS) {
			newest[k] = model.Reading{
				DeviceID: it.ev.DeviceID,
				Metric:   it.ev.Metric,
				Value:    it.ev.Value,
				TS:       it.ev.TS,
			}
		}
	}
	rs := make([]model.Reading, 0, len(newest))
	for _, r := range newest {
		rs = append(rs, r)
	}

	if err := p.cache.SetLatestBatch(ctx, rs); err != nil {
		log.Printf("cache update failed (non-fatal): %v", err)
	}
}
