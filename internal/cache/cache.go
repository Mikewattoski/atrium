package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Mikewattoski/atrium/internal/model"
)

const anyMetric = "any"

// setIfNewer stores the reading only if its timestamp (unix ms) is >= the
// cached one, so a stale write can never overwrite fresher data.
var setIfNewer = redis.NewScript(`
local cur = redis.call('HGET', KEYS[1], 'ts')
if (not cur) or tonumber(ARGV[1]) >= tonumber(cur) then
  redis.call('HSET', KEYS[1], 'ts', ARGV[1], 'data', ARGV[2])
  redis.call('PEXPIRE', KEYS[1], ARGV[3])
  return 1
end
return 0
`)

// incrWindow counts requests in a window and starts the expiry on the first one.
var incrWindow = redis.NewScript(`
local n = redis.call('INCR', KEYS[1])
if n == 1 then redis.call('PEXPIRE', KEYS[1], ARGV[1]) end
return n
`)

type Cache struct {
	rdb *redis.Client
	ttl time.Duration
	now func() time.Time
}

func New(addr string, ttl time.Duration) *Cache {
	return &Cache{
		rdb: redis.NewClient(&redis.Options{
			Addr:         addr,
			DialTimeout:  500 * time.Millisecond,
			ReadTimeout:  500 * time.Millisecond,
			WriteTimeout: 500 * time.Millisecond,
			MaxRetries:   1,
		}),
		ttl: ttl,
		now: time.Now,
	}
}

func (c *Cache) Ping(ctx context.Context) error { return c.rdb.Ping(ctx).Err() }
func (c *Cache) Close() error                   { return c.rdb.Close() }

// key: an empty metric means "latest reading of any metric".
func key(deviceID, metric string) string {
	if metric == "" {
		metric = anyMetric
	}
	return "latest:" + deviceID + ":" + metric
}

// GetLatest returns (nil, nil) on a cache miss.
func (c *Cache) GetLatest(ctx context.Context, deviceID, metric string) (*model.Reading, error) {
	raw, err := c.rdb.HGet(ctx, key(deviceID, metric), "data").Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var r model.Reading
	if err := json.Unmarshal([]byte(raw), &r); err != nil {
		return nil, fmt.Errorf("decode cached reading: %w", err)
	}
	return &r, nil
}

// SetLatest caches r under one key (metric "" = the "any metric" key).
func (c *Cache) SetLatest(ctx context.Context, deviceID, metric string, r model.Reading) error {
	pipe := c.rdb.Pipeline()
	if err := c.queue(ctx, pipe, key(deviceID, metric), r); err != nil {
		return err
	}
	_, err := pipe.Exec(ctx)
	return err
}

// SetLatestBatch caches each reading under its per-metric key and the
// per-device "any metric" key, in a single round trip.
func (c *Cache) SetLatestBatch(ctx context.Context, rs []model.Reading) error {
	if len(rs) == 0 {
		return nil
	}
	pipe := c.rdb.Pipeline()
	for _, r := range rs {
		for _, k := range []string{key(r.DeviceID, r.Metric), key(r.DeviceID, "")} {
			if err := c.queue(ctx, pipe, k, r); err != nil {
				return err
			}
		}
	}
	_, err := pipe.Exec(ctx)
	return err
}

func (c *Cache) queue(ctx context.Context, pipe redis.Pipeliner, k string, r model.Reading) error {
	data, err := json.Marshal(r)
	if err != nil {
		return err
	}
	setIfNewer.Eval(ctx, pipe, []string{k}, r.TS.UnixMilli(), string(data), c.ttl.Milliseconds())
	return nil
}

// Allow implements a fixed-window rate limiter: at most `limit` calls per
// `window` for the given id. On error it reports allowed=true (fail open).
func (c *Cache) Allow(ctx context.Context, id string, limit int, window time.Duration) (bool, time.Duration, error) {
	now := c.now()
	start := now.Truncate(window)
	k := fmt.Sprintf("rl:%s:%d", id, start.Unix())

	n, err := incrWindow.Run(ctx, c.rdb, []string{k}, window.Milliseconds()).Int64()
	if err != nil {
		return true, 0, err
	}
	if n > int64(limit) {
		return false, start.Add(window).Sub(now), nil
	}
	return true, 0, nil
}
