package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Mikewattoski/atrium/internal/cache"
	"github.com/Mikewattoski/atrium/internal/config"
	"github.com/Mikewattoski/atrium/internal/processor"
	"github.com/Mikewattoski/atrium/internal/queue"
)

func main() {
	cfg := config.Load()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("db pool: %v", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("db ping: %v", err)
	}

	rc := cache.New(cfg.RedisAddr, cfg.CacheTTL)
	defer rc.Close()
	if err := rc.Ping(ctx); err != nil {
		log.Printf("redis unavailable (%v): continuing without cache updates", err)
	}

	proc := processor.New(processor.NewPgStore(pool), processor.Options{
		BatchSize:     cfg.BatchSize,
		FlushInterval: cfg.FlushInterval,
		Threshold:     cfg.AlertTempThreshold,
		RetryDelay:    2 * time.Second,
	}).WithCache(rc)

	log.Printf("processor starting (batch=%d, flush=%s, alert threshold=%.1f, cache ttl=%s)",
		cfg.BatchSize, cfg.FlushInterval, cfg.AlertTempThreshold, cfg.CacheTTL)

	// prefetch == batch size, so a full batch can always be collected
	queue.RunConsumer(ctx, cfg.AMQPURL, cfg.BatchSize, proc.Run)

	log.Println("processor stopped")
}
