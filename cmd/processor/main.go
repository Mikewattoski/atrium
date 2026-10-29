package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

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

	proc := processor.New(processor.NewPgStore(pool), processor.Options{
		BatchSize:     cfg.BatchSize,
		FlushInterval: cfg.FlushInterval,
		Threshold:     cfg.AlertTempThreshold,
		RetryDelay:    2 * time.Second,
	})

	log.Printf("processor starting (batch=%d, flush=%s, alert threshold=%.1f)",
		cfg.BatchSize, cfg.FlushInterval, cfg.AlertTempThreshold)

	// prefetch == batch size, so a full batch can always be collected
	queue.RunConsumer(ctx, cfg.AMQPURL, cfg.BatchSize, proc.Run)

	log.Println("processor stopped")
}
