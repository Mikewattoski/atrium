package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Mikewattoski/atrium/internal/cache"
	"github.com/Mikewattoski/atrium/internal/config"
	"github.com/Mikewattoski/atrium/internal/handler"
	"github.com/Mikewattoski/atrium/internal/repository"
	"github.com/Mikewattoski/atrium/internal/service"
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
		log.Printf("redis unavailable (%v): serving from PostgreSQL only", err)
	}

	repo := repository.NewPostgres(pool)
	svc := service.New(repo).WithCache(rc)
	h := handler.New(svc, pool.Ping).
		WithLimiter(handler.RateLimit(rc, cfg.RateLimit, time.Minute))

	r := gin.New()
	r.Use(gin.Logger(), gin.Recovery())
	h.Register(r)

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           r,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("api listening on %s (cache ttl %s, rate limit %d/min)", cfg.HTTPAddr, cfg.CacheTTL, cfg.RateLimit)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("shutdown: %v", err)
	}
}
