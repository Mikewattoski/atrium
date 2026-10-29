package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/Mikewattoski/atrium/internal/config"
	"github.com/Mikewattoski/atrium/internal/ingest"
	"github.com/Mikewattoski/atrium/internal/queue"
	telemetryv1 "github.com/Mikewattoski/atrium/proto/telemetry/v1"
)

func main() {
	cfg := config.Load()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pub, err := queue.NewPublisher(cfg.AMQPURL)
	if err != nil {
		log.Fatalf("rabbitmq: %v", err)
	}
	defer pub.Close()

	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	srv := grpc.NewServer()
	telemetryv1.RegisterTelemetryServiceServer(srv, ingest.NewServer(pub))
	reflection.Register(srv)

	go func() {
		<-ctx.Done()
		log.Println("shutting down...")
		stopped := make(chan struct{})
		go func() {
			srv.GracefulStop()
			close(stopped)
		}()
		select {
		case <-stopped:
		case <-time.After(10 * time.Second):
			log.Println("graceful stop timed out, forcing stop")
			srv.Stop()
		}
	}()

	log.Printf("ingest gRPC listening on %s", cfg.GRPCAddr)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
