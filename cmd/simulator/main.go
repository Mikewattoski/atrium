package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	telemetryv1 "github.com/Mikewattoski/atrium/proto/telemetry/v1"
)

func main() {
	addr := flag.String("addr", "localhost:9090", "ingest gRPC address")
	devices := flag.Int("devices", 100, "number of simulated devices")
	interval := flag.Duration("interval", time.Second, "reading interval per device")
	flag.Parse()

	if *devices < 1 || *interval <= 0 {
		log.Fatal("devices must be >= 1 and interval must be > 0")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	out := make(chan *telemetryv1.Reading, 1024)

	// fan-in: many device goroutines -> one sender goroutine owning the stream
	var wg sync.WaitGroup
	for i := 1; i <= *devices; i++ {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			runDevice(ctx, id, *interval, out)
		}(fmt.Sprintf("dev-%03d", i))
	}

	sendDone := make(chan struct{})
	go func() {
		defer close(sendDone)
		sendLoop(ctx, *addr, out)
	}()

	log.Printf("simulating %d devices every %s -> %s", *devices, *interval, *addr)

	<-ctx.Done()
	log.Println("stopping devices...")
	wg.Wait()
	close(out) // sender drains what is left, then closes the stream
	<-sendDone
	log.Println("done")
}

// runDevice produces a temperature reading per tick: a mean-reverting random
// walk around 22C with an occasional 5-tick spike of +10C (to trigger alerts).
func runDevice(ctx context.Context, id string, interval time.Duration, out chan<- *telemetryv1.Reading) {
	temp := 21 + rand.Float64()*3
	spikeLeft := 0

	// jitter the start so devices don't all fire at the same instant
	select {
	case <-time.After(time.Duration(rand.Int64N(int64(interval)))):
	case <-ctx.Done():
		return
	}

	t := time.NewTicker(interval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			temp += (rand.Float64()-0.5)*0.6 + (22-temp)*0.02
			value := temp
			if spikeLeft > 0 {
				value += 10
				spikeLeft--
			} else if rand.Float64() < 0.005 {
				spikeLeft = 5
			}

			r := &telemetryv1.Reading{
				DeviceId: id,
				Metric:   "temperature",
				Value:    value,
				Ts:       timestamppb.New(time.Now().Truncate(time.Millisecond)),
			}
			select {
			case out <- r: // blocks when ingest is slow/down: natural backpressure
			case <-ctx.Done():
				return
			}
		}
	}
}

// sendLoop keeps a stream to ingest open, reconnecting after failures.
// It returns once `out` is closed and fully drained.
func sendLoop(ctx context.Context, addr string, out <-chan *telemetryv1.Reading) {
	for {
		done, err := streamOnce(addr, out)
		if done {
			return
		}
		log.Printf("stream error: %v (retrying in 2s)", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
}

func streamOnce(addr string, out <-chan *telemetryv1.Reading) (done bool, err error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return false, err
	}
	defer conn.Close()

	stream, err := telemetryv1.NewTelemetryServiceClient(conn).StreamReadings(context.Background())
	if err != nil {
		return false, err
	}

	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	sent := 0

	for {
		select {
		case r, ok := <-out:
			if !ok {
				ack, err := stream.CloseAndRecv()
				if err != nil {
					return false, err
				}
				log.Printf("final ack: accepted=%d rejected=%d", ack.Accepted, ack.Rejected)
				return true, nil
			}
			if err := stream.Send(r); err != nil {
				return false, err
			}
			sent++
		case <-tick.C:
			log.Printf("sent %d readings in the last 5s", sent)
			sent = 0
		}
	}
}
