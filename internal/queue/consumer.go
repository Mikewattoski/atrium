package queue

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// RunConsumer connects to RabbitMQ, declares the topology, sets the prefetch
// and hands the delivery channel to handle. If the connection drops it
// reconnects with exponential backoff. It returns once ctx is cancelled.
func RunConsumer(ctx context.Context, url string, prefetch int,
	handle func(ctx context.Context, deliveries <-chan amqp.Delivery)) {

	backoff := time.Second
	for {
		connected, err := consumeOnce(ctx, url, prefetch, handle)
		if ctx.Err() != nil {
			return
		}
		if connected {
			backoff = time.Second
		}
		log.Printf("consumer stopped: %v (reconnecting in %s)", err, backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 15*time.Second {
			backoff *= 2
		}
	}
}

func consumeOnce(ctx context.Context, url string, prefetch int,
	handle func(context.Context, <-chan amqp.Delivery)) (connected bool, err error) {

	conn, err := amqp.Dial(url)
	if err != nil {
		return false, fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return false, fmt.Errorf("channel: %w", err)
	}
	defer ch.Close()

	if err := DeclareTopology(ch); err != nil {
		return false, err
	}
	// prefetch = max unacknowledged messages the broker will push to us
	if err := ch.Qos(prefetch, 0, false); err != nil {
		return false, fmt.Errorf("qos: %w", err)
	}
	deliveries, err := ch.Consume(QueueName, "", false, false, false, false, nil)
	if err != nil {
		return false, fmt.Errorf("consume: %w", err)
	}

	log.Printf("consuming from %s (prefetch %d)", QueueName, prefetch)
	handle(ctx, deliveries)
	return true, errors.New("delivery channel closed")
}
