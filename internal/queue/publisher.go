package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Publisher publishes persistent messages with broker confirms and
// reconnects lazily if the connection or channel was closed.
type Publisher struct {
	url string

	mu   sync.Mutex
	conn *amqp.Connection
	ch   *amqp.Channel
}

func NewPublisher(url string) (*Publisher, error) {
	p := &Publisher{url: url}
	if err := p.connect(); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *Publisher) connect() error {
	if p.conn != nil {
		_ = p.conn.Close()
		p.conn, p.ch = nil, nil
	}
	conn, err := amqp.Dial(p.url)
	if err != nil {
		return fmt.Errorf("dial rabbitmq: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("open channel: %w", err)
	}
	if err := ch.Confirm(false); err != nil {
		_ = conn.Close()
		return fmt.Errorf("enable confirms: %w", err)
	}
	if err := DeclareTopology(ch); err != nil {
		_ = conn.Close()
		return err
	}
	p.conn, p.ch = conn, ch
	return nil
}

// Publish sends one message and waits for the broker's confirm.
func (p *Publisher) Publish(ctx context.Context, routingKey string, body []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.ch == nil || p.ch.IsClosed() {
		if err := p.connect(); err != nil {
			return err
		}
	}

	conf, err := p.ch.PublishWithDeferredConfirmWithContext(ctx, ExchangeName, routingKey, false, false,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Timestamp:    time.Now(),
			Body:         body,
		})
	if err != nil {
		return fmt.Errorf("publish: %w", err)
	}
	ok, err := conf.WaitContext(ctx)
	if err != nil {
		return fmt.Errorf("wait confirm: %w", err)
	}
	if !ok {
		return errors.New("broker nacked the message")
	}
	return nil
}

func (p *Publisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.conn != nil {
		return p.conn.Close()
	}
	return nil
}
