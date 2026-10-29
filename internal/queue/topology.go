package queue

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	ExchangeName = "telemetry"
	QueueName    = "readings.q"
	BindingKey   = "readings.#"

	DLXName = "telemetry.dlx"
	DLQName = "readings.dlq"
)

// DeclareTopology is idempotent: safe to call from every service on startup.
func DeclareTopology(ch *amqp.Channel) error {
	// dead-letter exchange + queue (poison messages end up here)
	if err := ch.ExchangeDeclare(DLXName, "fanout", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare dlx: %w", err)
	}
	if _, err := ch.QueueDeclare(DLQName, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare dlq: %w", err)
	}
	if err := ch.QueueBind(DLQName, "", DLXName, false, nil); err != nil {
		return fmt.Errorf("bind dlq: %w", err)
	}

	// main exchange + queue
	if err := ch.ExchangeDeclare(ExchangeName, "topic", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare exchange: %w", err)
	}
	_, err := ch.QueueDeclare(QueueName, true, false, false, false, amqp.Table{
		"x-dead-letter-exchange": DLXName,
	})
	if err != nil {
		return fmt.Errorf("declare queue: %w", err)
	}
	if err := ch.QueueBind(QueueName, BindingKey, ExchangeName, false, nil); err != nil {
		return fmt.Errorf("bind queue: %w", err)
	}
	return nil
}
