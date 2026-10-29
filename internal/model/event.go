package model

import "time"

// ReadingEvent is the message format published to RabbitMQ (JSON).
type ReadingEvent struct {
	DeviceID string    `json:"device_id"`
	Metric   string    `json:"metric"`
	Value    float64   `json:"value"`
	TS       time.Time `json:"ts"`
}
