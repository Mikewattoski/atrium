package model

import (
	"errors"
	"time"
)

var (
	ErrNotFound = errors.New("not found")
	ErrInvalid  = errors.New("invalid request")
)

type Device struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Type      string    `json:"type"`
	Location  string    `json:"location"`
	CreatedAt time.Time `json:"created_at"`
}

type Reading struct {
	ID       int64     `json:"id"`
	DeviceID string    `json:"device_id"`
	Metric   string    `json:"metric"`
	Value    float64   `json:"value"`
	TS       time.Time `json:"ts"`
}

type Alert struct {
	ID         int64      `json:"id"`
	DeviceID   string     `json:"device_id"`
	Metric     string     `json:"metric"`
	Value      float64    `json:"value"`
	Threshold  float64    `json:"threshold"`
	Status     string     `json:"status"`
	CreatedAt  time.Time  `json:"created_at"`
	ResolvedAt *time.Time `json:"resolved_at,omitempty"`
}
