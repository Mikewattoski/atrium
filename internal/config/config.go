package config

import (
	"log"
	"os"
	"strconv"
	"time"
)

type Config struct {
	HTTPAddr    string
	GRPCAddr    string
	DatabaseURL string
	AMQPURL     string

	// processor
	BatchSize          int
	FlushInterval      time.Duration
	AlertTempThreshold float64
}

func Load() Config {
	return Config{
		HTTPAddr:    getenv("HTTP_ADDR", ":8080"),
		GRPCAddr:    getenv("GRPC_ADDR", ":9090"),
		DatabaseURL: getenv("DATABASE_URL", "postgres://bp:bp_pass@localhost:5432/buildingpulse?sslmode=disable"),
		AMQPURL:     getenv("AMQP_URL", "amqp://guest:guest@localhost:5672/"),

		BatchSize:          getenvInt("BATCH_SIZE", 500),
		FlushInterval:      getenvDuration("FLUSH_INTERVAL", time.Second),
		AlertTempThreshold: getenvFloat("ALERT_TEMP_THRESHOLD", 30),
	}
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getenvInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		log.Printf("invalid %s=%q, using default %d", key, v, def)
		return def
	}
	return n
}

func getenvFloat(key string, def float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		log.Printf("invalid %s=%q, using default %v", key, v, def)
		return def
	}
	return f
}

func getenvDuration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		log.Printf("invalid %s=%q, using default %s", key, v, def)
		return def
	}
	return d
}
