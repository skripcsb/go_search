package config

import (
	"os"
	"time"
)

type Config struct {
	HTTPAddr          string
	NATSURL           string
	NATSSubject       string
	NATSQueue         string
	MaxTop            int
	RecomputeInterval time.Duration
	DedupWindow       time.Duration
	ReconnectWait     time.Duration
}

func Load() Config {
	return Config{
		HTTPAddr:          envOr("HTTP_ADDR", ":8080"),
		NATSURL:           envOr("NATS_URL", "nats://localhost:4222"),
		NATSSubject:       envOr("NATS_SUBJECT", "search.queries"),
		NATSQueue:         envOr("NATS_QUEUE", "searchtrends"),
		MaxTop:            1000,
		RecomputeInterval: 250 * time.Millisecond,
		DedupWindow:       5 * time.Minute,
		ReconnectWait:     500 * time.Millisecond,
	}
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
