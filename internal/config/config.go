package config

import (
	"os"
	"time"
)

type Config struct {
	HTTPAddr string
	// Kafka settings
	KafkaBrokers string
	KafkaTopic   string
	KafkaGroupID string
	MaxTop       int
	DedupWindow  time.Duration
	StopListFile string
}

func Load() Config {
	return Config{
		HTTPAddr:     envOr("HTTP_ADDR", ":8080"),
		KafkaBrokers: envOr("KAFKA_BROKERS", "localhost:9092"),
		KafkaTopic:   envOr("KAFKA_TOPIC", "search.events"),
		KafkaGroupID: envOr("KAFKA_GROUP_ID", "search-trends"),
		MaxTop:       1000,
		DedupWindow:  5 * time.Minute,
		StopListFile: envOr("STOPLIST_FILE", "stoplist.json"),
	}
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
