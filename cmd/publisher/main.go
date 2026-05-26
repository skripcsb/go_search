package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"searchtrends/internal/domain"
)

func main() {
	var (
		brokers = flag.String("brokers", envOr("KAFKA_BROKERS", "localhost:9092"), "comma-separated Kafka brokers")
		topic   = flag.String("topic", envOr("KAFKA_TOPIC", "search.events"), "Kafka topic")
		count   = flag.Int("count", 20, "number of events to publish")
	)
	flag.Parse()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(splitAndTrim(*brokers)...),
		kgo.DefaultProduceTopic(*topic),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	queries := []string{"iphone 15", "sneakers", "laptop", "air fryer", "backpack", "headphones", "winter coat"}
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	ctx := context.Background()

	for i := 0; i < *count; i++ {
		event := domain.SearchEvent{
			EventID:    fmt.Sprintf("evt-%d-%d", time.Now().UnixNano(), i),
			Query:      queries[rng.Intn(len(queries))],
			SessionID:  fmt.Sprintf("session-%d", i%5),
			UserID:     fmt.Sprintf("user-%d", i%10),
			Source:     "sample-publisher",
			OccurredAt: time.Now().UTC(),
		}
		payload, err := json.Marshal(event)
		if err != nil {
			log.Fatal(err)
		}
		if err := client.ProduceSync(ctx, &kgo.Record{Value: payload}).FirstErr(); err != nil {
			log.Fatal(err)
		}
	}
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func splitAndTrim(raw string) []string {
	parts := strings.Split(raw, ",")
	brokers := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			brokers = append(brokers, trimmed)
		}
	}
	return brokers
}
