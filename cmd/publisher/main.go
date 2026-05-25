package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/nats-io/nats.go"

	"searchtrends/internal/model"
)

func main() {
	var (
		url     = flag.String("url", envOr("NATS_URL", "nats://localhost:4222"), "NATS connection URL")
		subject = flag.String("subject", envOr("NATS_SUBJECT", "search.queries"), "subject to publish to")
		count   = flag.Int("count", 20, "number of events to publish")
	)
	flag.Parse()

	conn, err := nats.Connect(*url)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	queries := []string{"iphone 15", "sneakers", "laptop", "air fryer", "backpack", "headphones", "winter coat"}
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := 0; i < *count; i++ {
		event := model.SearchEvent{
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
		if err := conn.Publish(*subject, payload); err != nil {
			log.Fatal(err)
		}
	}
	if err := conn.Flush(); err != nil {
		log.Fatal(err)
	}
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}