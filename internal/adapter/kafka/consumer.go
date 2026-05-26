package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"searchtrends/internal/application/trends"
	"searchtrends/internal/domain"
	"searchtrends/internal/metrics"
)

type Config struct {
	Brokers        string
	Topic          string
	GroupID        string
	Logger         *log.Logger
	CommitInterval time.Duration
}

type Consumer struct {
	cfg     Config
	service *trends.Service
	metrics *metrics.Collector
	status  *Status
	client  *kgo.Client
}

func NewConsumer(cfg Config, service *trends.Service, collector *metrics.Collector, status *Status) (*Consumer, error) {
	brokers := splitAndTrim(cfg.Brokers)
	if len(brokers) == 0 {
		return nil, fmt.Errorf("kafka brokers are required")
	}
	if cfg.Topic == "" {
		return nil, fmt.Errorf("kafka topic is required")
	}
	if cfg.GroupID == "" {
		return nil, fmt.Errorf("kafka group id is required")
	}
	if cfg.CommitInterval <= 0 {
		cfg.CommitInterval = 5 * time.Second
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(cfg.GroupID),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.AutoCommitMarks(),
		kgo.BlockRebalanceOnPoll(),
		kgo.AutoCommitInterval(cfg.CommitInterval),
		kgo.OnPartitionsRevoked(func(ctx context.Context, cl *kgo.Client, _ map[string][]int32) {
			if err := cl.CommitMarkedOffsets(ctx); err != nil && cfg.Logger != nil {
				cfg.Logger.Printf("kafka revoke commit failed: %v", err)
			}
		}),
	)
	if err != nil {
		return nil, err
	}

	return &Consumer{cfg: cfg, service: service, metrics: collector, status: status, client: client}, nil
}

func (c *Consumer) Run(ctx context.Context) error {
	c.status.SetReady(true)
	defer c.status.SetReady(false)
	defer func() {
		commitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := c.client.CommitMarkedOffsets(commitCtx); err != nil && c.cfg.Logger != nil {
			c.cfg.Logger.Printf("kafka final commit failed: %v", err)
		}
		c.client.Close()
	}()

	for {
		fetches := c.client.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return ctx.Err()
		}
		if err := fetches.Err(); err != nil {
			if c.cfg.Logger != nil {
				c.cfg.Logger.Printf("kafka poll error: %v", err)
			}
			continue
		}

		fetches.EachRecord(func(record *kgo.Record) {
			var raw struct {
				Query      string    `json:"query"`
				EventID    string    `json:"event_id"`
				RequestID  string    `json:"request_id"`
				SessionID  string    `json:"session_id"`
				UserID     string    `json:"user_id"`
				Source     string    `json:"source"`
				Timestamp  string    `json:"timestamp"`
				OccurredAt time.Time `json:"occurred_at"`
			}
			if err := json.Unmarshal(record.Value, &raw); err != nil {
				if c.metrics != nil {
					c.metrics.EventsIgnored.WithLabelValues("invalid_payload").Inc()
				}
				c.client.MarkCommitRecords(record)
				return
			}
			// map raw -> domain.SearchEvent, accept either event_id or request_id
			evt := domain.SearchEvent{
				Query:     raw.Query,
				SessionID: raw.SessionID,
				UserID:    raw.UserID,
				Source:    raw.Source,
			}
			// prefer explicit event_id, fallback to request_id
			if raw.EventID != "" {
				evt.EventID = raw.EventID
			} else if raw.RequestID != "" {
				evt.EventID = raw.RequestID
			}
			// occurred time: prefer occurred_at, then timestamp string, else now
			if !raw.OccurredAt.IsZero() {
				evt.OccurredAt = raw.OccurredAt
			} else if raw.Timestamp != "" {
				if ts, err := time.Parse(time.RFC3339, raw.Timestamp); err == nil {
					evt.OccurredAt = ts
				} else {
					evt.OccurredAt = time.Now().UTC()
				}
			} else {
				evt.OccurredAt = time.Now().UTC()
			}

			if c.metrics != nil {
				c.metrics.ObserveConsumerLag(evt.OccurredAt)
			}
			result := c.service.Ingest(evt)
			if !result.Accepted && c.cfg.Logger != nil {
				c.cfg.Logger.Printf("event ignored: %s", result.Reason)
			}
			c.client.MarkCommitRecords(record)
		})

		if err := c.client.CommitMarkedOffsets(ctx); err != nil && ctx.Err() == nil {
			if c.cfg.Logger != nil {
				c.cfg.Logger.Printf("kafka commit failed: %v", err)
			}
		}
	}
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
