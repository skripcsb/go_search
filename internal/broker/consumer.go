package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"

	"searchtrends/internal/metrics"
	"searchtrends/internal/model"
	"searchtrends/internal/store"
)

type Config struct {
	URL           string
	Subject       string
	Queue         string
	ClientName    string
	Logger        *log.Logger
	ReconnectWait time.Duration
}

type Consumer struct {
	config  Config
	store   *store.Store
	metrics *metrics.Collector
}

func NewConsumer(config Config, st *store.Store, collector *metrics.Collector) *Consumer {
	return &Consumer{config: config, store: st, metrics: collector}
}

func (c *Consumer) Run(ctx context.Context) error {
	if c.config.Subject == "" {
		return fmt.Errorf("subject is required")
	}
	if c.config.Queue == "" {
		c.config.Queue = "searchtrends"
	}
	if c.config.ClientName == "" {
		c.config.ClientName = "searchtrends-consumer"
	}
	if c.config.ReconnectWait <= 0 {
		c.config.ReconnectWait = 500 * time.Millisecond
	}

	c.store.Start()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		conn, err := nats.Connect(
			c.config.URL,
			nats.Name(c.config.ClientName),
			nats.MaxReconnects(-1),
			nats.ReconnectWait(c.config.ReconnectWait),
		)
		if err != nil {
			if c.config.Logger != nil {
				c.config.Logger.Printf("nats connect failed: %v", err)
			}
			if !sleepOrDone(ctx, c.config.ReconnectWait) {
				return ctx.Err()
			}
			continue
		}

		sub, err := conn.QueueSubscribe(c.config.Subject, c.config.Queue, func(msg *nats.Msg) {
			var event model.SearchEvent
			if err := json.Unmarshal(msg.Data, &event); err != nil {
				if c.metrics != nil {
					c.metrics.EventsIgnored.WithLabelValues("invalid_payload").Inc()
				}
				return
			}
			result := c.store.AddEvent(event)
			if !result.Accepted && c.config.Logger != nil {
				c.config.Logger.Printf("event ignored: %s", result.Reason)
			}
		})
		if err != nil {
			conn.Close()
			if c.config.Logger != nil {
				c.config.Logger.Printf("nats subscribe failed: %v", err)
			}
			if !sleepOrDone(ctx, c.config.ReconnectWait) {
				return ctx.Err()
			}
			continue
		}
		if err := conn.Flush(); err != nil {
			_ = sub.Unsubscribe()
			conn.Close()
			if c.config.Logger != nil {
				c.config.Logger.Printf("nats flush failed: %v", err)
			}
			if !sleepOrDone(ctx, c.config.ReconnectWait) {
				return ctx.Err()
			}
			continue
		}

		<-ctx.Done()
		_ = conn.Drain()
		conn.Close()
		return ctx.Err()
	}
}

func sleepOrDone(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
