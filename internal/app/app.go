package app

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"searchtrends/internal/adapter/httpapi"
	"searchtrends/internal/adapter/kafka"
	"searchtrends/internal/application/trends"
	"searchtrends/internal/config"
	"searchtrends/internal/infra/memorystore"
	"searchtrends/internal/metrics"
)

type Service struct {
	store    *memorystore.Store
	consumer *kafka.Consumer
	http     *httpapi.Server
	status   *kafka.Status
	logger   *log.Logger
}

func New(cfg config.Config, logger *log.Logger) (*Service, error) {
	collector := metrics.New()
	st := memorystore.New(memorystore.Options{
		Window:       5 * time.Minute,
		MaxTop:       cfg.MaxTop,
		Metrics:      collector,
		DedupWindow:  cfg.DedupWindow,
		StopListFile: cfg.StopListFile,
	})
	service := trends.NewService(st)
	status := &kafka.Status{}
	consumer, err := kafka.NewConsumer(kafka.Config{
		Brokers: cfg.KafkaBrokers,
		Topic:   cfg.KafkaTopic,
		GroupID: cfg.KafkaGroupID,
		Logger:  logger,
	}, service, collector, status)
	if err != nil {
		return nil, err
	}

	return &Service{
		store:    st,
		consumer: consumer,
		http: httpapi.NewServer(httpapi.Options{
			Addr:    cfg.HTTPAddr,
			Service: service,
			Metrics: collector.Registry(),
			Logger:  logger,
			Ready:   status,
		}),
		status: status,
		logger: logger,
	}, nil
}

func (s *Service) Run(ctx context.Context) error {
	consumerErr := make(chan error, 1)
	go func() {
		consumerErr <- s.consumer.Run(ctx)
	}()

	serverErr := make(chan error, 1)
	go func() {
		s.logger.Printf("http listening on %s", s.http.Addr())
		serverErr <- s.http.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.status.SetReady(false)
		_ = s.http.Shutdown(shutdownCtx)
		s.store.Close()
		return ctx.Err()
	case err := <-consumerErr:
		if err == nil || errors.Is(err, context.Canceled) {
			return nil
		}
		return fmt.Errorf("consumer stopped: %w", err)
	case err := <-serverErr:
		if err == nil || errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("http server stopped: %w", err)
	}
}
