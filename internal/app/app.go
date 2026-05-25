package app

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"searchtrends/internal/broker"
	"searchtrends/internal/config"
	"searchtrends/internal/metrics"
	"searchtrends/internal/store"
	"searchtrends/internal/transport/httpapi"
)

type Service struct {
	store    *store.Store
	consumer *broker.Consumer
	http     *httpapi.Server
	logger   *log.Logger
}

func New(cfg config.Config, logger *log.Logger) (*Service, error) {
	collector := metrics.New()
	st := store.New(store.Options{
		Window:            5 * time.Minute,
		MaxTop:            cfg.MaxTop,
		RecomputeInterval: cfg.RecomputeInterval,
		Metrics:           collector,
		DedupWindow:       cfg.DedupWindow,
	})

	return &Service{
		store: st,
		consumer: broker.NewConsumer(broker.Config{
			URL:           cfg.NATSURL,
			Subject:       cfg.NATSSubject,
			Queue:         cfg.NATSQueue,
			ClientName:    "searchtrends-consumer",
			Logger:        logger,
			ReconnectWait: cfg.ReconnectWait,
		}, st, collector),
		http: httpapi.NewServer(httpapi.Options{
			Addr:    cfg.HTTPAddr,
			Store:   st,
			Metrics: collector.Registry(),
			Logger:  logger,
			MaxTop:  cfg.MaxTop,
			Window:  5 * time.Minute,
		}),
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
