package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"searchtrends/internal/app"
	"searchtrends/internal/config"
)

func main() {
	if err := run(); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	cfg := config.Load()
	logger := log.New(os.Stdout, "searchtrends: ", log.LstdFlags|log.Lmicroseconds)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	service, err := app.New(cfg, logger)
	if err != nil {
		return err
	}
	return service.Run(ctx)
}
