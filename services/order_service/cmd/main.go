package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"BHLA/services/order-service/internal/app"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	application, err := app.New(ctx)
	if err != nil {
		log.Fatalf("order-service: init: %v", err)
	}
	if err := application.Run(ctx); err != nil {
		log.Fatalf("order-service: run: %v", err)
	}
}
