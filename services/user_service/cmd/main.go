package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"BHLA/services/user_service/internal/app"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	application, err := app.New(ctx)
	if err != nil {
		log.Fatalf("user-service: init: %v", err)
	}
	if err := application.Run(ctx); err != nil {
		log.Fatalf("user-service: run: %v", err)
	}
}
