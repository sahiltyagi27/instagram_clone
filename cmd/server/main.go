package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	"instagram_clone/internal/handler"
	"instagram_clone/internal/service"
)

func main() {
	ctx := context.Background()

	storage, err := service.NewStorage(
		ctx,
		envOrDefault("S3_ENDPOINT", "http://localstack:4566"),
		envOrDefault("AWS_REGION", "us-east-1"),
		envOrDefault("S3_BUCKET", "instagram-media"),
	)
	if err != nil {
		log.Fatalf("initialize storage: %v", err)
	}

	server := &http.Server{
		Addr:              ":8080",
		Handler:           handler.NewUploadHandler(storage).Router(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Println("photo/video upload service listening on :8080")
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("server failed: %v", err)
	}
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
