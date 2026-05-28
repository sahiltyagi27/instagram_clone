package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	"instagram_clone/internal/handler"
	appkafka "instagram_clone/internal/kafka"
	"instagram_clone/internal/middleware"
	"instagram_clone/internal/service"

	"github.com/go-chi/chi/v5"
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

	jwtSecret := envOrDefault("JWT_SECRET", "dev-secret-do-not-use-in-prod")
	kafkaBroker := envOrDefault("KAFKA_BROKER", "kafka:9092")

	authService := service.NewAuthService(jwtSecret)
	storyService := service.NewStoryService(storage)
	feedService := service.NewFeedService()
	mediaProcessor := service.NewMediaProcessor(storage)

	producer := appkafka.NewKafkaProducer(kafkaBroker)
	defer producer.Close()

	consumer := appkafka.NewKafkaConsumer(kafkaBroker, mediaProcessor, feedService)
	defer consumer.Close()

	go storyService.StartExpiryWorker(ctx)
	go consumer.Start(ctx)

	router := chi.NewRouter()
	router.Mount("/auth", handler.NewAuthHandler(authService).Router())

	router.Group(func(r chi.Router) {
		r.Use(middleware.JWT(jwtSecret))
		r.Mount("/", handler.NewUploadHandler(storage, producer).Router())
		r.Mount("/stories", handler.NewStoryHandler(storyService, producer).Router())
		r.Mount("/feed", handler.NewFeedHandler(feedService).Router())
	})

	server := &http.Server{
		Addr:              ":8080",
		Handler:           router,
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
