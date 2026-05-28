package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"instagram_clone/internal/handler"
	appkafka "instagram_clone/internal/kafka"
	"instagram_clone/internal/middleware"
	"instagram_clone/internal/service"

	"github.com/go-chi/chi/v5"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	storage, err := service.NewStorage(
		ctx,
		envOrDefault("S3_ENDPOINT", "http://localstack:4566"),
		envOrDefault("AWS_REGION", "us-east-1"),
		envOrDefault("S3_BUCKET", "instagram-media"),
	)
	if err != nil {
		log.Fatalf("initialize storage: %v", err)
	}

	jwtSecret, err := jwtSecretFromEnv()
	if err != nil {
		log.Fatal(err)
	}
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
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	log.Println("photo/video upload service listening on :8080")
	errCh := make(chan error, 1)
	go func() {
		errCh <- server.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Fatalf("server shutdown failed: %v", err)
		}
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server failed: %v", err)
		}
	}
}

func jwtSecretFromEnv() (string, error) {
	if secret := os.Getenv("JWT_SECRET"); secret != "" {
		return secret, nil
	}

	env := os.Getenv("APP_ENV")
	if env == "" || env == "dev" || env == "local" || env == "test" {
		return "dev-secret-do-not-use-in-prod", nil
	}

	return "", errors.New("JWT_SECRET is required outside dev/local/test")
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
