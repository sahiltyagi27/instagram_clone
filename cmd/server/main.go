package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"instagram_clone/internal/handler"
	appkafka "instagram_clone/internal/kafka"
	"instagram_clone/internal/middleware"
	"instagram_clone/internal/service"
	"instagram_clone/internal/telemetry"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Initialise OpenTelemetry. If Jaeger is unavailable (e.g. local dev without
	// Docker Compose) we log a warning and continue without traces.
	shutdownTracer, err := telemetry.InitTracer(ctx,
		envOrDefault("OTEL_EXPORTER_OTLP_ENDPOINT", "jaeger:4318"),
	)
	if err != nil {
		slog.Warn("tracing disabled", "error", err)
	} else {
		defer func() {
			if err := shutdownTracer(context.Background()); err != nil {
				slog.Error("flush traces on shutdown", "error", err)
			}
		}()
	}

	storage, err := service.NewStorage(
		ctx,
		envOrDefault("S3_ENDPOINT", "http://minio:9000"),
		envOrDefault("S3_PUBLIC_ENDPOINT", ""),
		envOrDefault("AWS_REGION", "us-east-1"),
		envOrDefault("S3_BUCKET", "instagram-media"),
	)
	if err != nil {
		slog.Error("initialize storage", "error", err)
		os.Exit(1)
	}

	jwtSecret, err := jwtSecretFromEnv()
	if err != nil {
		slog.Error("JWT secret", "error", err)
		os.Exit(1)
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

	// Middleware must be registered before routes in Chi.
	router.Use(telemetry.PrometheusMiddleware)

	// Observability: Prometheus metrics scrape endpoint (unauthenticated).
	router.Handle("/metrics", promhttp.Handler())

	router.Mount("/auth", handler.NewAuthHandler(authService).Router())

	router.Group(func(r chi.Router) {
		r.Use(middleware.JWT(jwtSecret))
		r.Mount("/", handler.NewUploadHandler(storage, producer).Router())
		r.Mount("/stories", handler.NewStoryHandler(storyService, producer).Router())
		r.Mount("/feed", handler.NewFeedHandler(feedService).Router())
	})

	// Wrap the entire router with OTel HTTP tracing.
	server := &http.Server{
		Addr:              ":8080",
		Handler:           otelhttp.NewHandler(router, "http.server"),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	slog.Info("server listening", "addr", ":8080")
	errCh := make(chan error, 1)
	go func() {
		errCh <- server.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			slog.Error("server shutdown", "error", err)
		} else {
			slog.Info("server stopped gracefully")
		}
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("server failed", "error", err)
			os.Exit(1)
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
