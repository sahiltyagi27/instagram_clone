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

	"instagram_clone/internal/db"
	"instagram_clone/internal/handler"
	appkafka "instagram_clone/internal/kafka"
	"instagram_clone/internal/middleware"
	"instagram_clone/internal/service"
	"instagram_clone/internal/store"
	"instagram_clone/internal/telemetry"

	"github.com/go-chi/chi/v5"
	"github.com/go-redis/redis_rate/v10"
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

	// ── Postgres ──────────────────────────────────────────────────────────────
	pgPool, err := db.NewPostgresPool(ctx, envOrDefault("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/instagram_clone"))
	if err != nil {
		slog.Error("connect to postgres", "error", err)
		os.Exit(1)
	}
	defer pgPool.Close()

	if err := db.RunMigrations(
		envOrDefault("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/instagram_clone"),
		"internal/migrations",
	); err != nil {
		slog.Error("run migrations", "error", err)
		os.Exit(1)
	}
	slog.Info("database migrations applied")

	// ── Redis ─────────────────────────────────────────────────────────────────
	redisClient, err := db.NewRedisClient(ctx, envOrDefault("REDIS_ADDR", "localhost:6379"))
	if err != nil {
		slog.Error("connect to redis", "error", err)
		os.Exit(1)
	}
	defer redisClient.Close()

	// ── Stores ────────────────────────────────────────────────────────────────
	userStore := store.NewUserStore(pgPool)
	mediaStore := store.NewMediaStore(pgPool)
	storyStore := store.NewStoryStore(pgPool)
	feedStore := store.NewFeedStore(redisClient)

	// ── S3 / MinIO ────────────────────────────────────────────────────────────
	storage, err := service.NewStorage(
		ctx,
		envOrDefault("S3_ENDPOINT", "http://minio:9000"),
		envOrDefault("S3_PUBLIC_ENDPOINT", ""),
		envOrDefault("AWS_REGION", "us-east-1"),
		envOrDefault("S3_BUCKET", "instagram-media"),
		mediaStore,
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

	// ── Services ──────────────────────────────────────────────────────────────
	authService := service.NewAuthService(jwtSecret, userStore)
	storyService := service.NewStoryService(storage, storyStore)
	feedService := service.NewFeedService(feedStore)
	mediaProcessor := service.NewMediaProcessor(storage)

	// ── Kafka ─────────────────────────────────────────────────────────────────
	producer := appkafka.NewKafkaProducer(kafkaBroker)
	defer producer.Close()

	consumer := appkafka.NewKafkaConsumer(kafkaBroker, mediaProcessor, feedService)
	defer consumer.Close()

	go consumer.Start(ctx)
	go runPendingCleanup(ctx, mediaStore, storyStore)

	// ── Rate limiter ──────────────────────────────────────────────────────────
	rateLimiter := redis_rate.NewLimiter(redisClient)

	// ── HTTP router ───────────────────────────────────────────────────────────
	router := chi.NewRouter()

	// Middleware must be registered before routes in Chi.
	router.Use(telemetry.PrometheusMiddleware)

	// Observability: Prometheus metrics scrape endpoint (unauthenticated).
	router.Handle("/metrics", promhttp.Handler())

	router.Mount("/auth", handler.NewAuthHandler(authService).Router())

	// Write operations: stricter limit (20 req/min per user).
	// Key: ratelimit:write:{userID} — independent from the read budget.
	router.Group(func(r chi.Router) {
		r.Use(middleware.JWT(jwtSecret))
		r.Use(middleware.RateLimit(rateLimiter, "write", redis_rate.PerMinute(20)))
		r.Mount("/", handler.NewUploadHandler(storage, producer).Router())
		r.Mount("/stories", handler.NewStoryHandler(storyService, producer).Router())
	})

	// Read operations: more lenient limit (60 req/min per user).
	// Key: ratelimit:read:{userID} — independent from the write budget.
	router.Group(func(r chi.Router) {
		r.Use(middleware.JWT(jwtSecret))
		r.Use(middleware.RateLimit(rateLimiter, "read", redis_rate.PerMinute(60)))
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

// runPendingCleanup periodically:
//   - deletes media and story rows never confirmed within the pending TTL window
//   - physically removes confirmed stories whose 24-hour TTL has elapsed
func runPendingCleanup(ctx context.Context, media *store.MediaStore, stories *store.StoryStore) {
	ticker := time.NewTicker(store.PendingUploadTTL)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := media.DeleteStalePending(ctx); err != nil {
				slog.Error("cleanup stale pending media", "error", err)
			}
			if err := stories.DeleteStalePending(ctx); err != nil {
				slog.Error("cleanup stale pending stories", "error", err)
			}
			if err := stories.DeleteExpired(ctx); err != nil {
				slog.Error("cleanup expired stories", "error", err)
			}
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
