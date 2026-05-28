package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-redis/redis_rate/v10"
	"github.com/redis/go-redis/v9"
)

func newTestRateLimiter(t *testing.T) *redis_rate.Limiter {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	if client.Ping(context.Background()).Err() != nil {
		_ = client.Close()
		t.Skip("redis unavailable, skipping")
	}
	t.Cleanup(func() {
		// Clean up rate limit keys created during the test.
		client.Del(context.Background(), "ratelimit:rl_test_user")
		_ = client.Close()
	})
	return redis_rate.NewLimiter(client)
}

func okHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
}

func requestWithUser(userID string) *http.Request {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	return req.WithContext(ContextWithUserID(req.Context(), userID))
}

func TestRateLimitAllowsRequestsUnderLimit(t *testing.T) {
	limiter := newTestRateLimiter(t)
	// Burst of 5 — first 5 requests must all pass.
	mw := RateLimit(limiter, redis_rate.PerMinute(5))
	handler := mw(okHandler())

	for i := range 5 {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, requestWithUser("rl_test_user"))
		if rec.Code != http.StatusOK {
			t.Fatalf("request %d: status = %d, want 200", i+1, rec.Code)
		}
		if rec.Header().Get("X-RateLimit-Limit") == "" {
			t.Fatalf("request %d: missing X-RateLimit-Limit header", i+1)
		}
	}
}

func TestRateLimitRejectsRequestsOverLimit(t *testing.T) {
	limiter := newTestRateLimiter(t)
	// Burst of 2 — 3rd request must be rejected.
	mw := RateLimit(limiter, redis_rate.PerMinute(2))
	handler := mw(okHandler())

	for i := range 2 {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, requestWithUser("rl_test_user"))
		if rec.Code != http.StatusOK {
			t.Fatalf("request %d: status = %d, want 200", i+1, rec.Code)
		}
	}

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, requestWithUser("rl_test_user"))
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("3rd request: status = %d, want 429", rec.Code)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Fatal("expected Retry-After header on 429 response")
	}
}

func TestRateLimitSkipsWhenNoUserInContext(t *testing.T) {
	limiter := newTestRateLimiter(t)
	mw := RateLimit(limiter, redis_rate.PerMinute(1))
	handler := mw(okHandler())

	// Request with no user ID in context — middleware must pass through.
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 when no user in context", rec.Code)
	}
}
