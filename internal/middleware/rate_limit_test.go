package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-redis/redis_rate/v10"
	"github.com/redis/go-redis/v9"
)

const testScope = "test"

// newTestRateLimiter opens a Redis connection scoped to scope+userID.
// The rate-limit key is reset before the test starts AND after it finishes
// so leftover GCRA state from a previous run never bleeds into the next test.
func newTestRateLimiter(t *testing.T, userID string) *redis_rate.Limiter {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	if client.Ping(context.Background()).Err() != nil {
		_ = client.Close()
		t.Skip("redis unavailable, skipping")
	}
	limiter := redis_rate.NewLimiter(client)
	// The middleware calls Allow with this exact key; redis_rate stores it under
	// an internal "rate:" prefix. Use limiter.Reset (which applies that prefix)
	// rather than deleting the raw key — otherwise the real GCRA key survives
	// and leftover state bleeds across runs against a persistent Redis.
	key := "ratelimit:" + testScope + ":" + userID
	_ = limiter.Reset(context.Background(), key)
	t.Cleanup(func() {
		_ = limiter.Reset(context.Background(), key)
		_ = client.Close()
	})
	return limiter
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
	const userID = "rl_allow_user"
	limiter := newTestRateLimiter(t, userID)
	// Burst of 5 — first 5 requests must all pass.
	mw := RateLimit(limiter, testScope, redis_rate.PerMinute(5))
	handler := mw(okHandler())

	for i := range 5 {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, requestWithUser(userID))
		if rec.Code != http.StatusOK {
			t.Fatalf("request %d: status = %d, want 200", i+1, rec.Code)
		}
		if rec.Header().Get("X-RateLimit-Limit") == "" {
			t.Fatalf("request %d: missing X-RateLimit-Limit header", i+1)
		}
	}
}

func TestRateLimitRejectsRequestsOverLimit(t *testing.T) {
	const userID = "rl_reject_user"
	limiter := newTestRateLimiter(t, userID)
	// Burst of 2 — 3rd request must be rejected.
	mw := RateLimit(limiter, testScope, redis_rate.PerMinute(2))
	handler := mw(okHandler())

	for i := range 2 {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, requestWithUser(userID))
		if rec.Code != http.StatusOK {
			t.Fatalf("request %d: status = %d, want 200", i+1, rec.Code)
		}
	}

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, requestWithUser(userID))
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("3rd request: status = %d, want 429", rec.Code)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Fatal("expected Retry-After header on 429 response")
	}
}

func TestRateLimitSkipsWhenNoUserInContext(t *testing.T) {
	const userID = "rl_nouser_user"
	limiter := newTestRateLimiter(t, userID)
	mw := RateLimit(limiter, testScope, redis_rate.PerMinute(1))
	handler := mw(okHandler())

	// Request with no user ID in context — middleware must pass through.
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 when no user in context", rec.Code)
	}
}
