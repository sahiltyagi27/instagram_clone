package middleware

import (
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/go-redis/redis_rate/v10"
)

// RateLimit returns a per-user middleware that enforces the given rate using
// the GCRA algorithm backed by Redis. Requests from users who exceed the limit
// receive 429 Too Many Requests with Retry-After and X-RateLimit-* headers.
//
// If Redis is unavailable the middleware fails open (allows the request) so a
// Redis outage does not take the whole API down.
func RateLimit(limiter *redis_rate.Limiter, rate redis_rate.Limit) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			userID, ok := UserIDFromContext(r.Context())
			if !ok {
				// Should not happen on authenticated routes, but fail open rather
				// than blocking the request.
				next.ServeHTTP(w, r)
				return
			}

			res, err := limiter.Allow(r.Context(), "ratelimit:"+userID, rate)
			if err != nil {
				// Redis error — fail open so a cache outage doesn't take down the API.
				slog.ErrorContext(r.Context(), "rate limit check failed", "user_id", userID, "error", err)
				next.ServeHTTP(w, r)
				return
			}

			w.Header().Set("X-RateLimit-Limit", strconv.Itoa(rate.Burst))
			w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(res.Remaining))
			w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(time.Now().Add(res.ResetAfter).Unix(), 10))

			if res.Allowed == 0 {
				retryAfter := int64(res.RetryAfter.Seconds()) + 1
				w.Header().Set("Retry-After", strconv.FormatInt(retryAfter, 10))
				writeError(w, http.StatusTooManyRequests, "rate limit exceeded")
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
