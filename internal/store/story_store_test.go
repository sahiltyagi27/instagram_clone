package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// newTestStoryPool opens a Postgres connection and seeds the user required by
// the stories FK. Tests are skipped if Postgres is unavailable.
func newTestStoryPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	const dsn = "postgres://postgres:postgres@localhost:5432/instagram_clone"
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Skipf("postgres unavailable, skipping: %v", err)
	}
	if err := pool.Ping(context.Background()); err != nil {
		pool.Close()
		t.Skipf("postgres unavailable, skipping: %v", err)
	}

	ctx := context.Background()
	// Clear leftovers from previous runs (stories before user due to FK).
	pool.Exec(ctx, "DELETE FROM stories WHERE user_id = 'story_test_user'")
	pool.Exec(ctx, "DELETE FROM users WHERE id = 'story_test_user'")
	pool.Exec(ctx, `INSERT INTO users (id, username, email, password_hash, created_at)
		VALUES ('story_test_user', 'storytestuser', 'storytest@example.com', 'hash', NOW())
		ON CONFLICT (id) DO NOTHING`)

	t.Cleanup(func() {
		pool.Exec(ctx, "DELETE FROM stories WHERE user_id = 'story_test_user'")
		pool.Exec(ctx, "DELETE FROM users WHERE id = 'story_test_user'")
		pool.Close()
	})
	return pool
}

// countRows is a small helper that counts rows matching an id column.
func countRows(t *testing.T, pool *pgxpool.Pool, table, id string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(),
		"SELECT COUNT(*) FROM "+table+" WHERE id = $1", id,
	).Scan(&n); err != nil {
		t.Fatalf("count %s %q: %v", table, id, err)
	}
	return n
}

// TestStoryStoreConfirmRejectsExpiredPending verifies that the TTL guard in
// Confirm returns ErrStoryNotFound when the pending story is older than
// PendingUploadTTL (i.e. the presigned URL has already expired).
func TestStoryStoreConfirmRejectsExpiredPending(t *testing.T) {
	pool := newTestStoryPool(t)
	ss := NewStoryStore(pool)
	ctx := context.Background()

	stale := time.Now().UTC().Add(-(PendingUploadTTL + time.Minute))
	_, err := pool.Exec(ctx, `
		INSERT INTO stories (id, user_id, s3_key, url, created_at)
		VALUES ('stale_story', 'story_test_user', 'stories/key.jpg', 'http://example.com', $1)`,
		stale,
	)
	if err != nil {
		t.Fatalf("insert stale story: %v", err)
	}

	_, err = ss.Confirm(ctx, "stale_story", "story_test_user")
	if !errors.Is(err, ErrStoryNotFound) {
		t.Fatalf("Confirm error = %v, want ErrStoryNotFound", err)
	}
}

// TestStoryStoreDeleteStalePending verifies that DeleteStalePending removes
// unconfirmed (expires_at IS NULL) rows outside the TTL window while leaving
// freshly-created pending rows intact.
func TestStoryStoreDeleteStalePending(t *testing.T) {
	pool := newTestStoryPool(t)
	ss := NewStoryStore(pool)
	ctx := context.Background()

	stale := time.Now().UTC().Add(-(PendingUploadTTL + time.Minute))
	fresh := time.Now().UTC()

	for _, tc := range []struct {
		id        string
		createdAt time.Time
	}{
		{"story_stale_pending", stale},
		{"story_fresh_pending", fresh},
	} {
		_, err := pool.Exec(ctx, `
			INSERT INTO stories (id, user_id, s3_key, url, created_at)
			VALUES ($1, 'story_test_user', 'stories/key.jpg', 'http://example.com', $2)`,
			tc.id, tc.createdAt,
		)
		if err != nil {
			t.Fatalf("insert %s: %v", tc.id, err)
		}
	}

	if err := ss.DeleteStalePending(ctx); err != nil {
		t.Fatalf("DeleteStalePending: %v", err)
	}

	// Stale row must be physically gone.
	if n := countRows(t, pool, "stories", "story_stale_pending"); n != 0 {
		t.Fatalf("stale pending story still present after cleanup (count = %d)", n)
	}

	// Fresh row must survive.
	if n := countRows(t, pool, "stories", "story_fresh_pending"); n != 1 {
		t.Fatalf("fresh pending story count = %d, want 1", n)
	}
}

// TestStoryStoreDeleteExpired verifies that DeleteExpired physically removes
// confirmed stories whose expires_at has passed while leaving still-active
// stories untouched.
func TestStoryStoreDeleteExpired(t *testing.T) {
	pool := newTestStoryPool(t)
	ss := NewStoryStore(pool)
	ctx := context.Background()

	past := time.Now().UTC().Add(-time.Minute)
	future := time.Now().UTC().Add(24 * time.Hour)

	for _, tc := range []struct {
		id        string
		expiresAt time.Time
	}{
		{"story_expired", past},
		{"story_active", future},
	} {
		_, err := pool.Exec(ctx, `
			INSERT INTO stories (id, user_id, s3_key, url, created_at, expires_at)
			VALUES ($1, 'story_test_user', 'stories/key.jpg', 'http://example.com', NOW(), $2)`,
			tc.id, tc.expiresAt,
		)
		if err != nil {
			t.Fatalf("insert %s: %v", tc.id, err)
		}
	}

	if err := ss.DeleteExpired(ctx); err != nil {
		t.Fatalf("DeleteExpired: %v", err)
	}

	// Expired story must be physically removed.
	if n := countRows(t, pool, "stories", "story_expired"); n != 0 {
		t.Fatalf("expired story still present after DeleteExpired (count = %d)", n)
	}

	// Active story must survive.
	if n := countRows(t, pool, "stories", "story_active"); n != 1 {
		t.Fatalf("active story count = %d, want 1", n)
	}
}
