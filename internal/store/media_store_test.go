package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// newTestMediaPool opens a Postgres connection and seeds the user required by
// the media FK. Tests are skipped if Postgres is unavailable.
func newTestMediaPool(t *testing.T) *pgxpool.Pool {
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
	// Clear leftovers from previous runs (media before user due to FK).
	pool.Exec(ctx, "DELETE FROM media WHERE user_id = 'media_test_user'")
	pool.Exec(ctx, "DELETE FROM users WHERE id = 'media_test_user'")
	pool.Exec(ctx, `INSERT INTO users (id, username, email, password_hash, created_at)
		VALUES ('media_test_user', 'mediatestuser', 'mediatest@example.com', 'hash', NOW())
		ON CONFLICT (id) DO NOTHING`)

	t.Cleanup(func() {
		pool.Exec(ctx, "DELETE FROM media WHERE user_id = 'media_test_user'")
		pool.Exec(ctx, "DELETE FROM users WHERE id = 'media_test_user'")
		pool.Close()
	})
	return pool
}

// TestMediaStoreMarkUploadedRejectsExpiredPending verifies that the TTL guard
// in MarkUploaded prevents confirming a media row whose presigned URL has
// already expired (created_at older than PendingUploadTTL).
func TestMediaStoreMarkUploadedRejectsExpiredPending(t *testing.T) {
	pool := newTestMediaPool(t)
	ms := NewMediaStore(pool)
	ctx := context.Background()

	// Insert a pending row with a stale created_at directly so we can bypass
	// the normal Create path and set an arbitrary timestamp.
	stale := time.Now().UTC().Add(-(PendingUploadTTL + time.Minute))
	_, err := pool.Exec(ctx, `
		INSERT INTO media (id, user_id, type, status, file_name, content_type, s3_bucket, s3_key, created_at)
		VALUES ('stale_media', 'media_test_user', 'photo', 'pending', 'test.jpg', 'image/jpeg', 'bucket', 'key/test.jpg', $1)`,
		stale,
	)
	if err != nil {
		t.Fatalf("insert stale media: %v", err)
	}

	_, err = ms.MarkUploaded(ctx, "stale_media", "media_test_user")
	if !errors.Is(err, ErrMediaNotFound) {
		t.Fatalf("MarkUploaded error = %v, want ErrMediaNotFound", err)
	}
}

// TestMediaStoreDeleteStalePending verifies that DeleteStalePending removes
// only rows outside the pending TTL window, leaving fresh rows untouched.
func TestMediaStoreDeleteStalePending(t *testing.T) {
	pool := newTestMediaPool(t)
	ms := NewMediaStore(pool)
	ctx := context.Background()

	stale := time.Now().UTC().Add(-(PendingUploadTTL + time.Minute))
	fresh := time.Now().UTC()

	for _, tc := range []struct {
		id        string
		createdAt time.Time
	}{
		{"media_stale_pending", stale},
		{"media_fresh_pending", fresh},
	} {
		_, err := pool.Exec(ctx, `
			INSERT INTO media (id, user_id, type, status, file_name, content_type, s3_bucket, s3_key, created_at)
			VALUES ($1, 'media_test_user', 'photo', 'pending', 'test.jpg', 'image/jpeg', 'bucket', 'key/test.jpg', $2)`,
			tc.id, tc.createdAt,
		)
		if err != nil {
			t.Fatalf("insert %s: %v", tc.id, err)
		}
	}

	if err := ms.DeleteStalePending(ctx); err != nil {
		t.Fatalf("DeleteStalePending: %v", err)
	}

	// Stale row must be physically gone.
	if _, err := ms.GetByID(ctx, "media_stale_pending"); !errors.Is(err, ErrMediaNotFound) {
		t.Fatalf("stale media still present after cleanup: err = %v", err)
	}

	// Fresh row must survive.
	if _, err := ms.GetByID(ctx, "media_fresh_pending"); err != nil {
		t.Fatalf("fresh media unexpectedly deleted: %v", err)
	}
}
