package service

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"testing"
	"time"

	"instagram_clone/internal/model"
	"instagram_clone/internal/store"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestGeneratePresignedUploadURLStoresPendingMedia(t *testing.T) {
	storage := newTestStorage(t)

	resp, err := storage.GeneratePresignedUploadURL(context.Background(), model.PresignedURLRequest{
		UserID:      "user_123",
		FileName:    "../sunset.jpg",
		ContentType: "image/jpeg",
		MediaType:   model.MediaTypePhoto,
	})
	if err != nil {
		t.Fatalf("GeneratePresignedUploadURL returned error: %v", err)
	}

	if resp.MediaID == "" {
		t.Fatal("expected media id")
	}
	if resp.ExpiresIn != int64(PresignedURLExpiry.Seconds()) {
		t.Fatalf("ExpiresIn = %d, want %d", resp.ExpiresIn, int64(PresignedURLExpiry.Seconds()))
	}
	if resp.S3Bucket != "instagram-media-test" {
		t.Fatalf("S3Bucket = %q, want instagram-media-test", resp.S3Bucket)
	}
	wantKeyPrefix := "users/user_123/" + resp.MediaID + "/"
	if !strings.HasPrefix(resp.S3Key, wantKeyPrefix) {
		t.Fatalf("S3Key = %q, want prefix %q", resp.S3Key, wantKeyPrefix)
	}
	if !strings.HasSuffix(resp.S3Key, "/sunset.jpg") {
		t.Fatalf("S3Key = %q, want sanitized file basename", resp.S3Key)
	}

	parsedURL, err := url.Parse(resp.UploadURL)
	if err != nil {
		t.Fatalf("upload URL is not parseable: %v", err)
	}
	if parsedURL.Scheme != "http" || parsedURL.Host != "localhost:9000" {
		t.Fatalf("upload URL host = %q://%q, want http://localhost:9000", parsedURL.Scheme, parsedURL.Host)
	}

	media, err := storage.GetMedia(context.Background(), resp.MediaID)
	if err != nil {
		t.Fatalf("GetMedia returned error: %v", err)
	}
	if media.ID != resp.MediaID {
		t.Fatalf("media ID = %q, want %q", media.ID, resp.MediaID)
	}
	if media.Status != model.MediaStatusPending {
		t.Fatalf("media status = %q, want pending", media.Status)
	}
	if media.UploadedAt != nil {
		t.Fatal("expected UploadedAt to be nil before confirmation")
	}
	if media.FileName != "sunset.jpg" {
		t.Fatalf("FileName = %q, want sunset.jpg", media.FileName)
	}
	if media.ContentType != "image/jpeg" {
		t.Fatalf("ContentType = %q, want image/jpeg", media.ContentType)
	}
	if media.Type != model.MediaTypePhoto {
		t.Fatalf("Type = %q, want photo", media.Type)
	}
	if media.CreatedAt.IsZero() {
		t.Fatal("expected CreatedAt to be set")
	}
}

func TestConfirmMediaUploadedMarksMediaUploaded(t *testing.T) {
	storage := newTestStorage(t)
	resp := mustGeneratePresignedURL(t, storage, "user_123")

	beforeConfirm := time.Now().UTC()
	media, err := storage.ConfirmMediaUploaded(context.Background(), "user_123", resp.MediaID)
	if err != nil {
		t.Fatalf("ConfirmMediaUploaded returned error: %v", err)
	}

	if media.Status != model.MediaStatusUploaded {
		t.Fatalf("status = %q, want uploaded", media.Status)
	}
	if media.UploadedAt == nil {
		t.Fatal("expected UploadedAt to be set")
	}
	if media.UploadedAt.Before(beforeConfirm) {
		t.Fatalf("UploadedAt = %s, want after %s", media.UploadedAt, beforeConfirm)
	}

	stored, err := storage.GetMedia(context.Background(), resp.MediaID)
	if err != nil {
		t.Fatalf("GetMedia returned error: %v", err)
	}
	if stored.Status != model.MediaStatusUploaded {
		t.Fatalf("stored status = %q, want uploaded", stored.Status)
	}
}

func TestConfirmMediaUploadedIsIdempotent(t *testing.T) {
	storage := newTestStorage(t)
	resp := mustGeneratePresignedURL(t, storage, "user_123")

	first, err := storage.ConfirmMediaUploaded(context.Background(), "user_123", resp.MediaID)
	if err != nil {
		t.Fatalf("first ConfirmMediaUploaded returned error: %v", err)
	}

	// Second confirmation of the same media ID must succeed and return the same record.
	second, err := storage.ConfirmMediaUploaded(context.Background(), "user_123", resp.MediaID)
	if err != nil {
		t.Fatalf("second ConfirmMediaUploaded returned error: %v", err)
	}
	if second.Status != model.MediaStatusUploaded {
		t.Fatalf("status = %q, want uploaded", second.Status)
	}
	// uploaded_at must not change between calls.
	if !second.UploadedAt.Equal(*first.UploadedAt) {
		t.Fatalf("UploadedAt changed on re-confirm: first=%s second=%s", first.UploadedAt, second.UploadedAt)
	}
}

func TestConfirmMediaUploadedReturnsNotFoundForMissingOrWrongUser(t *testing.T) {
	storage := newTestStorage(t)
	resp := mustGeneratePresignedURL(t, storage, "user_123")

	tests := []struct {
		name    string
		userID  string
		mediaID string
	}{
		{name: "missing media", userID: "user_123", mediaID: "missing"},
		{name: "wrong user", userID: "other_user", mediaID: resp.MediaID},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := storage.ConfirmMediaUploaded(context.Background(), tt.userID, tt.mediaID)
			if !errors.Is(err, ErrMediaNotFound) {
				t.Fatalf("error = %v, want ErrMediaNotFound", err)
			}
		})
	}
}

func TestGetMediaReturnsNotFoundForUnknownID(t *testing.T) {
	storage := newTestStorage(t)

	_, err := storage.GetMedia(context.Background(), "missing")
	if !errors.Is(err, ErrMediaNotFound) {
		t.Fatalf("error = %v, want ErrMediaNotFound", err)
	}
}

func TestNewID(t *testing.T) {
	first, err := newID()
	if err != nil {
		t.Fatalf("newID returned error: %v", err)
	}
	second, err := newID()
	if err != nil {
		t.Fatalf("newID returned error: %v", err)
	}
	if len(first) != 32 {
		t.Fatalf("id length = %d, want 32", len(first))
	}
	if first == second {
		t.Fatal("expected generated ids to differ")
	}
}

func newTestStorage(t *testing.T) *Storage {
	t.Helper()
	t.Setenv("AWS_ACCESS_KEY_ID", "minioadmin")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

	pool := newTestPool(t)
	mediaStore := store.NewMediaStore(pool)

	storage, err := NewStorage(context.Background(), "http://localhost:9000", "", "us-east-1", "instagram-media-test", mediaStore)
	if err != nil {
		t.Fatalf("NewStorage returned error: %v", err)
	}
	return storage
}

// newTestPool opens a connection to the local Postgres used for integration tests
// and cleans up the media table after the test completes.
func newTestPool(t *testing.T) *pgxpool.Pool {
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
	// Clear any leftover data. Delete media before users due to FK constraint.
	pool.Exec(ctx, "DELETE FROM media")
	pool.Exec(ctx, "DELETE FROM users WHERE id = 'user_123'")
	// Seed the test user that media rows will reference via FK.
	pool.Exec(ctx, `INSERT INTO users (id, username, email, password_hash, created_at)
		VALUES ('user_123', 'testuser', 'test@example.com', 'testhash', NOW())
		ON CONFLICT (id) DO NOTHING`)
	t.Cleanup(func() {
		pool.Exec(ctx, "DELETE FROM media")
		pool.Exec(ctx, "DELETE FROM users WHERE id = 'user_123'")
		pool.Close()
	})
	return pool
}

func mustGeneratePresignedURL(t *testing.T, storage *Storage, userID string) *model.PresignedURLResponse {
	t.Helper()

	resp, err := storage.GeneratePresignedUploadURL(context.Background(), model.PresignedURLRequest{
		UserID:      userID,
		FileName:    "clip.mp4",
		ContentType: "video/mp4",
		MediaType:   model.MediaTypeVideo,
	})
	if err != nil {
		t.Fatalf("GeneratePresignedUploadURL returned error: %v", err)
	}
	return resp
}
