package service

import (
	"context"
	"errors"
	"testing"

	"instagram_clone/internal/model"
	"instagram_clone/internal/store"

	"github.com/jackc/pgx/v5/pgxpool"
)

func newTestStoryService(t *testing.T) *StoryService {
	t.Helper()
	t.Setenv("AWS_ACCESS_KEY_ID", "minioadmin")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

	const dsn = "postgres://postgres:postgres@localhost:5432/instagram_clone"
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil || pool.Ping(context.Background()) != nil {
		if pool != nil {
			pool.Close()
		}
		t.Skip("postgres unavailable, skipping")
	}

	// Seed a test user to satisfy the foreign key on stories.user_id.
	pool.Exec(context.Background(), `
		INSERT INTO users (id, username, email, password_hash) VALUES
		('user_123', 'test', 'test@example.com', 'x')
		ON CONFLICT DO NOTHING`)

	pool.Exec(context.Background(), "DELETE FROM stories")
	t.Cleanup(func() {
		pool.Exec(context.Background(), "DELETE FROM stories")
		pool.Exec(context.Background(), "DELETE FROM users WHERE id = 'user_123'")
		pool.Close()
	})

	storyStore := store.NewStoryStore(pool)
	mediaStore := store.NewMediaStore(pool)

	// Presigning is computed locally by the AWS SDK — no live S3 required.
	storage, err := NewStorage(context.Background(), "http://s3.test:9000", "", "us-east-1", "instagram-media-test", mediaStore)
	if err != nil {
		t.Fatalf("NewStorage returned error: %v", err)
	}

	return NewStoryService(storage, storyStore)
}

func TestStoryServiceGenerateConfirmAndFetchActiveStories(t *testing.T) {
	stories := newTestStoryService(t)

	resp, err := stories.GeneratePresignedURL(context.Background(), model.StoryPresignedURLRequest{
		UserID:      "user_123",
		FileName:    "../story.jpg",
		ContentType: "image/jpeg",
	})
	if err != nil {
		t.Fatalf("GeneratePresignedURL returned error: %v", err)
	}
	if resp.StoryID == "" {
		t.Fatal("expected story id")
	}
	if resp.S3Key != "stories/user_123/"+resp.StoryID+"/story.jpg" {
		t.Fatalf("S3Key = %q, want sanitized story key", resp.S3Key)
	}

	// Story is not yet confirmed — should not appear as active.
	if active := stories.GetActiveStoriesByUser(context.Background(), "user_123"); len(active) != 0 {
		t.Fatalf("active stories before confirm = %d, want 0", len(active))
	}

	story, err := stories.ConfirmUpload(context.Background(), "user_123", resp.StoryID)
	if err != nil {
		t.Fatalf("ConfirmUpload returned error: %v", err)
	}
	if story.ExpiresAt.IsZero() {
		t.Fatal("expected ExpiresAt to be set after confirm")
	}

	got, err := stories.GetStory(context.Background(), resp.StoryID)
	if err != nil {
		t.Fatalf("GetStory returned error: %v", err)
	}
	if got.ID != resp.StoryID {
		t.Fatalf("story id = %q, want %q", got.ID, resp.StoryID)
	}

	active := stories.GetActiveStoriesByUser(context.Background(), "user_123")
	if len(active) != 1 {
		t.Fatalf("active stories after confirm = %d, want 1", len(active))
	}
}

func TestStoryServiceNotFoundPaths(t *testing.T) {
	stories := newTestStoryService(t)

	_, err := stories.ConfirmUpload(context.Background(), "user_123", "missing")
	if !errors.Is(err, ErrStoryNotFound) {
		t.Fatalf("ConfirmUpload error = %v, want ErrStoryNotFound", err)
	}

	_, err = stories.GetStory(context.Background(), "missing")
	if !errors.Is(err, ErrStoryNotFound) {
		t.Fatalf("GetStory error = %v, want ErrStoryNotFound", err)
	}
}
