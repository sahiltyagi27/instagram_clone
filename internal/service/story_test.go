package service

import (
	"context"
	"errors"
	"testing"

	"instagram_clone/internal/model"
)

func TestStoryServiceGenerateConfirmAndFetchActiveStories(t *testing.T) {
	storage := newTestStorage(t)
	stories := NewStoryService(storage)

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

	if active := stories.GetActiveStoriesByUser(context.Background(), "user_123"); len(active) != 0 {
		t.Fatalf("active stories before confirm = %d, want 0", len(active))
	}

	story, err := stories.ConfirmUpload(context.Background(), "user_123", resp.StoryID)
	if err != nil {
		t.Fatalf("ConfirmUpload returned error: %v", err)
	}
	if story.ExpiresAt.IsZero() {
		t.Fatal("expected ExpiresAt to be set")
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
		t.Fatalf("active stories = %d, want 1", len(active))
	}
}

func TestStoryServiceNotFoundPaths(t *testing.T) {
	stories := NewStoryService(newTestStorage(t))

	_, err := stories.ConfirmUpload(context.Background(), "user_123", "missing")
	if !errors.Is(err, ErrStoryNotFound) {
		t.Fatalf("ConfirmUpload error = %v, want ErrStoryNotFound", err)
	}

	_, err = stories.GetStory(context.Background(), "missing")
	if !errors.Is(err, ErrStoryNotFound) {
		t.Fatalf("GetStory error = %v, want ErrStoryNotFound", err)
	}
}
