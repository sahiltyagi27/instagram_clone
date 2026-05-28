package service

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"instagram_clone/internal/model"
)

const (
	StoryTTL        = 24 * time.Hour
	PendingStoryTTL = 30 * time.Minute
)

var ErrStoryNotFound = errors.New("story not found")

type StoryService struct {
	storage *Storage

	mu      sync.RWMutex
	stories map[string]model.Story
}

func NewStoryService(storage *Storage) *StoryService {
	return &StoryService{
		storage: storage,
		stories: make(map[string]model.Story),
	}
}

func (s *StoryService) GeneratePresignedURL(ctx context.Context, req model.StoryPresignedURLRequest) (*model.StoryPresignedURLResponse, error) {
	storyID, err := newID()
	if err != nil {
		return nil, err
	}

	fileName := filepath.Base(strings.TrimSpace(req.FileName))
	key := fmt.Sprintf("stories/%s/%s/%s", req.UserID, storyID, fileName)
	uploadURL, err := s.storage.PresignPutObject(ctx, key, req.ContentType)
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	story := model.Story{
		ID:        storyID,
		UserID:    req.UserID,
		S3Key:     key,
		URL:       s.storage.ObjectURL(key),
		CreatedAt: now,
	}

	s.mu.Lock()
	s.stories[storyID] = story
	s.mu.Unlock()

	return &model.StoryPresignedURLResponse{
		StoryID:   storyID,
		UploadURL: uploadURL,
		S3Bucket:  s.storage.Bucket(),
		S3Key:     key,
		ExpiresIn: int64(PresignedURLExpiry.Seconds()),
	}, nil
}

func (s *StoryService) ConfirmUpload(_ context.Context, userID, storyID string) (*model.Story, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	story, ok := s.stories[storyID]
	if !ok || story.UserID != userID {
		return nil, ErrStoryNotFound
	}

	story.ExpiresAt = time.Now().UTC().Add(StoryTTL)
	s.stories[storyID] = story

	return &story, nil
}

func (s *StoryService) GetStory(_ context.Context, storyID string) (*model.Story, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	story, ok := s.stories[storyID]
	if !ok || !storyActive(story, time.Now().UTC()) {
		return nil, ErrStoryNotFound
	}
	return &story, nil
}

func (s *StoryService) GetActiveStoriesByUser(_ context.Context, userID string) []model.Story {
	now := time.Now().UTC()

	s.mu.RLock()
	defer s.mu.RUnlock()

	stories := make([]model.Story, 0)
	for _, story := range s.stories {
		if story.UserID == userID && storyActive(story, now) {
			stories = append(stories, story)
		}
	}
	return stories
}

func (s *StoryService) StartExpiryWorker(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.purgeExpired(time.Now().UTC())
		}
	}
}

func (s *StoryService) purgeExpired(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for id, story := range s.stories {
		if storyExpired(story, now) {
			delete(s.stories, id)
		}
	}
}

func storyExpired(story model.Story, now time.Time) bool {
	if story.CreatedAt.IsZero() {
		return true
	}
	if story.ExpiresAt.IsZero() {
		return !story.CreatedAt.Add(PendingStoryTTL).After(now)
	}
	return !story.ExpiresAt.After(now)
}

func storyActive(story model.Story, now time.Time) bool {
	return !story.ExpiresAt.IsZero() && story.ExpiresAt.After(now)
}
