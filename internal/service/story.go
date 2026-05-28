package service

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"instagram_clone/internal/model"
	"instagram_clone/internal/store"
)

var ErrStoryNotFound = store.ErrStoryNotFound

type StoryService struct {
	storage *Storage
	stories *store.StoryStore
}

func NewStoryService(storage *Storage, stories *store.StoryStore) *StoryService {
	return &StoryService{storage: storage, stories: stories}
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

	story := model.Story{
		ID:        storyID,
		UserID:    req.UserID,
		S3Key:     key,
		URL:       s.storage.ObjectURL(key),
		CreatedAt: time.Now().UTC(),
	}

	if err := s.stories.Create(ctx, story); err != nil {
		return nil, err
	}

	return &model.StoryPresignedURLResponse{
		StoryID:   storyID,
		UploadURL: uploadURL,
		S3Bucket:  s.storage.Bucket(),
		S3Key:     key,
		ExpiresIn: int64(PresignedURLExpiry.Seconds()),
	}, nil
}

func (s *StoryService) ConfirmUpload(ctx context.Context, userID, storyID string) (*model.Story, error) {
	story, err := s.stories.Confirm(ctx, storyID, userID)
	if err != nil {
		if errors.Is(err, store.ErrStoryNotFound) {
			return nil, ErrStoryNotFound
		}
		return nil, err
	}
	return &story, nil
}

func (s *StoryService) GetStory(ctx context.Context, storyID string) (*model.Story, error) {
	story, err := s.stories.GetByID(ctx, storyID)
	if err != nil {
		if errors.Is(err, store.ErrStoryNotFound) {
			return nil, ErrStoryNotFound
		}
		return nil, err
	}
	return &story, nil
}

func (s *StoryService) GetActiveStoriesByUser(ctx context.Context, userID string) []model.Story {
	stories, err := s.stories.GetActiveByUser(ctx, userID)
	if err != nil {
		return []model.Story{}
	}
	return stories
}
