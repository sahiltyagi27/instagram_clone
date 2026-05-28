package service

import (
	"context"
	"fmt"
	"log/slog"

	"instagram_clone/internal/model"
	"instagram_clone/internal/store"
)

type FeedService struct {
	feed *store.FeedStore
}

func NewFeedService(feed *store.FeedStore) *FeedService {
	return &FeedService{feed: feed}
}

func (s *FeedService) AddFeedItem(ctx context.Context, userID string, item model.FeedItem) {
	if err := s.feed.AddItem(ctx, userID, item); err != nil {
		slog.Error("add feed item to redis", "user_id", userID, "error", err)
	}
}

func (s *FeedService) GetFeed(ctx context.Context, userID string, limit, offset int) (model.FeedResponse, error) {
	resp, err := s.feed.GetFeed(ctx, userID, limit, offset)
	if err != nil {
		return model.FeedResponse{}, fmt.Errorf("get feed: %w", err)
	}
	return resp, nil
}
