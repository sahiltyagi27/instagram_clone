package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"instagram_clone/internal/model"
	"instagram_clone/internal/store"
)

// ErrInvalidCursor is returned by GetFeed when the caller supplies a cursor
// that cannot be decoded. Handlers should surface this as 400 Bad Request.
var ErrInvalidCursor = errors.New("invalid cursor")

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

func (s *FeedService) GetFeed(ctx context.Context, userID string, limit int, cursor string) (model.FeedResponse, error) {
	resp, err := s.feed.GetFeed(ctx, userID, limit, cursor)
	if err != nil {
		if errors.Is(err, store.ErrInvalidCursor) {
			return model.FeedResponse{}, ErrInvalidCursor
		}
		return model.FeedResponse{}, fmt.Errorf("get feed: %w", err)
	}
	return resp, nil
}
