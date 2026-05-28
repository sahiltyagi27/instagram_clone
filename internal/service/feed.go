package service

import (
	"context"
	"errors"
	"fmt"

	"instagram_clone/internal/model"
	"instagram_clone/internal/store"
)

// ErrInvalidCursor is returned by GetFeed when the caller supplies a cursor
// that cannot be decoded. Handlers should surface this as 400 Bad Request.
var ErrInvalidCursor = errors.New("invalid cursor")

// FollowerLister supplies the followers of a user for feed fan-out. It is
// satisfied by *store.FollowStore. A nil FollowerLister disables fan-out, in
// which case items are only written to the author's own feed.
type FollowerLister interface {
	GetFollowers(ctx context.Context, userID string) ([]string, error)
}

type FeedService struct {
	feed    *store.FeedStore
	follows FollowerLister
}

func NewFeedService(feed *store.FeedStore, follows FollowerLister) *FeedService {
	return &FeedService{feed: feed, follows: follows}
}

func (s *FeedService) AddFeedItem(ctx context.Context, userID string, item model.FeedItem) error {
	if err := s.feed.AddItem(ctx, userID, item); err != nil {
		return fmt.Errorf("add feed item to redis: %w", err)
	}
	return nil
}

// FanoutFeedItem writes item to the feeds of everyone who follows authorID, plus
// the author's own feed so their posts appear on their timeline. This is the
// fan-out-on-write step invoked by the Kafka consumer when media is processed.
// Returning an error lets the consumer retry and dead-letter on failure.
func (s *FeedService) FanoutFeedItem(ctx context.Context, authorID string, item model.FeedItem) error {
	recipients := map[string]struct{}{authorID: {}}
	if s.follows != nil {
		followers, err := s.follows.GetFollowers(ctx, authorID)
		if err != nil {
			return fmt.Errorf("get followers for fanout: %w", err)
		}
		for _, f := range followers {
			recipients[f] = struct{}{}
		}
	}
	for uid := range recipients {
		if err := s.feed.AddItem(ctx, uid, item); err != nil {
			return fmt.Errorf("fanout feed item to %s: %w", uid, err)
		}
	}
	return nil
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
