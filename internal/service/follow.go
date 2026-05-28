package service

import (
	"context"
	"errors"
	"fmt"

	"instagram_clone/internal/model"
	"instagram_clone/internal/store"
)

// ErrSelfFollow and ErrUserNotFound are re-exported so handlers can map them to
// HTTP status codes without importing the store package.
var (
	ErrSelfFollow   = store.ErrSelfFollow
	ErrUserNotFound = store.ErrUserNotFound
)

type FollowService struct {
	follows *store.FollowStore
}

func NewFollowService(follows *store.FollowStore) *FollowService {
	return &FollowService{follows: follows}
}

func (s *FollowService) Follow(ctx context.Context, followerID, followeeID string) error {
	if followerID == followeeID {
		return ErrSelfFollow
	}
	if err := s.follows.Follow(ctx, followerID, followeeID); err != nil {
		if errors.Is(err, store.ErrSelfFollow) {
			return ErrSelfFollow
		}
		if errors.Is(err, store.ErrUserNotFound) {
			return ErrUserNotFound
		}
		return fmt.Errorf("follow: %w", err)
	}
	return nil
}

func (s *FollowService) Unfollow(ctx context.Context, followerID, followeeID string) error {
	if err := s.follows.Unfollow(ctx, followerID, followeeID); err != nil {
		return fmt.Errorf("unfollow: %w", err)
	}
	return nil
}

func (s *FollowService) Following(ctx context.Context, userID string) (model.FollowListResponse, error) {
	ids, err := s.follows.GetFollowing(ctx, userID)
	if err != nil {
		return model.FollowListResponse{}, fmt.Errorf("list following: %w", err)
	}
	return newFollowListResponse(ids), nil
}

func (s *FollowService) Followers(ctx context.Context, userID string) (model.FollowListResponse, error) {
	ids, err := s.follows.GetFollowers(ctx, userID)
	if err != nil {
		return model.FollowListResponse{}, fmt.Errorf("list followers: %w", err)
	}
	return newFollowListResponse(ids), nil
}

func newFollowListResponse(ids []string) model.FollowListResponse {
	if ids == nil {
		ids = []string{}
	}
	return model.FollowListResponse{UserIDs: ids, Count: len(ids)}
}
