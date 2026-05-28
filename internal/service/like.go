package service

import (
	"context"
	"errors"
	"fmt"

	"instagram_clone/internal/model"
	"instagram_clone/internal/store"
)

type LikeService struct {
	likes *store.LikeStore
}

func NewLikeService(likes *store.LikeStore) *LikeService {
	return &LikeService{likes: likes}
}

// Like records a like and returns ErrMediaNotFound if the media does not exist.
func (s *LikeService) Like(ctx context.Context, mediaID, userID string) error {
	if err := s.likes.Like(ctx, mediaID, userID); err != nil {
		if errors.Is(err, store.ErrMediaNotFound) {
			return ErrMediaNotFound
		}
		return fmt.Errorf("like: %w", err)
	}
	return nil
}

func (s *LikeService) Unlike(ctx context.Context, mediaID, userID string) error {
	if err := s.likes.Unlike(ctx, mediaID, userID); err != nil {
		return fmt.Errorf("unlike: %w", err)
	}
	return nil
}

// Status reports the like count for a media item and whether userID liked it.
func (s *LikeService) Status(ctx context.Context, mediaID, userID string) (model.LikeStatusResponse, error) {
	count, err := s.likes.Count(ctx, mediaID)
	if err != nil {
		return model.LikeStatusResponse{}, fmt.Errorf("like status count: %w", err)
	}
	liked, err := s.likes.IsLiked(ctx, mediaID, userID)
	if err != nil {
		return model.LikeStatusResponse{}, fmt.Errorf("like status check: %w", err)
	}
	return model.LikeStatusResponse{MediaID: mediaID, Count: count, Liked: liked}, nil
}
