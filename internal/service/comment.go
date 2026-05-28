package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"instagram_clone/internal/model"
	"instagram_clone/internal/store"
)

// ErrCommentNotFound is re-exported for handler use.
var ErrCommentNotFound = store.ErrCommentNotFound

// ErrEmptyComment / ErrCommentTooLong are validation errors surfaced as 400.
var (
	ErrEmptyComment   = errors.New("comment body must not be empty")
	ErrCommentTooLong = fmt.Errorf("comment body must be at most %d characters", model.MaxCommentLength)
)

const defaultCommentLimit = 50

type CommentService struct {
	comments *store.CommentStore
}

func NewCommentService(comments *store.CommentStore) *CommentService {
	return &CommentService{comments: comments}
}

// Add validates and stores a comment. Returns ErrEmptyComment / ErrCommentTooLong
// for invalid bodies and ErrMediaNotFound if the media does not exist.
func (s *CommentService) Add(ctx context.Context, mediaID, userID, body string) (model.Comment, error) {
	body = strings.TrimSpace(body)
	if body == "" {
		return model.Comment{}, ErrEmptyComment
	}
	if len(body) > model.MaxCommentLength {
		return model.Comment{}, ErrCommentTooLong
	}

	id, err := newID()
	if err != nil {
		return model.Comment{}, err
	}
	comment := model.Comment{
		ID:        id,
		MediaID:   mediaID,
		UserID:    userID,
		Body:      body,
		CreatedAt: time.Now().UTC(),
	}
	if err := s.comments.Create(ctx, comment); err != nil {
		if errors.Is(err, store.ErrMediaNotFound) {
			return model.Comment{}, ErrMediaNotFound
		}
		return model.Comment{}, fmt.Errorf("add comment: %w", err)
	}
	return comment, nil
}

func (s *CommentService) List(ctx context.Context, mediaID string, limit int) (model.CommentListResponse, error) {
	if limit <= 0 {
		limit = defaultCommentLimit
	}
	comments, err := s.comments.ListByMedia(ctx, mediaID, limit)
	if err != nil {
		return model.CommentListResponse{}, fmt.Errorf("list comments: %w", err)
	}
	return model.CommentListResponse{Comments: comments}, nil
}

// Delete removes a comment owned by userID. Returns ErrCommentNotFound if the
// comment is missing or owned by another user.
func (s *CommentService) Delete(ctx context.Context, commentID, userID string) error {
	if err := s.comments.Delete(ctx, commentID, userID); err != nil {
		if errors.Is(err, store.ErrCommentNotFound) {
			return ErrCommentNotFound
		}
		return fmt.Errorf("delete comment: %w", err)
	}
	return nil
}
