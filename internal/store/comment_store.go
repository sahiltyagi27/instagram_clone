package store

import (
	"context"
	"errors"
	"fmt"

	"instagram_clone/internal/model"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrCommentNotFound is returned when a comment does not exist, or when a
// delete targets a comment the requesting user does not own.
var ErrCommentNotFound = errors.New("comment not found")

type CommentStore struct {
	pool *pgxpool.Pool
}

func NewCommentStore(pool *pgxpool.Pool) *CommentStore {
	return &CommentStore{pool: pool}
}

// Create inserts a comment. Returns ErrMediaNotFound if the media does not exist.
func (s *CommentStore) Create(ctx context.Context, c model.Comment) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO comments (id, media_id, user_id, body, created_at)
		VALUES ($1, $2, $3, $4, $5)`,
		c.ID, c.MediaID, c.UserID, c.Body, c.CreatedAt,
	)
	if err != nil {
		if isForeignKeyViolation(err) {
			return ErrMediaNotFound
		}
		return fmt.Errorf("insert comment: %w", err)
	}
	return nil
}

// ListByMedia returns up to limit comments for a media item, newest first.
func (s *CommentStore) ListByMedia(ctx context.Context, mediaID string, limit int) ([]model.Comment, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, media_id, user_id, body, created_at
		FROM comments
		WHERE media_id = $1
		ORDER BY created_at DESC
		LIMIT $2`, mediaID, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list comments: %w", err)
	}
	defer rows.Close()

	comments := make([]model.Comment, 0)
	for rows.Next() {
		var c model.Comment
		if err := rows.Scan(&c.ID, &c.MediaID, &c.UserID, &c.Body, &c.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan comment: %w", err)
		}
		comments = append(comments, c)
	}
	return comments, rows.Err()
}

// Delete removes a comment owned by userID under mediaID. Returns
// ErrCommentNotFound if no matching comment exists (missing, owned by someone
// else, or not under that media item).
func (s *CommentStore) Delete(ctx context.Context, mediaID, commentID, userID string) error {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM comments WHERE id = $1 AND media_id = $2 AND user_id = $3`,
		commentID, mediaID, userID,
	)
	if err != nil {
		return fmt.Errorf("delete comment: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrCommentNotFound
	}
	return nil
}
