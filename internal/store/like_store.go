package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type LikeStore struct {
	pool *pgxpool.Pool
}

func NewLikeStore(pool *pgxpool.Pool) *LikeStore {
	return &LikeStore{pool: pool}
}

// Like records that userID likes mediaID. It is idempotent: liking an
// already-liked item is a no-op. Returns ErrMediaNotFound if the media does
// not exist.
func (s *LikeStore) Like(ctx context.Context, mediaID, userID string) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO likes (media_id, user_id)
		VALUES ($1, $2)
		ON CONFLICT (media_id, user_id) DO NOTHING`,
		mediaID, userID,
	)
	if err != nil {
		if isForeignKeyViolation(err) {
			return ErrMediaNotFound
		}
		return fmt.Errorf("insert like: %w", err)
	}
	return nil
}

// Unlike removes a like. Unliking an item not liked is a no-op.
func (s *LikeStore) Unlike(ctx context.Context, mediaID, userID string) error {
	_, err := s.pool.Exec(ctx, `
		DELETE FROM likes WHERE media_id = $1 AND user_id = $2`,
		mediaID, userID,
	)
	if err != nil {
		return fmt.Errorf("delete like: %w", err)
	}
	return nil
}

func (s *LikeStore) Count(ctx context.Context, mediaID string) (int, error) {
	var n int
	err := s.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM likes WHERE media_id = $1`, mediaID,
	).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("count likes: %w", err)
	}
	return n, nil
}

func (s *LikeStore) IsLiked(ctx context.Context, mediaID, userID string) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM likes WHERE media_id = $1 AND user_id = $2
		)`, mediaID, userID,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check is liked: %w", err)
	}
	return exists, nil
}
