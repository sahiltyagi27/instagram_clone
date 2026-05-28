package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrSelfFollow is returned when a user attempts to follow themselves.
var ErrSelfFollow = errors.New("cannot follow yourself")

type FollowStore struct {
	pool *pgxpool.Pool
}

func NewFollowStore(pool *pgxpool.Pool) *FollowStore {
	return &FollowStore{pool: pool}
}

// Follow records that followerID follows followeeID. It is idempotent: following
// someone already followed is a no-op. Returns ErrUserNotFound if the followee
// does not exist and ErrSelfFollow if the two IDs are equal.
func (s *FollowStore) Follow(ctx context.Context, followerID, followeeID string) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO follows (follower_id, followee_id)
		VALUES ($1, $2)
		ON CONFLICT (follower_id, followee_id) DO NOTHING`,
		followerID, followeeID,
	)
	if err != nil {
		if isCheckViolation(err) {
			return ErrSelfFollow
		}
		if isForeignKeyViolation(err) {
			return ErrUserNotFound
		}
		return fmt.Errorf("insert follow: %w", err)
	}
	return nil
}

// Unfollow removes the follow edge. Unfollowing someone not followed is a no-op.
func (s *FollowStore) Unfollow(ctx context.Context, followerID, followeeID string) error {
	_, err := s.pool.Exec(ctx, `
		DELETE FROM follows WHERE follower_id = $1 AND followee_id = $2`,
		followerID, followeeID,
	)
	if err != nil {
		return fmt.Errorf("delete follow: %w", err)
	}
	return nil
}

func (s *FollowStore) IsFollowing(ctx context.Context, followerID, followeeID string) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM follows WHERE follower_id = $1 AND followee_id = $2
		)`, followerID, followeeID,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check is following: %w", err)
	}
	return exists, nil
}

// GetFollowing returns the IDs of users that followerID follows.
func (s *FollowStore) GetFollowing(ctx context.Context, followerID string) ([]string, error) {
	return s.queryUserIDs(ctx, `
		SELECT followee_id FROM follows WHERE follower_id = $1 ORDER BY created_at DESC`, followerID)
}

// GetFollowers returns the IDs of users that follow followeeID. This drives feed
// fan-out: when a user posts, the item is pushed to each follower's feed.
func (s *FollowStore) GetFollowers(ctx context.Context, followeeID string) ([]string, error) {
	return s.queryUserIDs(ctx, `
		SELECT follower_id FROM follows WHERE followee_id = $1 ORDER BY created_at DESC`, followeeID)
}

func (s *FollowStore) queryUserIDs(ctx context.Context, query, arg string) ([]string, error) {
	rows, err := s.pool.Query(ctx, query, arg)
	if err != nil {
		return nil, fmt.Errorf("query follows: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan follow id: %w", err)
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}
