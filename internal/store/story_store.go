package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"instagram_clone/internal/model"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrStoryNotFound = errors.New("story not found")

type StoryStore struct {
	pool *pgxpool.Pool
}

func NewStoryStore(pool *pgxpool.Pool) *StoryStore {
	return &StoryStore{pool: pool}
}

func (s *StoryStore) Create(ctx context.Context, story model.Story) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO stories (id, user_id, s3_key, url, created_at)
		VALUES ($1, $2, $3, $4, $5)`,
		story.ID, story.UserID, story.S3Key, story.URL, story.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert story: %w", err)
	}
	return nil
}

func (s *StoryStore) GetByID(ctx context.Context, id string) (model.Story, error) {
	var story model.Story
	err := s.pool.QueryRow(ctx, `
		SELECT id, user_id, s3_key, url, created_at, expires_at
		FROM stories
		WHERE id = $1 AND expires_at > NOW()`, id,
	).Scan(&story.ID, &story.UserID, &story.S3Key, &story.URL, &story.CreatedAt, &story.ExpiresAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return model.Story{}, ErrStoryNotFound
		}
		return model.Story{}, fmt.Errorf("get story by id: %w", err)
	}
	return story, nil
}

func (s *StoryStore) GetActiveByUser(ctx context.Context, userID string) ([]model.Story, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, user_id, s3_key, url, created_at, expires_at
		FROM stories
		WHERE user_id = $1 AND expires_at > NOW()
		ORDER BY created_at DESC`, userID,
	)
	if err != nil {
		return nil, fmt.Errorf("get active stories: %w", err)
	}
	defer rows.Close()

	var stories []model.Story
	for rows.Next() {
		var story model.Story
		if err := rows.Scan(&story.ID, &story.UserID, &story.S3Key, &story.URL, &story.CreatedAt, &story.ExpiresAt); err != nil {
			return nil, fmt.Errorf("scan story: %w", err)
		}
		stories = append(stories, story)
	}
	return stories, rows.Err()
}

func (s *StoryStore) Confirm(ctx context.Context, id, userID string) (model.Story, error) {
	cutoff := time.Now().UTC().Add(-PendingUploadTTL)
	var story model.Story
	err := s.pool.QueryRow(ctx, `
		UPDATE stories
		SET expires_at = NOW() + INTERVAL '24 hours'
		WHERE id = $1 AND user_id = $2
		  AND expires_at IS NULL
		  AND created_at > $3
		RETURNING id, user_id, s3_key, url, created_at, expires_at`,
		id, userID, cutoff,
	).Scan(&story.ID, &story.UserID, &story.S3Key, &story.URL, &story.CreatedAt, &story.ExpiresAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return model.Story{}, ErrStoryNotFound
		}
		return model.Story{}, fmt.Errorf("confirm story: %w", err)
	}
	return story, nil
}

// DeleteStalePending removes story rows that were never confirmed within the
// pending TTL window. Called periodically to keep the table clean.
func (s *StoryStore) DeleteStalePending(ctx context.Context) error {
	cutoff := time.Now().UTC().Add(-PendingUploadTTL)
	_, err := s.pool.Exec(ctx, `
		DELETE FROM stories WHERE expires_at IS NULL AND created_at < $1`, cutoff)
	if err != nil {
		return fmt.Errorf("delete stale pending stories: %w", err)
	}
	return nil
}

// DeleteExpired physically removes confirmed story rows whose 24-hour TTL has
// elapsed. Called periodically so expired stories do not accumulate forever.
func (s *StoryStore) DeleteExpired(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `
		DELETE FROM stories WHERE expires_at IS NOT NULL AND expires_at < NOW()`)
	if err != nil {
		return fmt.Errorf("delete expired stories: %w", err)
	}
	return nil
}
