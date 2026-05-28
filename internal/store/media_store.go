package store

import (
	"context"
	"errors"
	"fmt"

	"instagram_clone/internal/model"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrMediaNotFound = errors.New("media not found")

type MediaStore struct {
	pool *pgxpool.Pool
}

func NewMediaStore(pool *pgxpool.Pool) *MediaStore {
	return &MediaStore{pool: pool}
}

func (s *MediaStore) Create(ctx context.Context, m model.Media) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO media (id, user_id, type, status, file_name, content_type, s3_bucket, s3_key, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		m.ID, m.UserID, m.Type, m.Status, m.FileName, m.ContentType, m.S3Bucket, m.S3Key, m.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert media: %w", err)
	}
	return nil
}

func (s *MediaStore) GetByID(ctx context.Context, id string) (model.Media, error) {
	var m model.Media
	err := s.pool.QueryRow(ctx, `
		SELECT id, user_id, type, status, file_name, content_type, s3_bucket, s3_key, created_at, uploaded_at
		FROM media WHERE id = $1`, id,
	).Scan(&m.ID, &m.UserID, &m.Type, &m.Status, &m.FileName, &m.ContentType, &m.S3Bucket, &m.S3Key, &m.CreatedAt, &m.UploadedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return model.Media{}, ErrMediaNotFound
		}
		return model.Media{}, fmt.Errorf("get media by id: %w", err)
	}
	return m, nil
}

func (s *MediaStore) MarkUploaded(ctx context.Context, id, userID string) (model.Media, error) {
	var m model.Media
	err := s.pool.QueryRow(ctx, `
		UPDATE media
		SET status = 'uploaded', uploaded_at = NOW()
		WHERE id = $1 AND user_id = $2
		RETURNING id, user_id, type, status, file_name, content_type, s3_bucket, s3_key, created_at, uploaded_at`,
		id, userID,
	).Scan(&m.ID, &m.UserID, &m.Type, &m.Status, &m.FileName, &m.ContentType, &m.S3Bucket, &m.S3Key, &m.CreatedAt, &m.UploadedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return model.Media{}, ErrMediaNotFound
		}
		return model.Media{}, fmt.Errorf("mark media uploaded: %w", err)
	}
	return m, nil
}
