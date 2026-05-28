package service

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"instagram_clone/internal/model"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const PresignedURLExpiry = 15 * time.Minute

var ErrMediaNotFound = errors.New("media not found")

type Storage struct {
	bucket    string
	client    *s3.Client
	presigner *s3.PresignClient

	mu    sync.RWMutex
	media map[string]model.Media
}

func NewStorage(ctx context.Context, endpoint, region, bucket string) (*Storage, error) {
	cfg, err := config.LoadDefaultConfig(
		ctx,
		config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})

	return &Storage{
		bucket:    bucket,
		client:    client,
		presigner: s3.NewPresignClient(client),
		media:     make(map[string]model.Media),
	}, nil
}

func (s *Storage) GeneratePresignedUploadURL(ctx context.Context, req model.PresignedURLRequest) (*model.PresignedURLResponse, error) {
	mediaID, err := newID()
	if err != nil {
		return nil, err
	}

	fileName := filepath.Base(strings.TrimSpace(req.FileName))
	key := fmt.Sprintf("users/%s/%s/%s", req.UserID, mediaID, fileName)

	presigned, err := s.presigner.PresignPutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(key),
		ContentType: aws.String(req.ContentType),
	}, func(opts *s3.PresignOptions) {
		opts.Expires = PresignedURLExpiry
	})
	if err != nil {
		return nil, fmt.Errorf("presign put object: %w", err)
	}

	media := model.Media{
		ID:          mediaID,
		UserID:      req.UserID,
		Type:        req.MediaType,
		Status:      model.MediaStatusPending,
		FileName:    fileName,
		ContentType: req.ContentType,
		S3Bucket:    s.bucket,
		S3Key:       key,
		CreatedAt:   time.Now().UTC(),
	}

	s.mu.Lock()
	s.media[mediaID] = media
	s.mu.Unlock()

	return &model.PresignedURLResponse{
		MediaID:   mediaID,
		UploadURL: presigned.URL,
		S3Bucket:  s.bucket,
		S3Key:     key,
		ExpiresIn: int64(PresignedURLExpiry.Seconds()),
	}, nil
}

func (s *Storage) ConfirmMediaUploaded(_ context.Context, userID, mediaID string) (*model.Media, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	media, ok := s.media[mediaID]
	if !ok || media.UserID != userID {
		return nil, ErrMediaNotFound
	}

	now := time.Now().UTC()
	media.Status = model.MediaStatusUploaded
	media.UploadedAt = &now
	s.media[mediaID] = media

	return &media, nil
}

func (s *Storage) GetMedia(_ context.Context, mediaID string) (*model.Media, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	media, ok := s.media[mediaID]
	if !ok {
		return nil, ErrMediaNotFound
	}

	return &media, nil
}

func (s *Storage) Bucket() string {
	return s.bucket
}

func (s *Storage) PresignPutObject(ctx context.Context, key, contentType string) (string, error) {
	presigned, err := s.presigner.PresignPutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(key),
		ContentType: aws.String(contentType),
	}, func(opts *s3.PresignOptions) {
		opts.Expires = PresignedURLExpiry
	})
	if err != nil {
		return "", fmt.Errorf("presign put object: %w", err)
	}
	return presigned.URL, nil
}

func (s *Storage) GetObject(ctx context.Context, key string) ([]byte, string, error) {
	resp, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, "", fmt.Errorf("get object: %w", err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", fmt.Errorf("read object: %w", err)
	}

	contentType := ""
	if resp.ContentType != nil {
		contentType = *resp.ContentType
	}
	return data, contentType, nil
}

func (s *Storage) PutObject(ctx context.Context, key, contentType string, data []byte) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(key),
		ContentType: aws.String(contentType),
		Body:        bytes.NewReader(data),
	})
	if err != nil {
		return fmt.Errorf("put object: %w", err)
	}
	return nil
}

func newID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate media id: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}
