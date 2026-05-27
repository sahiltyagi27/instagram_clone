package model

import "time"

type MediaType string

const (
	MediaTypePhoto MediaType = "photo"
	MediaTypeVideo MediaType = "video"
)

func (m MediaType) Valid() bool {
	return m == MediaTypePhoto || m == MediaTypeVideo
}

type MediaStatus string

const (
	MediaStatusPending  MediaStatus = "pending"
	MediaStatusUploaded MediaStatus = "uploaded"
)

type Media struct {
	ID          string      `json:"id"`
	UserID      string      `json:"user_id"`
	Type        MediaType   `json:"type"`
	Status      MediaStatus `json:"status"`
	FileName    string      `json:"file_name"`
	ContentType string      `json:"content_type"`
	S3Bucket    string      `json:"s3_bucket"`
	S3Key       string      `json:"s3_key"`
	CreatedAt   time.Time   `json:"created_at"`
	UploadedAt  *time.Time  `json:"uploaded_at,omitempty"`
}

type PresignedURLRequest struct {
	UserID      string    `json:"user_id"`
	FileName    string    `json:"file_name"`
	ContentType string    `json:"content_type"`
	MediaType   MediaType `json:"media_type"`
}

type PresignedURLResponse struct {
	MediaID   string `json:"media_id"`
	UploadURL string `json:"upload_url"`
	S3Bucket  string `json:"s3_bucket"`
	S3Key     string `json:"s3_key"`
	ExpiresIn int64  `json:"expires_in"`
}

type ConfirmMediaRequest struct {
	UserID  string `json:"user_id"`
	MediaID string `json:"media_id"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}
