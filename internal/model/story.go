package model

import "time"

type Story struct {
	ID        string    `json:"id"`
	UserID    string    `json:"user_id"`
	S3Key     string    `json:"s3_key"`
	URL       string    `json:"url"`
	CreatedAt time.Time `json:"created_at"`
	ExpiresAt time.Time `json:"expires_at"`
}

type StoryPresignedURLRequest struct {
	UserID      string `json:"user_id"`
	FileName    string `json:"file_name"`
	ContentType string `json:"content_type"`
}

type StoryPresignedURLResponse struct {
	StoryID   string `json:"story_id"`
	UploadURL string `json:"upload_url"`
	S3Bucket  string `json:"s3_bucket"`
	S3Key     string `json:"s3_key"`
	ExpiresIn int64  `json:"expires_in"`
}

type ConfirmStoryRequest struct {
	UserID  string `json:"user_id"`
	StoryID string `json:"story_id"`
}
