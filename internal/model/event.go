package model

import "time"

type MediaUploadedEvent struct {
	MediaID   string    `json:"media_id"`
	UserID    string    `json:"user_id"`
	S3Key     string    `json:"s3_key"`
	MediaType string    `json:"media_type"`
	CreatedAt time.Time `json:"created_at"`
}

type StoryUploadedEvent struct {
	StoryID   string    `json:"story_id"`
	UserID    string    `json:"user_id"`
	S3Key     string    `json:"s3_key"`
	CreatedAt time.Time `json:"created_at"`
}
