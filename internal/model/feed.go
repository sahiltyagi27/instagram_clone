package model

import "time"

type FeedItem struct {
	MediaID      string    `json:"media_id"`
	UserID       string    `json:"user_id"`
	S3Key        string    `json:"s3_key"`
	ThumbnailURL string    `json:"thumbnail_url"`
	CreatedAt    time.Time `json:"created_at"`
}

type FeedResponse struct {
	Items  []FeedItem `json:"items"`
	Limit  int        `json:"limit"`
	Offset int        `json:"offset"`
	Total  int        `json:"total"`
}
