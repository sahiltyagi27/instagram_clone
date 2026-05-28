package model

import "time"

type FeedItem struct {
	MediaID      string `json:"media_id"`
	UserID       string `json:"user_id"`
	S3Key        string `json:"s3_key"`
	ThumbnailKey string `json:"thumbnail_key"`
	// TODO: populate ThumbnailURL with a CDN or public HTTP URL once a serving strategy is in place.
	ThumbnailURL string    `json:"thumbnail_url,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
}

type FeedResponse struct {
	Items      []FeedItem `json:"items"`
	Limit      int        `json:"limit"`
	NextCursor string     `json:"next_cursor,omitempty"` // empty = no more pages
}
