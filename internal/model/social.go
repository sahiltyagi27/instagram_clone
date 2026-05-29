package model

import "time"

// MaxCommentLength matches the body column width in the comments table.
const MaxCommentLength = 2200

type Like struct {
	MediaID   string    `json:"media_id"`
	UserID    string    `json:"user_id"`
	CreatedAt time.Time `json:"created_at"`
}

// LikeStatusResponse reports the like count for a media item and whether the
// requesting user has liked it.
type LikeStatusResponse struct {
	MediaID string `json:"media_id"`
	Count   int    `json:"count"`
	Liked   bool   `json:"liked"`
}

type Comment struct {
	ID        string    `json:"id"`
	MediaID   string    `json:"media_id"`
	UserID    string    `json:"user_id"`
	Body      string    `json:"body"`
	CreatedAt time.Time `json:"created_at"`
}

type CreateCommentRequest struct {
	Body string `json:"body"`
}

type CommentListResponse struct {
	Comments []Comment `json:"comments"`
}
