package model

import "time"

type Follow struct {
	FollowerID string    `json:"follower_id"`
	FolloweeID string    `json:"followee_id"`
	CreatedAt  time.Time `json:"created_at"`
}

// FollowListResponse is returned by the followers/following endpoints.
type FollowListResponse struct {
	UserIDs []string `json:"user_ids"`
	Count   int      `json:"count"`
}
