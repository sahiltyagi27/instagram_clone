package service

import (
	"testing"
	"time"

	"instagram_clone/internal/model"
)

func TestFeedServiceGetFeedSortsAndPaginates(t *testing.T) {
	feed := NewFeedService()
	now := time.Now().UTC()

	feed.AddFeedItem("user_123", model.FeedItem{MediaID: "old", UserID: "user_123", CreatedAt: now.Add(-time.Hour)})
	feed.AddFeedItem("user_123", model.FeedItem{MediaID: "new", UserID: "user_123", CreatedAt: now})
	feed.AddFeedItem("user_123", model.FeedItem{MediaID: "middle", UserID: "user_123", CreatedAt: now.Add(-time.Minute)})
	feed.AddFeedItem("other", model.FeedItem{MediaID: "other", UserID: "other", CreatedAt: now.Add(time.Hour)})

	resp := feed.GetFeed("user_123", 2, 1)

	if resp.Total != 3 {
		t.Fatalf("Total = %d, want 3", resp.Total)
	}
	if len(resp.Items) != 2 {
		t.Fatalf("items length = %d, want 2", len(resp.Items))
	}
	if resp.Items[0].MediaID != "middle" || resp.Items[1].MediaID != "old" {
		t.Fatalf("items = %#v, want middle then old", resp.Items)
	}
}
