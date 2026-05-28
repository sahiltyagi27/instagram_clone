package service

import (
	"sync"
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

func TestFeedServicePaginationBoundaries(t *testing.T) {
	feed := NewFeedService()
	feed.AddFeedItem("user_123", model.FeedItem{MediaID: "media_1", UserID: "user_123", CreatedAt: time.Now().UTC()})

	defaulted := feed.GetFeed("user_123", 0, -1)
	if defaulted.Limit != 20 || defaulted.Offset != 0 || len(defaulted.Items) != 1 {
		t.Fatalf("defaulted response = %#v, want limit 20 offset 0 one item", defaulted)
	}

	pastEnd := feed.GetFeed("user_123", 20, 10)
	if pastEnd.Offset != 1 || len(pastEnd.Items) != 0 {
		t.Fatalf("past-end response = %#v, want offset clamped to total and no items", pastEnd)
	}
}

func TestFeedServiceConcurrentWrites(t *testing.T) {
	feed := NewFeedService()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			feed.AddFeedItem("user_123", model.FeedItem{UserID: "user_123", CreatedAt: time.Now().UTC()})
		}()
	}
	wg.Wait()

	resp := feed.GetFeed("user_123", 100, 0)
	if resp.Total != 50 {
		t.Fatalf("Total = %d, want 50", resp.Total)
	}
}
