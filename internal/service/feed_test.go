package service

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"instagram_clone/internal/model"
	"instagram_clone/internal/store"

	"github.com/redis/go-redis/v9"
)

func newTestFeedService(t *testing.T) *FeedService {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	if client.Ping(context.Background()).Err() != nil {
		_ = client.Close()
		t.Skip("redis unavailable, skipping")
	}
	t.Cleanup(func() {
		client.Del(context.Background(), "feed:svc_user_123", "feed:svc_other")
		_ = client.Close()
	})
	return NewFeedService(store.NewFeedStore(client), nil)
}

func TestFeedServiceGetFeedSortsAndPaginates(t *testing.T) {
	feed := newTestFeedService(t)
	ctx := context.Background()
	now := time.Now().UTC()

	feed.AddFeedItem(ctx, "svc_user_123", model.FeedItem{MediaID: "old", UserID: "svc_user_123", CreatedAt: now.Add(-time.Hour)})
	feed.AddFeedItem(ctx, "svc_user_123", model.FeedItem{MediaID: "new", UserID: "svc_user_123", CreatedAt: now})
	feed.AddFeedItem(ctx, "svc_user_123", model.FeedItem{MediaID: "middle", UserID: "svc_user_123", CreatedAt: now.Add(-time.Minute)})
	feed.AddFeedItem(ctx, "svc_other", model.FeedItem{MediaID: "other", UserID: "svc_other", CreatedAt: now.Add(time.Hour)})

	// First page: limit=2 → newest two items ("new", "middle").
	page1, err := feed.GetFeed(ctx, "svc_user_123", 2, "")
	if err != nil {
		t.Fatalf("GetFeed page1 returned error: %v", err)
	}
	if len(page1.Items) != 2 {
		t.Fatalf("page1 items = %d, want 2", len(page1.Items))
	}
	if page1.Items[0].MediaID != "new" || page1.Items[1].MediaID != "middle" {
		t.Fatalf("page1 items = %v, want [new middle]", mediaIDs(page1.Items))
	}
	if page1.NextCursor == "" {
		t.Fatal("expected NextCursor to be set after page1")
	}

	// Second page using cursor → should return the remaining item ("old").
	page2, err := feed.GetFeed(ctx, "svc_user_123", 2, page1.NextCursor)
	if err != nil {
		t.Fatalf("GetFeed page2 returned error: %v", err)
	}
	if len(page2.Items) != 1 {
		t.Fatalf("page2 items = %d, want 1", len(page2.Items))
	}
	if page2.Items[0].MediaID != "old" {
		t.Fatalf("page2 items = %v, want [old]", mediaIDs(page2.Items))
	}
	if page2.NextCursor != "" {
		t.Fatalf("page2 NextCursor = %q, want empty (no more pages)", page2.NextCursor)
	}
}

func TestFeedServicePaginationBoundaries(t *testing.T) {
	feed := newTestFeedService(t)
	ctx := context.Background()

	feed.AddFeedItem(ctx, "svc_user_123", model.FeedItem{MediaID: "media_1", UserID: "svc_user_123", CreatedAt: time.Now().UTC()})

	// Default limit applied when limit=0.
	defaulted, err := feed.GetFeed(ctx, "svc_user_123", 0, "")
	if err != nil {
		t.Fatalf("GetFeed returned error: %v", err)
	}
	if defaulted.Limit != 20 || len(defaulted.Items) != 1 {
		t.Fatalf("defaulted response = %#v, want limit 20 and one item", defaulted)
	}

	// Past the end: cursor from the only item → next page is empty.
	if defaulted.NextCursor != "" {
		t.Fatalf("NextCursor = %q, want empty for single-page result", defaulted.NextCursor)
	}
}

func TestFeedServiceConcurrentWrites(t *testing.T) {
	feed := newTestFeedService(t)
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := range 50 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			// Use a unique MediaID per item so the crc32 tie-breaker gives each
			// concurrent upload a distinct score even within the same millisecond.
			feed.AddFeedItem(ctx, "svc_user_123", model.FeedItem{
				MediaID:   fmt.Sprintf("media_%d", n),
				UserID:    "svc_user_123",
				CreatedAt: time.Now().UTC(),
			})
		}(i)
	}
	wg.Wait()

	resp, err := feed.GetFeed(ctx, "svc_user_123", 100, "")
	if err != nil {
		t.Fatalf("GetFeed returned error: %v", err)
	}
	if len(resp.Items) != 50 {
		t.Fatalf("items = %d, want 50", len(resp.Items))
	}
}

func mediaIDs(items []model.FeedItem) []string {
	ids := make([]string, len(items))
	for i, item := range items {
		ids[i] = item.MediaID
	}
	return ids
}
