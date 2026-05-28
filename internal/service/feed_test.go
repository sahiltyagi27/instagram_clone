package service

import (
	"context"
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
	// Use a test-specific key prefix by flushing the test keys on cleanup.
	t.Cleanup(func() {
		client.Del(context.Background(), "feed:user_123", "feed:other")
		_ = client.Close()
	})
	return NewFeedService(store.NewFeedStore(client))
}

func TestFeedServiceGetFeedSortsAndPaginates(t *testing.T) {
	feed := newTestFeedService(t)
	now := time.Now().UTC()

	feed.AddFeedItem(context.Background(), "user_123", model.FeedItem{MediaID: "old", UserID: "user_123", CreatedAt: now.Add(-time.Hour)})
	feed.AddFeedItem(context.Background(), "user_123", model.FeedItem{MediaID: "new", UserID: "user_123", CreatedAt: now})
	feed.AddFeedItem(context.Background(), "user_123", model.FeedItem{MediaID: "middle", UserID: "user_123", CreatedAt: now.Add(-time.Minute)})
	feed.AddFeedItem(context.Background(), "other", model.FeedItem{MediaID: "other", UserID: "other", CreatedAt: now.Add(time.Hour)})

	resp, err := feed.GetFeed(context.Background(), "user_123", 2, 1)
	if err != nil {
		t.Fatalf("GetFeed returned error: %v", err)
	}

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
	feed := newTestFeedService(t)
	feed.AddFeedItem(context.Background(), "user_123", model.FeedItem{MediaID: "media_1", UserID: "user_123", CreatedAt: time.Now().UTC()})

	defaulted, err := feed.GetFeed(context.Background(), "user_123", 0, -1)
	if err != nil {
		t.Fatalf("GetFeed returned error: %v", err)
	}
	if defaulted.Limit != 20 || defaulted.Offset != 0 || len(defaulted.Items) != 1 {
		t.Fatalf("defaulted response = %#v, want limit 20 offset 0 one item", defaulted)
	}

	pastEnd, err := feed.GetFeed(context.Background(), "user_123", 20, 10)
	if err != nil {
		t.Fatalf("GetFeed returned error: %v", err)
	}
	if len(pastEnd.Items) != 0 {
		t.Fatalf("past-end items = %v, want empty", pastEnd.Items)
	}
}

func TestFeedServiceConcurrentWrites(t *testing.T) {
	feed := newTestFeedService(t)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			feed.AddFeedItem(context.Background(), "user_123", model.FeedItem{UserID: "user_123", CreatedAt: time.Now().UTC()})
		}()
	}
	wg.Wait()

	resp, err := feed.GetFeed(context.Background(), "user_123", 100, 0)
	if err != nil {
		t.Fatalf("GetFeed returned error: %v", err)
	}
	if resp.Total != 50 {
		t.Fatalf("Total = %d, want 50", resp.Total)
	}
}
