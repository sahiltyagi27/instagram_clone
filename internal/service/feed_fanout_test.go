package service

import (
	"context"
	"testing"
	"time"

	"instagram_clone/internal/model"
	"instagram_clone/internal/store"

	"github.com/redis/go-redis/v9"
)

// stubFollowerLister returns a fixed follower list, letting the fan-out logic be
// tested without a Postgres-backed FollowStore.
type stubFollowerLister struct {
	followers []string
	err       error
}

func (s stubFollowerLister) GetFollowers(context.Context, string) ([]string, error) {
	return s.followers, s.err
}

func newFanoutFeedService(t *testing.T, follows FollowerLister) (*FeedService, *redis.Client) {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	if client.Ping(context.Background()).Err() != nil {
		_ = client.Close()
		t.Skip("redis unavailable, skipping")
	}
	t.Cleanup(func() {
		client.Del(context.Background(), "feed:author", "feed:follower_a", "feed:follower_b")
		_ = client.Close()
	})
	return NewFeedService(store.NewFeedStore(client), follows), client
}

func TestFanoutFeedItemWritesToFollowersAndAuthor(t *testing.T) {
	feed, _ := newFanoutFeedService(t, stubFollowerLister{followers: []string{"follower_a", "follower_b"}})
	ctx := context.Background()

	item := model.FeedItem{MediaID: "media_1", UserID: "author", CreatedAt: time.Now().UTC()}
	if err := feed.FanoutFeedItem(ctx, "author", item); err != nil {
		t.Fatalf("FanoutFeedItem returned error: %v", err)
	}

	for _, uid := range []string{"author", "follower_a", "follower_b"} {
		resp, err := feed.GetFeed(ctx, uid, 10, "")
		if err != nil {
			t.Fatalf("GetFeed(%s) returned error: %v", uid, err)
		}
		if len(resp.Items) != 1 || resp.Items[0].MediaID != "media_1" {
			t.Fatalf("feed for %s = %v, want one item media_1", uid, mediaIDs(resp.Items))
		}
	}
}

func TestFanoutFeedItemNilListerWritesAuthorOnly(t *testing.T) {
	feed, _ := newFanoutFeedService(t, nil)
	ctx := context.Background()

	item := model.FeedItem{MediaID: "media_1", UserID: "author", CreatedAt: time.Now().UTC()}
	if err := feed.FanoutFeedItem(ctx, "author", item); err != nil {
		t.Fatalf("FanoutFeedItem returned error: %v", err)
	}

	authorFeed, err := feed.GetFeed(ctx, "author", 10, "")
	if err != nil {
		t.Fatalf("GetFeed(author) returned error: %v", err)
	}
	if len(authorFeed.Items) != 1 {
		t.Fatalf("author feed = %v, want one item", mediaIDs(authorFeed.Items))
	}

	followerFeed, err := feed.GetFeed(ctx, "follower_a", 10, "")
	if err != nil {
		t.Fatalf("GetFeed(follower_a) returned error: %v", err)
	}
	if len(followerFeed.Items) != 0 {
		t.Fatalf("follower feed = %v, want empty (fan-out disabled)", mediaIDs(followerFeed.Items))
	}
}
