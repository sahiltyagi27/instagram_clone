package store

import (
	"context"
	"encoding/json"
	"fmt"

	"instagram_clone/internal/model"

	"github.com/redis/go-redis/v9"
)

const feedKeyPrefix = "feed:"
const feedMaxItems = 1000

type FeedStore struct {
	client *redis.Client
}

func NewFeedStore(client *redis.Client) *FeedStore {
	return &FeedStore{client: client}
}

func (s *FeedStore) AddItem(ctx context.Context, userID string, item model.FeedItem) error {
	data, err := json.Marshal(item)
	if err != nil {
		return fmt.Errorf("marshal feed item: %w", err)
	}

	key := feedKeyPrefix + userID
	score := float64(item.CreatedAt.UnixMilli())

	pipe := s.client.Pipeline()
	pipe.ZAdd(ctx, key, redis.Z{Score: score, Member: string(data)})
	// Keep only the most recent 1000 items — remove everything beyond rank 999 from the high end.
	pipe.ZRemRangeByRank(ctx, key, 0, -(feedMaxItems + 1))
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("add feed item: %w", err)
	}
	return nil
}

func (s *FeedStore) GetFeed(ctx context.Context, userID string, limit, offset int) (model.FeedResponse, error) {
	if limit <= 0 {
		limit = 20
	}
	if offset < 0 {
		offset = 0
	}

	key := feedKeyPrefix + userID

	total, err := s.client.ZCard(ctx, key).Result()
	if err != nil {
		return model.FeedResponse{}, fmt.Errorf("zcard feed: %w", err)
	}

	// ZREVRANGE returns members from highest score (newest) to lowest (oldest).
	results, err := s.client.ZRevRange(ctx, key, int64(offset), int64(offset+limit-1)).Result()
	if err != nil {
		return model.FeedResponse{}, fmt.Errorf("zrevrange feed: %w", err)
	}

	items := make([]model.FeedItem, 0, len(results))
	for _, raw := range results {
		var item model.FeedItem
		if err := json.Unmarshal([]byte(raw), &item); err != nil {
			return model.FeedResponse{}, fmt.Errorf("unmarshal feed item: %w", err)
		}
		items = append(items, item)
	}

	return model.FeedResponse{
		Items:  items,
		Limit:  limit,
		Offset: offset,
		Total:  int(total),
	}, nil
}
