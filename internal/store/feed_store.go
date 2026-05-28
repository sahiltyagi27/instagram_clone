package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

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

// GetFeed returns up to limit items newer than (but not including) the item
// identified by cursor. An empty cursor starts from the newest item.
// The cursor in the response is the score of the last returned item; pass it
// back on the next call to get the following page. An empty NextCursor means
// there are no more items.
func (s *FeedStore) GetFeed(ctx context.Context, userID string, limit int, cursor string) (model.FeedResponse, error) {
	if limit <= 0 {
		limit = 20
	}

	key := feedKeyPrefix + userID

	// Build the upper-bound score for ZREVRANGEBYSCORE.
	// An empty cursor starts at +inf (newest item first).
	// A non-empty cursor is used as an exclusive lower bound on the next page:
	// the "(" prefix tells Redis to exclude that exact score.
	var maxScore string
	if cursor == "" {
		maxScore = "+inf"
	} else {
		maxScore = "(" + cursor
	}

	// Fetch one extra item so we can detect whether a next page exists.
	results, err := s.client.ZRangeArgsWithScores(ctx, redis.ZRangeArgs{
		Key:     key,
		Start:   maxScore, // highest score (newest) when Rev=true, ByScore=true
		Stop:    "-inf",
		ByScore: true,
		Rev:     true,
		Count:   int64(limit + 1),
	}).Result()
	if err != nil {
		return model.FeedResponse{}, fmt.Errorf("zrangebyscore feed: %w", err)
	}

	var nextCursor string
	if len(results) > limit {
		// More pages exist — set cursor to the score of the last item we'll return.
		nextCursor = strconv.FormatInt(int64(results[limit-1].Score), 10)
		results = results[:limit]
	}

	items := make([]model.FeedItem, 0, len(results))
	for _, z := range results {
		var item model.FeedItem
		if err := json.Unmarshal([]byte(z.Member.(string)), &item); err != nil {
			return model.FeedResponse{}, fmt.Errorf("unmarshal feed item: %w", err)
		}
		items = append(items, item)
	}

	return model.FeedResponse{
		Items:      items,
		Limit:      limit,
		NextCursor: nextCursor,
	}, nil
}
