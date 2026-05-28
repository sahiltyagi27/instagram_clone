package store

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"strconv"

	"instagram_clone/internal/model"

	"github.com/redis/go-redis/v9"
)

const feedKeyPrefix = "feed:"
const feedMaxItems = 1000

// ErrInvalidCursor is returned when the caller supplies a cursor that cannot
// be decoded. The handler should surface this as 400 Bad Request.
var ErrInvalidCursor = errors.New("invalid cursor")

// feedCursor is the compound pagination token. Encoding both the Redis score
// and the MediaID means the cursor uniquely identifies a position even when
// two items share the same score (same-millisecond uploads with a colliding
// crc32 suffix).
type feedCursor struct {
	Score   int64  `json:"s"`
	MediaID string `json:"m"`
}

func encodeFeedCursor(score int64, mediaID string) string {
	b, _ := json.Marshal(feedCursor{Score: score, MediaID: mediaID})
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeFeedCursor(s string) (feedCursor, error) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return feedCursor{}, ErrInvalidCursor
	}
	var c feedCursor
	if err := json.Unmarshal(b, &c); err != nil {
		return feedCursor{}, ErrInvalidCursor
	}
	return c, nil
}

type FeedStore struct {
	client *redis.Client
}

func NewFeedStore(client *redis.Client) *FeedStore {
	return &FeedStore{client: client}
}

// itemScore converts a FeedItem into a Redis sorted-set score.
//
// Base: CreatedAt.UnixMilli() — newest items have the highest score.
// Tie-breaker: a 3-digit suffix derived from a CRC32 of the MediaID.
// This makes same-millisecond scores distinct per MediaID with high
// probability (~1/1000 chance of collision), reducing how often the
// two-query same-score code path in GetFeed is needed. The compound
// cursor (score + MediaID) handles the remaining edge cases correctly.
func itemScore(item model.FeedItem) float64 {
	suffix := int64(crc32.ChecksumIEEE([]byte(item.MediaID)) % 1000)
	return float64(item.CreatedAt.UnixMilli()*1000 + suffix)
}

func (s *FeedStore) AddItem(ctx context.Context, userID string, item model.FeedItem) error {
	data, err := json.Marshal(item)
	if err != nil {
		return fmt.Errorf("marshal feed item: %w", err)
	}

	key := feedKeyPrefix + userID
	score := itemScore(item)

	pipe := s.client.Pipeline()
	pipe.ZAdd(ctx, key, redis.Z{Score: score, Member: string(data)})
	// Keep only the most recent 1000 items — remove everything beyond rank 999.
	pipe.ZRemRangeByRank(ctx, key, 0, -(feedMaxItems + 1))
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("add feed item: %w", err)
	}
	return nil
}

// GetFeed returns up to limit items starting after the position identified by
// cursor. An empty cursor returns the newest items first.
//
// The cursor is a base64-encoded compound token (score + mediaID) that uniquely
// identifies a position even when multiple items share the same score. Passing
// the NextCursor from one response as the cursor of the next call pages through
// the feed without gaps or duplicates. An empty NextCursor means no more pages.
func (s *FeedStore) GetFeed(ctx context.Context, userID string, limit int, cursor string) (model.FeedResponse, error) {
	if limit <= 0 {
		limit = 20
	}

	key := feedKeyPrefix + userID

	type scoredItem struct {
		score int64
		item  model.FeedItem
	}
	var collected []scoredItem

	if cursor == "" {
		// First page: fetch the newest limit+1 items.
		results, err := s.client.ZRangeArgsWithScores(ctx, redis.ZRangeArgs{
			Key:     key,
			Start:   "+inf",
			Stop:    "-inf",
			ByScore: true,
			Rev:     true,
			Count:   int64(limit + 1),
		}).Result()
		if err != nil {
			return model.FeedResponse{}, fmt.Errorf("zrangebyscore feed: %w", err)
		}
		for _, z := range results {
			var item model.FeedItem
			if err := json.Unmarshal([]byte(z.Member.(string)), &item); err != nil {
				return model.FeedResponse{}, fmt.Errorf("unmarshal feed item: %w", err)
			}
			collected = append(collected, scoredItem{int64(z.Score), item})
		}
	} else {
		cur, err := decodeFeedCursor(cursor)
		if err != nil {
			return model.FeedResponse{}, err // ErrInvalidCursor
		}

		scoreStr := strconv.FormatInt(cur.Score, 10)

		// ── Part A: same-score items that follow the cursor item ─────────────
		// ZRangeArgs (no Rev) returns same-score members in ascending lex order.
		// ZREVRANGEBYSCORE (Rev=true) returns them in DESCENDING lex order, so
		// we reverse the slice to reconstruct the exact iteration order, then
		// skip everything up to and including the cursor item.
		sameRaw, err := s.client.ZRangeArgs(ctx, redis.ZRangeArgs{
			Key:     key,
			Start:   scoreStr,
			Stop:    scoreStr,
			ByScore: true,
		}).Result()
		if err != nil {
			return model.FeedResponse{}, fmt.Errorf("zrangebyscore same-score: %w", err)
		}
		for i, j := 0, len(sameRaw)-1; i < j; i, j = i+1, j-1 {
			sameRaw[i], sameRaw[j] = sameRaw[j], sameRaw[i]
		}
		pastCursor := false
		for _, raw := range sameRaw {
			var item model.FeedItem
			if err := json.Unmarshal([]byte(raw), &item); err != nil {
				continue
			}
			if !pastCursor {
				if item.MediaID == cur.MediaID {
					pastCursor = true
				}
				continue
			}
			collected = append(collected, scoredItem{cur.Score, item})
			if len(collected) >= limit+1 {
				break
			}
		}

		// ── Part B: items with score strictly below the cursor ───────────────
		// Only fetch as many as still needed to fill limit+1.
		if len(collected) < limit+1 {
			results, err := s.client.ZRangeArgsWithScores(ctx, redis.ZRangeArgs{
				Key:     key,
				Start:   fmt.Sprintf("(%s", scoreStr), // exclusive
				Stop:    "-inf",
				ByScore: true,
				Rev:     true,
				Count:   int64(limit + 1 - len(collected)),
			}).Result()
			if err != nil {
				return model.FeedResponse{}, fmt.Errorf("zrangebyscore below-cursor: %w", err)
			}
			for _, z := range results {
				var item model.FeedItem
				if err := json.Unmarshal([]byte(z.Member.(string)), &item); err != nil {
					return model.FeedResponse{}, fmt.Errorf("unmarshal feed item: %w", err)
				}
				collected = append(collected, scoredItem{int64(z.Score), item})
			}
		}
	}

	// If we collected more than limit items there is a next page.
	var nextCursor string
	if len(collected) > limit {
		last := collected[limit-1]
		nextCursor = encodeFeedCursor(last.score, last.item.MediaID)
		collected = collected[:limit]
	}

	items := make([]model.FeedItem, 0, len(collected))
	for _, si := range collected {
		items = append(items, si.item)
	}

	return model.FeedResponse{
		Items:      items,
		Limit:      limit,
		NextCursor: nextCursor,
	}, nil
}
