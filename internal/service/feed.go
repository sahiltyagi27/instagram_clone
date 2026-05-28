package service

import (
	"sort"
	"sync"

	"instagram_clone/internal/model"
)

type FeedService struct {
	mu    sync.RWMutex
	items map[string][]model.FeedItem
}

func NewFeedService() *FeedService {
	return &FeedService{items: make(map[string][]model.FeedItem)}
}

func (s *FeedService) AddFeedItem(userID string, item model.FeedItem) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.items[userID] = append(s.items[userID], item)
}

func (s *FeedService) GetFeed(userID string, limit, offset int) model.FeedResponse {
	if limit <= 0 {
		limit = 20
	}
	if offset < 0 {
		offset = 0
	}

	s.mu.RLock()
	items := append([]model.FeedItem(nil), s.items[userID]...)
	s.mu.RUnlock()

	sort.Slice(items, func(i, j int) bool {
		return items[i].CreatedAt.After(items[j].CreatedAt)
	})

	total := len(items)
	if offset > total {
		offset = total
	}
	end := offset + limit
	if end > total {
		end = total
	}

	return model.FeedResponse{
		Items:  items[offset:end],
		Limit:  limit,
		Offset: offset,
		Total:  total,
	}
}
