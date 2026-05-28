package handler

import (
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"instagram_clone/internal/service"

	"github.com/go-chi/chi/v5"
)

type FeedHandler struct {
	feed *service.FeedService
}

func NewFeedHandler(feed *service.FeedService) *FeedHandler {
	return &FeedHandler{feed: feed}
}

func (h *FeedHandler) Router() http.Handler {
	r := chi.NewRouter()
	r.Get("/{user_id}", h.getFeed)
	return r
}

func (h *FeedHandler) getFeed(w http.ResponseWriter, r *http.Request) {
	userID := strings.TrimSpace(chi.URLParam(r, "user_id"))
	if userID == "" {
		writeError(w, http.StatusBadRequest, "user id is required")
		return
	}
	if !requestUserMatches(r, userID) {
		writeError(w, http.StatusForbidden, "cannot access another user's feed")
		return
	}

	limit := queryInt(r, "limit", 20)
	cursor := strings.TrimSpace(r.URL.Query().Get("cursor"))

	feed, err := h.feed.GetFeed(r.Context(), userID, limit, cursor)
	if err != nil {
		slog.ErrorContext(r.Context(), "get feed", "user_id", userID, "error", err)
		writeError(w, http.StatusServiceUnavailable, "feed temporarily unavailable")
		return
	}
	writeJSON(w, http.StatusOK, feed)
}

func queryInt(r *http.Request, key string, fallback int) int {
	value := strings.TrimSpace(r.URL.Query().Get(key))
	if value == "" {
		return fallback
	}

	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}
