package handler

import (
	"errors"
	"net/http"
	"strings"

	"instagram_clone/internal/service"

	"github.com/go-chi/chi/v5"
)

type FollowHandler struct {
	follows *service.FollowService
}

func NewFollowHandler(follows *service.FollowService) *FollowHandler {
	return &FollowHandler{follows: follows}
}

func (h *FollowHandler) Router() http.Handler {
	r := chi.NewRouter()
	r.Post("/{user_id}", h.follow)
	r.Delete("/{user_id}", h.unfollow)
	r.Get("/{user_id}/followers", h.followers)
	r.Get("/{user_id}/following", h.following)
	return r
}

func (h *FollowHandler) follow(w http.ResponseWriter, r *http.Request) {
	followerID, ok := userIDFromRequest(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "missing authenticated user")
		return
	}
	followeeID := strings.TrimSpace(chi.URLParam(r, "user_id"))
	if followeeID == "" {
		writeError(w, http.StatusBadRequest, "user id is required")
		return
	}

	if err := h.follows.Follow(r.Context(), followerID, followeeID); err != nil {
		switch {
		case errors.Is(err, service.ErrSelfFollow):
			writeError(w, http.StatusBadRequest, "cannot follow yourself")
		case errors.Is(err, service.ErrUserNotFound):
			writeError(w, http.StatusNotFound, "user not found")
		default:
			writeError(w, http.StatusInternalServerError, "failed to follow user")
		}
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *FollowHandler) unfollow(w http.ResponseWriter, r *http.Request) {
	followerID, ok := userIDFromRequest(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "missing authenticated user")
		return
	}
	followeeID := strings.TrimSpace(chi.URLParam(r, "user_id"))
	if followeeID == "" {
		writeError(w, http.StatusBadRequest, "user id is required")
		return
	}

	if err := h.follows.Unfollow(r.Context(), followerID, followeeID); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to unfollow user")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *FollowHandler) followers(w http.ResponseWriter, r *http.Request) {
	userID := strings.TrimSpace(chi.URLParam(r, "user_id"))
	if userID == "" {
		writeError(w, http.StatusBadRequest, "user id is required")
		return
	}

	resp, err := h.follows.Followers(r.Context(), userID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list followers")
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h *FollowHandler) following(w http.ResponseWriter, r *http.Request) {
	userID := strings.TrimSpace(chi.URLParam(r, "user_id"))
	if userID == "" {
		writeError(w, http.StatusBadRequest, "user id is required")
		return
	}

	resp, err := h.follows.Following(r.Context(), userID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list following")
		return
	}
	writeJSON(w, http.StatusOK, resp)
}
