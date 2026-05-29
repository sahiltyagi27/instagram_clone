package handler

import (
	"errors"
	"net/http"
	"strings"

	"instagram_clone/internal/service"

	"github.com/go-chi/chi/v5"
)

type LikeHandler struct {
	likes *service.LikeService
}

func NewLikeHandler(likes *service.LikeService) *LikeHandler {
	return &LikeHandler{likes: likes}
}

func (h *LikeHandler) Router() http.Handler {
	r := chi.NewRouter()
	r.Put("/{media_id}", h.like)
	r.Delete("/{media_id}", h.unlike)
	r.Get("/{media_id}", h.status)
	return r
}

func (h *LikeHandler) like(w http.ResponseWriter, r *http.Request) {
	userID, ok := userIDFromRequest(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "missing authenticated user")
		return
	}
	mediaID := strings.TrimSpace(chi.URLParam(r, "media_id"))
	if mediaID == "" {
		writeError(w, http.StatusBadRequest, "media id is required")
		return
	}

	if err := h.likes.Like(r.Context(), mediaID, userID); err != nil {
		if errors.Is(err, service.ErrMediaNotFound) {
			writeError(w, http.StatusNotFound, "media not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to like media")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *LikeHandler) unlike(w http.ResponseWriter, r *http.Request) {
	userID, ok := userIDFromRequest(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "missing authenticated user")
		return
	}
	mediaID := strings.TrimSpace(chi.URLParam(r, "media_id"))
	if mediaID == "" {
		writeError(w, http.StatusBadRequest, "media id is required")
		return
	}

	if err := h.likes.Unlike(r.Context(), mediaID, userID); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to unlike media")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *LikeHandler) status(w http.ResponseWriter, r *http.Request) {
	userID, ok := userIDFromRequest(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "missing authenticated user")
		return
	}
	mediaID := strings.TrimSpace(chi.URLParam(r, "media_id"))
	if mediaID == "" {
		writeError(w, http.StatusBadRequest, "media id is required")
		return
	}

	resp, err := h.likes.Status(r.Context(), mediaID, userID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to fetch like status")
		return
	}
	writeJSON(w, http.StatusOK, resp)
}
