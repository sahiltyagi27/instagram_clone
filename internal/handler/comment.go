package handler

import (
	"errors"
	"net/http"
	"strings"

	"instagram_clone/internal/model"
	"instagram_clone/internal/service"

	"github.com/go-chi/chi/v5"
)

type CommentHandler struct {
	comments *service.CommentService
}

func NewCommentHandler(comments *service.CommentService) *CommentHandler {
	return &CommentHandler{comments: comments}
}

func (h *CommentHandler) Router() http.Handler {
	r := chi.NewRouter()
	r.Post("/{media_id}", h.add)
	r.Get("/{media_id}", h.list)
	r.Delete("/{media_id}/{comment_id}", h.delete)
	return r
}

func (h *CommentHandler) add(w http.ResponseWriter, r *http.Request) {
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

	var req model.CreateCommentRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON request body")
		return
	}

	comment, err := h.comments.Add(r.Context(), mediaID, userID, req.Body)
	if err != nil {
		switch {
		case errors.Is(err, service.ErrEmptyComment):
			writeError(w, http.StatusBadRequest, "comment body must not be empty")
		case errors.Is(err, service.ErrCommentTooLong):
			writeError(w, http.StatusBadRequest, "comment body is too long")
		case errors.Is(err, service.ErrMediaNotFound):
			writeError(w, http.StatusNotFound, "media not found")
		default:
			writeError(w, http.StatusInternalServerError, "failed to add comment")
		}
		return
	}
	writeJSON(w, http.StatusCreated, comment)
}

func (h *CommentHandler) list(w http.ResponseWriter, r *http.Request) {
	mediaID := strings.TrimSpace(chi.URLParam(r, "media_id"))
	if mediaID == "" {
		writeError(w, http.StatusBadRequest, "media id is required")
		return
	}

	limit := queryInt(r, "limit", 0)
	resp, err := h.comments.List(r.Context(), mediaID, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list comments")
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h *CommentHandler) delete(w http.ResponseWriter, r *http.Request) {
	userID, ok := userIDFromRequest(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "missing authenticated user")
		return
	}
	commentID := strings.TrimSpace(chi.URLParam(r, "comment_id"))
	if commentID == "" {
		writeError(w, http.StatusBadRequest, "comment id is required")
		return
	}

	if err := h.comments.Delete(r.Context(), commentID, userID); err != nil {
		if errors.Is(err, service.ErrCommentNotFound) {
			writeError(w, http.StatusNotFound, "comment not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to delete comment")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
