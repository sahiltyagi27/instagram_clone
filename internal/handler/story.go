package handler

import (
	"errors"
	"net/http"
	"strings"

	appkafka "instagram_clone/internal/kafka"
	"instagram_clone/internal/middleware"
	"instagram_clone/internal/model"
	"instagram_clone/internal/service"

	"github.com/go-chi/chi/v5"
)

type StoryHandler struct {
	stories  *service.StoryService
	producer *appkafka.KafkaProducer
}

func NewStoryHandler(stories *service.StoryService, producer ...*appkafka.KafkaProducer) *StoryHandler {
	var p *appkafka.KafkaProducer
	if len(producer) > 0 {
		p = producer[0]
	}
	return &StoryHandler{stories: stories, producer: p}
}

func (h *StoryHandler) Router() http.Handler {
	r := chi.NewRouter()
	r.Post("/presigned-url", h.createPresignedURL)
	r.Post("/confirm", h.confirmStory)
	r.Get("/{id}", h.getStory)
	r.Get("/user/{user_id}", h.getStoriesByUser)
	return r
}

func (h *StoryHandler) createPresignedURL(w http.ResponseWriter, r *http.Request) {
	var req model.StoryPresignedURLRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON request body")
		return
	}

	req.UserID = userIDFromRequest(r, req.UserID)
	req.FileName = strings.TrimSpace(req.FileName)
	req.ContentType = strings.TrimSpace(req.ContentType)
	if req.UserID == "" || req.FileName == "" || req.ContentType == "" {
		writeError(w, http.StatusBadRequest, "user_id, file_name, and content_type are required")
		return
	}

	resp, err := h.stories.GeneratePresignedURL(r.Context(), req)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to generate story presigned upload URL")
		return
	}

	writeJSON(w, http.StatusCreated, resp)
}

func (h *StoryHandler) confirmStory(w http.ResponseWriter, r *http.Request) {
	var req model.ConfirmStoryRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON request body")
		return
	}

	req.UserID = userIDFromRequest(r, req.UserID)
	req.StoryID = strings.TrimSpace(req.StoryID)
	if req.UserID == "" || req.StoryID == "" {
		writeError(w, http.StatusBadRequest, "user_id and story_id are required")
		return
	}

	story, err := h.stories.ConfirmUpload(r.Context(), req.UserID, req.StoryID)
	if err != nil {
		if errors.Is(err, service.ErrStoryNotFound) {
			writeError(w, http.StatusNotFound, "story not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to confirm story upload")
		return
	}

	if h.producer != nil {
		if err := h.producer.PublishStoryUploaded(r.Context(), story.ID, story.UserID, story.S3Key); err != nil {
			writeError(w, http.StatusInternalServerError, "failed to publish story upload event")
			return
		}
	}

	writeJSON(w, http.StatusOK, story)
}

func (h *StoryHandler) getStory(w http.ResponseWriter, r *http.Request) {
	storyID := strings.TrimSpace(chi.URLParam(r, "id"))
	if storyID == "" {
		writeError(w, http.StatusBadRequest, "story id is required")
		return
	}

	story, err := h.stories.GetStory(r.Context(), storyID)
	if err != nil {
		if errors.Is(err, service.ErrStoryNotFound) {
			writeError(w, http.StatusNotFound, "story not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to fetch story")
		return
	}

	writeJSON(w, http.StatusOK, story)
}

func (h *StoryHandler) getStoriesByUser(w http.ResponseWriter, r *http.Request) {
	userID := strings.TrimSpace(chi.URLParam(r, "user_id"))
	if userID == "" {
		writeError(w, http.StatusBadRequest, "user id is required")
		return
	}

	writeJSON(w, http.StatusOK, h.stories.GetActiveStoriesByUser(r.Context(), userID))
}

func userIDFromRequest(r *http.Request, fallback string) string {
	if userID, ok := middleware.UserIDFromContext(r.Context()); ok {
		return userID
	}
	return strings.TrimSpace(fallback)
}
