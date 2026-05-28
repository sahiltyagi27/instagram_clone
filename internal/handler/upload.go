package handler

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	appkafka "instagram_clone/internal/kafka"
	"instagram_clone/internal/model"
	"instagram_clone/internal/service"

	"github.com/go-chi/chi/v5"
)

type UploadHandler struct {
	storage  *service.Storage
	producer *appkafka.KafkaProducer
}

func NewUploadHandler(storage *service.Storage, producer ...*appkafka.KafkaProducer) *UploadHandler {
	var p *appkafka.KafkaProducer
	if len(producer) > 0 {
		p = producer[0]
	}
	return &UploadHandler{storage: storage, producer: p}
}

func (h *UploadHandler) Router() http.Handler {
	r := chi.NewRouter()
	r.Post("/presigned-url", h.createPresignedURL)
	r.Post("/media/confirm", h.confirmMedia)
	r.Get("/media/{id}", h.getMedia)
	return r
}

func (h *UploadHandler) createPresignedURL(w http.ResponseWriter, r *http.Request) {
	var req model.PresignedURLRequest
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
	if !req.MediaType.Valid() {
		writeError(w, http.StatusBadRequest, "media_type must be photo or video")
		return
	}

	resp, err := h.storage.GeneratePresignedUploadURL(r.Context(), req)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to generate presigned upload URL")
		return
	}

	writeJSON(w, http.StatusCreated, resp)
}

func (h *UploadHandler) confirmMedia(w http.ResponseWriter, r *http.Request) {
	var req model.ConfirmMediaRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON request body")
		return
	}

	req.UserID = userIDFromRequest(r, req.UserID)
	req.MediaID = strings.TrimSpace(req.MediaID)
	if req.UserID == "" || req.MediaID == "" {
		writeError(w, http.StatusBadRequest, "user_id and media_id are required")
		return
	}

	media, err := h.storage.ConfirmMediaUploaded(r.Context(), req.UserID, req.MediaID)
	if err != nil {
		if errors.Is(err, service.ErrMediaNotFound) {
			writeError(w, http.StatusNotFound, "media not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to confirm media upload")
		return
	}

	if h.producer != nil {
		if err := h.producer.PublishMediaUploaded(r.Context(), media.ID, media.UserID, media.S3Key, string(media.Type)); err != nil {
			writeError(w, http.StatusInternalServerError, "failed to publish media upload event")
			return
		}
	}

	writeJSON(w, http.StatusOK, media)
}

func (h *UploadHandler) getMedia(w http.ResponseWriter, r *http.Request) {
	mediaID := strings.TrimSpace(chi.URLParam(r, "id"))
	if mediaID == "" {
		writeError(w, http.StatusBadRequest, "media id is required")
		return
	}

	media, err := h.storage.GetMedia(r.Context(), mediaID)
	if err != nil {
		if errors.Is(err, service.ErrMediaNotFound) {
			writeError(w, http.StatusNotFound, "media not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to fetch media")
		return
	}

	writeJSON(w, http.StatusOK, media)
}

func decodeJSON(r *http.Request, dest any) error {
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	return decoder.Decode(dest)
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, model.ErrorResponse{Error: message})
}
