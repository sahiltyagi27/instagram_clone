package handler

import (
	"errors"
	"net/http"
	"strings"

	"instagram_clone/internal/model"
	"instagram_clone/internal/service"

	"github.com/go-chi/chi/v5"
)

type AuthHandler struct {
	auth *service.AuthService
}

func NewAuthHandler(auth *service.AuthService) *AuthHandler {
	return &AuthHandler{auth: auth}
}

func (h *AuthHandler) Router() http.Handler {
	r := chi.NewRouter()
	r.Post("/signup", h.signup)
	r.Post("/login", h.login)
	return r
}

func (h *AuthHandler) signup(w http.ResponseWriter, r *http.Request) {
	var req model.SignupRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON request body")
		return
	}

	if strings.TrimSpace(req.Username) == "" || strings.TrimSpace(req.Email) == "" || req.Password == "" {
		writeError(w, http.StatusBadRequest, "username, email, and password are required")
		return
	}

	resp, err := h.auth.Signup(req)
	if err != nil {
		if errors.Is(err, service.ErrUserAlreadyExists) {
			writeError(w, http.StatusConflict, "user already exists")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to signup")
		return
	}

	writeJSON(w, http.StatusCreated, resp)
}

func (h *AuthHandler) login(w http.ResponseWriter, r *http.Request) {
	var req model.LoginRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON request body")
		return
	}

	req.Email = strings.TrimSpace(req.Email)
	if req.Email == "" || req.Password == "" {
		writeError(w, http.StatusBadRequest, "email and password are required")
		return
	}

	resp, err := h.auth.Login(req)
	if err != nil {
		if errors.Is(err, service.ErrInvalidCredentials) {
			writeError(w, http.StatusUnauthorized, "invalid credentials")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to login")
		return
	}

	writeJSON(w, http.StatusOK, resp)
}
