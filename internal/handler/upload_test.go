package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"instagram_clone/internal/middleware"
	"instagram_clone/internal/model"
	"instagram_clone/internal/service"
	"instagram_clone/internal/store"

	"github.com/jackc/pgx/v5/pgxpool"
)

const testJWTSecret = "test-secret"

func TestCreatePresignedURL(t *testing.T) {
	router := newTestRouter(t)

	body := `{
		"user_id": " user_123 ",
		"file_name": " sunset.jpg ",
		"content_type": " image/jpeg ",
		"media_type": "photo"
	}`
	rec := performRequest(router, http.MethodPost, "/presigned-url", body)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusCreated, rec.Body.String())
	}

	var resp model.PresignedURLResponse
	decodeResponse(t, rec, &resp)

	if resp.MediaID == "" {
		t.Fatal("expected media id")
	}
	if resp.UploadURL == "" {
		t.Fatal("expected upload URL")
	}
	if resp.S3Bucket != "instagram-media-test" {
		t.Fatalf("S3Bucket = %q, want instagram-media-test", resp.S3Bucket)
	}
	if !strings.HasPrefix(resp.S3Key, "users/user_123/"+resp.MediaID+"/") {
		t.Fatalf("S3Key = %q, want user/media prefix", resp.S3Key)
	}
	if resp.ExpiresIn != int64(service.PresignedURLExpiry.Seconds()) {
		t.Fatalf("ExpiresIn = %d, want %d", resp.ExpiresIn, int64(service.PresignedURLExpiry.Seconds()))
	}
}

func TestCreatePresignedURLValidation(t *testing.T) {
	router := newTestRouter(t)

	tests := []struct {
		name     string
		body     string
		wantBody string
	}{
		{
			name:     "invalid json",
			body:     `{`,
			wantBody: "invalid JSON request body",
		},
		{
			name: "unknown field",
			body: `{
				"user_id": "user_123",
				"file_name": "sunset.jpg",
				"content_type": "image/jpeg",
				"media_type": "photo",
				"extra": true
			}`,
			wantBody: "invalid JSON request body",
		},
		{
			name: "missing required fields",
			body: `{
				"user_id": "",
				"file_name": "",
				"content_type": "image/jpeg",
				"media_type": "photo"
			}`,
			wantBody: "user_id, file_name, and content_type are required",
		},
		{
			name: "invalid media type",
			body: `{
				"user_id": "user_123",
				"file_name": "sunset.jpg",
				"content_type": "image/jpeg",
				"media_type": "reel"
			}`,
			wantBody: "media_type must be photo or video",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := performRequest(router, http.MethodPost, "/presigned-url", tt.body)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
			}
			assertErrorResponse(t, rec, tt.wantBody)
		})
	}
}

func TestConfirmMedia(t *testing.T) {
	router := newTestRouter(t)
	created := createMedia(t, router, "user_123")

	rec := performRequest(router, http.MethodPost, "/media/confirm", `{
		"user_id": " user_123 ",
		"media_id": " `+created.MediaID+` "
	}`)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var media model.Media
	decodeResponse(t, rec, &media)

	if media.ID != created.MediaID {
		t.Fatalf("ID = %q, want %q", media.ID, created.MediaID)
	}
	if media.Status != model.MediaStatusUploaded {
		t.Fatalf("Status = %q, want uploaded", media.Status)
	}
	if media.UploadedAt == nil {
		t.Fatal("expected UploadedAt to be set")
	}
}

func TestConfirmMediaValidationAndNotFound(t *testing.T) {
	router := newTestRouter(t)

	tests := []struct {
		name       string
		body       string
		wantStatus int
		wantBody   string
	}{
		{
			name:       "invalid json",
			body:       `{`,
			wantStatus: http.StatusBadRequest,
			wantBody:   "invalid JSON request body",
		},
		{
			name:       "missing fields",
			body:       `{"user_id":"user_123","media_id":""}`,
			wantStatus: http.StatusBadRequest,
			wantBody:   "user_id and media_id are required",
		},
		{
			name:       "not found",
			body:       `{"user_id":"user_123","media_id":"missing"}`,
			wantStatus: http.StatusNotFound,
			wantBody:   "media not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := performRequest(router, http.MethodPost, "/media/confirm", tt.body)

			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d; body: %s", rec.Code, tt.wantStatus, rec.Body.String())
			}
			assertErrorResponse(t, rec, tt.wantBody)
		})
	}
}

func TestGetMedia(t *testing.T) {
	router := newTestRouter(t)
	created := createMedia(t, router, "user_123")

	rec := performRequest(router, http.MethodGet, "/media/"+created.MediaID, "")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var media model.Media
	decodeResponse(t, rec, &media)

	if media.ID != created.MediaID {
		t.Fatalf("ID = %q, want %q", media.ID, created.MediaID)
	}
	if media.Status != model.MediaStatusPending {
		t.Fatalf("Status = %q, want pending", media.Status)
	}
}

func TestGetMediaNotFound(t *testing.T) {
	router := newTestRouter(t)

	rec := performRequest(router, http.MethodGet, "/media/missing", "")

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusNotFound, rec.Body.String())
	}
	assertErrorResponse(t, rec, "media not found")
}

func newTestRouter(t *testing.T) http.Handler {
	t.Helper()
	t.Setenv("AWS_ACCESS_KEY_ID", "minioadmin")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

	const dsn = "postgres://postgres:postgres@localhost:5432/instagram_clone"
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil || pool.Ping(context.Background()) != nil {
		if pool != nil {
			pool.Close()
		}
		t.Skip("postgres unavailable, skipping")
	}
	ctx := context.Background()
	// Clear any leftover data. Delete media before users due to FK constraint.
	pool.Exec(ctx, "DELETE FROM media")
	pool.Exec(ctx, "DELETE FROM users WHERE id = 'user_123'")
	// Seed the test user that media rows will reference via FK.
	pool.Exec(ctx, `INSERT INTO users (id, username, email, password_hash, created_at)
		VALUES ('user_123', 'testuser', 'test@example.com', 'testhash', NOW())
		ON CONFLICT (id) DO NOTHING`)
	t.Cleanup(func() {
		pool.Exec(ctx, "DELETE FROM media")
		pool.Exec(ctx, "DELETE FROM users WHERE id = 'user_123'")
		pool.Close()
	})

	mediaStore := store.NewMediaStore(pool)
	storage, err := service.NewStorage(t.Context(), "http://localhost:9000", "", "us-east-1", "instagram-media-test", mediaStore)
	if err != nil {
		t.Fatalf("NewStorage returned error: %v", err)
	}
	return NewUploadHandler(storage).Router()
}

func createMedia(t *testing.T, router http.Handler, userID string) model.PresignedURLResponse {
	t.Helper()

	rec := performRequest(router, http.MethodPost, "/presigned-url", `{
		"user_id": "`+userID+`",
		"file_name": "sunset.jpg",
		"content_type": "image/jpeg",
		"media_type": "photo"
	}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusCreated, rec.Body.String())
	}

	var resp model.PresignedURLResponse
	decodeResponse(t, rec, &resp)
	return resp
}

func performRequest(handler http.Handler, method, path, body string) *httptest.ResponseRecorder {
	var reader *bytes.Reader
	if body == "" {
		reader = bytes.NewReader(nil)
	} else {
		reader = bytes.NewReader([]byte(body))
	}

	req := httptest.NewRequest(method, path, reader)
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(middleware.ContextWithUserID(req.Context(), "user_123"))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func decodeResponse(t *testing.T, rec *httptest.ResponseRecorder, dest any) {
	t.Helper()

	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", got)
	}
	if err := json.NewDecoder(rec.Body).Decode(dest); err != nil {
		t.Fatalf("decode response: %v", err)
	}
}

func assertErrorResponse(t *testing.T, rec *httptest.ResponseRecorder, want string) {
	t.Helper()

	var resp model.ErrorResponse
	decodeResponse(t, rec, &resp)
	if resp.Error != want {
		t.Fatalf("error = %q, want %q", resp.Error, want)
	}
}
