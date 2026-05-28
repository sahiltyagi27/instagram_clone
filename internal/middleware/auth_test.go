package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"instagram_clone/internal/service"
)

func TestJWTMiddlewareAcceptsBearerToken(t *testing.T) {
	token, err := service.GenerateJWT("test-secret", "user_123")
	if err != nil {
		t.Fatalf("GenerateJWT returned error: %v", err)
	}

	handler := JWT("test-secret")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		userID, ok := UserIDFromContext(r.Context())
		if !ok {
			t.Fatal("expected user id in context")
		}
		if userID != "user_123" {
			t.Fatalf("user id = %q, want user_123", userID)
		}
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNoContent)
	}
}

func TestJWTMiddlewareRejectsMissingToken(t *testing.T) {
	handler := JWT("test-secret")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("handler should not be called")
	}))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}
