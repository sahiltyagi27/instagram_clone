package handler

import (
	"context"
	"net/http"
	"testing"
	"time"

	"instagram_clone/internal/model"
	"instagram_clone/internal/service"
	"instagram_clone/internal/store"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// ── helpers ──────────────────────────────────────────────────────────────────

func newTestPGPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	const dsn = "postgres://postgres:postgres@localhost:5432/instagram_clone"
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil || pool.Ping(context.Background()) != nil {
		if pool != nil {
			pool.Close()
		}
		t.Skip("postgres unavailable, skipping")
	}
	// Scoped to this package's fixtures (user_123/other_user and the auth test's
	// email) rather than wholesale deletes, so it is safe to run in parallel
	// with other packages' tests against the shared database.
	t.Cleanup(func() {
		pool.Exec(context.Background(), "DELETE FROM stories WHERE user_id IN ('user_123', 'other_user')")
		pool.Exec(context.Background(), "DELETE FROM users WHERE id IN ('user_123', 'other_user') OR email = 'sahil@example.com'")
		pool.Close()
	})
	return pool
}

func newTestRedisClient(t *testing.T) *redis.Client {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	if client.Ping(context.Background()).Err() != nil {
		_ = client.Close()
		t.Skip("redis unavailable, skipping")
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// ── auth tests ───────────────────────────────────────────────────────────────

func TestAuthHandlerSignupAndLogin(t *testing.T) {
	pool := newTestPGPool(t)
	authSvc := service.NewAuthService("test-secret", store.NewUserStore(pool))
	router := NewAuthHandler(authSvc).Router()

	signupRec := performRequest(router, http.MethodPost, "/signup", `{
		"username": "sahil",
		"email": "sahil@example.com",
		"password": "secret123"
	}`)
	if signupRec.Code != http.StatusCreated {
		t.Fatalf("signup status = %d, want %d; body: %s", signupRec.Code, http.StatusCreated, signupRec.Body.String())
	}

	var signup model.AuthResponse
	decodeResponse(t, signupRec, &signup)
	if signup.Token == "" {
		t.Fatal("expected signup token")
	}
	if signup.User.PasswordHash != "" {
		t.Fatal("expected password hash to be omitted")
	}

	loginRec := performRequest(router, http.MethodPost, "/login", `{
		"email": "sahil@example.com",
		"password": "secret123"
	}`)
	if loginRec.Code != http.StatusOK {
		t.Fatalf("login status = %d, want %d; body: %s", loginRec.Code, http.StatusOK, loginRec.Body.String())
	}

	var login model.AuthResponse
	decodeResponse(t, loginRec, &login)
	if login.User.ID != signup.User.ID {
		t.Fatalf("login user id = %q, want %q", login.User.ID, signup.User.ID)
	}
}

func TestAuthHandlerValidationAndConflict(t *testing.T) {
	pool := newTestPGPool(t)
	authSvc := service.NewAuthService("test-secret", store.NewUserStore(pool))
	router := NewAuthHandler(authSvc).Router()

	rec := performRequest(router, http.MethodPost, "/signup", `{"username":"","email":"sahil@example.com","password":"secret123"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	assertErrorResponse(t, rec, "username, email, and password are required")

	rec = performRequest(router, http.MethodPost, "/signup", `{"username":"sahil","email":"sahil@example.com","password":"secret123"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusCreated)
	}

	rec = performRequest(router, http.MethodPost, "/signup", `{"username":"other","email":"sahil@example.com","password":"secret123"}`)
	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusConflict)
	}
	assertErrorResponse(t, rec, "user already exists")
}

// ── feed tests ────────────────────────────────────────────────────────────────

func TestFeedHandler(t *testing.T) {
	client := newTestRedisClient(t)
	feedStore := store.NewFeedStore(client)
	feedSvc := service.NewFeedService(feedStore, nil)

	// Seed one item directly via the service.
	feedSvc.AddFeedItem(context.Background(), "user_123", model.FeedItem{
		MediaID:   "media_1",
		UserID:    "user_123",
		CreatedAt: time.Now().UTC(),
	})
	t.Cleanup(func() { client.Del(context.Background(), "feed:user_123") })

	router := NewFeedHandler(feedSvc).Router()
	rec := performRequest(router, http.MethodGet, "/user_123?limit=1", "")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var resp model.FeedResponse
	decodeResponse(t, rec, &resp)
	if len(resp.Items) != 1 {
		t.Fatalf("feed response = %#v, want one item", resp)
	}
}

func TestFeedHandlerRejectsOtherUsers(t *testing.T) {
	client := newTestRedisClient(t)
	router := NewFeedHandler(service.NewFeedService(store.NewFeedStore(client), nil)).Router()

	rec := performRequest(router, http.MethodGet, "/other_user", "")

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusForbidden, rec.Body.String())
	}
	assertErrorResponse(t, rec, "cannot access another user's feed")
}

// ── story tests ───────────────────────────────────────────────────────────────

func TestStoryHandlerGenerateAndConfirm(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "minioadmin")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

	pool := newTestPGPool(t)
	// A dummy user must exist because stories.user_id references users.id.
	pool.Exec(context.Background(), `
		INSERT INTO users (id, username, email, password_hash) VALUES
		('user_123', 'test', 'test@example.com', 'x')
		ON CONFLICT DO NOTHING`)

	storyStore := store.NewStoryStore(pool)
	mediaStore := store.NewMediaStore(pool)

	// No live S3 server needed: PresignPutObject is computed locally by the AWS SDK.
	storage, err := service.NewStorage(context.Background(), "http://s3.test:9000", "", "us-east-1", "instagram-media-test", mediaStore)
	if err != nil {
		t.Fatalf("NewStorage returned error: %v", err)
	}
	stories := service.NewStoryService(storage, storyStore)
	router := NewStoryHandler(stories).Router()

	createRec := performRequest(router, http.MethodPost, "/presigned-url", `{
		"user_id": "user_123",
		"file_name": "story.jpg",
		"content_type": "image/jpeg"
	}`)
	if createRec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d; body: %s", createRec.Code, http.StatusCreated, createRec.Body.String())
	}

	var created model.StoryPresignedURLResponse
	decodeResponse(t, createRec, &created)

	confirmRec := performRequest(router, http.MethodPost, "/confirm", `{
		"user_id": "user_123",
		"story_id": "`+created.StoryID+`"
	}`)
	if confirmRec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body: %s", confirmRec.Code, http.StatusOK, confirmRec.Body.String())
	}

	var story model.Story
	decodeResponse(t, confirmRec, &story)
	if story.ID != created.StoryID {
		t.Fatalf("story id = %q, want %q", story.ID, created.StoryID)
	}
}
