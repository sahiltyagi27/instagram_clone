package service

import (
	"context"
	"errors"
	"testing"

	"instagram_clone/internal/model"
	"instagram_clone/internal/store"

	"github.com/jackc/pgx/v5/pgxpool"
)

func newTestAuthService(t *testing.T) *AuthService {
	t.Helper()

	const dsn = "postgres://postgres:postgres@localhost:5432/instagram_clone"
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil || pool.Ping(context.Background()) != nil {
		if pool != nil {
			pool.Close()
		}
		t.Skip("postgres unavailable, skipping")
	}
	// Scoped to this package's fixture email (not a wholesale DELETE FROM users)
	// so it is safe to run in parallel with other packages against the shared DB.
	const fixtureEmail = "svc-sahil@example.com"
	pool.Exec(context.Background(), "DELETE FROM users WHERE email = $1", fixtureEmail)
	t.Cleanup(func() {
		pool.Exec(context.Background(), "DELETE FROM users WHERE email = $1", fixtureEmail)
		pool.Close()
	})
	return NewAuthService("test-secret", store.NewUserStore(pool))
}

func TestAuthServiceSignupLoginAndValidateToken(t *testing.T) {
	auth := newTestAuthService(t)

	signup, err := auth.Signup(context.Background(), model.SignupRequest{
		Username: "sahil",
		Email:    "SVC-SAHIL@example.com",
		Password: "secret123",
	})
	if err != nil {
		t.Fatalf("Signup returned error: %v", err)
	}
	if signup.User.ID == "" {
		t.Fatal("expected user id")
	}
	if signup.User.Email != "svc-sahil@example.com" {
		t.Fatalf("email = %q, want normalized email", signup.User.Email)
	}
	if signup.User.PasswordHash != "" {
		t.Fatal("expected password hash to be omitted from response")
	}

	userID, err := ValidateJWT("test-secret", signup.Token)
	if err != nil {
		t.Fatalf("ValidateJWT returned error: %v", err)
	}
	if userID != signup.User.ID {
		t.Fatalf("token user id = %q, want %q", userID, signup.User.ID)
	}

	login, err := auth.Login(context.Background(), model.LoginRequest{Email: "svc-sahil@example.com", Password: "secret123"})
	if err != nil {
		t.Fatalf("Login returned error: %v", err)
	}
	if login.User.ID != signup.User.ID {
		t.Fatalf("login user id = %q, want %q", login.User.ID, signup.User.ID)
	}
}

func TestAuthServicePreservesPasswordWhitespace(t *testing.T) {
	auth := newTestAuthService(t)

	if _, err := auth.Signup(context.Background(), model.SignupRequest{
		Username: "sahil",
		Email:    "svc-sahil@example.com",
		Password: " secret123 ",
	}); err != nil {
		t.Fatalf("Signup returned error: %v", err)
	}

	if _, err := auth.Login(context.Background(), model.LoginRequest{Email: "svc-sahil@example.com", Password: "secret123"}); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("login without spaces error = %v, want ErrInvalidCredentials", err)
	}
	if _, err := auth.Login(context.Background(), model.LoginRequest{Email: "svc-sahil@example.com", Password: " secret123 "}); err != nil {
		t.Fatalf("login with spaces returned error: %v", err)
	}
}

func TestAuthServiceRejectsDuplicateAndInvalidLogin(t *testing.T) {
	auth := newTestAuthService(t)

	if _, err := auth.Signup(context.Background(), model.SignupRequest{Username: "sahil", Email: "svc-sahil@example.com", Password: "secret123"}); err != nil {
		t.Fatalf("Signup returned error: %v", err)
	}

	_, err := auth.Signup(context.Background(), model.SignupRequest{Username: "other", Email: "svc-sahil@example.com", Password: "secret123"})
	if !errors.Is(err, ErrUserAlreadyExists) {
		t.Fatalf("duplicate signup error = %v, want ErrUserAlreadyExists", err)
	}

	_, err = auth.Login(context.Background(), model.LoginRequest{Email: "svc-sahil@example.com", Password: "wrong"})
	if !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("login error = %v, want ErrInvalidCredentials", err)
	}
}

func TestPasswordHelpers(t *testing.T) {
	hash, err := HashPassword("secret123")
	if err != nil {
		t.Fatalf("HashPassword returned error: %v", err)
	}
	if hash == "secret123" {
		t.Fatal("expected password to be hashed")
	}
	if !CheckPassword(hash, "secret123") {
		t.Fatal("expected password check to pass")
	}
	if CheckPassword(hash, "wrong") {
		t.Fatal("expected password check to fail")
	}
}
