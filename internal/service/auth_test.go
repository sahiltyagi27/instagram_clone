package service

import (
	"errors"
	"testing"

	"instagram_clone/internal/model"
)

func TestAuthServiceSignupLoginAndValidateToken(t *testing.T) {
	auth := NewAuthService("test-secret")

	signup, err := auth.Signup(model.SignupRequest{
		Username: "sahil",
		Email:    "SAHIL@example.com",
		Password: "secret123",
	})
	if err != nil {
		t.Fatalf("Signup returned error: %v", err)
	}
	if signup.User.ID == "" {
		t.Fatal("expected user id")
	}
	if signup.User.Email != "sahil@example.com" {
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

	login, err := auth.Login(model.LoginRequest{Email: "sahil@example.com", Password: "secret123"})
	if err != nil {
		t.Fatalf("Login returned error: %v", err)
	}
	if login.User.ID != signup.User.ID {
		t.Fatalf("login user id = %q, want %q", login.User.ID, signup.User.ID)
	}
}

func TestAuthServiceRejectsDuplicateAndInvalidLogin(t *testing.T) {
	auth := NewAuthService("test-secret")
	if _, err := auth.Signup(model.SignupRequest{Username: "sahil", Email: "sahil@example.com", Password: "secret123"}); err != nil {
		t.Fatalf("Signup returned error: %v", err)
	}

	_, err := auth.Signup(model.SignupRequest{Username: "other", Email: "sahil@example.com", Password: "secret123"})
	if !errors.Is(err, ErrUserAlreadyExists) {
		t.Fatalf("duplicate signup error = %v, want ErrUserAlreadyExists", err)
	}

	_, err = auth.Login(model.LoginRequest{Email: "sahil@example.com", Password: "wrong"})
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
