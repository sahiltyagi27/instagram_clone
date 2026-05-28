package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"instagram_clone/internal/model"
	"instagram_clone/internal/store"

	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/bcrypt"
)

const jwtExpiry = 24 * time.Hour

var (
	ErrInvalidCredentials = errors.New("invalid credentials")
	ErrUserAlreadyExists  = errors.New("user already exists")
)

type AuthService struct {
	secret string
	users  *store.UserStore
}

type Claims struct {
	jwt.RegisteredClaims
}

func NewAuthService(secret string, users *store.UserStore) *AuthService {
	return &AuthService{secret: secret, users: users}
}

func (s *AuthService) Signup(ctx context.Context, req model.SignupRequest) (*model.AuthResponse, error) {
	email := strings.ToLower(strings.TrimSpace(req.Email))
	username := strings.TrimSpace(req.Username)

	if email == "" || username == "" || req.Password == "" {
		return nil, ErrInvalidCredentials
	}

	hash, err := HashPassword(req.Password)
	if err != nil {
		return nil, err
	}
	id, err := newID()
	if err != nil {
		return nil, err
	}

	user := model.User{
		ID:           id,
		Username:     username,
		Email:        email,
		PasswordHash: hash,
		CreatedAt:    time.Now().UTC(),
	}

	if err := s.users.Create(ctx, user); err != nil {
		if errors.Is(err, store.ErrUserAlreadyExists) {
			return nil, ErrUserAlreadyExists
		}
		return nil, err
	}

	token, err := GenerateJWT(s.secret, user.ID)
	if err != nil {
		return nil, err
	}

	return &model.AuthResponse{User: publicUser(user), Token: token}, nil
}

func (s *AuthService) Login(ctx context.Context, req model.LoginRequest) (*model.AuthResponse, error) {
	email := strings.ToLower(strings.TrimSpace(req.Email))
	if email == "" || req.Password == "" {
		return nil, ErrInvalidCredentials
	}

	user, err := s.users.GetByEmail(ctx, email)
	if err != nil {
		if errors.Is(err, store.ErrUserNotFound) {
			return nil, ErrInvalidCredentials
		}
		return nil, err
	}

	if !CheckPassword(user.PasswordHash, req.Password) {
		return nil, ErrInvalidCredentials
	}

	token, err := GenerateJWT(s.secret, user.ID)
	if err != nil {
		return nil, err
	}

	return &model.AuthResponse{User: publicUser(user), Token: token}, nil
}

func (s *AuthService) GetUser(ctx context.Context, userID string) (model.User, bool) {
	user, err := s.users.GetByID(ctx, userID)
	if err != nil {
		return model.User{}, false
	}
	return publicUser(user), true
}

func HashPassword(password string) (string, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", fmt.Errorf("hash password: %w", err)
	}
	return string(hash), nil
}

func CheckPassword(hash, password string) bool {
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)) == nil
}

func GenerateJWT(secret, userID string) (string, error) {
	claims := Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   userID,
			ExpiresAt: jwt.NewNumericDate(time.Now().UTC().Add(jwtExpiry)),
			IssuedAt:  jwt.NewNumericDate(time.Now().UTC()),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString([]byte(secret))
	if err != nil {
		return "", fmt.Errorf("sign jwt: %w", err)
	}
	return signed, nil
}

func ValidateJWT(secret, tokenString string) (string, error) {
	token, err := jwt.ParseWithClaims(tokenString, &Claims{}, func(token *jwt.Token) (any, error) {
		if token.Method != jwt.SigningMethodHS256 {
			return nil, fmt.Errorf("unexpected signing method: %s", token.Header["alg"])
		}
		return []byte(secret), nil
	})
	if err != nil {
		return "", fmt.Errorf("parse jwt: %w", err)
	}

	claims, ok := token.Claims.(*Claims)
	if !ok || !token.Valid || claims.Subject == "" {
		return "", ErrInvalidCredentials
	}
	return claims.Subject, nil
}

func publicUser(user model.User) model.User {
	user.PasswordHash = ""
	return user
}
