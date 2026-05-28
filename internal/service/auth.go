package service

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"instagram_clone/internal/model"

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

	mu           sync.RWMutex
	usersByID    map[string]model.User
	usersByEmail map[string]model.User
}

type Claims struct {
	jwt.RegisteredClaims
}

func NewAuthService(secret string) *AuthService {
	return &AuthService{
		secret:       secret,
		usersByID:    make(map[string]model.User),
		usersByEmail: make(map[string]model.User),
	}
}

func (s *AuthService) Signup(req model.SignupRequest) (*model.AuthResponse, error) {
	email := strings.ToLower(strings.TrimSpace(req.Email))
	username := strings.TrimSpace(req.Username)
	password := req.Password

	if email == "" || username == "" || password == "" {
		return nil, ErrInvalidCredentials
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.usersByEmail[email]; ok {
		return nil, ErrUserAlreadyExists
	}

	hash, err := HashPassword(password)
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
	token, err := GenerateJWT(s.secret, user.ID)
	if err != nil {
		return nil, err
	}

	s.usersByID[user.ID] = user
	s.usersByEmail[email] = user

	return &model.AuthResponse{User: publicUser(user), Token: token}, nil
}

func (s *AuthService) Login(req model.LoginRequest) (*model.AuthResponse, error) {
	email := strings.ToLower(strings.TrimSpace(req.Email))
	password := req.Password
	if email == "" || password == "" {
		return nil, ErrInvalidCredentials
	}

	s.mu.RLock()
	user, ok := s.usersByEmail[email]
	s.mu.RUnlock()
	if !ok || !CheckPassword(user.PasswordHash, password) {
		return nil, ErrInvalidCredentials
	}

	token, err := GenerateJWT(s.secret, user.ID)
	if err != nil {
		return nil, err
	}

	return &model.AuthResponse{User: publicUser(user), Token: token}, nil
}

func (s *AuthService) GetUser(userID string) (model.User, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	user, ok := s.usersByID[userID]
	return publicUser(user), ok
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
