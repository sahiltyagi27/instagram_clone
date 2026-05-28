package main

import "testing"

func TestEnvOrDefault(t *testing.T) {
	t.Setenv("TEST_ENV_OR_DEFAULT", "configured")

	if got := envOrDefault("TEST_ENV_OR_DEFAULT", "fallback"); got != "configured" {
		t.Fatalf("envOrDefault() = %q, want configured", got)
	}
	if got := envOrDefault("TEST_ENV_OR_DEFAULT_MISSING", "fallback"); got != "fallback" {
		t.Fatalf("envOrDefault() = %q, want fallback", got)
	}
}

func TestJWTSecretFromEnv(t *testing.T) {
	t.Setenv("JWT_SECRET", "configured-secret")
	t.Setenv("APP_ENV", "prod")

	secret, err := jwtSecretFromEnv()
	if err != nil {
		t.Fatalf("jwtSecretFromEnv returned error: %v", err)
	}
	if secret != "configured-secret" {
		t.Fatalf("secret = %q, want configured-secret", secret)
	}
}

func TestJWTSecretFromEnvRequiresSecretOutsideDev(t *testing.T) {
	t.Setenv("JWT_SECRET", "")
	t.Setenv("APP_ENV", "prod")

	if _, err := jwtSecretFromEnv(); err == nil {
		t.Fatal("expected error")
	}
}
