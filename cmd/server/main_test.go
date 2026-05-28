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
