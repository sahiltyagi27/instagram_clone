package kafka

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRetryWithBackoffSucceedsOnFirstAttempt(t *testing.T) {
	calls := 0
	err := retryWithBackoff(context.Background(), 3, func() error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
}

func TestRetryWithBackoffRetriesOnFailure(t *testing.T) {
	sentinel := errors.New("transient error")
	calls := 0
	err := retryWithBackoff(context.Background(), 3, func() error {
		calls++
		if calls < 3 {
			return sentinel
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil after eventual success, got %v", err)
	}
	if calls != 3 {
		t.Fatalf("calls = %d, want 3", calls)
	}
}

func TestRetryWithBackoffReturnsLastErrorAfterExhaustion(t *testing.T) {
	sentinel := errors.New("permanent error")
	calls := 0
	err := retryWithBackoff(context.Background(), 3, func() error {
		calls++
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("error = %v, want sentinel", err)
	}
	if calls != 3 {
		t.Fatalf("calls = %d, want 3", calls)
	}
}

func TestRetryWithBackoffRespectsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	calls := 0
	done := make(chan error, 1)
	go func() {
		done <- retryWithBackoff(ctx, 5, func() error {
			calls++
			return errors.New("fail")
		})
	}()

	// Cancel after first attempt fires.
	time.Sleep(50 * time.Millisecond)
	cancel()

	err := <-done
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1 before cancel", calls)
	}
}
