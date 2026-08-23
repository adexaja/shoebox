package shoebox

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/adexaja/shoebox/internal/retry"
)

// TestWebhookHandler_Success verifies that a 2xx response is treated as
// delivered (handler returns nil → message acked).
func TestWebhookHandler_Success(t *testing.T) {
	var got struct {
		body    string
		msgID   string
		queue   string
		attempt string
		ctype   string
	}
	var mu sync.Mutex

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		got.body = string(body)
		got.msgID = r.Header.Get("X-Shoebox-Message-ID")
		got.queue = r.Header.Get("X-Shoebox-Queue")
		got.attempt = r.Header.Get("X-Shoebox-Attempt")
		got.ctype = r.Header.Get("Content-Type")
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	h := WebhookHandler(srv.URL, nil)

	err := h(context.Background(), Message{
		ID:      "msg-123",
		Queue:   "orders",
		Payload: []byte(`{"event":"created"}`),
	})
	if err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if got.body != `{"event":"created"}` {
		t.Fatalf("body = %q", got.body)
	}
	if got.msgID != "msg-123" {
		t.Fatalf("X-Shoebox-Message-ID = %q, want msg-123", got.msgID)
	}
	if got.queue != "orders" {
		t.Fatalf("X-Shoebox-Queue = %q, want orders", got.queue)
	}
	if got.attempt != "1" {
		t.Fatalf("X-Shoebox-Attempt = %q, want 1", got.attempt)
	}
	if got.ctype != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", got.ctype)
	}
}

// TestWebhookHandler_Non2xx verifies that a non-2xx response returns an error
// (so the broker retries / dead-letters).
func TestWebhookHandler_Non2xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("internal error"))
	}))
	defer srv.Close()

	h := WebhookHandler(srv.URL, nil)
	err := h(context.Background(), Message{
		ID:      "msg-456",
		Queue:   "orders",
		Payload: []byte("test"),
	})

	if err == nil {
		t.Fatal("expected error for 500 response")
	}
}

// TestWebhookHandler_AttemptHeader verifies the attempt header reflects
// the message's attempt count.
func TestWebhookHandler_AttemptHeader(t *testing.T) {
	var attempt string
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		attempt = r.Header.Get("X-Shoebox-Attempt")
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	h := WebhookHandler(srv.URL, nil)
	// Attempts=2 means this is the 3rd delivery.
	_ = h(context.Background(), Message{
		ID:       "msg-789",
		Queue:    "orders",
		Payload:  []byte("x"),
		Attempts: 2,
	})

	mu.Lock()
	defer mu.Unlock()
	if attempt != "3" {
		t.Fatalf("X-Shoebox-Attempt = %q, want 3", attempt)
	}
}

// TestWebhookHandler_CustomContentType verifies WithWebhookContentType.
func TestWebhookHandler_CustomContentType(t *testing.T) {
	var ctype string
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		ctype = r.Header.Get("Content-Type")
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	h := WebhookHandler(srv.URL, nil, WithWebhookContentType("text/plain"))
	_ = h(context.Background(), Message{
		ID:      "msg",
		Queue:   "q",
		Payload: []byte("hello"),
	})

	mu.Lock()
	defer mu.Unlock()
	if ctype != "text/plain" {
		t.Fatalf("Content-Type = %q, want text/plain", ctype)
	}
}

// TestWebhookHandler_Unreachable verifies that a connection error returns
// an error (not a panic).
func TestWebhookHandler_Unreachable(t *testing.T) {
	h := WebhookHandler("http://[IP_REDACTED]:1/notrunning", &http.Client{
		Timeout: 500 * time.Millisecond,
	})
	err := h(context.Background(), Message{
		ID:      "msg",
		Queue:   "q",
		Payload: []byte("x"),
	})
	if err == nil {
		t.Fatal("expected error for unreachable server")
	}
}

// TestWebhookHandler_BadURL verifies that a malformed URL returns an error
// immediately (not a panic).
func TestWebhookHandler_BadURL(t *testing.T) {
	h := WebhookHandler("://bad-url", nil)
	err := h(context.Background(), Message{
		ID:      "msg",
		Queue:   "q",
		Payload: []byte("x"),
	})
	if err == nil {
		t.Fatal("expected error for malformed URL")
	}
}

// --- Integration test: full broker dispatch with webhook handler ---

// TestWebhook_EndToEnd verifies that enqueuing a message on a queue with
// a registered webhook handler results in the payload being POSTed to the
// target, and that a failing target triggers retries and eventually DLQ.
func TestWebhook_EndToEnd(t *testing.T) {
	var delivered atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		delivered.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	q, err := New(Options{
		Storage: Memory,
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = q.Shutdown(ctx)
	}()

	// Register a webhook handler with fast backoff and low max-retries.
	q.Handle("orders", WebhookHandler(srv.URL, nil), HandlerOptions{
		MaxRetries: 1,
		Backoff:    retry.Constant(50 * time.Millisecond),
	})

	// Enqueue a message.
	if err := q.Enqueue("orders", []byte(`{"order":"abc"}`)); err != nil {
		t.Fatal(err)
	}

	// Wait for delivery.
	deadline := time.After(3 * time.Second)
	for delivered.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("message was not delivered via webhook before timeout")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	if delivered.Load() != 1 {
		t.Fatalf("delivered = %d, want 1", delivered.Load())
	}
}

// TestWebhook_EndToEnd_Failure verifies that a failing webhook target
// triggers retries and eventually moves the message to the DLQ.
func TestWebhook_EndToEnd_Failure(t *testing.T) {
	var attempts atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	q, err := New(Options{
		Storage: Memory,
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = q.Shutdown(ctx)
	}()

	// MaxRetries=1 → 2 total attempts (initial + 1 retry), then DLQ.
	q.Handle("flaky", WebhookHandler(srv.URL, nil), HandlerOptions{
		MaxRetries: 1,
		Backoff:    retry.Constant(50 * time.Millisecond),
	})

	if err := q.Enqueue("flaky", []byte("will-fail")); err != nil {
		t.Fatal(err)
	}

	// Wait for the message to land in the DLQ.
	deadline := time.After(5 * time.Second)
	for {
		stats, err := q.Stats(context.Background(), "flaky")
		if err != nil {
			t.Fatal(err)
		}
		if stats.Dead > 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("message not dead-lettered; attempts=%d stats=%+v", attempts.Load(), stats)
		default:
			time.Sleep(20 * time.Millisecond)
		}
	}

	// Should have been attempted at least twice (initial + retry).
	if got := attempts.Load(); got < 2 {
		t.Fatalf("attempts = %d, want >= 2", got)
	}
}
