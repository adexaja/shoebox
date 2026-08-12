package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/adexaja/shoebox/internal/dlq"
	"github.com/adexaja/shoebox/internal/storage"
)

// newTestHandler creates an API handler backed by memory storage with a
// few messages pre-enqueued.
func newTestHandler(t *testing.T) (*Handler, storage.Storage) {
	t.Helper()
	store := storage.NewMemory()
	dlqMgr := dlq.NewManager(store)
	h := NewHandler(store, dlqMgr, nil)
	return h, store
}

func enqueueMessage(t *testing.T, store storage.Storage, queue, payload string) string {
	t.Helper()
	msg := storage.Message{
		ID:        storage.NewMessageID(),
		Queue:     queue,
		Payload:   []byte(payload),
		CreatedAt: time.Now(),
	}
	if err := store.Enqueue(context.Background(), queue, msg); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}
	return msg.ID
}

// --- Enqueue endpoint ---

func TestEnqueue_Success(t *testing.T) {
	h, store := newTestHandler(t)

	body := `{"payload":"hello world"}`
	req := httptest.NewRequest(http.MethodPost, "/queues/orders/messages", strings.NewReader(body))
	req.SetPathValue("name", "orders")
	w := httptest.NewRecorder()

	h.enqueue(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d; body: %s", w.Code, http.StatusCreated, w.Body.String())
	}

	var resp enqueueResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.ID == "" {
		t.Fatal("expected non-empty message ID")
	}

	// Verify the message was stored.
	msgs, err := store.Dequeue(context.Background(), "orders", 1)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message in store, got %d", len(msgs))
	}
	if string(msgs[0].Payload) != "hello world" {
		t.Fatalf("payload = %q, want %q", msgs[0].Payload, "hello world")
	}
}

func TestEnqueue_WithDelay(t *testing.T) {
	h, _ := newTestHandler(t)

	body := `{"payload":"delayed","delay":"100ms"}`
	req := httptest.NewRequest(http.MethodPost, "/queues/orders/messages", strings.NewReader(body))
	req.SetPathValue("name", "orders")
	w := httptest.NewRecorder()

	h.enqueue(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusCreated)
	}

	// The message should not be immediately visible.
	req2 := httptest.NewRequest(http.MethodGet, "/queues/orders/messages/next", nil)
	req2.SetPathValue("name", "orders")
	w2 := httptest.NewRecorder()
	h.consume(w2, req2)

	if w2.Code != http.StatusNotFound {
		t.Fatalf("expected 404 (message delayed), got %d; body: %s", w2.Code, w2.Body.String())
	}
}

func TestEnqueue_EmptyPayload(t *testing.T) {
	h, _ := newTestHandler(t)

	body := `{"payload":""}`
	req := httptest.NewRequest(http.MethodPost, "/queues/orders/messages", strings.NewReader(body))
	req.SetPathValue("name", "orders")
	w := httptest.NewRecorder()

	h.enqueue(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestEnqueue_RejectsReservedDLQName(t *testing.T) {
	h, _ := newTestHandler(t)
	req := httptest.NewRequest(http.MethodPost, "/queues/orders.dlq/messages", strings.NewReader(`{"payload":"x"}`))
	req.SetPathValue("name", "orders.dlq")
	w := httptest.NewRecorder()

	h.enqueue(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestEnqueue_InvalidJSON(t *testing.T) {
	h, _ := newTestHandler(t)

	req := httptest.NewRequest(http.MethodPost, "/queues/orders/messages", strings.NewReader("not json"))
	req.SetPathValue("name", "orders")
	w := httptest.NewRecorder()

	h.enqueue(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestEnqueue_Metadata(t *testing.T) {
	h, store := newTestHandler(t)

	body := `{"payload":"test","metadata":{"trace_id":"abc-123","source":"api"}}`
	req := httptest.NewRequest(http.MethodPost, "/queues/orders/messages", strings.NewReader(body))
	req.SetPathValue("name", "orders")
	w := httptest.NewRecorder()

	h.enqueue(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d; body: %s", w.Code, http.StatusCreated, w.Body.String())
	}

	msgs, err := store.Dequeue(context.Background(), "orders", 1)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if msgs[0].Metadata["trace_id"] != "abc-123" {
		t.Fatalf("trace_id = %q, want %q", msgs[0].Metadata["trace_id"], "abc-123")
	}
	if msgs[0].Metadata["source"] != "api" {
		t.Fatalf("source = %q, want %q", msgs[0].Metadata["source"], "api")
	}
}

// --- Consume endpoint ---

func TestConsume_Success(t *testing.T) {
	h, store := newTestHandler(t)
	id := enqueueMessage(t, store, "orders", "job-1")

	req := httptest.NewRequest(http.MethodGet, "/queues/orders/messages/next", nil)
	req.SetPathValue("name", "orders")
	w := httptest.NewRecorder()

	h.consume(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body: %s", w.Code, http.StatusOK, w.Body.String())
	}

	var msg storage.Message
	if err := json.NewDecoder(w.Body).Decode(&msg); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if msg.ID != id {
		t.Fatalf("ID = %q, want %q", msg.ID, id)
	}
}

func TestConsume_EmptyQueue(t *testing.T) {
	h, _ := newTestHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/queues/empty/messages/next", nil)
	req.SetPathValue("name", "empty")
	w := httptest.NewRecorder()

	h.consume(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusNotFound)
	}
}

// --- Stats endpoint ---

func TestStats_Success(t *testing.T) {
	h, store := newTestHandler(t)
	enqueueMessage(t, store, "orders", "msg-1")
	enqueueMessage(t, store, "orders", "msg-2")

	req := httptest.NewRequest(http.MethodGet, "/queues/orders/stats", nil)
	req.SetPathValue("name", "orders")
	w := httptest.NewRecorder()

	h.stats(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}

	var stats storage.QueueStats
	if err := json.NewDecoder(w.Body).Decode(&stats); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if stats.Depth != 2 {
		t.Fatalf("depth = %d, want 2", stats.Depth)
	}
}

// --- Ack endpoint ---

func TestAck_Success(t *testing.T) {
	h, store := newTestHandler(t)
	id := enqueueMessage(t, store, "orders", "to-ack")

	// Dequeue first so the message is in-flight.
	_, _ = store.Dequeue(context.Background(), "orders", 1)

	// Actually, memory storage removes on Dequeue. Re-enqueue for ack test.
	id = enqueueMessage(t, store, "orders", "to-ack-2")

	req := httptest.NewRequest(http.MethodDelete, "/queues/orders/messages/{id}", nil)
	req.SetPathValue("name", "orders")
	req.SetPathValue("id", id)
	w := httptest.NewRecorder()

	h.ack(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body: %s", w.Code, http.StatusOK, w.Body.String())
	}
}

// --- DLQ endpoints ---

func TestListDLQ_Empty(t *testing.T) {
	h, _ := newTestHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/queues/orders/dlq", nil)
	req.SetPathValue("name", "orders")
	w := httptest.NewRecorder()

	h.listDLQ(w, req)

	// Should return 200 with empty array, not 404.
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}
}

func TestReplayDLQ_NotFound(t *testing.T) {
	h, _ := newTestHandler(t)

	req := httptest.NewRequest(http.MethodPost, "/queues/orders/dlq/nonexistent/replay", nil)
	req.SetPathValue("name", "orders")
	req.SetPathValue("id", "nonexistent")
	w := httptest.NewRecorder()

	h.replayDLQ(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusNotFound)
	}
}

// --- Register (integration) ---

func TestRegister_AllRoutes(t *testing.T) {
	h, _ := newTestHandler(t)
	mux := http.NewServeMux()
	h.Register(mux)

	srv := httptest.NewServer(mux)
	defer srv.Close()

	// Enqueue.
	resp, err := http.Post(srv.URL+"/queues/integration/messages",
		"application/json", strings.NewReader(`{"payload":"test"}`))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("enqueue: %d", resp.StatusCode)
	}

	// Consume.
	resp, err = http.Get(srv.URL + "/queues/integration/messages/next")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("consume: %d", resp.StatusCode)
	}

	// Stats.
	resp, err = http.Get(srv.URL + "/queues/integration/stats")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("stats: %d", resp.StatusCode)
	}

	// DLQ.
	resp, err = http.Get(srv.URL + "/queues/integration/dlq")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("dlq: %d", resp.StatusCode)
	}
}

// --- Middleware tests ---

func TestRecoveryMiddleware_CatchesPanic(t *testing.T) {
	logger := newTestLogger()
	mw := RecoveryMiddleware(logger)
	panicHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("boom")
	})

	srv := httptest.NewServer(mw(panicHandler))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusInternalServerError)
	}
}

func TestAuthMiddleware_NoToken(t *testing.T) {
	mw := AuthMiddleware("secret")
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	})

	srv := httptest.NewServer(mw(next))
	defer srv.Close()

	// No auth header → 401.
	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusUnauthorized)
	}
	if called {
		t.Fatal("handler should not be called without auth")
	}
}

func TestAuthMiddleware_ValidToken(t *testing.T) {
	mw := AuthMiddleware("secret")
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	})

	srv := httptest.NewServer(mw(next))
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL, nil)
	req.Header.Set("X-API-Key", "secret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	if !called {
		t.Fatal("handler should be called with valid auth")
	}
}

func TestAuthMiddleware_EmptyToken_PassesThrough(t *testing.T) {
	mw := AuthMiddleware("") // empty → no auth
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	})

	srv := httptest.NewServer(mw(next))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	if !called {
		t.Fatal("handler should be called when auth token is empty")
	}
}

func TestRequestIDMiddleware_SetsHeader(t *testing.T) {
	mw := RequestIDMiddleware()
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	srv := httptest.NewServer(mw(next))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}

	id := resp.Header.Get("X-Request-ID")
	if id == "" {
		t.Fatal("expected X-Request-ID header to be set")
	}
}

func TestRequestIDMiddleware_PreservesCallerID(t *testing.T) {
	mw := RequestIDMiddleware()
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	srv := httptest.NewServer(mw(next))
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL, nil)
	req.Header.Set("X-Request-ID", "caller-id-123")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	if got := resp.Header.Get("X-Request-ID"); got != "caller-id-123" {
		t.Fatalf("X-Request-ID = %q, want %q", got, "caller-id-123")
	}
}

func TestLoggingMiddleware_LogsRequest(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	mw := LoggingMiddleware(logger)
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	srv := httptest.NewServer(mw(next))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/test-path")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	logOutput := buf.String()
	if !strings.Contains(logOutput, "http") {
		t.Fatalf("expected 'http' in log output: %s", logOutput)
	}
	if !strings.Contains(logOutput, "/test-path") {
		t.Fatalf("expected path in log output: %s", logOutput)
	}
}

func TestChain_Ordering(t *testing.T) {
	var order []string
	makeMW := func(name string) Middleware {
		return func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				order = append(order, name)
				next.ServeHTTP(w, r)
			})
		}
	}

	chain := Chain(makeMW("outer"), makeMW("inner"))
	handler := chain(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		order = append(order, "handler")
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	expected := []string{"outer", "inner", "handler"}
	if len(order) != len(expected) {
		t.Fatalf("order = %v, want %v", order, expected)
	}
	for i, v := range expected {
		if order[i] != v {
			t.Fatalf("order[%d] = %q, want %q", i, order[i], v)
		}
	}
}

func TestStatusWriter_CapturesStatus(t *testing.T) {
	mw := LoggingMiddleware(nil) // uses slog.Default()
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot) // 418
	})

	srv := httptest.NewServer(mw(next))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusTeapot {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusTeapot)
	}
}

// --- Helpers ---

// newTestLogger returns a logger that discards output.
func newTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
