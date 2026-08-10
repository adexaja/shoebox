package dashboard

import (
	"context"
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

func newTestDashboard(t *testing.T, queues []string) (*Dashboard, storage.Storage) {
	t.Helper()
	store := storage.NewMemory()

	// Pre-enqueue some messages so stats are non-zero.
	for _, q := range queues {
		for i := 0; i < 3; i++ {
			msg := storage.Message{
				ID:        storage.NewMessageID(),
				Queue:     q,
				Payload:   []byte("payload-" + q),
				CreatedAt: time.Now(),
			}
			if err := store.Enqueue(context.Background(), q, msg); err != nil {
				t.Fatalf("Enqueue: %v", err)
			}
		}
	}

	queuesFn := func() []string { return queues }
	dlqMgr := dlq.NewManager(store)
	d := New(store, dlqMgr, queuesFn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	return d, store
}

func TestDashboard_Index(t *testing.T) {
	d, _ := newTestDashboard(t, []string{"orders", "emails"})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()

	d.index(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}
	body := w.Body.String()
	if !strings.Contains(body, "shoebox") {
		t.Fatal("expected 'shoebox' in HTML")
	}
	if !strings.Contains(body, "orders") {
		t.Fatal("expected 'orders' queue in HTML")
	}
	if !strings.Contains(body, "emails") {
		t.Fatal("expected 'emails' queue in HTML")
	}
}

func TestDashboard_StatsPartial(t *testing.T) {
	d, _ := newTestDashboard(t, []string{"orders"})

	req := httptest.NewRequest(http.MethodGet, "/partials/stats", nil)
	w := httptest.NewRecorder()

	d.statsPartial(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}
	body := w.Body.String()
	if !strings.Contains(body, "orders") {
		t.Fatal("expected 'orders' in stats partial")
	}
	// Should contain a depth of 3.
	if !strings.Contains(body, ">3<") {
		t.Fatalf("expected depth=3 in stats partial, got: %s", body)
	}
}

func TestDashboard_DLQPartial_Empty(t *testing.T) {
	d, _ := newTestDashboard(t, []string{"orders"})

	req := httptest.NewRequest(http.MethodGet, "/partials/dlq?queue=orders", nil)
	w := httptest.NewRecorder()

	d.dlqPartial(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}
	body := w.Body.String()
	if !strings.Contains(body, "No dead-lettered") {
		t.Fatalf("expected 'No dead-lettered' message, got: %s", body)
	}
}

func TestDashboard_DLQPartial_MissingQueue(t *testing.T) {
	d, _ := newTestDashboard(t, nil)

	req := httptest.NewRequest(http.MethodGet, "/partials/dlq", nil)
	w := httptest.NewRecorder()

	d.dlqPartial(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestDashboard_NoQueues(t *testing.T) {
	d, _ := newTestDashboard(t, nil)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()

	d.index(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}
	body := w.Body.String()
	if !strings.Contains(body, "No queues registered") {
		t.Fatalf("expected 'No queues registered' message, got: %s", body)
	}
}

func TestDashboard_JSONHandler(t *testing.T) {
	d, _ := newTestDashboard(t, []string{"orders"})

	req := httptest.NewRequest(http.MethodGet, "/api/stats.json", nil)
	w := httptest.NewRecorder()

	d.JSONHandler(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}
	body := w.Body.String()
	if !strings.Contains(body, "orders") {
		t.Fatalf("expected 'orders' in JSON output: %s", body)
	}
	if !strings.Contains(body, `"Depth":3`) {
		t.Fatalf("expected Depth=3 in JSON output: %s", body)
	}
}

func TestDashboard_Register_Routes(t *testing.T) {
	d, _ := newTestDashboard(t, []string{"orders"})

	mux := http.NewServeMux()
	d.Register(mux)

	srv := httptest.NewServer(mux)
	defer srv.Close()

	// Index.
	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("index: %d", resp.StatusCode)
	}

	// Stats partial.
	resp, err = http.Get(srv.URL + "/partials/stats")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("stats: %d", resp.StatusCode)
	}

	// DLQ partial.
	resp, err = http.Get(srv.URL + "/partials/dlq?queue=orders")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("dlq: %d", resp.StatusCode)
	}
}

func TestTruncatePayload(t *testing.T) {
	tests := []struct {
		input  string
		maxLen int
		want   string
	}{
		{"short", 10, "short"},
		{"exactly-10", 10, "exactly-10"},
		{"this is too long", 5, "this …"},
		{"", 5, ""},
	}

	for _, tt := range tests {
		got := truncatePayload([]byte(tt.input), tt.maxLen)
		if got != tt.want {
			t.Errorf("truncatePayload(%q, %d) = %q, want %q", tt.input, tt.maxLen, got, tt.want)
		}
	}
}
