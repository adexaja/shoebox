package shoebox

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// newTestQueue builds a Queue with an isolated Prometheus registry so metrics
// don't collide with the default registry across tests.
func newTestQueue(t *testing.T) *Queue {
	t.Helper()
	reg := prometheus.NewRegistry()
	q, err := New(Options{
		Storage:         Memory,
		Concurrency:     2,
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		MetricsRegistry: reg,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = q.Shutdown(ctx)
	})
	return q
}

// collectCounter reads a single counter value for the given queue label.
func collectCounter(t *testing.T, cv *prometheus.CounterVec, queue string) float64 {
	t.Helper()
	m := &dto.Metric{}
	if err := cv.WithLabelValues(queue).Write(m); err != nil {
		t.Fatalf("write metric: %v", err)
	}
	return m.Counter.GetValue()
}

// TestMetrics_ProcessedAndErrors verifies that MetricsMiddleware increments
// the processed counter on success and the errors counter on failure.
func TestMetrics_ProcessedAndErrors(t *testing.T) {
	q := newTestQueue(t)

	q.Use(
		RecoveryMiddleware(),
		MetricsMiddleware(q.metrics),
	)
	q.Handle("jobs", func(ctx context.Context, m Message) error {
		if string(m.Payload) == "fail" {
			return errors.New("bad payload")
		}
		return nil
	})

	_ = q.Enqueue("jobs", []byte("ok"))
	_ = q.Enqueue("jobs", []byte("fail"))
	_ = q.Enqueue("jobs", []byte("ok"))

	// Wait for all three to be processed.
	waitFor(t, func() bool {
		return collectCounter(t, q.metrics.Processed, "jobs") == 2 &&
			collectCounter(t, q.metrics.Errors, "jobs") == 1
	}, 3*time.Second)
}

// TestPanicRecovery verifies that a panicking handler is recovered, converted
// to an error, and does not crash the broker. The message should be retried
// (or DLQ'd if MaxRetries is 0).
func TestPanicRecovery(t *testing.T) {
	q := newTestQueue(t)
	var panicCount int64

	q.Use(RecoveryMiddleware())
	q.Handle("dangerous", func(ctx context.Context, m Message) error {
		atomic.AddInt64(&panicCount, 1)
		panic("boom")
	}, HandlerOptions{MaxRetries: 0})

	_ = q.Enqueue("dangerous", []byte("explode"))

	// Wait for the panic to be recovered and processed.
	waitFor(t, func() bool {
		return atomic.LoadInt64(&panicCount) >= 1
	}, 3*time.Second)

	// The broker should still be healthy.
	if !q.Healthy() {
		t.Fatal("broker unhealthy after panic recovery")
	}

	// Verify the DLQ got the message (MaxRetries=0 → immediate dead-letter).
	waitFor(t, func() bool {
		stats, _ := q.Stats(context.Background(), "dangerous")
		return stats.Dead >= 1
	}, 3*time.Second)
}

func TestSQLiteBrokerPersistsDeadLetter(t *testing.T) {
	reg := prometheus.NewRegistry()
	q, err := New(Options{
		Storage:         SQLite,
		Path:            t.TempDir() + "/shoebox.db",
		Concurrency:     1,
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		MetricsRegistry: reg,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = q.Shutdown(ctx)
	}()

	q.Handle("orders", func(context.Context, Message) error {
		return errors.New("poison")
	}, HandlerOptions{MaxRetries: 0})
	if err := q.Enqueue("orders", []byte("payload")); err != nil {
		t.Fatal(err)
	}

	waitFor(t, func() bool {
		stats, err := q.Stats(context.Background(), "orders")
		return err == nil && stats.Dead == 1
	}, 3*time.Second)

	dead, err := q.Store().List(context.Background(), "orders.dlq", 10)
	if err != nil {
		t.Fatalf("List DLQ: %v", err)
	}
	if len(dead) != 1 || string(dead[0].Payload) != "payload" {
		t.Fatalf("DLQ = %+v, want one payload", dead)
	}
}

func TestEnqueueRejectsReservedDLQName(t *testing.T) {
	q := newTestQueue(t)
	if err := q.Enqueue("orders.dlq", []byte("payload")); err == nil {
		t.Fatal("Enqueue accepted reserved .dlq queue name")
	}
}

// TestMiddleware_Ordering verifies that middleware applies in registration
// order: the first Use argument is outermost. We verify this by recording
// the order in which middleware "enters" relative to the handler.
func TestMiddleware_Ordering(t *testing.T) {
	q := newTestQueue(t)
	var order []string
	var orderMu sync.Mutex

	record := func(name string) Middleware {
		return func(next HandlerFunc) HandlerFunc {
			return func(ctx context.Context, m Message) error {
				orderMu.Lock()
				order = append(order, "enter:"+name)
				orderMu.Unlock()
				err := next(ctx, m)
				orderMu.Lock()
				order = append(order, "exit:"+name)
				orderMu.Unlock()
				return err
			}
		}
	}

	// Use registers in order: outer, middle, inner.
	q.Use(record("outer"), record("middle"), record("inner"))
	q.Handle("q", func(ctx context.Context, m Message) error {
		orderMu.Lock()
		order = append(order, "handler")
		orderMu.Unlock()
		return nil
	})

	_ = q.Enqueue("q", []byte("x"))

	waitFor(t, func() bool {
		orderMu.Lock()
		defer orderMu.Unlock()
		return len(order) >= 7
	}, 3*time.Second)

	orderMu.Lock()
	defer orderMu.Unlock()

	// Expected: outer enters first, exits last; inner enters last, exits first.
	want := []string{
		"enter:outer", "enter:middle", "enter:inner",
		"handler",
		"exit:inner", "exit:middle", "exit:outer",
	}
	if len(order) < len(want) {
		t.Fatalf("order has %d entries, want at least %d: %v", len(order), len(want), order)
	}
	for i, w := range want {
		if order[i] != w {
			t.Errorf("order[%d] = %q, want %q\nfull: %v", i, order[i], w, order)
			break
		}
	}
}

// TestHealthy_AfterShutdown verifies Healthy returns false after Shutdown.
func TestHealthy_AfterShutdown(t *testing.T) {
	q := newTestQueue(t)
	if !q.Healthy() {
		t.Fatal("Healthy should be true before shutdown")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := q.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	if q.Healthy() {
		t.Fatal("Healthy should be false after shutdown")
	}
}

// TestMetricsHandler_ExposesMetrics verifies the /metrics HTTP handler
// returns the Prometheus exposition format with our metrics present.
func TestMetricsHandler_ExposesMetrics(t *testing.T) {
	q := newTestQueue(t)

	q.Use(MetricsMiddleware(q.metrics))
	q.Handle("webhooks", func(ctx context.Context, m Message) error { return nil })
	_ = q.Enqueue("webhooks", []byte("x"))

	waitFor(t, func() bool {
		return collectCounter(t, q.metrics.Processed, "webhooks") == 1
	}, 3*time.Second)

	// Scrape the handler.
	handler := q.MetricsHandler()
	req, _ := http.NewRequest("GET", "/metrics", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	body := rr.Body.String()
	if !strings.Contains(body, "shoebox_messages_processed_total") {
		t.Errorf("metrics body missing processed counter:\n%s", body)
	}
}

// TestLoggingMiddleware_LogLevels verifies that successful handler
// invocations log at Info (not Warn), and errors log at Warn. We read the
// log output after shutdown to avoid a race between the handler goroutine
// writing and the test reading.
func TestLoggingMiddleware_LogLevels(t *testing.T) {
	var mu sync.Mutex
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&threadSafeWriter{mu: &mu, w: &buf}, nil))

	reg := prometheus.NewRegistry()
	q, _ := New(Options{
		Storage:         Memory,
		Concurrency:     1,
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		MetricsRegistry: reg,
	})
	q.Use(LoggingMiddlewareWith(logger))
	q.Handle("q", func(ctx context.Context, m Message) error {
		if string(m.Payload) == "fail" {
			return errors.New("nope")
		}
		return nil
	})

	_ = q.Enqueue("q", []byte("ok"))
	_ = q.Enqueue("q", []byte("fail"))

	// Drain: wait for both log messages to appear (thread-safe via the
	// mutex-protected writer).
	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return strings.Contains(buf.String(), "shoebox: handled") &&
			strings.Contains(buf.String(), "shoebox: handler error")
	}, 3*time.Second)

	// Shutdown before reading the buffer to ensure no more writes happen.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = q.Shutdown(ctx)

	mu.Lock()
	output := buf.String()
	mu.Unlock()

	if !strings.Contains(output, "shoebox: handled") {
		t.Errorf("expected INFO log for success, got:\n%s", output)
	}
	if !strings.Contains(output, "level=WARN") {
		t.Errorf("expected WARN log for error, got:\n%s", output)
	}
}

// threadSafeWriter wraps an io.Writer with a mutex so slog can write
// concurrently from multiple goroutines safely.
type threadSafeWriter struct {
	mu *sync.Mutex
	w  io.Writer
}

func (t *threadSafeWriter) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.w.Write(p)
}

// waitFor polls cond until it returns true or the timeout expires.
func waitFor(t *testing.T, cond func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition not met within %v", timeout)
}
