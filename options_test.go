package shoebox

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestNewValidation(t *testing.T) {
	tests := []struct {
		name string
		opts Options
		want string
	}{
		{name: "sqlite requires path", opts: Options{Storage: SQLite}, want: "requires Options.Path"},
		{name: "postgres requires dsn", opts: Options{Storage: Postgres}, want: "requires Options.DSN"},
		{name: "unknown storage", opts: Options{Storage: StorageKind(99)}, want: "unknown storage kind"},
		{name: "unknown dedupe policy", opts: Options{Storage: Memory, Dedupe: DedupeOptions{Policy: "invalid"}}, want: "unsupported dedupe policy"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := New(tc.opts)
			if err == nil || !contains(err.Error(), tc.want) {
				t.Fatalf("New() error = %v, want text %q", err, tc.want)
			}
		})
	}
}

func TestEnqueueRejectsInvalidQueueName(t *testing.T) {
	q := newTestQueue(t)
	if err := q.Enqueue("bad queue", []byte("payload")); err == nil {
		t.Fatal("Enqueue accepted invalid queue name")
	}
}

func TestEnqueueOptionsCopyMetadata(t *testing.T) {
	original := map[string]string{"trace_id": "abc"}
	opt := WithMetadata(original)
	var got EnqueueOptions
	opt(&got)
	original["trace_id"] = "changed"
	if got.Metadata["trace_id"] != "abc" {
		t.Fatalf("metadata was not copied: %v", got.Metadata)
	}

	when := time.Now().Add(time.Minute)
	Delay(2 * time.Second)(&got)
	Schedule(when)(&got)
	DedupeKey("dedupe")(&got)
	WithPriority(High)(&got)
	if got.Delay != 2*time.Second || !got.Schedule.Equal(when) || got.DedupeKey != "dedupe" || got.Priority != High {
		t.Fatalf("options were not applied: %+v", got)
	}
}

func TestQueueAccessorsAndDepthGauge(t *testing.T) {
	reg := prometheus.NewRegistry()
	q, err := New(Options{
		Storage:         Memory,
		MetricsRegistry: reg,
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer q.Shutdown(context.Background())

	q.Handle("orders", func(context.Context, Message) error { return nil })
	if err := q.Enqueue("orders", []byte("payload")); err != nil {
		t.Fatal(err)
	}
	if len(q.Queues()) != 1 || q.Queues()[0] != "orders" {
		t.Fatalf("Queues() = %v, want [orders]", q.Queues())
	}
	if q.Store() == nil {
		t.Fatal("Store() returned nil")
	}
	q.UpdateDepthGauges(context.Background())
	if _, err := q.Stats(context.Background(), "orders"); err != nil {
		t.Fatalf("Stats(): %v", err)
	}
}

func TestTimeoutMiddleware(t *testing.T) {
	called := false
	h := TimeoutMiddleware(time.Millisecond)(func(ctx context.Context, _ Message) error {
		called = true
		if _, ok := ctx.Deadline(); !ok {
			t.Fatal("handler context has no deadline")
		}
		return nil
	})
	if err := h(context.Background(), Message{}); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("handler was not called")
	}
}

func TestMetricsMiddlewareRejectsNil(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("MetricsMiddleware(nil) did not panic")
		}
	}()
	MetricsMiddleware(nil)
}

func contains(s, sub string) bool {
	return strings.Contains(s, sub)
}
