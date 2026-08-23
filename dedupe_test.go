package shoebox

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// TestDedupe_SameKeyWithinWindow verifies that two enqueues with the same
// dedupe key within the TTL window result in only one message being delivered.
func TestDedupe_SameKeyWithinWindow(t *testing.T) {
	q := newTestQueue(t)
	var count int64

	q.Handle("jobs", func(_ context.Context, m Message) error {
		atomic.AddInt64(&count, 1)
		return nil
	})

	// Enqueue the same key twice — the second should be silently dropped.
	if err := q.Enqueue("jobs", []byte("first"), DedupeKey("task-1")); err != nil {
		t.Fatalf("Enqueue first: %v", err)
	}
	if err := q.Enqueue("jobs", []byte("second"), DedupeKey("task-1")); err != nil {
		t.Fatalf("Enqueue second: %v", err)
	}

	// Only one message should be processed.
	waitFor(t, func() bool {
		return atomic.LoadInt64(&count) >= 1
	}, 3*time.Second)

	// Give a short window to see if the duplicate sneaks through.
	time.Sleep(200 * time.Millisecond)
	if got := atomic.LoadInt64(&count); got != 1 {
		t.Fatalf("delivered %d messages, want 1 (duplicate not suppressed)", got)
	}
}

// TestDedupe_DifferentKeysBothStored verifies that different dedupe keys do
// not interfere — both messages are delivered.
func TestDedupe_DifferentKeysBothStored(t *testing.T) {
	q := newTestQueue(t)
	var count int64

	q.Handle("jobs", func(_ context.Context, m Message) error {
		atomic.AddInt64(&count, 1)
		return nil
	})

	if err := q.Enqueue("jobs", []byte("a"), DedupeKey("key-a")); err != nil {
		t.Fatalf("Enqueue a: %v", err)
	}
	if err := q.Enqueue("jobs", []byte("b"), DedupeKey("key-b")); err != nil {
		t.Fatalf("Enqueue b: %v", err)
	}

	waitFor(t, func() bool {
		return atomic.LoadInt64(&count) >= 2
	}, 3*time.Second)
	if got := atomic.LoadInt64(&count); got != 2 {
		t.Fatalf("delivered %d messages, want 2", got)
	}
}

// TestDedupe_NoKeyAlwaysDelivered verifies that messages without a dedupe key
// are never suppressed, even if they have identical payloads.
func TestDedupe_NoKeyAlwaysDelivered(t *testing.T) {
	q := newTestQueue(t)
	var count int64

	q.Handle("jobs", func(_ context.Context, m Message) error {
		atomic.AddInt64(&count, 1)
		return nil
	})

	for i := 0; i < 3; i++ {
		if err := q.Enqueue("jobs", []byte("same-payload")); err != nil {
			t.Fatalf("Enqueue %d: %v", i, err)
		}
	}

	waitFor(t, func() bool {
		return atomic.LoadInt64(&count) >= 3
	}, 3*time.Second)
	if got := atomic.LoadInt64(&count); got != 3 {
		t.Fatalf("delivered %d messages, want 3", got)
	}
}

// TestDedupe_PerQueueIsolation verifies that the same dedupe key on different
// queues does not interfere — both are delivered.
func TestDedupe_PerQueueIsolation(t *testing.T) {
	q := newTestQueue(t)
	var count int64

	q.Handle("a", func(_ context.Context, m Message) error {
		atomic.AddInt64(&count, 1)
		return nil
	})
	q.Handle("b", func(_ context.Context, m Message) error {
		atomic.AddInt64(&count, 1)
		return nil
	})

	// Same key, different queues — both should be delivered.
	if err := q.Enqueue("a", []byte("x"), DedupeKey("shared")); err != nil {
		t.Fatalf("Enqueue a: %v", err)
	}
	if err := q.Enqueue("b", []byte("x"), DedupeKey("shared")); err != nil {
		t.Fatalf("Enqueue b: %v", err)
	}

	waitFor(t, func() bool {
		return atomic.LoadInt64(&count) >= 2
	}, 3*time.Second)
	if got := atomic.LoadInt64(&count); got != 2 {
		t.Fatalf("delivered %d messages, want 2 (per-queue isolation failed)", got)
	}
}

func TestDedupe_DurableRequiresPostgres(t *testing.T) {
	for _, kind := range []StorageKind{Memory, SQLite} {
		opts := Options{Storage: kind, Path: t.TempDir() + "/queue.db",
			Dedupe: DedupeOptions{Policy: DedupePolicyDurable}}
		if _, err := New(opts); err == nil {
			t.Fatalf("durable dedupe accepted for storage %d", kind)
		}
	}
}
