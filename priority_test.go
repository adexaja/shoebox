package shoebox

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"
)

// newPriorityQueue builds a Queue with concurrency=1 so that messages are
// dispatched one at a time in strict dequeue order. With concurrency>1 the
// dispatcher grabs a batch and processes them concurrently, making the
// handler invocation order nondeterministic even though Dequeue returns
// them in priority order. Concurrency=1 isolates the ORDER BY behavior.
func newPriorityQueue(t *testing.T) *Queue {
	t.Helper()
	q, err := New(Options{
		Storage:     Memory,
		Concurrency: 1,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		q.Shutdown(ctx)
	})
	return q
}

// TestPriority_DequeueOrder verifies that messages with higher priority are
// delivered before lower-priority ones, regardless of enqueue order, when all
// are due at the same time. Ties at the same priority level fall back to FIFO
// (created_at ASC).
//
// This is a public-API e2e test through the full broker → storage → handler
// path. The storage backends all ORDER BY priority DESC, created_at ASC
// (Memory sorts the same way), so the handler receives High, Normal, Low in
// that order.
func TestPriority_DequeueOrder(t *testing.T) {
	q := newPriorityQueue(t)

	order := make(chan string, 3)
	q.Handle("jobs", func(_ context.Context, m Message) error {
		order <- string(m.Payload)
		return nil
	})

	// Pause so we can enqueue all three before the dispatcher grabs any.
	// Without this, concurrency=1 could dequeue "low" before "high" is
	// enqueued — the sort can't order messages that aren't in storage yet.
	q.Pause("jobs")

	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}
	must(q.Enqueue("jobs", []byte("low"), WithPriority(Low)))
	must(q.Enqueue("jobs", []byte("high"), WithPriority(High)))
	must(q.Enqueue("jobs", []byte("normal"), WithPriority(Normal)))

	// Resume — now all three are in storage together and the priority sort
	// determines the dequeue order.
	q.Resume("jobs")

	// Expect delivery in priority order: High (2), Normal (1), Low (0).
	want := []string{"high", "normal", "low"}
	for _, w := range want {
		select {
		case got := <-order:
			if got != w {
				t.Fatalf("got %q, want %q (full order so far: %v)", got, w, want)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for %q", w)
		}
	}
}

// TestPriority_DefaultsToZero verifies that messages enqueued without an
// explicit priority get the default (Low = 0), and that a message with
// explicit High priority is delivered ahead of default-priority messages
// enqueued earlier.
func TestPriority_DefaultsToZero(t *testing.T) {
	q := newPriorityQueue(t)

	order := make(chan string, 2)
	q.Handle("jobs", func(_ context.Context, m Message) error {
		order <- string(m.Payload)
		return nil
	})

	// Pause so both messages are in storage before the dispatcher runs.
	q.Pause("jobs")

	// "default" has no priority option → Low (0).
	if err := q.Enqueue("jobs", []byte("default")); err != nil {
		t.Fatalf("Enqueue default: %v", err)
	}
	// "boosted" has High (2) → should be dequeued first despite being
	// enqueued second.
	if err := q.Enqueue("jobs", []byte("boosted"), WithPriority(High)); err != nil {
		t.Fatalf("Enqueue boosted: %v", err)
	}

	q.Resume("jobs")

	select {
	case got := <-order:
		if got != "boosted" {
			t.Fatalf("expected boosted first, got %q", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for boosted")
	}
	select {
	case got := <-order:
		if got != "default" {
			t.Fatalf("expected default second, got %q", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for default")
	}
}

// TestPriority_FIFOatSameLevel verifies that messages at the same priority
// level are delivered in FIFO (created_at) order, not starved or reordered.
func TestPriority_FIFOatSameLevel(t *testing.T) {
	q := newPriorityQueue(t)

	order := make(chan string, 3)
	q.Handle("jobs", func(_ context.Context, m Message) error {
		order <- string(m.Payload)
		return nil
	})

	// All Normal priority — expect strict FIFO.
	for _, p := range []string{"first", "second", "third"} {
		if err := q.Enqueue("jobs", []byte(p), WithPriority(Normal)); err != nil {
			t.Fatalf("Enqueue %s: %v", p, err)
		}
	}

	want := []string{"first", "second", "third"}
	for _, w := range want {
		select {
		case got := <-order:
			if got != w {
				t.Fatalf("got %q, want %q", got, w)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for %q", w)
		}
	}
}
