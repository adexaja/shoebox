package shoebox

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// TestPause_AccumulatesThenResumeDelivers verifies that messages enqueued
// while a queue is paused accumulate in storage and are delivered only after
// Resume is called.
func TestPause_AccumulatesThenResumeDelivers(t *testing.T) {
	q := newTestQueue(t)
	var count int64

	q.Handle("jobs", func(_ context.Context, m Message) error {
		atomic.AddInt64(&count, 1)
		return nil
	})

	// Pause before enqueuing.
	q.Pause("jobs")

	// Enqueue three messages while paused.
	for i := 0; i < 3; i++ {
		if err := q.Enqueue("jobs", []byte("x")); err != nil {
			t.Fatalf("Enqueue %d: %v", i, err)
		}
	}

	// Wait a bit — nothing should be delivered while paused.
	time.Sleep(300 * time.Millisecond)
	if got := atomic.LoadInt64(&count); got != 0 {
		t.Fatalf("delivered %d while paused, want 0", got)
	}

	// Resume — all three should now be delivered.
	q.Resume("jobs")

	waitFor(t, func() bool {
		return atomic.LoadInt64(&count) >= 3
	}, 3*time.Second)
	if got := atomic.LoadInt64(&count); got != 3 {
		t.Fatalf("delivered %d after resume, want 3", got)
	}
}

// TestPause_Idempotent verifies that calling Pause twice and Resume once
// results in a running queue (Resume wins over the double-Pause).
func TestPause_Idempotent(t *testing.T) {
	q := newTestQueue(t)
	var count int64

	q.Handle("jobs", func(_ context.Context, m Message) error {
		atomic.AddInt64(&count, 1)
		return nil
	})

	q.Pause("jobs")
	q.Pause("jobs") // idempotent
	_ = q.Enqueue("jobs", []byte("x"))
	time.Sleep(200 * time.Millisecond)

	if got := atomic.LoadInt64(&count); got != 0 {
		t.Fatalf("delivered %d while double-paused, want 0", got)
	}

	q.Resume("jobs")
	waitFor(t, func() bool {
		return atomic.LoadInt64(&count) >= 1
	}, 3*time.Second)
}

// TestDrain_ProcessesRemainingThenStops verifies that Drain processes all
// remaining messages and stops the queue's dispatcher, while Resume starts it
// again for messages enqueued afterward.
func TestDrain_ProcessesRemainingThenStops(t *testing.T) {
	q := newTestQueue(t)
	var count int64

	q.Handle("jobs", func(_ context.Context, m Message) error {
		atomic.AddInt64(&count, 1)
		return nil
	})

	// Enqueue three messages.
	for i := 0; i < 3; i++ {
		if err := q.Enqueue("jobs", []byte("x")); err != nil {
			t.Fatalf("Enqueue %d: %v", i, err)
		}
	}

	// Drain should process all three then stop the dispatcher.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := q.Drain(ctx, "jobs"); err != nil {
		t.Fatalf("Drain: %v", err)
	}

	if got := atomic.LoadInt64(&count); got != 3 {
		t.Fatalf("processed %d after drain, want 3", got)
	}

	// Enqueue another message while drained. It remains pending until Resume.
	if err := q.Enqueue("jobs", []byte("after-drain")); err != nil {
		t.Fatalf("Enqueue after drain: %v", err)
	}
	time.Sleep(300 * time.Millisecond)
	if got := atomic.LoadInt64(&count); got != 3 {
		t.Fatalf("processed %d while drained, want 3", got)
	}

	q.Resume("jobs")
	waitFor(t, func() bool { return atomic.LoadInt64(&count) == 4 }, 3*time.Second)
	if got := atomic.LoadInt64(&count); got != 4 {
		t.Fatalf("processed %d after resume, want 4", got)
	}
}

// TestPause_DuringActiveDispatch verifies that calling Pause while handlers
// are running lets in-flight handlers finish but blocks new dequeues.
func TestPause_DuringActiveDispatch(t *testing.T) {
	q := newTestQueue(t)

	var processed int64
	var inflight int64
	block := make(chan struct{})

	q.Handle("jobs", func(_ context.Context, m Message) error {
		atomic.AddInt64(&inflight, 1)
		// Block until the test releases us.
		<-block
		atomic.AddInt64(&processed, 1)
		atomic.AddInt64(&inflight, -1)
		return nil
	})

	// Enqueue one message — it will block in the handler.
	_ = q.Enqueue("jobs", []byte("first"))
	waitFor(t, func() bool { return atomic.LoadInt64(&inflight) >= 1 }, 2*time.Second)

	// Pause while the handler is running.
	q.Pause("jobs")

	// Enqueue more messages while paused — they should NOT be picked up.
	_ = q.Enqueue("jobs", []byte("second"))
	_ = q.Enqueue("jobs", []byte("third"))

	time.Sleep(200 * time.Millisecond)

	// Release the blocked handler.
	close(block)

	// Wait for the first handler to finish.
	waitFor(t, func() bool { return atomic.LoadInt64(&processed) >= 1 }, 2*time.Second)

	// Only one message should have been processed (the one that was
	// already in-flight when we paused). The other two are still queued
	// behind the pause.
	time.Sleep(200 * time.Millisecond)
	if got := atomic.LoadInt64(&processed); got != 1 {
		t.Fatalf("processed %d during pause, want 1", got)
	}

	// Resume — the remaining two should now be delivered.
	q.Resume("jobs")
	waitFor(t, func() bool { return atomic.LoadInt64(&processed) >= 3 }, 3*time.Second)
	if got := atomic.LoadInt64(&processed); got != 3 {
		t.Fatalf("processed %d after resume, want 3", got)
	}
}
