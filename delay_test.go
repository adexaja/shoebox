package shoebox

import (
	"context"
	"testing"
	"time"
)

// TestDelay_NotVisibleUntilDue is the e2e check for delayed messages: a
// message enqueued with Delay(500ms) must not reach its handler before the
// delay elapses, then must be delivered shortly after.
//
// Every backend filters on ScheduledAt before Dequeue, so the broker cannot
// deliver early: the dispatcher polls on a 250ms tick specifically so
// messages whose time just came up get picked up.
func TestDelay_NotVisibleUntilDue(t *testing.T) {
	q := newTestQueue(t)

	delivered := make(chan time.Time, 1)
	q.Handle("jobs", func(_ context.Context, m Message) error {
		delivered <- time.Now()
		return nil
	})

	const delay = 500 * time.Millisecond
	start := time.Now()
	if err := q.Enqueue("jobs", []byte("x"), Delay(delay)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// Phase 1: the message must stay invisible for the first 200ms (well
	// short of the 500ms delay). A delivery here means the delay was ignored.
	select {
	case <-delivered:
		t.Fatalf("message delivered after %v, want it held for %v", time.Since(start), delay)
	case <-time.After(200 * time.Millisecond):
	}

	// Phase 2: once the delay has passed it must show up, and not before.
	select {
	case got := <-delivered:
		if elapsed := got.Sub(start); elapsed < delay {
			t.Fatalf("delivered after %v, want >= %v", elapsed, delay)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("message never delivered within 3s of the delay elapsing")
	}
}