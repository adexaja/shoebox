package shoebox

import (
	"context"
	"testing"
	"time"
)

// TestSchedule_VisibleAtTime is the e2e check for absolute scheduling: a
// message enqueued with Schedule(t) must not be visible before t, then must
// be delivered at (or after) t.
func TestSchedule_VisibleAtTime(t *testing.T) {
	q := newTestQueue(t)

	delivered := make(chan time.Time, 1)
	q.Handle("jobs", func(_ context.Context, m Message) error {
		delivered <- time.Now()
		return nil
	})

	target := time.Now().Add(500 * time.Millisecond)
	if err := q.Enqueue("jobs", []byte("x"), Schedule(target)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// Phase 1: not visible before the scheduled time.
	select {
	case <-delivered:
		t.Fatalf("message delivered before scheduled time %v", target)
	case <-time.After(200 * time.Millisecond):
	}

	// Phase 2: visible at/after the scheduled time.
	select {
	case got := <-delivered:
		if got.Before(target) {
			t.Fatalf("delivered %v before scheduled time %v", got, target)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("message never delivered at scheduled time %v", target)
	}
}