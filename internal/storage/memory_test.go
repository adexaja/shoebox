package storage

import (
	"context"
	"errors"
	"testing"
	"time"
)

func mustEnqueue(t *testing.T, m *Memory, queue string, msg Message) {
	t.Helper()
	if err := m.Enqueue(context.Background(), queue, msg); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
}

// TestDequeue_FIFOOrder verifies messages are returned in the order they were
// enqueued, up to the requested limit.
func TestDequeue_FIFOOrder(t *testing.T) {
	m := NewMemory()
	for i, p := range []string{"a", "b", "c", "d", "e"} {
		mustEnqueue(t, m, "q", Message{ID: p, Payload: []byte{byte(i)}})
	}

	got, err := m.Dequeue(context.Background(), "q", 3)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d messages, want 3", len(got))
	}
	for i, want := range []string{"a", "b", "c"} {
		if got[i].ID != want {
			t.Errorf("msg[%d].ID = %q, want %q", i, got[i].ID, want)
		}
	}

	// The next dequeue must resume in FIFO order — i.e. the kept tail is
	// still ordered, not reversed or reshuffled.
	got2, err := m.Dequeue(context.Background(), "q", 10)
	if err != nil {
		t.Fatalf("Dequeue 2: %v", err)
	}
	for i, want := range []string{"d", "e"} {
		if got2[i].ID != want {
			t.Errorf("msg2[%d].ID = %q, want %q", i, got2[i].ID, want)
		}
	}
}

// TestDequeue_VisibleAtFiltering verifies a message with a future ScheduledAt
// is not returned until that time elapses, and that it is not lost in the
// meantime (the kept-slice compaction must preserve it).
func TestDequeue_VisibleAtFiltering(t *testing.T) {
	m := NewMemory()
	// Due now.
	mustEnqueue(t, m, "q", Message{ID: "due", ScheduledAt: time.Now().Add(-time.Second)})
	// Due in 80ms.
	mustEnqueue(t, m, "q", Message{ID: "later", ScheduledAt: time.Now().Add(80 * time.Millisecond)})
	// Due now.
	mustEnqueue(t, m, "q", Message{ID: "due2", ScheduledAt: time.Now().Add(-time.Second)})

	// First dequeue: only the two due messages, in FIFO order, skipping "later".
	got, err := m.Dequeue(context.Background(), "q", 5)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if len(got) != 2 || got[0].ID != "due" || got[1].ID != "due2" {
		t.Fatalf("got %v, want [due due2]", ids(got))
	}

	// "later" is still pending — dequeue must report empty, not skip it.
	if got, err := m.Dequeue(context.Background(), "q", 5); !errors.Is(err, ErrEmpty) {
		t.Fatalf("before due: got %v err=%v, want ErrEmpty", ids(got), err)
	}

	// After the delay, "later" becomes due and is returned alone.
	time.Sleep(120 * time.Millisecond)
	got, err = m.Dequeue(context.Background(), "q", 5)
	if err != nil {
		t.Fatalf("Dequeue after delay: %v", err)
	}
	if len(got) != 1 || got[0].ID != "later" {
		t.Fatalf("got %v, want [later]", ids(got))
	}
}

// TestDequeue_ZeroScheduledAtIsDue mirrors the broker's Enqueue behavior: a
// message with a zero ScheduledAt is treated as due immediately (Enqueue
// defaults it to CreatedAt/now).
func TestDequeue_ZeroScheduledAtIsDue(t *testing.T) {
	m := NewMemory()
	mustEnqueue(t, m, "q", Message{ID: "x"}) // ScheduledAt zero → Enqueue sets it

	got, err := m.Dequeue(context.Background(), "q", 1)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if len(got) != 1 || got[0].ID != "x" {
		t.Fatalf("got %v, want [x]", ids(got))
	}
}

// TestDequeue_EmptyQueueReturnsErrEmpty verifies the sentinel, which the
// broker relies on (it ignores ErrEmpty to detect an idle queue).
func TestDequeue_EmptyQueueReturnsErrEmpty(t *testing.T) {
	m := NewMemory()
	got, err := m.Dequeue(context.Background(), "q", 5)
	if !errors.Is(err, ErrEmpty) {
		t.Fatalf("err = %v, want ErrEmpty (got=%v)", err, got)
	}
	if got != nil {
		t.Fatalf("got = %v, want nil on empty", got)
	}
}

// TestStats_DepthExcludesInFlight is the key property the broker's drain logic
// depends on: Depth counts only pending messages, not in-flight ones. Once a
// message is dequeued it leaves the pending slice, so Depth drops — even
// before it is Acked.
func TestStats_DepthExcludesInFlight(t *testing.T) {
	m := NewMemory()
	for _, p := range []string{"a", "b", "c"} {
		mustEnqueue(t, m, "q", Message{ID: p})
	}

	if s, _ := m.Stats(context.Background(), "q"); s.Depth != 3 {
		t.Fatalf("after enqueue, depth = %d, want 3", s.Depth)
	}

	if _, err := m.Dequeue(context.Background(), "q", 2); err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	// Two messages are now in-flight (not yet Acked). Depth must reflect only
	// the one still pending.
	if s, _ := m.Stats(context.Background(), "q"); s.Depth != 1 {
		t.Fatalf("after dequeue 2, depth = %d, want 1 (in-flight excluded)", s.Depth)
	}
}

// TestStats_Counters verifies the cumulative counters move on each lifecycle
// transition: Processed on Ack, Retries on Nack, Dead on Dead.
func TestStats_Counters(t *testing.T) {
	m := NewMemory()
	mustEnqueue(t, m, "q", Message{ID: "ok"})
	mustEnqueue(t, m, "q", Message{ID: "retry"})

	if _, err := m.Dequeue(context.Background(), "q", 2); err != nil {
		t.Fatalf("Dequeue: %v", err)
	}

	if err := m.Ack(context.Background(), "q", "ok"); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if err := m.Nack(context.Background(), "q", "retry", errors.New("boom")); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	if err := m.Dead(context.Background(), "q", "retry", errors.New("dead")); err != nil {
		t.Fatalf("Dead: %v", err)
	}

	s, err := m.Stats(context.Background(), "q")
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if s.Processed != 1 {
		t.Errorf("Processed = %d, want 1", s.Processed)
	}
	if s.Retries != 1 {
		t.Errorf("Retries = %d, want 1", s.Retries)
	}
	if s.Dead != 1 {
		t.Errorf("Dead = %d, want 1", s.Dead)
	}
}

func ids(msgs []Message) []string {
	out := make([]string, len(msgs))
	for i, m := range msgs {
		out[i] = m.ID
	}
	return out
}
