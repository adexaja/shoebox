package dlq

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/adexaja/shoebox/internal/storage"
)

func newTestManager(t *testing.T) (*Manager, storage.Storage) {
	t.Helper()
	store := storage.NewMemory()
	return NewManager(store), store
}

// TestDLQ_List verifies that dead-lettered messages are listable through the
// Manager.
func TestDLQ_List(t *testing.T) {
	mgr, store := newTestManager(t)
	ctx := context.Background()

	// Simulate a message that has been dead-lettered: enqueue it to the
	// shadow queue with error info.
	msg := storage.Message{
		ID:       "poison-1",
		Queue:    "orders.dlq",
		Payload:  []byte(`{"bad":"data"}`),
		Attempts: 5,
		Error:    "handler panic: nil pointer dereference",
		DeadAt:   time.Now(),
	}
	if err := store.Enqueue(ctx, "orders.dlq", msg); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	records, err := mgr.List(ctx, "orders", 10)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}
	r := records[0]
	if r.OriginalQueue != "orders" {
		t.Errorf("OriginalQueue = %q, want %q", r.OriginalQueue, "orders")
	}
	if r.ErrorMessage != msg.Error {
		t.Errorf("ErrorMessage = %q, want %q", r.ErrorMessage, msg.Error)
	}
	if r.Attempts != 5 {
		t.Errorf("Attempts = %d, want 5", r.Attempts)
	}
}

// TestDLQ_Inspect verifies finding a single record by ID.
func TestDLQ_Inspect(t *testing.T) {
	mgr, store := newTestManager(t)
	ctx := context.Background()

	_ = store.Enqueue(ctx, "orders.dlq", storage.Message{ID: "a", Payload: []byte("x"), Error: "boom"})
	_ = store.Enqueue(ctx, "orders.dlq", storage.Message{ID: "b", Payload: []byte("y"), Error: "kaboom"})

	r, err := mgr.Inspect(ctx, "orders", "b")
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if r.ID != "b" {
		t.Errorf("ID = %q, want %q", r.ID, "b")
	}
	if r.ErrorMessage != "kaboom" {
		t.Errorf("ErrorMessage = %q, want %q", r.ErrorMessage, "kaboom")
	}
}

// TestDLQ_InspectNotFound returns ErrEmpty for a missing ID.
func TestDLQ_InspectNotFound(t *testing.T) {
	mgr, store := newTestManager(t)
	ctx := context.Background()
	_ = store.Enqueue(ctx, "orders.dlq", storage.Message{ID: "a"})

	_, err := mgr.Inspect(ctx, "orders", "nonexistent")
	if !errors.Is(err, storage.ErrEmpty) {
		t.Errorf("err = %v, want ErrEmpty", err)
	}
}

// TestDLQ_Replay verifies that a dead-lettered message is moved back to the
// source queue and removed from the DLQ.
func TestDLQ_Replay(t *testing.T) {
	mgr, store := newTestManager(t)
	ctx := context.Background()

	msg := storage.Message{
		ID:       "recoverable",
		Queue:    "orders.dlq",
		Payload:  []byte("retry-me"),
		Attempts: 3,
		Error:    "transient failure",
	}
	_ = store.Enqueue(ctx, "orders.dlq", msg)

	// Replay it back to "orders".
	if err := mgr.Replay(ctx, "orders", "recoverable"); err != nil {
		t.Fatalf("Replay: %v", err)
	}

	// The message should now be dequeueable from the source queue.
	got, err := store.Dequeue(ctx, "orders", 1)
	if err != nil {
		t.Fatalf("Dequeue from source after replay: %v", err)
	}
	if len(got) != 1 || got[0].ID != "recoverable" {
		t.Fatalf("got %v, want [recoverable]", got)
	}
	if string(got[0].Payload) != "retry-me" {
		t.Errorf("payload = %q, want %q", string(got[0].Payload), "retry-me")
	}
	// Error and DeadAt should be cleared on replay.
	if got[0].Error != "" {
		t.Errorf("Error = %q, want empty after replay", got[0].Error)
	}
	if !got[0].DeadAt.IsZero() {
		t.Errorf("DeadAt = %v, want zero after replay", got[0].DeadAt)
	}

	// The DLQ should now be empty.
	_, err = mgr.List(ctx, "orders", 10)
	if !errors.Is(err, storage.ErrEmpty) {
		t.Errorf("after replay, DLQ List err = %v, want ErrEmpty", err)
	}
}

// TestDLQ_ReplayNotFound returns ErrEmpty for a missing ID.
func TestDLQ_ReplayNotFound(t *testing.T) {
	mgr, _ := newTestManager(t)
	if err := mgr.Replay(context.Background(), "orders", "nope"); !errors.Is(err, storage.ErrEmpty) {
		t.Errorf("err = %v, want ErrEmpty", err)
	}
}

// TestDLQ_ListEmpty returns ErrEmpty when the DLQ has no messages.
func TestDLQ_ListEmpty(t *testing.T) {
	mgr, _ := newTestManager(t)
	_, err := mgr.List(context.Background(), "orders", 10)
	if !errors.Is(err, storage.ErrEmpty) {
		t.Errorf("err = %v, want ErrEmpty", err)
	}
}
