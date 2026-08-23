package storage

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

// newTestSQLite opens an in-memory SQLite database for testing. Using
// ":memory:" gives us a fresh, isolated database per test with no file
// cleanup needed.
func newTestSQLite(t *testing.T) *SQLite {
	t.Helper()
	s, err := NewSQLite(context.Background(), ":memory:")
	if err != nil {
		t.Fatalf("NewSQLite: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// mustEnqueueStore is the storage.Storage variant of mustEnqueue (which only
// accepts *Memory). This works for any backend.
func mustEnqueueStore(t *testing.T, s Storage, queue string, msg Message) {
	t.Helper()
	if err := s.Enqueue(context.Background(), queue, msg); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
}

// TestSQLite_EnqueueDequeue verifies basic FIFO enqueue and dequeue.
func TestSQLite_EnqueueDequeue(t *testing.T) {
	s := newTestSQLite(t)
	ctx := context.Background()

	for _, p := range []string{"a", "b", "c"} {
		if err := s.Enqueue(ctx, "q", Message{ID: p, Payload: []byte(p)}); err != nil {
			t.Fatalf("Enqueue %s: %v", p, err)
		}
	}

	got, err := s.Dequeue(ctx, "q", 10)
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
}

// TestSQLite_DequeueFIFOOrder verifies messages come out in the same order
// they went in, across multiple dequeue batches.
func TestSQLite_DequeueFIFOOrder(t *testing.T) {
	s := newTestSQLite(t)
	ctx := context.Background()
	for i, p := range []string{"a", "b", "c", "d", "e"} {
		mustEnqueueStore(t, s, "q", Message{ID: p, Payload: []byte{byte(i)}})
	}

	got, _ := s.Dequeue(ctx, "q", 3)
	if len(got) != 3 {
		t.Fatalf("first dequeue: got %d, want 3", len(got))
	}
	for i, want := range []string{"a", "b", "c"} {
		if got[i].ID != want {
			t.Errorf("first[%d] = %q, want %q", i, got[i].ID, want)
		}
	}

	got2, _ := s.Dequeue(ctx, "q", 10)
	if len(got2) != 2 {
		t.Fatalf("second dequeue: got %d, want 2", len(got2))
	}
	for i, want := range []string{"d", "e"} {
		if got2[i].ID != want {
			t.Errorf("second[%d] = %q, want %q", i, got2[i].ID, want)
		}
	}
}

// TestSQLite_DequeueVisibleAtFiltering verifies scheduled messages are not
// returned until their scheduled time.
func TestSQLite_DequeueVisibleAtFiltering(t *testing.T) {
	s := newTestSQLite(t)
	ctx := context.Background()

	mustEnqueueStore(t, s, "q", Message{ID: "due", ScheduledAt: time.Now().Add(-time.Second)})
	mustEnqueueStore(t, s, "q", Message{ID: "later", ScheduledAt: time.Now().Add(80 * time.Millisecond)})
	mustEnqueueStore(t, s, "q", Message{ID: "due2", ScheduledAt: time.Now().Add(-time.Second)})

	got, err := s.Dequeue(ctx, "q", 5)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if len(got) != 2 || got[0].ID != "due" || got[1].ID != "due2" {
		t.Fatalf("got %v, want [due due2]", ids(got))
	}

	// "later" is not due yet.
	if _, err := s.Dequeue(ctx, "q", 5); !errors.Is(err, ErrEmpty) {
		t.Fatalf("before due: err = %v, want ErrEmpty", err)
	}

	time.Sleep(120 * time.Millisecond)
	got, err = s.Dequeue(ctx, "q", 5)
	if err != nil {
		t.Fatalf("Dequeue after delay: %v", err)
	}
	if len(got) != 1 || got[0].ID != "later" {
		t.Fatalf("got %v, want [later]", ids(got))
	}
}

// TestSQLite_EmptyQueueReturnsErrEmpty verifies the sentinel error.
func TestSQLite_EmptyQueueReturnsErrEmpty(t *testing.T) {
	s := newTestSQLite(t)
	if _, err := s.Dequeue(context.Background(), "q", 5); !errors.Is(err, ErrEmpty) {
		t.Fatalf("err = %v, want ErrEmpty", err)
	}
}

// TestSQLite_AckRemovesMessage verifies Ack deletes the message so it's not
// redelivered.
func TestSQLite_AckRemovesMessage(t *testing.T) {
	s := newTestSQLite(t)
	ctx := context.Background()
	mustEnqueueStore(t, s, "q", Message{ID: "x"})
	mustEnqueueStore(t, s, "q", Message{ID: "y"})

	if _, err := s.Dequeue(ctx, "q", 1); err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if err := s.Ack(ctx, "q", "x"); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	// Only "y" should remain.
	got, _ := s.Dequeue(ctx, "q", 10)
	if len(got) != 1 || got[0].ID != "y" {
		t.Fatalf("after ack, got %v, want [y]", ids(got))
	}
}

// TestSQLite_Stats verifies counters move correctly through the lifecycle.
func TestSQLite_Stats(t *testing.T) {
	s := newTestSQLite(t)
	ctx := context.Background()
	mustEnqueueStore(t, s, "q", Message{ID: "ok"})
	mustEnqueueStore(t, s, "q", Message{ID: "retry"})

	if _, err := s.Dequeue(ctx, "q", 2); err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if err := s.Ack(ctx, "q", "ok"); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if err := s.Nack(ctx, "q", "retry", errors.New("boom")); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	if err := s.Dead(ctx, "q", "retry", errors.New("dead")); err != nil {
		t.Fatalf("Dead: %v", err)
	}

	stats, err := s.Stats(ctx, "q")
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.Processed != 1 {
		t.Errorf("Processed = %d, want 1", stats.Processed)
	}
	if stats.Errors != 1 {
		t.Errorf("Errors = %d, want 1", stats.Errors)
	}
	if stats.Retries != 1 {
		t.Errorf("Retries = %d, want 1", stats.Retries)
	}
	if stats.Dead != 1 {
		t.Errorf("Dead = %d, want 1", stats.Dead)
	}
}

// TestSQLite_StatsDepthExcludesInFlight verifies that once a message is
// dequeued (status='processing') it is no longer counted in Depth.
func TestSQLite_StatsDepthExcludesInFlight(t *testing.T) {
	s := newTestSQLite(t)
	ctx := context.Background()
	for _, p := range []string{"a", "b", "c"} {
		mustEnqueueStore(t, s, "q", Message{ID: p})
	}

	if stats, _ := s.Stats(ctx, "q"); stats.Depth != 3 {
		t.Fatalf("after enqueue, depth = %d, want 3", stats.Depth)
	}
	if _, err := s.Dequeue(ctx, "q", 2); err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if stats, _ := s.Stats(ctx, "q"); stats.Depth != 1 {
		t.Fatalf("after dequeue 2, depth = %d, want 1", stats.Depth)
	}
}

// TestSQLite_CrashRecovery is the E2-S1 regression test. It simulates a crash
// by closing the database mid-flight (leaving messages in 'processing'), then
// reopens and verifies those messages are reclaimed to 'pending' and
// redelivered.
func TestSQLite_CrashRecovery(t *testing.T) {
	ctx := context.Background()

	// Use a temp file so the data survives the close.
	path := filepath.Join(t.TempDir(), "test.db")
	s1, err := NewSQLite(ctx, path)
	if err != nil {
		t.Fatalf("NewSQLite s1: %v", err)
	}

	mustEnqueueStore(t, s1, "q", Message{ID: "survivor", Payload: []byte("data")})

	// Dequeue but DON'T ack — simulates a crash mid-handler.
	msgs, err := s1.Dequeue(ctx, "q", 1)
	if err != nil {
		t.Fatalf("Dequeue s1: %v", err)
	}
	if len(msgs) != 1 || msgs[0].ID != "survivor" {
		t.Fatalf("got %v, want [survivor]", ids(msgs))
	}

	// "Crash": close without acking.
	_ = s1.Close()

	// Reopen — Reclaim should transition 'processing' → 'pending'.
	s2, err := NewSQLite(ctx, path)
	if err != nil {
		t.Fatalf("NewSQLite s2: %v", err)
	}
	defer func() { _ = s2.Close() }()

	// The message must be redelivered.
	msgs2, err := s2.Dequeue(ctx, "q", 1)
	if err != nil {
		t.Fatalf("Dequeue s2: %v", err)
	}
	if len(msgs2) != 1 || msgs2[0].ID != "survivor" {
		t.Fatalf("after recovery, got %v, want [survivor]", ids(msgs2))
	}
	if string(msgs2[0].Payload) != "data" {
		t.Errorf("payload = %q, want %q", string(msgs2[0].Payload), "data")
	}
}

// TestSQLite_DeadLetterFlow verifies that Dead transitions the message to
// status='dead' and List returns it.
func TestSQLite_DeadLetterFlow(t *testing.T) {
	s := newTestSQLite(t)
	ctx := context.Background()
	mustEnqueueStore(t, s, "orders", Message{ID: "poison", Payload: []byte("bad")})

	msgs, err := s.Dequeue(ctx, "orders", 1)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}

	// Mark as dead.
	if err := s.Dead(ctx, "orders", msgs[0].ID, errors.New("handler failed")); err != nil {
		t.Fatalf("Dead: %v", err)
	}

	// List should return it from the source queue (DLQ view queries
	// status='dead' on the same table).
	dead, err := s.List(ctx, "orders", 10)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(dead) != 1 {
		t.Fatalf("got %d dead messages, want 1", len(dead))
	}
	if dead[0].ID != "poison" {
		t.Errorf("dead[0].ID = %q, want %q", dead[0].ID, "poison")
	}
	if dead[0].Error != "handler failed" {
		t.Errorf("dead[0].Error = %q, want %q", dead[0].Error, "handler failed")
	}
}

// TestSQLite_ListEmptyReturnsErrEmpty verifies List on a queue with no dead
// messages returns the sentinel.
func TestSQLite_ListEmptyReturnsErrEmpty(t *testing.T) {
	s := newTestSQLite(t)
	if _, err := s.List(context.Background(), "q", 10); !errors.Is(err, ErrEmpty) {
		t.Fatalf("err = %v, want ErrEmpty", err)
	}
}

// TestSQLite_MetadataRoundTrip verifies metadata survives the JSON
// serialise/deserialise cycle.
func TestSQLite_MetadataRoundTrip(t *testing.T) {
	s := newTestSQLite(t)
	ctx := context.Background()
	meta := map[string]string{"trace_id": "abc-123", "url": "https://example.com/hook"}
	mustEnqueueStore(t, s, "q", Message{ID: "m", Metadata: meta})

	got, err := s.Dequeue(ctx, "q", 1)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d messages, want 1", len(got))
	}
	for k, want := range meta {
		if got[0].Metadata[k] != want {
			t.Errorf("metadata[%q] = %q, want %q", k, got[0].Metadata[k], want)
		}
	}
}

// TestSQLite_Reclaim transitions processing messages back to pending.
func TestSQLite_Reclaim(t *testing.T) {
	s := newTestSQLite(t)
	ctx := context.Background()
	mustEnqueueStore(t, s, "q", Message{ID: "x"})

	// Dequeue to set status='processing'.
	if _, err := s.Dequeue(ctx, "q", 1); err != nil {
		t.Fatalf("Dequeue: %v", err)
	}

	// Reclaim.
	if err := s.Reclaim(ctx, "q"); err != nil {
		t.Fatalf("Reclaim: %v", err)
	}

	// Message should be dequeueable again.
	got, err := s.Dequeue(ctx, "q", 1)
	if err != nil {
		t.Fatalf("Dequeue after reclaim: %v", err)
	}
	if len(got) != 1 || got[0].ID != "x" {
		t.Fatalf("got %v, want [x]", ids(got))
	}
}

// TestSQLite_MigrationUpgradeFromV1 mirrors the Postgres upgrade test: a
// database carrying only the 0001 schema (no priority column, user_version
// 0) must be detected as a legacy database, upgraded to the latest
// migration, and remain fully functional.
func TestSQLite_MigrationUpgradeFromV1(t *testing.T) {
	path := filepath.Join(t.TempDir(), "upgrade.db")
	ctx := context.Background()

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open raw: %v", err)
	}
	if _, err := db.ExecContext(ctx, upMigration(t, "sqlite", 1).SQL); err != nil {
		_ = db.Close()
		t.Fatalf("apply 0001: %v", err)
	}
	_ = db.Close()

	s, err := NewSQLite(ctx, path)
	if err != nil {
		t.Fatalf("NewSQLite on v1 database: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	// Priority ordering (0002's column) must work end to end.
	mustEnqueueStore(t, s, "q", Message{ID: "low", Payload: []byte("low")})
	mustEnqueueStore(t, s, "q", Message{ID: "high", Payload: []byte("high"), Priority: 2})
	got, err := s.Dequeue(ctx, "q", 10)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if len(got) != 2 || got[0].ID != "high" || got[1].ID != "low" {
		t.Fatalf("dequeue after upgrade = %v, want [high low]", ids(got))
	}

	version, err := sqliteUserVersion(ctx, s.db)
	if err != nil {
		t.Fatalf("read user_version: %v", err)
	}
	if version != 3 {
		t.Fatalf("user_version after upgrade = %d, want 3", version)
	}
}
