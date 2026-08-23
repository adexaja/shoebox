package storage

import (
	"context"
	"errors"
	"fmt"
	"github.com/adexaja/shoebox/migrations"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// testDSN is the Postgres connection string used by storage tests and
// benchmarks. It is taken from SHOEBOX_TEST_POSTGRES_DSN when set — CI
// injects the service-container credentials this way. Without it, tests
// fall back to a local development server using trust/peer auth. Tests
// skip when the server is unreachable.
var testDSN = func() string {
	if dsn := os.Getenv("SHOEBOX_TEST_POSTGRES_DSN"); dsn != "" {
		return dsn
	}
	return "host=localhost port=5432 dbname=shoebox user=postgres sslmode=disable"
}()

func TestQuotePostgresIdentifier(t *testing.T) {
	got, err := quotePostgresIdentifier(`worker"queue`)
	if err != nil {
		t.Fatal(err)
	}
	if got != `"worker""queue"` {
		t.Fatalf("quoted identifier = %q, want %q", got, `"worker""queue"`)
	}
	if _, err := quotePostgresIdentifier(""); err == nil {
		t.Fatal("expected empty identifier to fail")
	}
	if _, err := quotePostgresIdentifier("bad\x00schema"); err == nil {
		t.Fatal("expected NUL-containing identifier to fail")
	}
}

// skipIfNoPostgres skips the test if the local Postgres is not reachable.
func skipIfNoPostgres(t *testing.T) {
	t.Helper()
	s, err := NewPostgres(context.Background(), testDSN)
	if err != nil {
		t.Skipf("Postgres not available: %v", err)
	}
	s.Close()
}

// newTestPostgres opens a fresh Postgres backend and cleans the tables so
// each test starts from a known-empty state.
func newTestPostgres(t *testing.T) *Postgres {
	t.Helper()
	skipIfNoPostgres(t)
	s, err := NewPostgres(context.Background(), testDSN)
	if err != nil {
		t.Fatalf("NewPostgres: %v", err)
	}
	// Reset to a fresh database so each test starts from a known-empty
	// state. The migration table must go too, otherwise the re-open below
	// would trust its recorded version and skip rebuilding the schema.
	ctx := context.Background()
	for _, table := range []string{
		"shoebox_messages", "shoebox_stats", "shoebox_schema_migrations",
	} {
		if _, err := s.pool.Exec(ctx, "DROP TABLE IF EXISTS "+table); err != nil {
			s.Close()
			t.Fatalf("drop %s: %v", table, err)
		}
	}
	s.Close()

	s, err = NewPostgres(ctx, testDSN)
	if err != nil {
		t.Fatalf("NewPostgres (re-open): %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func mustPgEnqueue(t *testing.T, s *Postgres, queue string, msg Message) {
	t.Helper()
	if err := s.Enqueue(context.Background(), queue, msg); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
}

func TestPostgres_EnqueueDequeue(t *testing.T) {
	s := newTestPostgres(t)
	ctx := context.Background()

	for _, p := range []string{"a", "b", "c"} {
		mustPgEnqueue(t, s, "q", Message{ID: p, Payload: []byte(p)})
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

func TestPostgres_DequeueFIFOOrder(t *testing.T) {
	s := newTestPostgres(t)
	ctx := context.Background()
	for i, p := range []string{"a", "b", "c", "d", "e"} {
		mustPgEnqueue(t, s, "q", Message{ID: p, Payload: []byte{byte(i)}})
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

func TestPostgres_DequeueVisibleAtFiltering(t *testing.T) {
	s := newTestPostgres(t)
	ctx := context.Background()

	mustPgEnqueue(t, s, "q", Message{ID: "due", ScheduledAt: time.Now().Add(-time.Second)})
	mustPgEnqueue(t, s, "q", Message{ID: "later", ScheduledAt: time.Now().Add(80 * time.Millisecond)})
	mustPgEnqueue(t, s, "q", Message{ID: "due2", ScheduledAt: time.Now().Add(-time.Second)})

	got, err := s.Dequeue(ctx, "q", 5)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if len(got) != 2 || got[0].ID != "due" || got[1].ID != "due2" {
		t.Fatalf("got %v, want [due due2]", ids(got))
	}

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

func TestPostgres_EmptyQueueReturnsErrEmpty(t *testing.T) {
	s := newTestPostgres(t)
	if _, err := s.Dequeue(context.Background(), "q", 5); !errors.Is(err, ErrEmpty) {
		t.Fatalf("err = %v, want ErrEmpty", err)
	}
}

func TestPostgres_AckRemovesMessage(t *testing.T) {
	s := newTestPostgres(t)
	ctx := context.Background()
	mustPgEnqueue(t, s, "q", Message{ID: "x", Payload: []byte("x")})
	mustPgEnqueue(t, s, "q", Message{ID: "y", Payload: []byte("y")})

	if _, err := s.Dequeue(ctx, "q", 1); err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if err := s.Ack(ctx, "q", "x"); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	got, _ := s.Dequeue(ctx, "q", 10)
	if len(got) != 1 || got[0].ID != "y" {
		t.Fatalf("after ack, got %v, want [y]", ids(got))
	}
}

func TestPostgres_Stats(t *testing.T) {
	s := newTestPostgres(t)
	ctx := context.Background()
	mustPgEnqueue(t, s, "q", Message{ID: "ok", Payload: []byte("x")})
	mustPgEnqueue(t, s, "q", Message{ID: "retry", Payload: []byte("y")})

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

func TestPostgres_StatsDepthExcludesInFlight(t *testing.T) {
	s := newTestPostgres(t)
	ctx := context.Background()
	for _, p := range []string{"a", "b", "c"} {
		mustPgEnqueue(t, s, "q", Message{ID: p, Payload: []byte(p)})
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

func TestPostgres_CrashRecovery(t *testing.T) {
	ctx := context.Background()
	skipIfNoPostgres(t)

	// Open, enqueue, dequeue (→ processing), close without acking.
	s1, err := NewPostgres(ctx, testDSN)
	if err != nil {
		t.Fatalf("NewPostgres s1: %v", err)
	}
	// Clean slate for this test.
	s1.pool.Exec(ctx, `DELETE FROM shoebox_messages`)
	s1.pool.Exec(ctx, `DELETE FROM shoebox_stats`)

	mustPgEnqueue(t, s1, "q", Message{ID: "survivor", Payload: []byte("data")})

	msgs, err := s1.Dequeue(ctx, "q", 1)
	if err != nil {
		t.Fatalf("Dequeue s1: %v", err)
	}
	if len(msgs) != 1 || msgs[0].ID != "survivor" {
		t.Fatalf("got %v, want [survivor]", ids(msgs))
	}
	s1.Close()

	// Reopen — Reclaim should transition 'processing' → 'pending'.
	s2, err := NewPostgres(ctx, testDSN)
	if err != nil {
		t.Fatalf("NewPostgres s2: %v", err)
	}
	defer func() {
		s2.pool.Exec(ctx, `DELETE FROM shoebox_messages`)
		s2.pool.Exec(ctx, `DELETE FROM shoebox_stats`)
		s2.Close()
	}()

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

func TestPostgres_DeadLetterFlow(t *testing.T) {
	s := newTestPostgres(t)
	ctx := context.Background()
	mustPgEnqueue(t, s, "orders", Message{ID: "poison", Payload: []byte("bad")})

	msgs, err := s.Dequeue(ctx, "orders", 1)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if err := s.Dead(ctx, "orders", msgs[0].ID, errors.New("handler failed")); err != nil {
		t.Fatalf("Dead: %v", err)
	}

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

func TestPostgres_ListEmptyReturnsErrEmpty(t *testing.T) {
	s := newTestPostgres(t)
	if _, err := s.List(context.Background(), "q", 10); !errors.Is(err, ErrEmpty) {
		t.Fatalf("err = %v, want ErrEmpty", err)
	}
}

func TestPostgres_MetadataRoundTrip(t *testing.T) {
	s := newTestPostgres(t)
	ctx := context.Background()
	meta := map[string]string{"trace_id": "abc-123", "url": "https://example.com/hook"}
	mustPgEnqueue(t, s, "q", Message{ID: "m", Metadata: meta, Payload: []byte("x")})

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

func TestPostgres_Reclaim(t *testing.T) {
	s := newTestPostgres(t)
	ctx := context.Background()
	mustPgEnqueue(t, s, "q", Message{ID: "x", Payload: []byte("x")})

	if _, err := s.Dequeue(ctx, "q", 1); err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if err := s.Reclaim(ctx, "q"); err != nil {
		t.Fatalf("Reclaim: %v", err)
	}

	got, err := s.Dequeue(ctx, "q", 1)
	if err != nil {
		t.Fatalf("Dequeue after reclaim: %v", err)
	}
	if len(got) != 1 || got[0].ID != "x" {
		t.Fatalf("got %v, want [x]", ids(got))
	}
}

// TestPostgres_ConcurrentDequeueNoDoubleDelivery is the E2-S4 regression
// test. Two goroutines dequeue concurrently from the same queue using
// separate connections. FOR UPDATE SKIP LOCKED guarantees no message is
// delivered to both consumers.
func TestPostgres_ConcurrentDequeueNoDoubleDelivery(t *testing.T) {
	skipIfNoPostgres(t)

	// Each consumer needs its own Postgres instance (own pool) so they
	// don't share a transaction. But they share the same database table.
	s1, err := NewPostgres(context.Background(), testDSN)
	if err != nil {
		t.Fatalf("NewPostgres s1: %v", err)
	}
	defer s1.Close()
	ctx := context.Background()
	s1.pool.Exec(ctx, `DELETE FROM shoebox_messages`)
	s1.pool.Exec(ctx, `DELETE FROM shoebox_stats`)

	s2, err := NewPostgres(context.Background(), testDSN)
	if err != nil {
		t.Fatalf("NewPostgres s2: %v", err)
	}
	defer func() {
		s2.pool.Exec(ctx, `DELETE FROM shoebox_messages`)
		s2.pool.Exec(ctx, `DELETE FROM shoebox_stats`)
		s2.Close()
	}()

	// Enqueue 50 messages.
	const total = 50
	for i := 0; i < total; i++ {
		mustPgEnqueue(t, s1, "shared", Message{
			ID:      fmt.Sprintf("msg-%d", i),
			Payload: []byte("x"),
		})
	}

	var got atomic.Int64
	var wg sync.WaitGroup
	wg.Add(2)

	// Consumer 1.
	go func() {
		defer wg.Done()
		for {
			msgs, err := s1.Dequeue(ctx, "shared", 5)
			if errors.Is(err, ErrEmpty) {
				return
			}
			if err != nil {
				t.Errorf("consumer1 dequeue: %v", err)
				return
			}
			got.Add(int64(len(msgs)))
		}
	}()

	// Consumer 2.
	go func() {
		defer wg.Done()
		for {
			msgs, err := s2.Dequeue(ctx, "shared", 5)
			if errors.Is(err, ErrEmpty) {
				return
			}
			if err != nil {
				t.Errorf("consumer2 dequeue: %v", err)
				return
			}
			got.Add(int64(len(msgs)))
		}
	}()

	wg.Wait()

	if delivered := got.Load(); delivered != total {
		t.Errorf("total delivered = %d, want %d (double-delivery detected)", delivered, total)
	}
}

// upMigration returns the dialect's up migration with the given version.
func upMigration(t *testing.T, dialect string, version int) migrations.Migration {
	t.Helper()
	ups, err := migrations.Up(dialect)
	if err != nil {
		t.Fatalf("migrations.Up(%s): %v", dialect, err)
	}
	for _, m := range ups {
		if m.Version == version {
			return m
		}
	}
	t.Fatalf("no %s up migration with version %d", dialect, version)
	return migrations.Migration{}
}

// dropPostgresTables removes every shoebox table, leaving an empty database.
func dropPostgresTables(t *testing.T, s *Postgres) {
	t.Helper()
	for _, table := range []string{
		"shoebox_messages", "shoebox_stats", "shoebox_schema_migrations",
	} {
		if _, err := s.pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+table); err != nil {
			t.Fatalf("drop %s: %v", table, err)
		}
	}
}

// TestPostgres_MigrationUpgradeFromV1 simulates a database created before
// the migration runner existed, carrying only the 0001 schema (no priority
// column, no version rows). Opening it with the current library must detect
// the legacy baseline, apply 0002, and end up fully functional.
func TestPostgres_MigrationUpgradeFromV1(t *testing.T) {
	skipIfNoPostgres(t)
	ctx := context.Background()

	// Empty the database, then apply only migration 0001 by hand.
	s0 := newTestPostgres(t)
	dropPostgresTables(t, s0)
	if _, err := s0.pool.Exec(ctx, upMigration(t, "postgres", 1).SQL); err != nil {
		t.Fatalf("apply 0001: %v", err)
	}
	s0.Close()

	// Open with the current library: baseline detection must record 1 and
	// migration 0002 must be applied on open.
	s, err := NewPostgres(ctx, testDSN)
	if err != nil {
		t.Fatalf("NewPostgres on v1 database: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	// Priority ordering (0002's column) must work end to end.
	mustPgEnqueue(t, s, "q", Message{ID: "low", Payload: []byte("low")})
	mustPgEnqueue(t, s, "q", Message{ID: "high", Payload: []byte("high"), Priority: 2})
	got, err := s.Dequeue(ctx, "q", 10)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if len(got) != 2 || got[0].ID != "high" || got[1].ID != "low" {
		t.Fatalf("dequeue after upgrade = [%s %s], want [high low]",
			got[0].ID, got[1].ID)
	}

	var version int
	if err := s.pool.QueryRow(ctx,
		`SELECT MAX(version) FROM shoebox_schema_migrations`).Scan(&version); err != nil {
		t.Fatalf("read version: %v", err)
	}
	if version != 2 {
		t.Fatalf("schema version after upgrade = %d, want 2", version)
	}
}

// TestPostgres_ConcurrentOpenMigratesOnce verifies the advisory lock
// serialises concurrent opens: several processes opening the same fresh
// database all succeed, and each migration is recorded exactly once.
func TestPostgres_ConcurrentOpenMigratesOnce(t *testing.T) {
	skipIfNoPostgres(t)
	ctx := context.Background()

	// Empty database.
	s0 := newTestPostgres(t)
	dropPostgresTables(t, s0)
	s0.Close()

	const openers = 4
	var wg sync.WaitGroup
	errCh := make(chan error, openers)
	for range openers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, err := NewPostgres(ctx, testDSN)
			if err != nil {
				errCh <- err
				return
			}
			defer s.Close()
			if err := s.Enqueue(ctx, "q", Message{
				ID: NewMessageID(), Payload: []byte("m"),
			}); err != nil {
				errCh <- err
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent open: %v", err)
	}

	s, err := NewPostgres(ctx, testDSN)
	if err != nil {
		t.Fatalf("verify open: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	// One row per applied migration (0001, 0002): the version primary key
	// plus the advisory lock make duplicates impossible.
	var rows int
	if err := s.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM shoebox_schema_migrations`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 2 {
		t.Fatalf("migration rows after %d concurrent opens = %d, want 2", openers, rows)
	}

	// Every opener's enqueue survived: migration and message traffic did
	// not interfere.
	var depth int
	if err := s.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM shoebox_messages`).Scan(&depth); err != nil {
		t.Fatal(err)
	}
	if depth != openers {
		t.Fatalf("messages enqueued during concurrent open = %d, want %d", depth, openers)
	}
}
