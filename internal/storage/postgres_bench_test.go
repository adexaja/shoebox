package storage

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// newBenchPostgres opens a fresh Postgres backend and cleans the tables so
// the benchmark starts from a known-empty state. DROP + re-open so schema
// changes (e.g. the priority column) are picked up — NewPostgres uses
// CREATE TABLE IF NOT EXISTS, which won't add columns to an existing table.
// Skips if local Postgres is not reachable.
func newBenchPostgres(b *testing.B) *Postgres {
	b.Helper()
	s, err := NewPostgres(context.Background(), testDSN)
	if err != nil {
		b.Skipf("Postgres not available: %v", err)
	}
	ctx := context.Background()
	if _, err := s.pool.Exec(ctx, `DROP TABLE IF EXISTS shoebox_messages`); err != nil {
		s.Close()
		b.Fatalf("drop messages: %v", err)
	}
	if _, err := s.pool.Exec(ctx, `DROP TABLE IF EXISTS shoebox_stats`); err != nil {
		s.Close()
		b.Fatalf("drop stats: %v", err)
	}
	s.Close()

	s, err = NewPostgres(ctx, testDSN)
	if err != nil {
		b.Fatalf("NewPostgres (re-open): %v", err)
	}
	b.Cleanup(func() { s.Close() })
	return s
}

// BenchmarkPostgresEnqueue measures a single Enqueue — one round-trip insert
// in its own transaction. Network + commit bound; the number reflects
// Postgres durability cost, not the broker.
func BenchmarkPostgresEnqueue(b *testing.B) {
	s := newBenchPostgres(b)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg := benchMsg(fmt.Sprintf("m-%d", i))
		if err := s.Enqueue(ctx, "q", msg); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPostgresDequeue measures the dequeue path at a fixed queue depth:
// a transaction that SELECTs the next due message (FOR UPDATE SKIP LOCKED)
// and transitions it to 'processing'. Each iteration Acks the dequeued
// message and re-enqueues one to keep the queue at `depth` (steady-state
// dispatcher behaviour).
func BenchmarkPostgresDequeue(b *testing.B) {
	const depth = 1000
	s := newBenchPostgres(b)
	ctx := context.Background()

	refill := func(prefix string, from int) {
		for i := 0; i < depth; i++ {
			msg := benchMsg(fmt.Sprintf("%s-%d", prefix, from+i))
			if err := s.Enqueue(ctx, "q", msg); err != nil {
				b.Fatal(err)
			}
		}
	}
	refill("pre", 0)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msgs, err := s.Dequeue(ctx, "q", 1)
		if errors.Is(err, ErrEmpty) {
			// Queue drained mid-run (shouldn't happen at steady state);
			// top it back up and try once more.
			refill("re", i)
			msgs, err = s.Dequeue(ctx, "q", 1)
		}
		if err != nil {
			b.Fatal(err)
		}
		if len(msgs) > 0 {
			if err := s.Ack(ctx, "q", msgs[0].ID); err != nil {
				b.Fatal(err)
			}
		}
		if err := s.Enqueue(ctx, "q", benchMsg(fmt.Sprintf("re-%d", i))); err != nil {
			b.Fatal(err)
		}
	}
}
