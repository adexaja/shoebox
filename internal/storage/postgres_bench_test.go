package storage

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// newBenchPostgres opens a fresh Postgres backend and cleans the tables so
// the benchmark starts from a known-empty state. DROP + re-open so schema
// changes (e.g. the priority column) are picked up — NewPostgres uses
// CREATE TABLE IF NOT EXISTS, which won't add columns to an existing table.
// Skips if local Postgres is not reachable.
func newBenchPostgres(b *testing.B) *Postgres {
	b.Helper()
	ctx := context.Background()
	schema := fmt.Sprintf("bench_%d", time.Now().UnixNano())
	setup, err := pgxpool.New(ctx, testDSN)
	if err != nil {
		b.Skipf("Postgres not available: %v", err)
	}
	if _, err := setup.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+schema); err != nil {
		setup.Close()
		b.Skipf("Postgres not available: %v", err)
	}
	setup.Close()
	s, err := NewPostgres(ctx, testDSN, schema)
	if err != nil {
		b.Skipf("Postgres not available: %v", err)
	}
	b.Cleanup(func() {
		_ = s.Close()
		pool, err := pgxpool.New(ctx, testDSN)
		if err == nil {
			_, _ = pool.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
			pool.Close()
		}
	})
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

// BenchmarkPostgresEnqueueBatch measures one transaction per batch.
func BenchmarkPostgresEnqueueBatch(b *testing.B) {
	for _, size := range []int{1, 10, 50, 100} {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			s := newBenchPostgres(b)
			ctx := context.Background()
			batch := make([]Message, size)
			b.ReportAllocs()
			b.ReportMetric(float64(size), "messages/op")
			b.ResetTimer()
			for i := range b.N {
				for j := range batch {
					batch[j] = benchMsg(fmt.Sprintf("batch-%d-%d", i, j))
				}
				if err := s.EnqueueBatch(ctx, "q", batch); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*size), "ns/message")
		})
	}
}

// BenchmarkPostgresAckBatch measures deleting processing rows in one transaction.
func BenchmarkPostgresAckBatch(b *testing.B) {
	for _, size := range []int{1, 10, 50, 100} {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			s := newBenchPostgres(b)
			ctx := context.Background()
			b.ReportAllocs()
			b.ReportMetric(float64(size), "messages/op")
			for i := range b.N {
				b.StopTimer()
				messages := make([]Message, size)
				for j := range messages {
					messages[j] = benchMsg(fmt.Sprintf("ack-%d-%d", i, j))
				}
				if err := s.EnqueueBatch(ctx, "q", messages); err != nil {
					b.Fatal(err)
				}
				claimed, err := s.Dequeue(ctx, "q", size)
				if err != nil {
					b.Fatal(err)
				}
				ids := make([]string, len(claimed))
				for j := range claimed {
					ids[j] = claimed[j].ID
				}
				b.StartTimer()
				if err := s.AckBatch(ctx, "q", ids); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*size), "ns/message")
		})
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
