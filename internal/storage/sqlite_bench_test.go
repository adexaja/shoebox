package storage

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
)

// newBenchSQLite opens a fresh SQLite database in a temp dir. Path differs
// per bench so runs don't share data.
func newBenchSQLite(b *testing.B) *SQLite {
	b.Helper()
	s, err := NewSQLite(context.Background(), filepath.Join(b.TempDir(), "bench.db"))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = s.Close() })
	return s
}

// BenchmarkSQLiteEnqueue measures a single Enqueue — one transaction
// (journal + fsync). This is disk-bound; the number reflects the SQLite
// WAL-less default durability cost, not the broker.
func BenchmarkSQLiteEnqueue(b *testing.B) {
	s := newBenchSQLite(b)
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

// BenchmarkSQLiteEnqueueBatch measures one transaction per batch.
func BenchmarkSQLiteEnqueueBatch(b *testing.B) {
	for _, size := range []int{1, 10, 50, 100} {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			s := newBenchSQLite(b)
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

// BenchmarkSQLiteAckBatch measures deleting processing rows in one transaction.
func BenchmarkSQLiteAckBatch(b *testing.B) {
	for _, size := range []int{1, 10, 50, 100} {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			s := newBenchSQLite(b)
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

// BenchmarkSQLiteDequeue measures the dequeue path at a fixed queue depth:
// a transaction that SELECTs the next due message and transitions it to
// 'processing'. Each iteration Acks the previous message and re-enqueues one
// to keep the queue at `depth` (steady-state dispatcher behaviour).
func BenchmarkSQLiteDequeue(b *testing.B) {
	const depth = 1000
	s := newBenchSQLite(b)
	ctx := context.Background()

	refill := func(prefix string, from int) {
		for i := 0; i < depth; i++ {
			msg := benchMsg(fmt.Sprintf("%s-%d", prefix, from+i))
			if err := s.Enqueue(ctx, "q", msg); err != nil {
				b.Fatal(err)
			}
		}
	}
	refill("fill", 0)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msgs, err := s.Dequeue(ctx, "q", 1)
		if err == ErrEmpty {
			// Preserve depth: dequeue consumed a batch ahead of our steady
			// re-enqueue pace.
			refill("refill", i)
			continue
		}
		if err != nil {
			b.Fatal(err)
		}
		if err := s.Ack(ctx, "q", msgs[0].ID); err != nil {
			b.Fatal(err)
		}
		if err := s.Enqueue(ctx, "q", benchMsg(fmt.Sprintf("re-%d", i))); err != nil {
			b.Fatal(err)
		}
	}
}
