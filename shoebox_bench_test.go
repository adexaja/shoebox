package shoebox

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// benchmarkDSN is the local development Postgres connection string. The broker
// Postgres throughput benchmark skips if Postgres is unreachable, so the
// default `go test ./...` (which only runs benchmarks under -bench) stays
// green on machines without Postgres.
const benchmarkDSN = "host=localhost port=5432 dbname=shoebox user=postgres password=123 sslmode=disable"

// benchQueue builds a Queue with the given storage kind and a discarding
// logger, with cleanup draining on exit.
func benchQueue(tb testing.TB, kind StorageKind, b *testing.B) *Queue {
	b.Helper()
	opts := Options{
		Storage:     kind,
		Concurrency: 8,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	switch kind {
	case SQLite:
		opts.Path = filepath.Join(b.TempDir(), "bench.db")
	case Postgres:
		opts.DSN = benchmarkDSN
	}
	q, err := New(opts)
	if err != nil {
		if kind == Postgres {
			b.Skipf("Postgres not available: %v", err)
		}
		b.Fatal(err)
	}
	b.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		q.Shutdown(ctx)
	})
	return q
}

// BenchmarkBrokerThroughput_Memory measures end-to-end throughput through the
// public API: fire-and-forget Enqueue into the in-memory backend, dispatched
// across 8 workers, acked on success. This is the headline number users care
// about — messages/sec through the whole broker, not a single primitive.
func BenchmarkBrokerThroughput_Memory(b *testing.B) {
	q := benchQueue(b, Memory, b)

	var processed atomic.Int64
	q.Handle("q", func(_ context.Context, m Message) error {
		processed.Add(1)
		return nil
	})

	// Warm up the handler + dispatcher, then measure the steady-state drain.
	b.ResetTimer()
	enqueueAndWait(b, q, &processed, b.N)
	b.StopTimer()

	// Report throughput as ns/op is meaningless for a drain; report ops as
	// messages via SetBytes so benchstat shows MB/s-equivalent alongside.
	b.SetBytes(int64(len("shoebox benchmark payload")))
}

// Throughput benchmarks for SQLite dequeue uses a fresh temp DB per run.
func BenchmarkBrokerThroughput_SQLite(b *testing.B) {
	q := benchQueue(b, SQLite, b)

	var processed atomic.Int64
	q.Handle("q", func(_ context.Context, m Message) error {
		processed.Add(1)
		return nil
	})

	b.ResetTimer()
	enqueueAndWait(b, q, &processed, b.N)
	b.StopTimer()
	b.SetBytes(int64(len("shoebox benchmark payload")))
}

// BenchmarkBrokerThroughput_Postgres measures end-to-end throughput through
// the public API against the Postgres backend. Each message round-trips
// through two committed transactions (enqueue + dequeue/ack), so the number
// reflects Postgres durability, not the broker. Skips if Postgres is down.
func BenchmarkBrokerThroughput_Postgres(b *testing.B) {
	q := benchQueue(b, Postgres, b)

	var processed atomic.Int64
	q.Handle("q", func(_ context.Context, m Message) error {
		processed.Add(1)
		return nil
	})

	b.ResetTimer()
	enqueueAndWait(b, q, &processed, b.N)
	b.StopTimer()
	b.SetBytes(int64(len("shoebox benchmark payload")))
}

// enqueueAndWait enqueues n messages and blocks until all n have been
// processed (or the loop's deadline hits — the benchmark would then be
// measuring failure).
func enqueueAndWait(b *testing.B, q *Queue, processed *atomic.Int64, n int) {
	b.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for i := 0; i < n; i++ {
		if err := q.Enqueue("q", []byte("shoebox benchmark payload")); err != nil {
			b.Fatal(err)
		}
		if i%1000 == 0 && time.Now().After(deadline) {
			b.Fatalf("timed out at message %d/%d (processed %d)", i, n, processed.Load())
		}
	}
	for {
		if processed.Load() >= int64(n) {
			return
		}
		if time.Now().After(deadline) {
			b.Fatalf("drain timed out: processed %d of %d", processed.Load(), n)
		}
		time.Sleep(time.Millisecond)
	}
}