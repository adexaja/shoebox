package storage

import (
	"context"
	"fmt"
	"testing"
)

// benchMsg returns a minimal due message for benchmark loops. The Memory
// backend fills zero CreatedAt/ScheduledAt at Enqueue time anyway.
func benchMsg(id string) Message {
	return Message{ID: id, Payload: []byte("shoebox benchmark payload")}
}

// BenchmarkMemoryEnqueue measures the append cost of the in-memory backend.
func BenchmarkMemoryEnqueue(b *testing.B) {
	m := NewMemory()
	ctx := context.Background()
	msg := benchMsg("x")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg.ID = fmt.Sprintf("m-%d", i)
		if err := m.Enqueue(ctx, "q", msg); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkMemoryDequeue_SteadyState measures one dequeue at a fixed queue
// depth. Each iteration dequeues 1 message and re-enqueues it, so the queue
// stays at `depth` — the cost a dispatcher sees when polling. The re-enqueue
// marks the queue dirty, so the priority sort is included: O(depth log depth)
// per dirty dequeue, O(1) on clean idle polls.
func BenchmarkMemoryDequeue_SteadyState(b *testing.B) {
	const depth = 1000
	m := NewMemory()
	ctx := context.Background()
	for i := 0; i < depth; i++ {
		if err := m.Enqueue(ctx, "q", benchMsg(fmt.Sprintf("m-%d", i))); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msgs, err := m.Dequeue(ctx, "q", 1)
		if err != nil {
			b.Fatal(err)
		}
		if err := m.Enqueue(ctx, "q", benchMsg(fmt.Sprintf("re-%d", i))); err != nil {
			b.Fatal(err)
		}
		_ = msgs[0].ID
	}
}

// BenchmarkMemoryDequeue_Batch measures draining a batch of 100 from a deep
// queue. Each iteration first re-enqueues 100 messages, so the queue depth
// stays near-constant across the run and Dequeue always finds a full batch.
// This is the dispatcher's catch-up path for concurrency > 1.
func BenchmarkMemoryDequeue_Batch(b *testing.B) {
	const depth = 10_000
	m := NewMemory()
	ctx := context.Background()
	for i := 0; i < depth; i++ {
		if err := m.Enqueue(ctx, "q", benchMsg(fmt.Sprintf("m-%d", i))); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	var k int
	for i := 0; i < b.N; i++ {
		// Top up a full batch before each dequeue to keep depth constant.
		for j := 0; j < 100; j++ {
			if err := m.Enqueue(ctx, "q", benchMsg(fmt.Sprintf("re-%d", k))); err != nil {
				b.Fatal(err)
			}
			k++
		}
		msgs, err := m.Dequeue(ctx, "q", 100)
		if err != nil {
			b.Fatal(err)
		}
		_ = msgs
	}
}
