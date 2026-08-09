package storage

import (
	"context"
	"sync"
	"time"
)

// Memory is an in-process storage backend. Messages live in a slice protected
// by a mutex. It is fast, dependency-free, and volatile: if the process dies,
// every queued message is gone.
//
// Target scale per the PRD: hundreds to low thousands of messages per
// minute. The linear-scan Ack and Nack are fine at that size; if Memory
// ever needs to scale up, switch to an index by ID.
type Memory struct {
	mu       sync.Mutex
	queues   map[string][]Message
	counters map[string]*QueueStats
}

// NewMemory returns an empty in-memory storage backend.
func NewMemory() *Memory {
	return &Memory{
		queues:   make(map[string][]Message),
		counters: make(map[string]*QueueStats),
	}
}

func (m *Memory) statsFor(queue string) *QueueStats {
	s, ok := m.counters[queue]
	if !ok {
		s = &QueueStats{Queue: queue}
		m.counters[queue] = s
	}
	return s
}

// Enqueue appends a message to the tail of its queue.
func (m *Memory) Enqueue(_ context.Context, queue string, msg Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = time.Now()
	}
	if msg.ScheduledAt.IsZero() {
		msg.ScheduledAt = msg.CreatedAt
	}
	m.queues[queue] = append(m.queues[queue], msg)
	return nil
}

// Dequeue returns up to `limit` messages whose ScheduledAt is in the past,
// in FIFO order. The messages are removed from the pending set; the broker
// is expected to Ack or Nack them.
//
// Implementation note: we hold the lock for the whole scan + pop. That is
// fine for the target scale; if you push this past thousands of messages
// in flight, consider a sharded queue.
func (m *Memory) Dequeue(_ context.Context, queue string, limit int) ([]Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	pending := m.queues[queue]
	if len(pending) == 0 {
		return nil, ErrEmpty
	}

	now := time.Now()
	out := make([]Message, 0, limit)
	kept := pending[:0]

	for _, msg := range pending {
		if len(out) >= limit {
			kept = append(kept, msg)
			continue
		}
		if msg.ScheduledAt.After(now) {
			// Not yet due — keep waiting.
			kept = append(kept, msg)
			continue
		}
		out = append(out, msg)
	}

	m.queues[queue] = kept
	if len(out) == 0 {
		return nil, ErrEmpty
	}
	return out, nil
}

// Ack removes a message from the in-flight set. In Memory there is no
// separate in-flight set; the message is already gone from the queue
// after Dequeue. Ack is therefore a counter increment.
func (m *Memory) Ack(_ context.Context, queue, msgID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.statsFor(queue).Processed++
	_ = msgID
	return nil
}

// Nack records a failed delivery. The broker is responsible for
// re-enqueueing the message with the updated ScheduledAt and Attempts;
// we just bump the retry counter.
func (m *Memory) Nack(_ context.Context, queue, msgID string, _ error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.statsFor(queue)
	s.Retries++
	if msgID == "" {
		s.Dead++
	}
	return nil
}

// Dead records that a message has been moved to the {queue}.dlq shadow
// queue. The broker has already done the move; this is the stat bump.
func (m *Memory) Dead(_ context.Context, queue, _ string, _ error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.statsFor(queue).Dead++
	return nil
}

// Stats returns a snapshot of the queue's counters and current depth.
func (m *Memory) Stats(_ context.Context, queue string) (QueueStats, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := *m.statsFor(queue)
	s.Depth = len(m.queues[queue])
	return s, nil
}

// Close is a no-op for Memory; included for interface compliance.
func (m *Memory) Close() error { return nil }
