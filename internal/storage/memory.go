package storage

import (
	"context"
	"sort"
	"sync"
	"time"
)

// Memory is an in-process storage backend. Messages live in a slice protected
// by a mutex. It is fast, dependency-free, and volatile: if the process dies,
// every queued message is gone.
//
// Target scale per the PRD: hundreds to low thousands of messages per
// minute. Replay still scans its small DLQ slice; if Memory ever needs more
// scale, switch to indexes by queue and ID.
type Memory struct {
	mu        sync.Mutex
	queues    map[string][]Message
	counters  map[string]*QueueStats
	inflight  map[string]map[string]struct{}
	dirty     map[string]bool
	schedules map[string]Schedule
}

// NewMemory returns an empty in-memory storage backend.
func NewMemory() *Memory {
	return &Memory{
		queues:    make(map[string][]Message),
		counters:  make(map[string]*QueueStats),
		inflight:  make(map[string]map[string]struct{}),
		dirty:     make(map[string]bool),
		schedules: make(map[string]Schedule),
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
	m.dirty[queue] = true
	return nil
}

// Dequeue returns up to `limit` messages whose ScheduledAt is in the past,
// in FIFO order. The messages are removed from the pending set; the broker
// is expected to Ack, Retry, or DeadLetter them.
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

	// Sort pending by (priority DESC, created_at ASC) so the scan picks
	// the highest-priority due message first. Only re-sort when the queue
	// has been modified since the last Dequeue — the dispatcher polls every
	// 250ms, and re-sorting an unchanged slice is wasted work.
	if m.dirty[queue] {
		sort.SliceStable(pending, func(i, j int) bool {
			if pending[i].Priority != pending[j].Priority {
				return pending[i].Priority > pending[j].Priority
			}
			return pending[i].CreatedAt.Before(pending[j].CreatedAt)
		})
		delete(m.dirty, queue)
	}

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
	if m.inflight[queue] == nil {
		m.inflight[queue] = make(map[string]struct{}, len(out))
	}
	for _, msg := range out {
		m.inflight[queue][msg.ID] = struct{}{}
	}
	return out, nil
}

// Ack removes a message from the in-flight set.
func (m *Memory) Ack(_ context.Context, queue, msgID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.inflight[queue][msgID]; !ok {
		return nil
	}
	delete(m.inflight[queue], msgID)
	m.statsFor(queue).Processed++
	return nil
}

// Retry records a failed delivery and re-enqueues the updated message as one
// in-memory transition.
func (m *Memory) Retry(_ context.Context, queue string, msg Message, _ error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.inflight[queue], msg.ID)
	msg.Queue = queue
	m.queues[queue] = append(m.queues[queue], msg)
	m.dirty[queue] = true
	s := m.statsFor(queue)
	s.Errors++
	s.Retries++
	return nil
}

// DeadLetter moves a failed message to its DLQ and records the terminal
// outcome as one in-memory transition.
func (m *Memory) DeadLetter(_ context.Context, queue string, msg Message, err error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.inflight[queue], msg.ID)
	dead := deadLetterMessage(queue, msg, err, time.Now().UTC())
	m.queues[dead.Queue] = append(m.queues[dead.Queue], dead)
	m.dirty[dead.Queue] = true
	m.statsFor(queue).Dead++
	return nil
}

// Replay moves a DLQ message back to its source queue as one in-memory
// transition.
func (m *Memory) Replay(_ context.Context, queue, msgID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	dlq := queue + ".dlq"
	pending := m.queues[dlq]
	for i, msg := range pending {
		if msg.ID != msgID {
			continue
		}
		m.queues[queue] = append(m.queues[queue], replayMessage(queue, msg, time.Now().UTC()))
		m.dirty[queue] = true
		m.statsFor(dlq).Processed++
		m.queues[dlq] = append(pending[:i], pending[i+1:]...)
		return nil
	}
	return ErrEmpty
}

// Stats returns a snapshot of the queue's counters and current depth.
func (m *Memory) Stats(_ context.Context, queue string) (QueueStats, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := *m.statsFor(queue)
	s.Depth = len(m.queues[queue])
	return s, nil
}

// List returns up to `limit` messages from a queue without removing them.
// For DLQ inspection: the broker writes dead messages to {queue}.dlq, and
// callers use List to browse them. The messages are returned in FIFO order.
func (m *Memory) List(_ context.Context, queue string, limit int) ([]Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	pending := m.queues[queue]
	if len(pending) == 0 {
		return nil, ErrEmpty
	}
	if limit > len(pending) {
		limit = len(pending)
	}
	out := make([]Message, limit)
	copy(out, pending[:limit])
	return out, nil
}

// Reclaim transitions stale in-flight messages back to pending. For Memory
// this is a no-op: messages are removed from the pending slice on Dequeue
// (not kept in a separate in-flight set), so if the process dies they are
// simply gone. Persistent backends use Reclaim for crash recovery.
func (m *Memory) Reclaim(_ context.Context, _ string) error { return nil }

func (m *Memory) CreateSchedule(_ context.Context, s Schedule) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.schedules[s.ID]; exists {
		return ErrScheduleExists
	}
	m.schedules[s.ID] = s
	return nil
}

func (m *Memory) UpdateSchedule(_ context.Context, s Schedule) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.schedules[s.ID]; !exists {
		return ErrEmpty
	}
	m.schedules[s.ID] = s
	return nil
}

func (m *Memory) DeleteSchedule(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.schedules, id)
	return nil
}

func (m *Memory) ListSchedules(_ context.Context, queue string) ([]Schedule, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Schedule, 0, len(m.schedules))
	for _, s := range m.schedules {
		if queue == "" || s.Queue == queue {
			s.Payload = append([]byte(nil), s.Payload...)
			out = append(out, s)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (m *Memory) DueSchedules(_ context.Context, now time.Time, limit int) ([]Schedule, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Schedule, 0, limit)
	for _, s := range m.schedules {
		if s.Enabled && !s.NextRunAt.After(now) {
			s.Payload = append([]byte(nil), s.Payload...)
			out = append(out, s)
			if len(out) == limit {
				break
			}
		}
	}
	return out, nil
}

func (m *Memory) RunSchedule(_ context.Context, schedule Schedule, now, next time.Time) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	current, ok := m.schedules[schedule.ID]
	if !ok || !current.Enabled || current.NextRunAt.After(now) ||
		!current.NextRunAt.Equal(schedule.NextRunAt) {
		return false, nil
	}
	m.queues[schedule.Queue] = append(m.queues[schedule.Queue], periodicMessage(schedule, now))
	m.dirty[schedule.Queue] = true
	current.NextRunAt = next
	current.UpdatedAt = time.Now().UTC()
	m.schedules[schedule.ID] = current
	return true, nil
}

// Close is a no-op for Memory; included for interface compliance.
func (m *Memory) Close() error { return nil }
