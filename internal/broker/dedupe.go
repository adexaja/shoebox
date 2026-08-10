package broker

import (
	"strings"
	"sync"
	"time"
)

// DefaultDedupeTTL is the window during which a duplicate enqueue (same queue
// + key) is silently dropped. Five minutes covers most idempotency windows
// (e.g. webhook redelivery bursts) without unbounded memory growth.
const DefaultDedupeTTL = 5 * time.Minute

// dedupeTable suppresses duplicate enqueues within a per-key TTL window.
//
// The key namespace is per-queue: ("orders", "order-123") and ("emails",
// "order-123") are independent. The table is a simple map guarded by a mutex;
// lazy expiry bounds memory by sweeping expired entries when the map grows
// past a threshold.
//
// Design: the dedupe layer lives in the broker (not storage) so every backend
// gets the feature for free (ADR 0006 §Deduplication). The trade-off is that
// dedupe state is lost on restart — acceptable for an idempotency guard, not
// for exactly-once delivery.
type dedupeTable struct {
	mu   sync.Mutex
	seen map[string]time.Time // composite key → expiry
}

func newDedupeTable() *dedupeTable {
	return &dedupeTable{seen: make(map[string]time.Time)}
}

// checkAndAdd returns true if the (queue, key) pair is already live — meaning
// this is a duplicate enqueue that should be silently suppressed. On a new or
// expired key it records the entry and returns false.
//
// The TTL is read from the broker's configured dedupeTTL (defaults to
// DefaultDedupeTTL).
func (d *dedupeTable) checkAndAdd(queue, key string, ttl time.Duration) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	k := queue + "\x00" + key
	now := time.Now()

	if exp, ok := d.seen[k]; ok && now.Before(exp) {
		return true // still live → duplicate
	}

	// New or expired — record (or refresh) the entry.
	d.seen[k] = now.Add(ttl)

	// Opportunistic sweep: when the map grows past the threshold, remove
	// expired entries to bound memory. This is O(n) but runs infrequently.
	if len(d.seen) > 1024 {
		for ek, exp := range d.seen {
			if !now.Before(exp) {
				delete(d.seen, ek)
			}
		}
	}

	return false
}

// purge removes all entries for a queue. Called when a queue's dispatcher is
// permanently stopped (Drain) to release memory.
func (d *dedupeTable) purge(queue string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	prefix := queue + "\x00"
	for k := range d.seen {
		if strings.HasPrefix(k, prefix) {
			delete(d.seen, k)
		}
	}
}
