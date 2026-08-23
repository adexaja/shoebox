package shoebox

import (
	"log/slog"
	"time"

	"github.com/adexaja/shoebox/internal/retry"
	"github.com/prometheus/client_golang/prometheus"
)

// StorageKind selects a storage backend.
type StorageKind int

const (
	// Memory is the in-process ring buffer. Fast, volatile, zero dependencies.
	Memory StorageKind = iota
	// SQLite uses an embedded database file. Zero config, survives restarts.
	SQLite
	// Postgres uses an external Postgres database via SKIP LOCKED.
	Postgres
)

// Options configures a new Queue.
type Options struct {
	// Storage selects the storage backend. Required.
	Storage StorageKind

	// Path is used by SQLite (file path) and ignored by other backends.
	Path string

	// DSN is used by Postgres and ignored by other backends.
	DSN string

	// Schema is the PostgreSQL schema used by the queue tables. It defaults to
	// "public" and is ignored by other backends.
	Schema string

	// Concurrency is the per-queue worker pool size. Defaults to 4.
	Concurrency int

	// Logger receives structured events. Defaults to slog.Default().
	Logger *slog.Logger

	// MetricsRegistry is the Prometheus registry used by MetricsMiddleware
	// and MetricsHandler. If nil, prometheus.DefaultRegisterer is used.
	// Set this to a custom registry when running multiple shoebox instances
	// in the same process or to isolate metrics in tests.
	MetricsRegistry *prometheus.Registry

	// Dedupe selects the in-memory deduplication policy. An empty policy uses
	// the backward-compatible unbounded TTL store.
	Dedupe DedupeOptions
}

// DedupePolicy selects how in-memory deduplication state is retained.
type DedupePolicy string

const (
	DedupePolicyUnboundedTTL DedupePolicy = "unbounded_ttl"
	DedupePolicyBoundedLRU   DedupePolicy = "bounded_lru"
	DedupePolicyDurable      DedupePolicy = "durable"
)

// DedupeOptions configures in-memory message deduplication.
type DedupeOptions struct {
	Policy   DedupePolicy
	Capacity int
}

// HandlerOptions configures a single registered handler.
type HandlerOptions struct {
	// MaxRetries before the message is moved to the dead-letter queue.
	// Defaults to 5.
	MaxRetries int

	// Backoff computes the delay before a retry. Defaults to exponential
	// 1s..60s.
	Backoff retry.Backoff

	// Timeout is the per-message deadline handed to the handler via its
	// context. Zero means no deadline (the handler runs until it returns);
	// a non-zero Timeout is what lets Shutdown and Drain complete even
	// when a handler is blocked.
	Timeout time.Duration
}

// EnqueueOptions is a functional-options bundle for Enqueue.
type EnqueueOptions struct {
	// Delay makes the message invisible until now+Delay. Zero means immediate.
	Delay time.Duration

	// Schedule is an absolute time the message becomes visible. If both Delay
	// and Schedule are set, Schedule wins.
	Schedule time.Time

	// DedupeKey, when set, suppresses enqueues of messages with the same
	// queue and key within the dedupe window (five minutes by default,
	// bounded by DedupeOptions). A duplicate is dropped silently: Enqueue
	// returns nil and nothing is written to storage. Dedupe state is
	// in-memory and does not survive a restart.
	DedupeKey string

	// Priority orders delivery within the queue: higher-priority messages
	// are dequeued first, ties break FIFO by created_at. The default (Low)
	// preserves plain FIFO order.
	Priority Priority

	// Metadata is shallow-copied onto the enqueued message. Use WithMetadata
	// to set it; do not mutate the map after Enqueue returns.
	Metadata map[string]string
}

// Priority is a delivery-ordering hint within a single queue.
type Priority int

const (
	Low Priority = iota
	Normal
	High
)

// PeriodicJob describes a persistent schedule that enqueues the payload at
// every interval. StartAt defaults to the registration time.
type PeriodicJob struct {
	ID      string
	Queue   string
	Payload []byte
	Every   time.Duration
	StartAt time.Time
	Enabled bool
}

// EnqueueOpt mutates an EnqueueOptions.
type EnqueueOpt func(*EnqueueOptions)

// Delay returns an option that defers visibility by d.
func Delay(d time.Duration) EnqueueOpt {
	return func(o *EnqueueOptions) { o.Delay = d }
}

// Schedule returns an option that defers visibility until t.
func Schedule(t time.Time) EnqueueOpt {
	return func(o *EnqueueOptions) { o.Schedule = t }
}

// DedupeKey returns an option that tags the message for de-duplication.
// Enqueues carrying a key already seen (same queue) within the dedupe
// window are dropped silently. See EnqueueOptions.DedupeKey for the
// guarantees and limits.
func DedupeKey(k string) EnqueueOpt {
	return func(o *EnqueueOptions) { o.DedupeKey = k }
}

// WithMetadata returns an option that sets the message's metadata map. The
// map is shallow-copied; later mutations to the passed-in map do not
// affect the enqueued message.
//
// Metadata is the right place for tracing, correlation IDs, and
// handler-side hints like the webhook target URL.
func WithMetadata(m map[string]string) EnqueueOpt {
	return func(o *EnqueueOptions) {
		cp := make(map[string]string, len(m))
		for k, v := range m {
			cp[k] = v
		}
		o.Metadata = cp
	}
}

// WithPriority returns an option that sets the message's priority:
//
//	q.Enqueue("orders", payload, shoebox.WithPriority(shoebox.High))
func WithPriority(p Priority) EnqueueOpt {
	return func(o *EnqueueOptions) { o.Priority = p }
}
