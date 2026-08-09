package shoebox

import (
	"log/slog"
	"time"

	"github.com/adexaja/shoebox/internal/retry"
)

// StorageKind selects a storage backend. Only Memory is implemented in Week 1;
// SQLite and Postgres are placeholders for Week 2.
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

	// Concurrency is the per-queue worker pool size. Defaults to 4.
	Concurrency int

	// Logger receives structured events. Defaults to slog.Default().
	Logger *slog.Logger
}

// HandlerOptions configures a single registered handler.
type HandlerOptions struct {
	// MaxRetries before the message is moved to the dead-letter queue.
	// Defaults to 5.
	MaxRetries int

	// Backoff computes the delay before a retry. Defaults to exponential
	// 1s..60s.
	Backoff retry.Backoff
}

// EnqueueOptions is a functional-options bundle for Enqueue.
type EnqueueOptions struct {
	// Delay makes the message invisible until now+Delay. Zero means immediate.
	Delay time.Duration

	// Schedule is an absolute time the message becomes visible. If both Delay
	// and Schedule are set, Schedule wins.
	Schedule time.Time

	// DedupeKey, when set, suppresses enqueues of messages with the same key
	// within the configured window. Implemented in v0.3.
	DedupeKey string

	// Priority is the v0.3 priority field. Reserved; ignored in v0.1.
	Priority Priority

	// Metadata is shallow-copied onto the enqueued message. Use WithMetadata
	// to set it; do not mutate the map after Enqueue returns.
	Metadata map[string]string
}

// Priority is reserved for v0.3 priority queues.
type Priority int

const (
	Low Priority = iota
	Normal
	High
)

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
// Implemented in v0.3; the value is preserved in metadata for now.
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

// WithPriority returns an option that sets the v0.3 priority hint. Reserved
// for future use; the broker does not act on the value in v0.1.
//
// The PRD sketch writes `shoebox.Priority(shoebox.High)`; we expose
// `WithPriority` to keep `Priority` free as a type name. The call site is
// therefore:
//
//	q.Enqueue("orders", payload, shoebox.WithPriority(shoebox.High))
func WithPriority(p Priority) EnqueueOpt {
	return func(o *EnqueueOptions) { o.Priority = p }
}
