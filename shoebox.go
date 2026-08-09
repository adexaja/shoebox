// Package shoebox is an embedded message queue for Go. See the README for a
// quick start and the parent docs/ directory (epics, user stories, ADRs) for
// the longer story.
package shoebox

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/rezki/shoebox/internal/broker"
	"github.com/rezki/shoebox/internal/retry"
	"github.com/rezki/shoebox/internal/storage"
)

// Queue is the public-facing message queue. It is safe for concurrent use.
type Queue struct {
	b *broker.Broker
}

// New constructs a Queue backed by the storage kind selected in opts. For
// Week 1 only Memory is implemented; selecting SQLite or Postgres returns
// nil with an error.
func New(opts Options) (*Queue, error) {
	if opts.Concurrency <= 0 {
		opts.Concurrency = 4
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}

	store, err := buildStorage(opts)
	if err != nil {
		return nil, err
	}

	return &Queue{
		b: broker.New(broker.Options{
			Storage:     store,
			Concurrency: opts.Concurrency,
			Logger:      opts.Logger,
		}),
	}, nil
}

// buildStorage returns the Storage implementation for the requested kind.
func buildStorage(opts Options) (storage.Storage, error) {
	switch opts.Storage {
	case Memory:
		return storage.NewMemory(), nil
	case SQLite:
		return nil, fmt.Errorf("shoebox: SQLite storage is a Week 2 deliverable (see docs/tasks.md E2)")
	case Postgres:
		return nil, fmt.Errorf("shoebox: Postgres storage is a Week 2 deliverable (see docs/tasks.md E2)")
	default:
		return nil, fmt.Errorf("shoebox: unknown storage kind %d", opts.Storage)
	}
}

// Handle registers a handler for the given queue. The handler runs on a
// background goroutine and is invoked until it returns nil.
//
// If Handle is called twice for the same queue, the second registration
// replaces the first. To run multiple workers per queue, increase
// Options.Concurrency.
//
// Note: shoebox.Message is an alias for storage.Message, so the public
// HandlerFunc and the broker's HandlerFunc are the same function type —
// no conversion is needed at registration time.
func (q *Queue) Handle(queue string, h HandlerFunc, opts ...HandlerOptions) {
	ho := HandlerOptions{MaxRetries: 5}
	if len(opts) > 0 {
		ho = opts[0]
	}
	if ho.MaxRetries < 0 {
		ho.MaxRetries = 0
	}
	if ho.Backoff == nil {
		ho.Backoff = retry.Exponential(1*time.Second, 60*time.Second)
	}
	q.b.Register(queue, h, broker.HandlerOptions{
		MaxRetries: ho.MaxRetries,
		Backoff:    ho.Backoff,
	})
}

// Enqueue adds a message to the queue. The call returns once the message is
// durably stored (or, for Memory, in the in-process ring buffer). It does
// not wait for the handler to run.
//
// Per ADR 0002 (fire-and-forget), Enqueue does not return a delivery
// promise; use Shutdown to wait for in-flight messages.
func (q *Queue) Enqueue(queue string, payload []byte, opts ...EnqueueOpt) error {
	eo := EnqueueOptions{}
	for _, opt := range opts {
		opt(&eo)
	}
	return q.b.Enqueue(context.Background(), queue, payload, broker.EnqueueOpts{
		Delay:    eo.Delay,
		Schedule: eo.Schedule,
		Metadata: eo.Metadata,
	})
}

// Use registers one or more middleware. Middleware applies in the order
// given: the first argument is the outermost wrapper, so it sees the
// message before any other middleware.
//
// Use must be called before Handle for the queue it should affect, since
// the middleware chain is captured at registration time.
func (q *Queue) Use(mw ...Middleware) {
	q.b.Use(asBrokerMiddleware(mw)...)
}

// asBrokerMiddleware converts a slice of public Middleware into the
// broker's Middleware type. The two types are structurally identical
// (both are `func(HandlerFunc) HandlerFunc` with HandlerFunc being
// `func(ctx, storage.Message) error`); this conversion is a no-op at
// runtime but keeps the type system honest.
func asBrokerMiddleware(mw []Middleware) []broker.Middleware {
	out := make([]broker.Middleware, len(mw))
	for i, m := range mw {
		out[i] = broker.Middleware(m)
	}
	return out
}

// Shutdown stops accepting new enqueues and waits for in-flight handlers to
// complete or for ctx to expire, whichever comes first.
//
// After Shutdown returns, the Queue is unusable. Build a new one with New.
func (q *Queue) Shutdown(ctx context.Context) error {
	return q.b.Shutdown(ctx)
}

// Healthy reports whether the queue is currently dispatching messages.
// It returns false once Shutdown has been called.
func (q *Queue) Healthy() bool {
	return q.b.Healthy()
}

// Stats returns counters for the named queue. The returned QueueStats is a
// snapshot; depth is read live, the other fields are cumulative since the
// Queue was created.
func (q *Queue) Stats(ctx context.Context, queue string) (QueueStats, error) {
	return q.b.Stats(ctx, queue)
}
