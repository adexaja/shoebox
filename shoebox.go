// Package shoebox is an embedded message queue for Go. See the README for a
// quick start and the parent docs/ directory (epics, user stories, ADRs) for
// the longer story.
package shoebox

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/adexaja/shoebox/internal/broker"
	"github.com/adexaja/shoebox/internal/naming"
	"github.com/adexaja/shoebox/internal/retry"
	"github.com/adexaja/shoebox/internal/storage"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Queue is the public-facing message queue. It is safe for concurrent use.
type Queue struct {
	b        *broker.Broker
	metrics  *Metrics
	registry *prometheus.Registry
}

// New constructs a Queue backed by the storage kind selected in opts.
//
// Memory is in-process and volatile. SQLite persists to opts.Path and
// survives restarts. Postgres uses opts.DSN.
func New(opts Options) (*Queue, error) {
	if opts.Concurrency <= 0 {
		opts.Concurrency = 4
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}

	store, err := buildStorage(context.Background(), opts)
	if err != nil {
		return nil, err
	}

	q := &Queue{
		b: broker.New(broker.Options{
			Storage:     store,
			Concurrency: opts.Concurrency,
			Logger:      opts.Logger,
		}),
		registry: opts.MetricsRegistry,
	}
	q.metrics = NewMetrics("", opts.MetricsRegistry)
	return q, nil
}

// buildStorage returns the Storage implementation for the requested kind.
func buildStorage(ctx context.Context, opts Options) (storage.Storage, error) {
	switch opts.Storage {
	case Memory:
		return storage.NewMemory(), nil
	case SQLite:
		if opts.Path == "" {
			return nil, fmt.Errorf("shoebox: SQLite storage requires Options.Path")
		}
		return storage.NewSQLite(ctx, opts.Path)
	case Postgres:
		if opts.DSN == "" {
			return nil, fmt.Errorf("shoebox: Postgres storage requires Options.DSN")
		}
		return storage.NewPostgres(ctx, opts.DSN)
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
		Timeout:    ho.Timeout,
	})
}

// Enqueue adds a message to the queue. The call returns once the message is
// durably stored (or, for Memory, in the in-process ring buffer). It does
// not wait for the handler to run.
//
// Per ADR 0002 (fire-and-forget), Enqueue does not return a delivery
// promise; use Shutdown to wait for in-flight messages.
func (q *Queue) Enqueue(queue string, payload []byte, opts ...EnqueueOpt) error {
	if !naming.ValidQueueName(queue) {
		return fmt.Errorf("shoebox: invalid queue name %q", queue)
	}
	eo := EnqueueOptions{}
	for _, opt := range opts {
		opt(&eo)
	}
	return q.b.Enqueue(context.Background(), queue, payload, broker.EnqueueOpts{
		Delay:     eo.Delay,
		Schedule:  eo.Schedule,
		Priority:  int(eo.Priority),
		DedupeKey: eo.DedupeKey,
		Metadata:  eo.Metadata,
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

// Queues returns the names of all registered queues.
func (q *Queue) Queues() []string {
	return q.b.Queues()
}

// Store returns the underlying storage interface. Exposed so that packages
// like dlq can access the storage layer without a separate handle.
func (q *Queue) Store() storage.Storage {
	return q.b.Store()
}

// MetricsHandler returns an http.Handler that exposes Prometheus metrics.
// Mount it at /metrics:
//
//	http.Handle("/metrics", q.MetricsHandler())
//
// If a custom registry was passed via Options.MetricsRegistry, it is used;
// otherwise the default Prometheus registry is used. The handler also updates
// the queue_depth gauge for every registered queue on each scrape so the
// gauge reflects the current depth without a background polling goroutine.
func (q *Queue) MetricsHandler() http.Handler {
	registry := q.registry
	if registry == nil {
		return promhttp.Handler()
	}
	return promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
}

// UpdateDepthGauges refreshes the queue_depth gauge for every registered
// queue by reading live depth from storage. Call this periodically (e.g. every
// 5 seconds) or from a custom /metrics handler before scraping. The
// MetricsHandler returned by this method does NOT call this automatically;
// the caller is responsible for the polling cadence.
func (q *Queue) UpdateDepthGauges(ctx context.Context) {
	for _, queue := range q.Queues() {
		stats, err := q.Stats(ctx, queue)
		if err != nil {
			continue
		}
		q.metrics.Depth.WithLabelValues(queue).Set(float64(stats.Depth))
	}
}

// Pause stops the dispatcher from dequeuing messages on the named queue.
// In-flight handlers continue to run; new messages accumulate in storage
// until Resume is called. Pause is idempotent.
func (q *Queue) Pause(queue string) {
	q.b.Pause(queue)
}

// Resume allows the dispatcher to start dequeuing messages again on the named
// queue. Resume is idempotent.
func (q *Queue) Resume(queue string) {
	q.b.Resume(queue)
}

// Drain processes all remaining messages on the named queue to quiescence
// (empty Dequeue + zero in-flight workers), then stops that queue's
// dispatcher. It blocks until drain completes or ctx expires.
//
// Unlike Shutdown, Drain only affects a single queue — other queues keep
// running.
func (q *Queue) Drain(ctx context.Context, queue string) error {
	return q.b.Drain(ctx, queue)
}
