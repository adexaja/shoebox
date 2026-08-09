// Package broker is the dispatch engine. It owns the Storage, the
// registered handlers, the per-queue worker goroutines, and the shutdown
// lifecycle. The public shoebox.Queue is a thin wrapper around Broker.
package broker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rezki/shoebox/internal/retry"
	"github.com/rezki/shoebox/internal/storage"
)

// Options configures a Broker.
type Options struct {
	Storage     storage.Storage
	Concurrency int
	Logger      *slog.Logger
}

// Broker is the dispatch engine. One per Queue. Safe for concurrent use.
type Broker struct {
	store       storage.Storage
	concurrency int
	logger      logSink

	mwMu  sync.RWMutex
	mw    []Middleware

	hMu       sync.RWMutex
	handlers  map[string]*handler
	dispatchC map[string]chan struct{} // one wakeup channel per queue

	stopOnce sync.Once
	stopCh   chan struct{}
	wg       sync.WaitGroup

	shuttingDown atomic.Bool
}

// New constructs a Broker.
func New(opts Options) *Broker {
	if opts.Concurrency <= 0 {
		opts.Concurrency = 4
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Broker{
		store:       opts.Storage,
		concurrency: opts.Concurrency,
		logger:      opts.Logger,
		handlers:    make(map[string]*handler),
		dispatchC:   make(map[string]chan struct{}),
		stopCh:      make(chan struct{}),
	}
}

// Use registers middleware applied to every subsequently registered
// handler. The chain is captured at Register time, so Use must be called
// before Handle.
func (b *Broker) Use(mw ...Middleware) {
	b.mwMu.Lock()
	defer b.mwMu.Unlock()
	b.mw = append(b.mw, mw...)
}

// Register binds h to queue. If queue already has a handler, it is
// replaced. The first time a queue is seen, its dispatcher goroutine is
// started.
func (b *Broker) Register(queue string, fn HandlerFunc, opts HandlerOptions) {
	if opts.MaxRetries < 0 {
		opts.MaxRetries = 0
	}
	if opts.Backoff == nil {
		opts.Backoff = retry.Exponential(1*time.Second, 60*time.Second)
	}

	b.mwMu.RLock()
	mw := append([]Middleware(nil), b.mw...)
	b.mwMu.RUnlock()

	h := &handler{
		fn:   fn,
		opts: opts,
		chain: chain(mw, fn),
	}

	b.hMu.Lock()
	_, existed := b.handlers[queue]
	b.handlers[queue] = h
	if !existed {
		// Spawn one dispatcher goroutine per queue; it pulls workers off
		// the shared storage.
		b.dispatchC[queue] = make(chan struct{}, 1)
		b.wg.Add(1)
		go b.dispatch(queue)
	}
	b.hMu.Unlock()
}

// Enqueue adds a message to the queue and pokes the dispatcher. It is
// safe to call from inside a handler (e.g. to enqueue a follow-up job),
// including during Shutdown — the broker is designed to drain in-flight
// work before stopping.
func (b *Broker) Enqueue(ctx context.Context, queue string, payload []byte, opts EnqueueOpts) error {
	now := time.Now()
	scheduled := now
	switch {
	case !opts.Schedule.IsZero():
		scheduled = opts.Schedule
	case opts.Delay > 0:
		scheduled = now.Add(opts.Delay)
	}

	msg := storage.Message{
		ID:          newID(),
		Queue:       queue,
		Payload:     payload,
		Attempts:    0,
		MaxRetries:  0, // filled in by dispatcher; not used at enqueue time
		CreatedAt:   now,
		ScheduledAt: scheduled,
		Metadata:    opts.Metadata,
	}
	if err := b.store.Enqueue(ctx, queue, msg); err != nil {
		return err
	}

	b.hMu.RLock()
	wake, ok := b.dispatchC[queue]
	b.hMu.RUnlock()
	if ok {
		select {
		case wake <- struct{}{}:
		default:
		}
	}
	return nil
}

// EnqueueOpts is the broker's view of the public EnqueueOptions.
type EnqueueOpts struct {
	Delay    time.Duration
	Schedule time.Time
	Metadata map[string]string
}

// dispatch is the per-queue worker loop. It runs `concurrency` handlers
// concurrently and pulls the next batch from storage as soon as any
// worker is free.
func (b *Broker) dispatch(queue string) {
	defer b.wg.Done()

	sem := make(chan struct{}, b.concurrency)
	for {
		msgs, err := b.store.Dequeue(context.Background(), queue, b.concurrency)
		if err != nil {
			if !errors.Is(err, storage.ErrEmpty) {
				b.logger.ErrorContext(context.Background(), "shoebox: dequeue error",
					slog.String("queue", queue), slog.Any("err", err))
			}
			// Nothing to do — wait for a wakeup, a stop, or a periodic tick.
			b.hMu.RLock()
			wake := b.dispatchC[queue]
			b.hMu.RUnlock()
			select {
			case <-b.stopCh:
				return
			case <-wake:
			case <-time.After(250 * time.Millisecond):
				// Periodic poll so messages whose ScheduledAt just elapsed
				// get picked up even without a wake.
			}
			continue
		}

		for i := range msgs {
			msg := msgs[i]
			sem <- struct{}{}
			b.wg.Add(1)
			go func() {
				defer b.wg.Done()
				defer func() { <-sem }()
				b.handleOne(queue, msg)
			}()
		}
	}
}

// handleOne runs the registered handler for a single message. On success
// the message is acked; on failure it is retried with backoff or moved
// to the dead-letter shadow queue.
func (b *Broker) handleOne(queue string, msg storage.Message) {
	b.hMu.RLock()
	h, ok := b.handlers[queue]
	b.hMu.RUnlock()
	if !ok {
		// No handler — re-queue with a small delay so a Register that
		// races with Enqueue eventually picks the message up.
		next := time.Now().Add(100 * time.Millisecond)
		msg.ScheduledAt = next
		_ = b.store.Enqueue(context.Background(), queue, msg)
		return
	}

	// Cap attempts at the handler's MaxRetries.
	if h.opts.MaxRetries > 0 && msg.Attempts > h.opts.MaxRetries {
		b.toDeadLetter(queue, msg)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := h.chain(ctx, msg)
	if err == nil {
		_ = b.store.Ack(context.Background(), queue, msg.ID)
		return
	}

	b.logger.WarnContext(context.Background(), "shoebox: handler error",
		slog.String("queue", queue),
		slog.String("id", msg.ID),
		slog.Int("attempt", msg.Attempts),
		slog.Any("err", err))

	msg.Attempts++
	if msg.Attempts > h.opts.MaxRetries {
		b.toDeadLetter(queue, msg)
		return
	}

	delay := h.opts.Backoff.Next(msg.Attempts)
	msg.ScheduledAt = time.Now().Add(delay)
	_ = b.store.Nack(context.Background(), queue, msg.ID, err)
	if err := b.store.Enqueue(context.Background(), queue, msg); err != nil {
		b.logger.ErrorContext(context.Background(), "shoebox: re-enqueue failed",
			slog.String("queue", queue), slog.String("id", msg.ID), slog.Any("err", err))
	}
}

// toDeadLetter writes the message to the {queue}.dlq shadow queue and
// bumps the dead counter. The DLQ is a plain queue as far as the
// storage layer is concerned; inspection/replay APIs come in Week 4.
func (b *Broker) toDeadLetter(queue string, msg storage.Message) {
	dlq := queue + ".dlq"
	msg.ScheduledAt = time.Now()
	if err := b.store.Enqueue(context.Background(), dlq, msg); err != nil {
		b.logger.ErrorContext(context.Background(), "shoebox: dlq enqueue failed",
			slog.String("queue", queue), slog.String("id", msg.ID), slog.Any("err", err))
	}
	_ = b.store.Dead(context.Background(), queue, msg.ID, errors.New("max retries exceeded"))
}

// Shutdown drains the queues and stops the dispatchers. It waits for:
//
//  1. All in-flight handlers to complete (including any follow-up
//     enqueues they make, recursively).
//  2. All queues to be empty (no more messages waiting to be delivered).
//  3. Dispatcher goroutines to exit.
//
// Shutdown returns nil on a clean drain. If ctx expires before the
// drain completes, Shutdown returns ctx.Err() and the dispatchers
// are stopped regardless — in-flight work may be cut short.
func (b *Broker) Shutdown(ctx context.Context) error {
	b.shuttingDown.Store(true)

	// Wake all dispatchers so they observe the drain request.
	b.hMu.RLock()
	for _, wake := range b.dispatchC {
		select {
		case wake <- struct{}{}:
		default:
		}
	}
	b.hMu.RUnlock()

	// Wait for queues to empty, or for ctx to expire.
	poll := time.NewTicker(50 * time.Millisecond)
	defer poll.Stop()
	for {
		if b.allQueuesEmpty() {
			break
		}
		select {
		case <-ctx.Done():
			// Force-stop everything.
			b.stopOnce.Do(func() { close(b.stopCh) })
			b.wg.Wait()
			return ctx.Err()
		case <-poll.C:
		}
		// Keep nudging in case new messages were enqueued mid-drain.
		b.hMu.RLock()
		for _, wake := range b.dispatchC {
			select {
			case wake <- struct{}{}:
			default:
			}
		}
		b.hMu.RUnlock()
	}

	// All queues empty. Signal dispatchers to exit and wait.
	b.stopOnce.Do(func() { close(b.stopCh) })
	done := make(chan struct{})
	go func() { b.wg.Wait(); close(done) }()

	select {
	case <-done:
		return b.store.Close()
	case <-ctx.Done():
		return ctx.Err()
	}
}

// allQueuesEmpty returns true when every known queue has depth 0.
func (b *Broker) allQueuesEmpty() bool {
	b.hMu.RLock()
	queues := make([]string, 0, len(b.handlers))
	for q := range b.handlers {
		queues = append(queues, q)
	}
	b.hMu.RUnlock()

	for _, q := range queues {
		s, err := b.store.Stats(context.Background(), q)
		if err != nil {
			return false
		}
		if s.Depth > 0 {
			return false
		}
	}
	return true
}

// Healthy reports whether the broker is still dispatching. It returns
// false once Shutdown has been called.
func (b *Broker) Healthy() bool { return !b.shuttingDown.Load() }

// Stats returns a snapshot of queue stats.
func (b *Broker) Stats(ctx context.Context, queue string) (storage.QueueStats, error) {
	return b.store.Stats(ctx, queue)
}

// newID returns a 16-byte random hex string. We avoid pulling in a UUID
// library to keep the dependency surface small (see ADR 0004); the value
// only needs to be unique within a process for the in-memory backend,
// and uniqueness across processes is handled by persistent backends
// using their own ID strategies.
func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand failing is unrecoverable on a healthy system; fall
		// back to a time-based identifier so we never panic in a user's
		// hot path.
		now := time.Now().UnixNano()
		return "ts-" + hex.EncodeToString([]byte{
			byte(now >> 56), byte(now >> 48), byte(now >> 40), byte(now >> 32),
			byte(now >> 24), byte(now >> 16), byte(now >> 8), byte(now),
		})
	}
	return hex.EncodeToString(b[:])
}
