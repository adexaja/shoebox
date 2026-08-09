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

	"github.com/adexaja/shoebox/internal/retry"
	"github.com/adexaja/shoebox/internal/storage"
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

	mwMu sync.RWMutex
	mw   []Middleware

	hMu       sync.RWMutex
	handlers  map[string]*handler
	dispatchC map[string]chan struct{} // one wakeup channel per queue
	inflight  map[string]*atomic.Int64 // per-queue in-flight worker count (drain quiescence)
	workerMu  sync.Mutex               // protects the inflight map values during access

	// stopCh  closes to initiate a graceful drain: dispatchers drain their
	//         queue to quiescence (including follow-up enqueues) then exit.
	// abortCh closes to force-abort: dispatchers exit immediately, leaving
	//         in-flight workers to finish on their own. Used when Shutdown's
	//         ctx expires.
	stopOnce sync.Once
	stopCh   chan struct{}
	abortOnce sync.Once
	abortCh   chan struct{}
	wg        sync.WaitGroup // counts dispatchers + workers; Shutdown waits on this

	closeOnce sync.Once
	closed    atomic.Bool

	// shutdownCtx is the context Shutdown hands to all store calls so a
	// blocking backend can observe cancellation. Defaults to Background until
	// Shutdown replaces it.
	shutdownCtx    context.Context
	shutdownCtxMu  sync.RWMutex

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
		inflight:    make(map[string]*atomic.Int64),
		stopCh:      make(chan struct{}),
		abortCh:     make(chan struct{}),
		shutdownCtx: context.Background(),
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
		b.inflight[queue] = &atomic.Int64{}
		b.wg.Add(1)
		go b.dispatch(queue)
	}
	b.hMu.Unlock()
}

// storeCtx returns the context to use for storage calls. During shutdown it
// is the Shutdown caller's ctx (so blocking backends can be cancelled);
// otherwise it is context.Background(). See E1-CONC-6.
func (b *Broker) storeCtx() context.Context {
	b.shutdownCtxMu.RLock()
	defer b.shutdownCtxMu.RUnlock()
	return b.shutdownCtx
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
// concurrently and pulls the next batch from storage as soon as any worker is
// free.
//
// Shutdown semantics (see ADR 0002, drain amendment):
//   - When stopCh closes, the dispatcher enters drain mode: it keeps
//     dequeueing and processing until a dequeue returns empty AND its
//     per-queue in-flight worker counter is zero (so no surviving handler
//     can re-enqueue after the decision), then exits. This is the
//     authoritative quiescence check that fixes E1-CONC-1/4.
//   - When abortCh closes (Shutdown ctx expired), the dispatcher exits
//     immediately; in-flight workers finish on their own and are tracked by
//     the global wg (CONC-2: one unified exit path).
func (b *Broker) dispatch(queue string) {
	defer b.wg.Done()

	sem := make(chan struct{}, b.concurrency)
	for {
		// Hard-abort wins over everything.
		select {
		case <-b.abortCh:
			return
		default:
		}

		msgs, err := b.store.Dequeue(b.storeCtx(), queue, b.concurrency)
		if err != nil && !errors.Is(err, storage.ErrEmpty) {
			b.logger.ErrorContext(b.storeCtx(), "shoebox: dequeue error",
				slog.String("queue", queue), slog.Any("err", err))
		}

		if len(msgs) == 0 {
			// Nothing ready right now. Either idle, or draining.
			select {
			case <-b.abortCh:
				return
			default:
			}

			if b.draining() && b.quiescent(queue) {
				// Authoritative drain-complete for this queue: the queue is
				// empty AND no worker is in flight, so nothing can re-enqueue
				// after we exit.
				return
			}

			b.hMu.RLock()
			wake := b.dispatchC[queue]
			b.hMu.RUnlock()
			select {
			case <-b.abortCh:
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
			ifq := b.inflightFor(queue)
			ifq.Add(1)
			b.wg.Add(1)
			go func() {
				defer b.wg.Done()
				defer ifq.Add(-1)
				defer func() { <-sem }()
				b.handleOne(queue, msg)
			}()
		}
	}
}

// draining reports whether a graceful drain has been requested.
func (b *Broker) draining() bool {
	select {
	case <-b.stopCh:
		return true
	default:
		return false
	}
}

// quiescent reports whether a queue has no in-flight workers. Combined with
// "a dequeue returned empty," this is the authoritative drain condition — no
// surviving handler can re-enqueue after this returns true. See E1-CONC-1.
func (b *Broker) quiescent(queue string) bool {
	return b.inflightFor(queue).Load() == 0
}

// inflightFor returns the per-queue in-flight worker counter. Guarded because
// Shutdown may read it concurrently with the dispatcher. A counter is created
// on demand for queues the broker writes to but never dispatches (e.g. a
// {queue}.dlq shadow queue); it stays at zero, which is the correct quiescent
// value for such queues.
func (b *Broker) inflightFor(queue string) *atomic.Int64 {
	b.workerMu.Lock()
	defer b.workerMu.Unlock()
	ifq, ok := b.inflight[queue]
	if !ok {
		ifq = &atomic.Int64{}
		b.inflight[queue] = ifq
	}
	return ifq
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
		_ = b.store.Enqueue(b.storeCtx(), queue, msg)
		return
	}

	// Cap attempts at the handler's MaxRetries.
	if h.opts.MaxRetries > 0 && msg.Attempts > h.opts.MaxRetries {
		b.toDeadLetter(queue, msg)
		return
	}

	// Per-handler deadline. Without this a wedged handler hangs the drain
	// forever; with it, the context expires and the handler (if cooperative)
	// returns, letting the drain progress. See E1-CONC-3.
	ctx, cancel := b.handlerCtx(h)
	defer cancel()
	err := h.chain(ctx, msg)
	if err == nil {
		_ = b.store.Ack(b.storeCtx(), queue, msg.ID)
		return
	}

	b.logger.WarnContext(b.storeCtx(), "shoebox: handler error",
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
	_ = b.store.Nack(b.storeCtx(), queue, msg.ID, err)
	if err := b.store.Enqueue(b.storeCtx(), queue, msg); err != nil {
		b.logger.ErrorContext(b.storeCtx(), "shoebox: re-enqueue failed",
			slog.String("queue", queue), slog.String("id", msg.ID), slog.Any("err", err))
	}
}

// handlerCtx builds the context handed to a handler: a cancellation that
// respects the shutdown ctx, plus a per-handler Timeout deadline when set.
func (b *Broker) handlerCtx(h *handler) (context.Context, context.CancelFunc) {
	parent := b.storeCtx()
	if h.opts.Timeout > 0 {
		return context.WithTimeout(parent, h.opts.Timeout)
	}
	return context.WithCancel(parent)
}

// toDeadLetter writes the message to the {queue}.dlq shadow queue and
// bumps the dead counter. The DLQ is a plain queue as far as the
// storage layer is concerned; inspection/replay APIs come in Week 4.
func (b *Broker) toDeadLetter(queue string, msg storage.Message) {
	dlq := queue + ".dlq"
	msg.ScheduledAt = time.Now()
	if err := b.store.Enqueue(b.storeCtx(), dlq, msg); err != nil {
		b.logger.ErrorContext(b.storeCtx(), "shoebox: dlq enqueue failed",
			slog.String("queue", queue), slog.String("id", msg.ID), slog.Any("err", err))
	}
	_ = b.store.Dead(b.storeCtx(), queue, msg.ID, errors.New("max retries exceeded"))
}

// Shutdown drains the queues and stops the dispatchers.
//
// It initiates a graceful drain (close stopCh): each dispatcher processes its
// queue to authoritative quiescence — empty AND no in-flight workers — then
// exits, so follow-up enqueues made by in-flight handlers are not orphaned
// (E1-CONC-1/4).
//
// Shutdown returns nil once all dispatchers and workers have exited, then
// closes the store exactly once (E1-CONC-5). If ctx expires first, it
// force-aborts (close abortCh) so dispatchers exit immediately; in-flight
// workers that haven't returned are left to finish (tracked by wg) — their
// work is best-effort at that point.
//
// There is a single exit path either way: wait on wg (bounded by abort), then
// closeOnce(store.Close). No branch returns without closing the store
// (E1-CONC-2).
func (b *Broker) Shutdown(ctx context.Context) error {
	if b.closed.Swap(true) {
		return errAlreadyClosed
	}
	b.shuttingDown.Store(true)

	// Hand our ctx to all storage calls so a blocking backend can be
	// cancelled once we start tearing down (E1-CONC-6). We wrap it so that
	// even after ctx expires we keep a non-nil context for the final
	// wg.Wait()/Close.
	shutdownCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	b.shutdownCtxMu.Lock()
	b.shutdownCtx = shutdownCtx
	b.shutdownCtxMu.Unlock()

	// 1) Request graceful drain.
	b.stopOnce.Do(func() { close(b.stopCh) })
	b.wakeAll()

	// 2) Wait for dispatchers+workers to exit, or force-abort on ctx expiry.
	done := make(chan struct{})
	go func() { b.wg.Wait(); close(done) }()
	select {
	case <-done:
		b.closeOnce.Do(func() { _ = b.store.Close() })
		return nil
	case <-ctx.Done():
		// Force-abort: dispatchers exit immediately; workers finish on their
		// own (wg covers them). We MUST still wait for wg so we don't close
		// the store under a running worker.
		b.abortOnce.Do(func() { close(b.abortCh) })
		// Give workers a bounded chance to notice ctx cancellation through
		// handlerCtx; then wait regardless.
		<-done
		b.closeOnce.Do(func() { _ = b.store.Close() })
		return ctx.Err()
	}
}

// wakeAll nudges every dispatcher's wakeup channel.
func (b *Broker) wakeAll() {
	b.hMu.RLock()
	defer b.hMu.RUnlock()
	for _, wake := range b.dispatchC {
		select {
		case wake <- struct{}{}:
		default:
		}
	}
}

// errAlreadyClosed is returned by a second Shutdown call.
var errAlreadyClosed = errors.New("shoebox: queue already shut down")

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
