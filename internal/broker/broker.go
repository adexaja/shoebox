// Package broker is the dispatch engine. It owns the Storage, the
// registered handlers, the per-queue worker goroutines, and the shutdown
// lifecycle. The public shoebox.Queue is a thin wrapper around Broker.
package broker

import (
	"context"
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
	Storage       storage.Storage
	Concurrency   int
	Logger        *slog.Logger
	Dedupe        DedupeOptions
	DedupeMetrics DedupeMetrics
}

// DedupeMetrics receives optional deduplication instrumentation from the
// public metrics layer without coupling the broker to a metrics dependency.
type DedupeMetrics interface {
	DedupeHit(queue string)
	DedupeMiss(queue string)
	DedupeEviction()
	DedupeEntries(entries int)
}

// Broker is the dispatch engine. One per Queue. Safe for concurrent use.
type Broker struct {
	store       storage.Storage
	concurrency int
	logger      logSink

	mwMu sync.RWMutex
	mw   []Middleware

	hMu         sync.RWMutex
	handlers    map[string]*handler
	dispatchC   map[string]chan struct{} // one wakeup channel per queue
	dispatching map[string]bool
	inflight    map[string]*atomic.Int64 // per-queue in-flight worker count (drain quiescence)
	workerMu    sync.Mutex               // protects the inflight map values during access

	// stopCh  closes to initiate a graceful drain: dispatchers drain their
	//         queue to quiescence (including follow-up enqueues) then exit.
	// abortCh closes to force-abort: dispatchers exit immediately, leaving
	//         in-flight workers to finish on their own. Used when Shutdown's
	//         ctx expires.
	stopOnce  sync.Once
	stopCh    chan struct{}
	abortOnce sync.Once
	abortCh   chan struct{}
	wg        sync.WaitGroup // counts dispatchers + workers; Shutdown waits on this

	closeOnce sync.Once
	closed    atomic.Bool

	// shutdownCtx is the context Shutdown hands to all store calls so a
	// blocking backend can observe cancellation. Defaults to Background until
	// Shutdown replaces it.
	shutdownCtx   context.Context
	shutdownCtxMu sync.RWMutex

	shuttingDown atomic.Bool

	// dedupe suppresses duplicate enqueues within a per-key TTL window
	// (ADR 0006 §Deduplication).
	dedupe        dedupeStore
	dedupeMetrics DedupeMetrics
	dedupeTTL     time.Duration

	// paused holds per-queue atomic flags. When set, the dispatcher skips
	// dequeuing that queue so messages accumulate until Resume.
	paused   map[string]*atomic.Bool
	pausedMu sync.Mutex

	// qDraining holds per-queue drain flags. When set, the dispatcher for
	// that queue enters per-queue drain mode: process to quiescence, then
	// stop only that queue (not the whole broker).
	qDraining   map[string]*atomic.Bool
	drainDone   map[string]chan struct{}
	qDrainingMu sync.Mutex
}

// New constructs a Broker.
func New(opts Options) *Broker {
	if opts.Concurrency <= 0 {
		opts.Concurrency = 4
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	dedupe, err := newDedupeStore(opts.Dedupe, func() {
		if opts.DedupeMetrics != nil {
			opts.DedupeMetrics.DedupeEviction()
		}
	})
	if err != nil {
		// Public construction validates this before calling broker.New. Keep
		// the internal constructor backward-compatible for existing callers.
		dedupe = newTTLDedupeStore()
	}
	return &Broker{
		store:         opts.Storage,
		concurrency:   opts.Concurrency,
		logger:        opts.Logger,
		handlers:      make(map[string]*handler),
		dispatchC:     make(map[string]chan struct{}),
		dispatching:   make(map[string]bool),
		inflight:      make(map[string]*atomic.Int64),
		stopCh:        make(chan struct{}),
		abortCh:       make(chan struct{}),
		shutdownCtx:   context.Background(),
		dedupe:        dedupe,
		dedupeMetrics: opts.DedupeMetrics,
		dedupeTTL:     DefaultDedupeTTL,
		paused:        make(map[string]*atomic.Bool),
		qDraining:     make(map[string]*atomic.Bool),
		drainDone:     make(map[string]chan struct{}),
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
		fn:    fn,
		opts:  opts,
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
	}
	if !b.dispatching[queue] {
		b.dispatching[queue] = true
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
	// Deduplication: if a DedupeKey is set and a matching key is still live
	// for this queue, silently suppress this enqueue (no error, no store
	// write). The caller's contract is fire-and-forget, so returning nil is
	// the right signal — the message was "accepted" even if deduplicated.
	if opts.DedupeKey != "" && b.dedupe.SeenOrAdd(queue+":"+opts.DedupeKey, b.dedupeTTL) {
		if b.dedupeMetrics != nil {
			b.dedupeMetrics.DedupeHit(queue)
			b.dedupeMetrics.DedupeEntries(b.dedupe.Len())
		}
		return nil
	}
	if opts.DedupeKey != "" && b.dedupeMetrics != nil {
		b.dedupeMetrics.DedupeMiss(queue)
		b.dedupeMetrics.DedupeEntries(b.dedupe.Len())
	}

	now := time.Now()
	scheduled := now
	switch {
	case !opts.Schedule.IsZero():
		scheduled = opts.Schedule
	case opts.Delay > 0:
		scheduled = now.Add(opts.Delay)
	}

	msg := storage.Message{
		ID:          storage.NewMessageID(),
		Queue:       queue,
		Payload:     payload,
		Attempts:    0,
		MaxRetries:  0, // filled in by dispatcher; not used at enqueue time
		CreatedAt:   now,
		ScheduledAt: scheduled,
		Priority:    opts.Priority,
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
	Delay     time.Duration
	Schedule  time.Time
	Priority  int
	DedupeKey string
	Metadata  map[string]string
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
	defer func() {
		b.hMu.Lock()
		b.dispatching[queue] = false
		b.hMu.Unlock()
	}()

	sem := make(chan struct{}, b.concurrency)
	for {
		// Hard-abort wins over everything.
		select {
		case <-b.abortCh:
			return
		default:
		}

		// Per-queue pause: skip dequeuing while paused, but keep the loop
		// alive so Resume can pick it up immediately.
		if b.isPaused(queue) {
			select {
			case <-b.abortCh:
				return
			case <-time.After(100 * time.Millisecond):
			}
			continue
		}

		msgs, err := b.store.Dequeue(b.storeCtx(), queue, b.concurrency)
		if err != nil && !errors.Is(err, storage.ErrEmpty) {
			b.logger.ErrorContext(b.storeCtx(), "shoebox: dequeue error",
				slog.String("queue", queue), slog.Any("err", err))
		}

		if len(msgs) == 0 {
			// Nothing is ready right now. A queue can still contain messages
			// scheduled for the future (notably retry messages), so only finish
			// draining when the storage reports no pending messages at all.
			select {
			case <-b.abortCh:
				return
			default:
			}

			if b.draining() && b.drainComplete(queue) {
				return
			}

			// Per-queue drain (Drain method): same quiescence condition,
			// but only stops THIS queue's dispatcher, not the whole broker.
			if b.isQueueDraining(queue) && b.drainComplete(queue) {
				b.signalDrainDone(queue)
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

// drainComplete reports whether a queue has no pending messages and no
// in-flight workers. Dequeue returning ErrEmpty only means that no message is
// due now; scheduled retries may still be pending and must be allowed to run.
func (b *Broker) drainComplete(queue string) bool {
	if !b.quiescent(queue) {
		return false
	}

	stats, err := b.store.Stats(b.storeCtx(), queue)
	if err != nil {
		b.logger.ErrorContext(b.storeCtx(), "shoebox: drain stats failed",
			slog.String("queue", queue), slog.Any("err", err))
		return false
	}
	return stats.Depth == 0
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
		if err := b.store.Enqueue(b.storeCtx(), queue, msg); err != nil {
			b.logger.ErrorContext(b.storeCtx(), "shoebox: re-enqueue (no handler) failed",
				slog.String("queue", queue), slog.String("id", msg.ID), slog.Any("err", err))
		}
		return
	}

	// Cap attempts at the handler's MaxRetries.
	if h.opts.MaxRetries > 0 && msg.Attempts > h.opts.MaxRetries {
		b.toDeadLetter(queue, msg, errors.New("max retries exceeded"))
		return
	}

	// Per-handler deadline. Without this a wedged handler hangs the drain
	// forever; with it, the context expires and the handler (if cooperative)
	// returns, letting the drain progress. See E1-CONC-3.
	ctx, cancel := b.handlerCtx(h)
	defer cancel()
	err := h.chain(ctx, msg)
	if err == nil {
		if err := b.store.Ack(b.storeCtx(), queue, msg.ID); err != nil {
			b.logger.ErrorContext(b.storeCtx(), "shoebox: ack failed",
				slog.String("queue", queue), slog.String("id", msg.ID), slog.Any("err", err))
		}
		return
	}

	b.logger.WarnContext(b.storeCtx(), "shoebox: handler error",
		slog.String("queue", queue),
		slog.String("id", msg.ID),
		slog.Int("attempt", msg.Attempts),
		slog.Any("err", err))

	msg.Attempts++
	if msg.Attempts > h.opts.MaxRetries {
		b.toDeadLetter(queue, msg, err)
		return
	}

	delay := h.opts.Backoff.Next(msg.Attempts)
	msg.ScheduledAt = time.Now().Add(delay)
	if err := b.store.Nack(b.storeCtx(), queue, msg.ID, err); err != nil {
		b.logger.ErrorContext(b.storeCtx(), "shoebox: nack failed",
			slog.String("queue", queue), slog.String("id", msg.ID), slog.Any("err", err))
	}
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
// bumps the dead counter. The dead message retains its original payload,
// the last handler error, the retry count, and a timestamp (E2-S3).
func (b *Broker) toDeadLetter(queue string, msg storage.Message, handlerErr error) {
	dlq := queue + ".dlq"
	sourceID := msg.ID
	msg.Queue = dlq
	msg.ScheduledAt = time.Now()
	msg.DeadAt = time.Now()
	if handlerErr != nil {
		msg.Error = handlerErr.Error()
	}
	// Persistent backends use a globally unique message ID. Give the DLQ
	// record its own ID so it can coexist with the source row, which is
	// marked dead below. Replay will remove this DLQ row before requeueing.
	msg.ID = storage.NewMessageID()
	if err := b.store.Enqueue(b.storeCtx(), dlq, msg); err != nil {
		b.logger.ErrorContext(b.storeCtx(), "shoebox: dlq enqueue failed",
			slog.String("queue", queue), slog.String("id", msg.ID), slog.Any("err", err))
	}
	if err := b.store.Dead(b.storeCtx(), queue, sourceID, handlerErr); err != nil {
		b.logger.ErrorContext(b.storeCtx(), "shoebox: dead-letter stat failed",
			slog.String("queue", queue), slog.String("id", sourceID), slog.Any("err", err))
	}
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
		// Do not block past the caller's deadline on a non-cooperative handler.
		// The store remains open until all workers finish, so it is never closed
		// underneath a handler still using it.
		go func() {
			<-done
			b.closeOnce.Do(func() { _ = b.store.Close() })
		}()
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

// Queues returns the names of all registered queues. Used by the metrics
// depth collector to know which queues to poll on scrape.
func (b *Broker) Queues() []string {
	b.hMu.RLock()
	defer b.hMu.RUnlock()
	out := make([]string, 0, len(b.handlers))
	for q := range b.handlers {
		out = append(out, q)
	}
	return out
}

// Stats returns a snapshot of queue stats.
func (b *Broker) Stats(ctx context.Context, queue string) (storage.QueueStats, error) {
	return b.store.Stats(ctx, queue)
}

// Store returns the underlying storage interface. Exposed so the public Queue
// can pass it to packages like dlq without a separate handle.
func (b *Broker) Store() storage.Storage {
	return b.store
}

// --- Pause / Resume / Drain (ADR 0006 §Pause & drain) ---

// Pause stops the dispatcher from dequeuing messages on the given queue.
// Messages already in-flight continue to run; new messages accumulate in
// storage until Resume is called. Pause is idempotent.
func (b *Broker) Pause(queue string) {
	b.pausedFlag(queue).Store(true)
}

// Resume allows the dispatcher to start dequeuing messages again. If the
// dispatcher has already exited (e.g. after Drain), Resume re-registers the
// handler's dispatcher. Resume is idempotent.
func (b *Broker) Resume(queue string) {
	b.pausedFlag(queue).Store(false)
	b.qDrainingMu.Lock()
	if flag, ok := b.qDraining[queue]; ok {
		flag.Store(false)
	}
	b.qDrainingMu.Unlock()
	b.hMu.Lock()
	if _, ok := b.handlers[queue]; ok && !b.dispatching[queue] && !b.closed.Load() {
		b.dispatching[queue] = true
		b.wg.Add(1)
		go b.dispatch(queue)
	}
	b.hMu.Unlock()
	// Nudge the dispatcher in case it's sleeping in the poll loop.
	b.hMu.RLock()
	wake, ok := b.dispatchC[queue]
	b.hMu.RUnlock()
	if ok {
		select {
		case wake <- struct{}{}:
		default:
		}
	}
}

// Drain processes all remaining messages on the given queue to quiescence
// (empty Dequeue + zero in-flight workers), then stops that queue's
// dispatcher. It blocks until the drain completes or ctx expires.
//
// Unlike Shutdown, Drain only affects a single queue — other queues keep
// running. After Drain returns, the queue's dispatcher has exited; Pause +
// Resume can restart it (Resume re-registers if needed).
func (b *Broker) Drain(ctx context.Context, queue string) error {
	// Check if the queue has a running dispatcher. If not, nothing to drain.
	b.hMu.RLock()
	_, hasHandler := b.handlers[queue]
	b.hMu.RUnlock()
	if !hasHandler {
		return nil
	}

	// Set up the per-queue drain flag and a fresh done channel.
	// The done channel is created (or replaced) under the lock so that
	// signalDrainDone sees the same channel we wait on below.
	b.qDrainingMu.Lock()
	flag, ok := b.qDraining[queue]
	if !ok {
		flag = &atomic.Bool{}
		b.qDraining[queue] = flag
	}
	flag.Store(true)
	// Replace the done channel with a fresh one so a previous (closed)
	// channel from an earlier Drain doesn't cause a false immediate return.
	done := make(chan struct{})
	b.drainDone[queue] = done
	b.qDrainingMu.Unlock()

	// Signal the dispatcher to wake up and notice the drain flag.
	b.hMu.RLock()
	wake, ok := b.dispatchC[queue]
	b.hMu.RUnlock()
	if ok {
		select {
		case wake <- struct{}{}:
		default:
		}
	}

	select {
	case <-done:
	case <-ctx.Done():
		// Clean up: clear the flag so the dispatcher doesn't exit later.
		flag.Store(false)
		return ctx.Err()
	}

	// Cleanup: purge dedupe entries for this queue to free memory.
	if purger, ok := b.dedupe.(queueDedupePurger); ok {
		purger.purgeQueue(queue)
	}
	return nil
}

// --- helpers ---

// pausedFlag returns the atomic flag for a queue, creating one on demand.
func (b *Broker) pausedFlag(queue string) *atomic.Bool {
	b.pausedMu.Lock()
	defer b.pausedMu.Unlock()
	f, ok := b.paused[queue]
	if !ok {
		f = &atomic.Bool{}
		b.paused[queue] = f
	}
	return f
}

// isPaused reports whether a queue is paused.
func (b *Broker) isPaused(queue string) bool {
	return b.pausedFlag(queue).Load()
}

// isQueueDraining reports whether a per-queue drain has been requested.
func (b *Broker) isQueueDraining(queue string) bool {
	b.qDrainingMu.Lock()
	defer b.qDrainingMu.Unlock()
	f, ok := b.qDraining[queue]
	if !ok {
		return false
	}
	return f.Load()
}

// signalDrainDone closes the done channel for a queue's drain, unblocking
// Drain's wait. Drain creates a fresh channel each time, so this close is
// always the first and only close for that channel.
func (b *Broker) signalDrainDone(queue string) {
	b.qDrainingMu.Lock()
	defer b.qDrainingMu.Unlock()
	if ch, ok := b.drainDone[queue]; ok {
		close(ch)
	}
}
