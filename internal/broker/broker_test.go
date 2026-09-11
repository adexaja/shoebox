package broker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/adexaja/shoebox/internal/retry"
	"github.com/adexaja/shoebox/internal/storage"
)

// quietBroker builds a Broker whose logs go to io.Discard so test output
// stays readable.
func quietBroker(t *testing.T, store storage.Storage, concurrency int) *Broker {
	t.Helper()
	if concurrency <= 0 {
		concurrency = 4
	}
	return New(Options{
		Storage:     store,
		Concurrency: concurrency,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
}

// closeCounter wraps a Storage and counts Close() calls so tests can assert
// the broker closes the store exactly once across every exit path.
type closeCounter struct {
	storage.Storage
	closes *atomic.Int64
}

func (c *closeCounter) Close() error {
	c.closes.Add(1)
	if cl, ok := c.Storage.(interface{ Close() error }); ok {
		_ = cl.Close()
	}
	return nil
}

type enqueueCountingStore struct {
	storage.Storage
	enqueues     int
	batches      int
	ackBatches   int
	failBatch    bool
	ackBatchCh   chan struct{}
	failAckBatch bool
}

func (s *enqueueCountingStore) Enqueue(ctx context.Context, queue string, msg storage.Message) error {
	s.enqueues++
	return s.Storage.Enqueue(ctx, queue, msg)
}

func (s *enqueueCountingStore) EnqueueBatch(ctx context.Context, queue string, messages []storage.Message) error {
	s.batches++
	if s.failBatch {
		return errors.New("batch failed")
	}
	return s.Storage.EnqueueBatch(ctx, queue, messages)
}
func (s *enqueueCountingStore) AckBatch(ctx context.Context, queue string, ids []string) error {
	s.ackBatches++
	if s.failAckBatch {
		return errors.New("ack batch failed")
	}
	err := s.Storage.AckBatch(ctx, queue, ids)
	if err == nil && s.ackBatchCh != nil {
		select {
		case s.ackBatchCh <- struct{}{}:
		default:
		}
	}
	return err
}

func TestEnqueueUsesSingleStoragePath(t *testing.T) {
	store := &enqueueCountingStore{Storage: storage.NewMemory()}
	b := quietBroker(t, store, 1)

	if err := b.Enqueue(context.Background(), "q", []byte("x"), EnqueueOpts{}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if store.enqueues != 1 || store.batches != 0 {
		t.Fatalf("calls: Enqueue=%d EnqueueBatch=%d, want 1/0", store.enqueues, store.batches)
	}
}

func TestEnqueueBatchUsesBatchStoragePath(t *testing.T) {
	store := &enqueueCountingStore{Storage: storage.NewMemory()}
	b := quietBroker(t, store, 1)

	if err := b.EnqueueBatch(context.Background(), "q", []EnqueueBatchItem{
		{Payload: []byte("a")},
		{Payload: []byte("b")},
	}); err != nil {
		t.Fatalf("EnqueueBatch: %v", err)
	}
	if store.enqueues != 0 || store.batches != 1 {
		t.Fatalf("calls: Enqueue=%d EnqueueBatch=%d, want 0/1", store.enqueues, store.batches)
	}
}

func TestEnqueueBatchRollsBackDedupeOnStoreFailure(t *testing.T) {
	mem := storage.NewMemory()
	store := &enqueueCountingStore{Storage: mem, failBatch: true}
	b := quietBroker(t, store, 1)

	err := b.EnqueueBatch(context.Background(), "q", []EnqueueBatchItem{
		{Payload: []byte("failed"), Opts: EnqueueOpts{DedupeKey: "k"}},
	})

	if err == nil {
		t.Fatal("EnqueueBatch succeeded despite storage failure")
	}
	store.failBatch = false
	if err := b.Enqueue(context.Background(), "q", []byte("retry"), EnqueueOpts{DedupeKey: "k"}); err != nil {
		t.Fatalf("Enqueue retry: %v", err)
	}
	msgs, err := mem.Dequeue(context.Background(), "q", 1)
	if err != nil {
		t.Fatalf("Dequeue retry: %v", err)
	}
	if len(msgs) != 1 || string(msgs[0].Payload) != "retry" {
		t.Fatalf("dequeued = %v, want retry", msgs)
	}
}
func TestAckBatchFlushesOnSize(t *testing.T) {
	store := &enqueueCountingStore{
		Storage:    storage.NewMemory(),
		ackBatchCh: make(chan struct{}, 1),
	}
	b := New(Options{
		Storage:          store,
		Concurrency:      1,
		Logger:           slog.New(slog.NewTextHandler(io.Discard, nil)),
		AckBatchSize:     2,
		AckFlushInterval: time.Hour,
	})
	defer func() { _ = b.Shutdown(context.Background()) }()
	b.Register("q", func(context.Context, storage.Message) error { return nil }, HandlerOptions{})

	for _, payload := range []string{"a", "b"} {
		if err := b.Enqueue(context.Background(), "q", []byte(payload), EnqueueOpts{}); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}
	waitForAckBatch(t, store.ackBatchCh)
	if store.ackBatches != 1 {
		t.Fatalf("ack batches = %d, want 1", store.ackBatches)
	}
}

func TestAckBatchFlushesOnInterval(t *testing.T) {
	store := &enqueueCountingStore{
		Storage:    storage.NewMemory(),
		ackBatchCh: make(chan struct{}, 1),
	}
	b := New(Options{
		Storage:          store,
		Concurrency:      1,
		Logger:           slog.New(slog.NewTextHandler(io.Discard, nil)),
		AckBatchSize:     100,
		AckFlushInterval: 5 * time.Millisecond,
	})
	defer func() { _ = b.Shutdown(context.Background()) }()
	b.Register("q", func(context.Context, storage.Message) error { return nil }, HandlerOptions{})

	if err := b.Enqueue(context.Background(), "q", []byte("a"), EnqueueOpts{}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	waitForAckBatch(t, store.ackBatchCh)
}

func TestAckBatchFlushesOnDrainAndShutdown(t *testing.T) {
	t.Run("drain", func(t *testing.T) {
		store := &enqueueCountingStore{
			Storage:    storage.NewMemory(),
			ackBatchCh: make(chan struct{}, 1),
		}
		b := New(Options{
			Storage:          store,
			Concurrency:      1,
			Logger:           slog.New(slog.NewTextHandler(io.Discard, nil)),
			AckBatchSize:     100,
			AckFlushInterval: time.Hour,
		})
		b.Register("q", func(context.Context, storage.Message) error { return nil }, HandlerOptions{})
		if err := b.Enqueue(context.Background(), "q", []byte("a"), EnqueueOpts{}); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		if err := b.Drain(context.Background(), "q"); err != nil {
			t.Fatalf("Drain: %v", err)
		}
		if store.ackBatches != 1 {
			t.Fatalf("ack batches = %d, want 1", store.ackBatches)
		}
		if err := b.Shutdown(context.Background()); err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	})

	t.Run("shutdown", func(t *testing.T) {
		store := &enqueueCountingStore{
			Storage:    storage.NewMemory(),
			ackBatchCh: make(chan struct{}, 1),
		}
		b := New(Options{
			Storage:          store,
			Concurrency:      1,
			Logger:           slog.New(slog.NewTextHandler(io.Discard, nil)),
			AckBatchSize:     100,
			AckFlushInterval: time.Hour,
		})
		b.Register("q", func(context.Context, storage.Message) error { return nil }, HandlerOptions{})
		if err := b.Enqueue(context.Background(), "q", []byte("a"), EnqueueOpts{}); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		if err := b.Shutdown(context.Background()); err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
		if store.ackBatches != 1 {
			t.Fatalf("ack batches = %d, want 1", store.ackBatches)
		}
	})
}

func waitForAckBatch(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for AckBatch")
	}
}

// TestShutdown_DrainFollowupDuringDrain is the regression test for E1-CONC-1
// and E1-CONC-4. It forces the exact race the audit described: a handler,
// while Shutdown is draining, enqueues a follow-up message *after* the
// dispatcher has had every opportunity to observe an empty queue and decide
// the drain is complete.
//
// The authoritative drain condition (empty dequeue AND zero in-flight workers)
// is what guarantees the follow-up is picked up. On the old depth-polling
// logic the dispatcher would exit during the seed worker's hold — because the
// seed is in-flight, not in the queue, so depth reads zero — and the follow-up
// would be orphaned.
func TestShutdown_DrainFollowupDuringDrain(t *testing.T) {
	store := storage.NewMemory()
	b := quietBroker(t, store, 1) // concurrency 1 to serialize the race

	var processed atomic.Int64
	seedStarted := make(chan struct{})
	releaseSeed := make(chan struct{})

	b.Register("jobs", func(ctx context.Context, m storage.Message) error {
		if string(m.Payload) == "seed" {
			// Signal the seed worker is now in-flight (inflight == 1).
			once := sync.Once{}
			once.Do(func() { close(seedStarted) })
			// Hold the seed worker open. While it is held, the queue reads
			// empty (the seed is in-flight, not pending) but inflight == 1.
			select {
			case <-releaseSeed:
			case <-ctx.Done():
				return ctx.Err()
			}
			// Now enqueue the follow-up *during* the drain. This is the
			// re-enqueue a surviving handler makes.
			if err := b.Enqueue(ctx, "jobs", []byte("followup"), EnqueueOpts{}); err != nil {
				return err
			}
		}
		processed.Add(1)
		return nil
	}, HandlerOptions{MaxRetries: 3, Timeout: 5 * time.Second})

	if err := b.Enqueue(context.Background(), "jobs", []byte("seed"), EnqueueOpts{}); err != nil {
		t.Fatalf("enqueue seed: %v", err)
	}

	// Wait until the seed worker is in-flight (inflight == 1).
	<-seedStarted

	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- b.Shutdown(context.Background()) }()

	// Wait for the drain to be requested, then give the dispatcher time to
	// loop once: dequeue empty, observe draining, and (on buggy code) exit.
	// The generous window is deliberate — it is the regression trigger.
	for !b.draining() {
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(300 * time.Millisecond)

	// Release the seed worker. It enqueues the follow-up and returns.
	close(releaseSeed)

	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatalf("shutdown: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown hung: follow-up enqueue was orphaned (CONC-1/CONC-4)")
	}

	if got := processed.Load(); got != 2 {
		t.Errorf("expected seed + follow-up processed (2), got %d — follow-up orphaned", got)
	}
}

// TestShutdown_DrainsScheduledRetry verifies that Shutdown waits for a retry
// whose backoff has not elapsed yet. Dequeue returns ErrEmpty while the retry
// is scheduled in the future, but the message is still pending.
func TestShutdown_DrainsScheduledRetry(t *testing.T) {
	store := storage.NewMemory()
	b := quietBroker(t, store, 1)

	var attempts atomic.Int64
	b.Register("jobs", func(_ context.Context, m storage.Message) error {
		if attempts.Add(1) == 1 {
			return errors.New("transient")
		}
		return nil
	}, HandlerOptions{
		MaxRetries: 1,
		Backoff:    retry.Constant(50 * time.Millisecond),
	})

	if err := b.Enqueue(context.Background(), "jobs", []byte("retry-me"), EnqueueOpts{}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := b.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	if got := attempts.Load(); got != 2 {
		t.Fatalf("handler attempts = %d, want 2", got)
	}
	stats, err := store.Stats(context.Background(), "jobs")
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.Processed != 1 || stats.Retries != 1 || stats.Depth != 0 {
		t.Fatalf("stats = %+v, want processed=1 retries=1 depth=0", stats)
	}
}

// TestShutdown_TimeoutUnblocksWedgedHandler is the regression test for
// E1-CONC-3. A handler that blocks until its context expires must not hang
// the drain: the per-handler Timeout expires the context, a cooperative
// handler returns, and Shutdown completes well within budget. Without the
// Timeout wired through handlerCtx the drain would block forever.
func TestShutdown_TimeoutUnblocksWedgedHandler(t *testing.T) {
	store := storage.NewMemory()
	b := quietBroker(t, store, 1)

	b.Register("stuck", func(ctx context.Context, m storage.Message) error {
		<-ctx.Done() // cooperative: returns when the per-handler ctx expires
		return ctx.Err()
	}, HandlerOptions{MaxRetries: 0, Timeout: 50 * time.Millisecond})

	if err := b.Enqueue(context.Background(), "stuck", []byte("x"), EnqueueOpts{}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	start := time.Now()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := b.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	elapsed := time.Since(start)

	// The handler's Timeout is 50ms; Shutdown must complete far inside the
	// 3s budget. If the Timeout were not applied, Shutdown would block on
	// wg until the 3s shutdownCtx expired (or hang forever after abort).
	if elapsed > 1*time.Second {
		t.Fatalf("shutdown took %v; wedged handler was not unblocked by Timeout (CONC-3)", elapsed)
	}
}

// TestShutdown_ClosesStoreOnce is the regression test for E1-CONC-2 and
// E1-CONC-5. The store must be closed exactly once on the normal drain path,
// and a second Shutdown must not close it again.
func TestShutdown_ClosesStoreOnce(t *testing.T) {
	store := &closeCounter{Storage: storage.NewMemory(), closes: &atomic.Int64{}}
	b := quietBroker(t, store, 2)

	b.Register("q", func(ctx context.Context, m storage.Message) error { return nil }, HandlerOptions{})
	for i := 0; i < 5; i++ {
		if err := b.Enqueue(context.Background(), "q", []byte("x"), EnqueueOpts{}); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := b.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if got := store.closes.Load(); got != 1 {
		t.Errorf("after normal shutdown, store closed %d times, want 1", got)
	}

	// Second Shutdown must be rejected and must not close again.
	if err := b.Shutdown(context.Background()); !errors.Is(err, errAlreadyClosed) {
		t.Errorf("second shutdown err = %v, want errAlreadyClosed", err)
	}
	if got := store.closes.Load(); got != 1 {
		t.Errorf("after double shutdown, store closed %d times, want 1 (CONC-5)", got)
	}
}

// TestShutdown_ForceAbortStillClosesStore verifies that when Shutdown's
// context expires before in-flight workers finish, the broker returns without
// closing the store underneath the worker, then closes it exactly once after
// the worker exits.
func TestShutdown_ForceAbortStillClosesStore(t *testing.T) {
	store := &closeCounter{Storage: storage.NewMemory(), closes: &atomic.Int64{}}
	b := quietBroker(t, store, 1)

	started := make(chan struct{})
	handlerDone := make(chan struct{})
	b.Register("q", func(ctx context.Context, m storage.Message) error {
		close(started)
		// Sleep past the Shutdown budget so its ctx expires and forces abort.
		time.Sleep(250 * time.Millisecond)
		close(handlerDone)
		return nil
	}, HandlerOptions{})

	if err := b.Enqueue(context.Background(), "q", []byte("x"), EnqueueOpts{}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	<-started // worker is in-flight before we start Shutdown

	// 20ms budget vs a 250ms handler: ctx expires first, triggering abort.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := b.Shutdown(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("shutdown err = %v, want DeadlineExceeded", err)
	}

	// The worker must have finished — Shutdown waited on wg even after abort.
	select {
	case <-handlerDone:
	case <-time.After(3 * time.Second):
		t.Fatal("in-flight worker was not awaited after force-abort (CONC-2)")
	}

	deadline := time.Now().Add(time.Second)
	for store.closes.Load() != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := store.closes.Load(); got != 1 {
		t.Errorf("after worker exit, store closed %d times, want 1 (CONC-2/5)", got)
	}
}

// TestConcurrentEnqueueAndShutdown stresses the Enqueue/Shutdown race under
// the race detector. Enqueue is safe to call concurrently with Shutdown
// (including after drain has begun); the contract is simply that it does not
// panic, does not race, and Shutdown eventually completes.
func TestConcurrentEnqueueAndShutdown(t *testing.T) {
	store := storage.NewMemory()
	b := quietBroker(t, store, 8)

	b.Register("q", func(ctx context.Context, m storage.Message) error { return nil }, HandlerOptions{})

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_ = b.Enqueue(context.Background(), "q", []byte("x"), EnqueueOpts{})
			}
		}()
	}

	// Let producers run, then stop them and drain concurrently.
	time.Sleep(50 * time.Millisecond)
	close(stop)

	done := make(chan struct{})
	go func() {
		_ = b.Shutdown(context.Background())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("shutdown hung under concurrent enqueue")
	}
	wg.Wait()
}
