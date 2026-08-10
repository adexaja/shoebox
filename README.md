# shoebox

Embedded message queue for Go. No broker. No Docker. No YAML for exchanges and
bindings. Import the library, call `shoebox.Enqueue("orders", payload)`, keep
moving.

The name is the pitch: when you outgrow a Go channel but RabbitMQ feels like
bringing a forklift to move a shoebox, **shoebox** is the thing you wish
existed.

## Quick start

```go
import "github.com/adexaja/shoebox"

func main() {
    q, _ := shoebox.New(shoebox.Options{
        Storage: shoebox.Memory, // or shoebox.SQLite, shoebox.Postgres
    })

    q.Handle("orders", func(ctx context.Context, msg shoebox.Message) error {
        // process the order
        return nil
    })

    _ = q.Enqueue("orders", []byte(`{"order_id": 123}`))
}
```

## Features

### Storage backends

| Backend | Config | Survives restarts | Notes |
|---------|--------|-------------------|-------|
| **Memory** | `shoebox.Memory` | No | In-process slice + mutex. Zero dependencies. Default for dev/testing. |
| **SQLite** | `shoebox.SQLite` + `Options.Path` | Yes | Pure-Go driver (`modernc.org/sqlite`, no CGo). Crash recovery via status lifecycle. |
| **Postgres** | `shoebox.Postgres` + `Options.DSN` | Yes | `pgx/v5` with connection pool. `SELECT … FOR UPDATE SKIP LOCKED` for safe concurrent consumers. |

```go
// SQLite — survives restarts, zero config
q, _ := shoebox.New(shoebox.Options{
    Storage: shoebox.SQLite,
    Path:    "/var/lib/myapp/queue.db",
})

// Postgres — multiple consumer processes, horizontal scaling
q, _ := shoebox.New(shoebox.Options{
    Storage: shoebox.Postgres,
    DSN:     "host=localhost port=5432 dbname=shoebox user=postgres sslmode=disable",
})
```

### Retry with backoff

Configurable max retries per handler with exponential, constant, or custom
backoff strategies. Messages that exhaust their retries are moved to a
dead-letter queue automatically.

```go
q.Handle("webhooks", handler, shoebox.HandlerOptions{
    MaxRetries: 10,
    Timeout:    30 * time.Second,
})
```

### Dead-letter queue (DLQ)

Every queue gets a `{queue}.dlq` shadow queue. Failed messages retain their
original payload, the last error, retry count, and timestamp. List, inspect,
and replay programmatically:

```go
mgr := dlq.NewManager(q.Store())
records, _ := mgr.List(ctx, "orders", 50)     // browse dead messages
record, _ := mgr.Inspect(ctx, "orders", id)   // inspect a single message
mgr.Replay(ctx, "orders", id)                 // move back to source queue
```

### Graceful shutdown

`Shutdown(ctx)` drains all queues to authoritative quiescence — every
in-flight handler completes (or the context expires), follow-up enqueues from
handlers are picked up, and the store is closed exactly once.

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
q.Shutdown(ctx)
```

### Observability

Prometheus metrics (per-queue labels): depth gauge, processed/errors/retries/
dead counters, handler duration histogram. Mount the handler at `/metrics`:

```go
http.Handle("/metrics", q.MetricsHandler())
```

Pass a custom registry via `Options.MetricsRegistry` to isolate metrics in
tests or run multiple shoebox instances in one process.

### Middleware

Built-in middleware applies in registration order (first `Use` is outermost):

```go
q.Use(shoebox.RecoveryMiddleware())           // recover panics → error → retry/DLQ
q.Use(shoebox.MetricsMiddleware(q.metrics))   // record Prometheus metrics
q.Use(shoebox.LoggingMiddleware())            // structured slog logs
q.Use(shoebox.TimeoutMiddleware(30*time.Second))
```

`RecoveryMiddleware` catches handler panics, logs the stack trace, and
converts the panic to an error so the message is retried or dead-lettered
instead of crashing the process.

### Standalone server (`shoeboxd`)

Run shoebox as a standalone HTTP server with a dashboard, REST API, Prometheus
metrics, and per-queue webhook push delivery:

```sh
shoeboxd --config=config.yaml
```

Example `config.yaml`:

```yaml
server:
  addr: ":8080"
  auth_token: "secret"

storage:
  kind: sqlite
  path: "shoebox.db"

webhooks:
  orders:
    url: "https://hooks.example.com/orders"
    timeout: 10s
```

CLI flags (`--addr`, `--storage`, `--path`, `--dsn`, `--auth-token`) override
config-file values. See `cmd/shoeboxd/config.example.yaml`.

**HTTP API** (6 endpoints under `/api/`):

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/queues/{name}/messages` | Enqueue a message |
| `GET` | `/queues/{name}/messages/next` | Pull-consume one message |
| `GET` | `/queues/{name}/stats` | Queue depth + counters |
| `GET` | `/queues/{name}/dlq` | List dead-letter messages |
| `POST` | `/queues/{name}/dlq/{id}/replay` | Replay a dead message |
| `DELETE` | `/queues/{name}/messages/{id}` | Ack/delete a message |

**Webhook push delivery** — declare webhooks in the config file and the broker
POSTs each message to the target URL. Non-2xx triggers the normal
retry/backoff/DLQ path. Also usable in library mode:

```go
q.Handle("orders", shoebox.WebhookHandler("https://hooks.example.com/orders"))
```

### Delayed & scheduled messages

Defer visibility until a future time — no external scheduler needed:

```go
// Visible after 30 minutes
q.Enqueue("reminders", payload, shoebox.Delay(30*time.Minute))

// Visible at a specific time
q.Enqueue("reports", payload, shoebox.Schedule(time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC)))
```

If both are set, `Schedule` wins. All backends filter `scheduled_at <= now`
on Dequeue; the dispatcher's 250ms poll picks up messages whose delay just
elapsed.

### Priority queues

Higher-priority messages jump ahead of lower-priority ones within the same
queue. Ties at the same priority level fall back to FIFO (`created_at ASC`).

```go
q.Enqueue("emails", payload, shoebox.WithPriority(shoebox.High))
q.Enqueue("emails", payload)                              // default: Low
```

Constants: `shoebox.Low` (0), `shoebox.Normal` (1), `shoebox.High` (2).
Implemented as `ORDER BY priority DESC, created_at ASC` on all backends.

### Message deduplication

Suppress duplicate enqueues within a per-key TTL window (default 5 minutes).
The second Enqueue with the same key is silently dropped (returns nil, no
store write):

```go
q.Enqueue("orders", payload, shoebox.DedupeKey("order-123"))
q.Enqueue("orders", payload, shoebox.DedupeKey("order-123")) // silently dropped
```

Keys are scoped per-queue — `("orders", "x")` and `("emails", "x")` are
independent. Dedupe state lives in the broker (not storage), so it does not
survive a restart.

### Queue lifecycle: Pause, Resume, Drain

Control dispatching without shutting down the whole broker:

```go
q.Pause("orders")   // stop dequeuing; in-flight handlers finish naturally
// ... back up, migrate, etc. ...
q.Resume("orders")  // dispatching resumes

// Process everything to quiescence, then stop just this queue's dispatcher:
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
q.Drain(ctx, "orders")
```

`Pause`/`Resume` are atomic flags (no I/O). `Drain` processes the named
queue to authoritative quiescence (empty Dequeue + zero in-flight workers),
then stops only that queue's dispatcher — other queues keep running.

## Status

| Epic | Title | Status |
|------|-------|--------|
| E1 | Core broker + memory storage | ✅ Done |
| E2 | Persistence + retry + DLQ | ✅ Done |
| E3 | Observability + middleware | ✅ Done |
| E4 | Standalone server + webhooks | ✅ Done |
| E5 | Polish + launch | ✅ Done |
| E6 | Advanced features (delay, dedupe, priority, pause/drain) | ✅ Done |

**130+ tests**, all `-race`-clean. See the parent `docs/` directory
(not git-tracked) for epics, user stories, tasks, and ADRs.

## Layout

```
.
├── shoebox.go              # public API
├── webhook.go              # WebhookHandler (push delivery)
├── options.go              # Options, HandlerOptions, EnqueueOptions
├── message.go              # Message and QueueStats types
├── middleware.go           # built-in middleware
├── metrics.go              # Prometheus collectors
├── migrations/             # versioned SQL migrations (SQLite + Postgres)
├── cmd/shoeboxd/           # standalone server binary (E4)
├── examples/               # runnable examples
│   ├── lead-assignment/    # round-robin + follow-up enqueue
│   ├── webhook-retry/      # exponential backoff + DLQ
│   └── email-sender/       # batch email with retry/backoff
└── internal/
    ├── broker/             # dispatch engine + lifecycle + dedupe
    ├── storage/            # Storage interface + Memory/SQLite/Postgres
    ├── retry/              # exponential + constant backoff
    ├── dlq/                # dead-letter queue manager
    ├── api/                # HTTP API (E4)
    ├── config/             # YAML config parser (shoeboxd)
    └── dashboard/          # web UI (E4)
```

## Docker

```sh
# Build the image (multi-stage, distroless runtime — ~15 MB)
docker build -t shoeboxd .

# Run with defaults (memory storage, port 8080)
docker run -p 8080:8080 shoeboxd

# Run with SQLite persistence + config file
docker run -p 8080:8080 \
  -v $(pwd)/config.yaml:/etc/shoebox/config.yaml:ro \
  -v shoebox-data:/data \
  shoeboxd --config=/etc/shoebox/config.yaml
```

The image runs as non-root (`distroless/static-debian12:nonroot`). Override
any setting with CLI flags (they take precedence over the config file).


# Benchmarks

> Hardware: Apple M2, darwin/arm64. Go 1.26. Measured 2026-08-11.
> Run: `go test -bench . -benchmem -run '^$' ./internal/storage/`
> and `go test -bench 'BrokerThroughput' -benchmem -run '^$' -benchtime 5000x .`
> Postgres benchmarks need a reachable local Postgres
> (`host=localhost dbname=shoebox`) and skip when it is down.

## Storage primitives (raw backend)

| Benchmark | ns/op | B/op | allocs/op |
|-----------|------:|-----:|----------:|
| MemoryEnqueue | ~500 | ~1 000 | 1 |
| MemoryDequeue_SteadyState (depth 1000, dequeue 1 + re-enqueue 1) | ~14 300 | 472 | 6 |
| MemoryDequeue_Batch (depth 10k, dequeue 100 + top-up 100) | ~159 000 | 24 200 | 303 |
| SQLiteEnqueue (one tx + fsync) | ~356 000 | 2 410 | 35 |
| SQLiteDequeue (one tx, depth 1000) | ~1 780 000 | 9 300 | 167 |
| PostgresEnqueue (one round-trip tx) | ~146 000 | 552 | 14 |
| PostgresDequeue (one tx, depth 1000) | ~781 000 | 2 703 | 56 |

## Broker throughput (public API, end-to-end)

Concurrency 8, payload 24 bytes, `-benchtime 5000x`.

| Benchmark | ns/op (per message) | throughput |
|-----------|--------------------:|-----------:|
| BrokerThroughput_Memory | 2 899 | 8.6 MB/s ≈ ~345 000 msg/s |
| BrokerThroughput_SQLite | 887 333 | 0.03 MB/s ≈ ~1 100 msg/s |
| BrokerThroughput_Postgres | 261 273 | 0.10 MB/s ≈ ~3 800 msg/s |

## Interpretation

- **Memory Enqueue is ~500ns** — a single `append` + mutex + timestamp, one
  allocation for the `Message` copy. As fast as it gets for this design.
- **Steady-state dequeue at depth 1000 is ~14µs.** That includes the dirty
  priority sort (`O(depth log depth)` ≈ 10k comparisons at depth 1000). The
  dirty-flag optimization (E6 audit fix #4) means idle polls — where nothing
  was enqueued since the last dequeue — skip the sort entirely, so the
  steady-state **idle poll cost is ~0** once no messages arrive.
- **Batch dequeue (~159µs)** scales with the batch: 100 messages moved from
  the pending slice in ~159µs ≈ **~630k messages/sec** sustained drain.
- **SQLite is disk-bound**: each Enqueue is a committed transaction
  (journal + fsync). ~350µs/op enqueue and ~ 1.78ms/op dequeue are the cost of
  durability, not the broker. Broker Throughput_SQLite (~1 100 msg/s) reflects
  this: every message round-trips through two transactions (enqueue + dequeue/ack).
- **Postgres is network+commit bound**: a round-trip insert per Enqueue and
  `FOR UPDATE SKIP LOCKED` per Dequeue. ~146µs/op enqueue, ~781µs/op dequeue,
  14/56 allocs. It still beats SQLite (~2.3x) here — Postgres's WAL + pooled
  connections amortize fsync far better than SQLite's per-commit sync.
  Broker Throughput_Postgres (~3 800 msg/s) is the same two-transaction
  round-trip as SQLite.
- **Headline:** with the in-memory backend, the full broker path (enqueue →
  dispatcher → handler → ack) sustains **~345k messages/sec** on a single M2
  core-pair. SQLite trades ~300x throughput for persistence.

## Cost of correctness

The broker adds about **8 allocs/op** over the raw memory enqueue `1 alloc/op`
— HandlerContext wrapper, middleware chain, the per-message `Message` copy
through dispatch. Negligible.


## License

MIT.
