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

## Status

| Epic | Title | Status |
|------|-------|--------|
| E1 | Core broker + memory storage | ✅ Done |
| E2 | Persistence + retry + DLQ | ✅ Done |
| E3 | Observability + middleware | ✅ Done |
| E4 | Standalone server | ⏳ Pending |
| E5 | Polish + launch | ⏳ Pending |

All code is tested under `go test -race`. See the parent `docs/` directory
(not git-tracked) for epics, user stories, tasks, and ADRs.

## Layout

```
.
├── shoebox.go              # public API
├── options.go              # Options, HandlerOptions, EnqueueOptions
├── message.go              # Message and QueueStats types
├── middleware.go           # built-in middleware
├── migrations/             # versioned SQL migrations (SQLite + Postgres)
├── cmd/shoeboxd/           # standalone server binary (E4)
├── examples/               # runnable examples
│   ├── lead-assignment/    # round-robin + follow-up enqueue
│   └── webhook-retry/      # exponential backoff + DLQ
└── internal/
    ├── broker/             # dispatch engine + lifecycle
    ├── storage/            # Storage interface + Memory/SQLite/Postgres
    ├── retry/              # exponential + constant backoff
    ├── dlq/                # dead-letter queue manager
    ├── api/                # HTTP API (E4)
    └── dashboard/          # web UI (E4)
```

## License

MIT.
