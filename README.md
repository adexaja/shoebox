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

## Status

| Epic | Title | Status |
|------|-------|--------|
| E1 | Core broker + memory storage | ✅ Done |
| E2 | Persistence + retry + DLQ | ✅ Done |
| E3 | Observability + middleware | ✅ Done |
| E4 | Standalone server + webhooks | ✅ Done |
| E5 | Polish + launch | ✅ Done |
| E6 | Advanced features (delay, dedupe, priority, pause/drain) | 📋 Planned |

**115+ tests**, all `-race`-clean. See the parent `docs/` directory
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
    ├── broker/             # dispatch engine + lifecycle
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

## License

MIT.
