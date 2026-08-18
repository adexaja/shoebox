# shoebox

[![Go Reference](https://pkg.go.dev/badge/github.com/adexaja/shoebox.svg)](https://pkg.go.dev/github.com/adexaja/shoebox)
[![Build Status](https://github.com/adexaja/shoebox/actions/workflows/ci.yml/badge.svg)](https://github.com/adexaja/shoebox/actions/workflows/ci.yml)

`shoebox` is an embedded message queue for Go. It runs in the application
process and supports in-memory, SQLite, and PostgreSQL storage. There is no
separate broker to deploy.

## Install shoeboxd

Install the standalone server command with:

```sh
go install github.com/adexaja/shoebox/cmd/shoeboxd@latest
```

Then run `shoeboxd --config=config.yaml`. With modern Go versions, use
`go install` for executables; `go get` adds library dependencies to a module.

Use it when a Go channel is too limited but operating RabbitMQ or Kafka is not
justified for the workload.

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

## Storage

| Backend | Configuration | Survives restart | Notes |
|---------|---------------|------------------|-------|
| Memory | `shoebox.Memory` | No | In-process storage protected by a mutex. Useful for tests and local development. |
| SQLite | `shoebox.SQLite` and `Options.Path` | Yes | Uses the pure-Go `modernc.org/sqlite` driver; no CGo. Recovers messages using the status lifecycle. |
| PostgreSQL | `shoebox.Postgres` and `Options.DSN` | Yes | Uses `pgx/v5` and a connection pool. Concurrent consumers claim work with `SELECT … FOR UPDATE SKIP LOCKED`. |

SQLite example:

```go
q, _ := shoebox.New(shoebox.Options{
    Storage: shoebox.SQLite,
    Path:    "/var/lib/myapp/queue.db",
})
```

PostgreSQL example:

```go
q, _ := shoebox.New(shoebox.Options{
    Storage: shoebox.Postgres,
    DSN:     "host=localhost port=5432 dbname=shoebox user=postgres sslmode=disable",
    Schema:  "worker", // optional; defaults to "public"
})
```

## Delivery behavior

### Retries and dead letters

Handlers can set a retry limit, timeout, and backoff strategy. The supported
backoff strategies are exponential, constant, and custom. When a message
exhausts its retries, it is moved to a dead-letter queue.

```go
q.Handle("webhooks", handler, shoebox.HandlerOptions{
    MaxRetries: 10,
    Timeout:    30 * time.Second,
})
```

Each queue has a corresponding `{queue}.dlq` queue. Dead-letter records retain
the original payload, last error, retry count, and timestamp.

```go
mgr := dlq.NewManager(q.Store())
records, _ := mgr.List(ctx, "orders", 50)
record, _ := mgr.Inspect(ctx, "orders", id)
mgr.Replay(ctx, "orders", id)
```

### Delayed and scheduled messages

Messages can become visible after a delay or at a specific time. If both
options are supplied, `Schedule` takes precedence.

```go
q.Enqueue("reminders", payload, shoebox.Delay(30*time.Minute))
q.Enqueue("reports", payload,
    shoebox.Schedule(time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC)))
```

All backends only dequeue messages with `scheduled_at <= now`. The dispatcher
polls every 250 ms for messages whose delay has elapsed.

### Priority

Priority is applied within a queue. Higher-priority messages are delivered
first; messages with equal priority retain FIFO order by `created_at`.

```go
q.Enqueue("emails", payload, shoebox.WithPriority(shoebox.High))
q.Enqueue("emails", payload) // default: Low
```

The available levels are `Low` (0), `Normal` (1), and `High` (2). Backends
implement this as `ORDER BY priority DESC, created_at ASC`.

### Deduplication

`DedupeKey` suppresses repeated enqueues for the same queue and key during the
configured TTL window. The default window is five minutes. A duplicate returns
`nil` and does not write to storage.

```go
q.Enqueue("orders", payload, shoebox.DedupeKey("order-123"))
q.Enqueue("orders", payload, shoebox.DedupeKey("order-123")) // dropped
```

Dedupe state is held in the broker, not in storage. It is scoped per queue and
does not survive a restart.

## Shutdown and queue control

`Shutdown(ctx)` waits for in-flight handlers, including follow-up messages they
enqueue, until the queue reaches quiescence or the context expires. The store
is closed once.

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
q.Shutdown(ctx)
```

Individual queues can be paused, resumed, or drained without stopping the
broker:

```go
q.Pause("orders")  // stop dequeuing; in-flight handlers continue
q.Resume("orders")

ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
q.Drain(ctx, "orders")
```

`Pause` and `Resume` change an atomic flag and do not perform I/O. `Drain`
processes the named queue until it has no dequeuable messages and no in-flight
workers, then stops that queue's dispatcher. Other queues continue running.

## Middleware and metrics

Middleware runs in registration order; the first `Use` call is the outermost
layer.

```go
q.Use(shoebox.RecoveryMiddleware())
q.Use(shoebox.MetricsMiddleware(q.metrics))
q.Use(shoebox.LoggingMiddleware())
q.Use(shoebox.TimeoutMiddleware(30 * time.Second))
```

`RecoveryMiddleware` turns handler panics into errors, logs the stack trace,
and sends the message through the normal retry and DLQ path.

The Prometheus integration exposes per-queue depth, processed/error/retry/dead
counters, and a handler-duration histogram:

```go
http.Handle("/metrics", q.MetricsHandler())
```

Set `Options.MetricsRegistry` to use a custom registry, for example when
testing or running multiple instances in one process.

## Standalone server

`shoeboxd` exposes the queue over HTTP and includes a dashboard, REST API,
Prometheus metrics, and webhook delivery.

```sh
shoeboxd --config=config.yaml
```

Example configuration:

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

CLI flags (`--addr`, `--storage`, `--path`, `--dsn`, and `--auth-token`)
override values from the config file. See
[`cmd/shoeboxd/config.example.yaml`](cmd/shoeboxd/config.example.yaml).

The HTTP API is under `/api/`:

| Method | Path | Purpose |
|--------|------|---------|
| `POST` | `/queues/{name}/messages` | Enqueue a message |
| `GET` | `/queues/{name}/messages/next` | Pull one message |
| `GET` | `/queues/{name}/stats` | Read queue depth and counters |
| `GET` | `/queues/{name}/dlq` | List dead-letter messages |
| `POST` | `/queues/{name}/dlq/{id}/replay` | Replay a dead-letter message |
| `DELETE` | `/queues/{name}/messages/{id}` | Acknowledge/delete a message |

Webhook delivery can be configured per queue. A non-2xx response follows the
same retry, backoff, and DLQ path as any other handler. It is also available in
library mode:

```go
q.Handle("orders", shoebox.WebhookHandler("https://hooks.example.com/orders"))
```

## License

MIT.
