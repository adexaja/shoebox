# shoebox

[![Go Reference](https://pkg.go.dev/badge/github.com/adexaja/shoebox.svg)](https://pkg.go.dev/github.com/adexaja/shoebox)
[![Build Status](https://github.com/adexaja/shoebox/actions/workflows/ci.yml/badge.svg)](https://github.com/adexaja/shoebox/actions/workflows/ci.yml)
[![Coverage](https://codecov.io/gh/adexaja/shoebox/graph/badge.svg)](https://codecov.io/gh/adexaja/shoebox)

`shoebox` is an embedded message queue for Go. It runs in the application
process and supports in-memory, SQLite, and PostgreSQL storage. There is no
separate broker to deploy.

Coverage is generated in CI and uploaded to [Codecov](https://codecov.io/gh/adexaja/shoebox).

## Install shoeboxd

Install the standalone server command with:

```sh
go install github.com/adexaja/shoebox/cmd/shoeboxd@latest
```

Then run `shoeboxd --config=config.yaml`. With modern Go versions, use
`go install` for executables; `go get` adds library dependencies to a module.

Use it when a Go channel is too limited but operating RabbitMQ or Kafka is not
justified for the workload.

## Why Shoebox?

Shoebox is for applications that need durable background work without
deploying a separate broker. It can run in-process with Memory or SQLite, or
use PostgreSQL when multiple application processes need to consume the same
queues.

| Project | Best fit | Main difference from Shoebox |
|---------|----------|------------------------------|
| Shoebox | Embedded queues and workers | Memory, SQLite, and PostgreSQL, plus retry, DLQ, HTTP API, dashboard, and webhooks |
| [backlite](https://github.com/mikestefanello/backlite) | Type-safe embedded SQLite tasks | SQLite-focused and generic task-oriented |
| [Asynq](https://github.com/hibiken/asynq) | Distributed task processing | Requires Redis |
| [Machinery](https://github.com/RichardKnop/machinery) | Distributed job queues | Uses external message brokers |

## Quick start

```go
package main

import (
	"context"
	"log"

	"github.com/adexaja/shoebox"
)

func main() {
	q, err := shoebox.New(shoebox.Options{Storage: shoebox.Memory})
	if err != nil {
		log.Fatal(err)
	}
	defer q.Shutdown(context.Background())

	q.Handle("orders", func(ctx context.Context, msg shoebox.Message) error {
		// process the order
		return nil
	})

	if err := q.Enqueue("orders", []byte(`{"order_id": 123}`)); err != nil {
		log.Fatal(err)
	}
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

### Upgrades

Both persistent backends migrate their schema automatically on open: fresh
databases are created at the latest version, and databases from older
shoebox releases are upgraded in place (forward-only, each migration in a
transaction). Concurrent processes opening the same Postgres database
serialise on an advisory lock, so upgrades under load are safe.

The canonical DDL lives in [`migrations/`](migrations/) using the
`NNNN_name.<dialect>.<up|down>.sql` convention, so external migration tools
can consume the same files. Down migrations exist for manual rollback but
are never applied automatically.

## Delivery behavior

Shoebox provides at-least-once processing. A message is acknowledged only
after its handler returns successfully. If a process crashes while a message
is being handled, persistent backends can make the message available again on
startup. Handlers should therefore be safe to retry. Deduplication is
best-effort and in-memory, not exactly-once delivery.

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

The default policy is `UnboundedTTL`, which preserves all live deduplication
keys until their five-minute TTL expires. To bound memory, select `BoundedLRU`:

```go
broker, err := shoebox.New(shoebox.Options{
	Dedupe: shoebox.DedupeOptions{
		Policy:   shoebox.DedupePolicyBoundedLRU,
		Capacity: 100_000,
	},
})
```

`BoundedLRU` limits memory usage, but a deduplication key may be evicted before
its TTL expires. A duplicate message may therefore be accepted again after
capacity-based eviction. Deduplication is best-effort and in-memory; it is not
durable exactly-once delivery.

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
q.Use(shoebox.LoggingMiddleware())
q.Use(shoebox.TimeoutMiddleware(30 * time.Second))
```

To add handler-level metrics explicitly, create a metrics set:

```go
metrics := shoebox.NewMetrics("", nil)
q.Use(shoebox.MetricsMiddleware(metrics))
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

Install
```sh
go install github.com/adexaja/shoebox/cmd/shoeboxd@latest
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

CLI flags (`--addr`, `--storage`, `--path`, `--dsn`, `--schema`, and `--auth-token`)
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

Example enqueue and pull flow:

```sh
curl -X POST http://localhost:8080/api/queues/orders/messages \
  -H 'Authorization: Bearer secret' \
  -H 'Content-Type: application/json' \
  -d '{"payload":"{\"order_id\":123}"}'

curl http://localhost:8080/api/queues/orders/messages/next \
  -H 'Authorization: Bearer secret'
```

The dashboard is available at `/`, and Prometheus metrics at `/metrics`.
Configure `dashboard_user` and `dashboard_password` for dashboard Basic Auth;
`auth_token` protects API requests.

Webhook delivery can be configured per queue. A non-2xx response follows the
same retry, backoff, and DLQ path as any other handler. It is also available in
library mode:

```go
q.Handle("orders", shoebox.WebhookHandler("https://hooks.example.com/orders"))
```

## License

MIT — see [LICENSE](LICENSE).

## Status

Shoebox is actively developed. The public API is intended for embedded queues,
workers, and standalone-server use cases. Contributions and bug reports are
welcome.
