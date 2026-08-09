# shoebox

Embedded message queue for Go. No broker. No Docker. No YAML for exchanges and
bindings. Import the library, call `shoebox.Enqueue("orders", payload)`, keep
moving.

The name is the pitch: when you outgrow a Go channel but RabbitMQ feels like
bringing a forklift to move a shoebox, **shoebox** is the thing you wish
existed.

## Quick start

```go
import "github.com/rezki/shoebox"

func main() {
    q := shoebox.New(shoebox.Options{
        Storage: shoebox.Memory, // or shoebox.SQLite, shoebox.Postgres
    })

    q.Handle("orders", func(ctx context.Context, msg shoebox.Message) error {
        // process the order
        return nil
    })

    _ = q.Enqueue("orders", []byte(`{"order_id": 123}`))
}
```

## Status

**Week 1 — Core broker + memory storage** is in progress. The in-memory
storage backend and the broker dispatch loop are working; SQLite, Postgres,
retry/DLQ, middleware, and the standalone server are placeholders for the
upcoming weeks.

See the parent `docs/` directory (not git-tracked) for:

- `docs/epics.md` — high-level milestones
- `docs/user-stories.md` — user stories per epic
- `docs/tasks.md` — granular task breakdown
- `docs/adr/` — architecture decision records

## Layout

```
.
├── shoebox.go              # public API
├── options.go              # Options, HandlerOptions, EnqueueOptions
├── message.go              # Message and QueueStats types
├── middleware.go           # built-in middleware
├── cmd/shoeboxd/           # standalone server binary (Week 4)
├── examples/               # runnable examples
└── internal/
    ├── broker/             # dispatch + lifecycle
    ├── storage/            # Storage interface + backends
    ├── retry/              # backoff strategies
    ├── dlq/                # dead-letter queue (Week 2)
    ├── api/                # HTTP API (Week 4)
    └── dashboard/          # web UI (Week 4)
```

## License

MIT.
