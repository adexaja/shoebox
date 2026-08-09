package broker

import (
	"context"
	"log/slog"

	"github.com/rezki/shoebox/internal/retry"
	"github.com/rezki/shoebox/internal/storage"
)

// HandlerFunc is the function signature of a registered handler. It is
// distinct from the public shoebox.HandlerFunc so the broker doesn't
// import the public package (which would create an import cycle); the
// public Queue converts at registration time.
type HandlerFunc func(ctx context.Context, m storage.Message) error

// handler bundles a registered handler with its options and the
// pre-composed middleware chain.
type handler struct {
	fn    HandlerFunc
	opts  HandlerOptions
	chain HandlerFunc
}

// HandlerOptions configures a single handler. Mirrors the public
// shoebox.HandlerOptions; the public Queue converts at registration time.
type HandlerOptions struct {
	MaxRetries int
	Backoff    retry.Backoff
}

// logSink is the subset of *slog.Logger the broker uses. It exists so
// tests can swap in a no-op logger if they don't want to assert against
// log output.
type logSink interface {
	InfoContext(ctx context.Context, msg string, args ...any)
	ErrorContext(ctx context.Context, msg string, args ...any)
	WarnContext(ctx context.Context, msg string, args ...any)
	DebugContext(ctx context.Context, msg string, args ...any)
}

var _ logSink = (*slog.Logger)(nil)
