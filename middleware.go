package shoebox

import (
	"context"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/adexaja/shoebox/internal/broker"
)

// Middleware wraps a HandlerFunc. The chain is composed left-to-right: the
// first middleware registered is the outermost wrapper, so it sees the request
// first and the response last.
//
// This is the same shape as HTTP middleware in net/http, just over messages.
type Middleware = broker.Middleware

// HandlerFunc is the function signature registered with Handle. Aliased to
// the broker's HandlerFunc so the public API and the internal engine share
// a single function type (no conversion at registration time).
type HandlerFunc = broker.HandlerFunc

// LoggingMiddleware logs the start and end of every handler invocation. It
// uses slog.Default() unless a logger is provided via WithLogger.
func LoggingMiddleware() Middleware {
	return LoggingMiddlewareWith(slog.Default())
}

// LoggingMiddlewareWith is LoggingMiddleware with an explicit logger.
func LoggingMiddlewareWith(logger *slog.Logger) Middleware {
	if logger == nil {
		logger = slog.Default()
	}
	return func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, m Message) error {
			start := time.Now()
			logger.DebugContext(ctx, "shoebox: handling",
				slog.String("queue", m.Queue),
				slog.String("id", m.ID),
				slog.Int("attempt", m.Attempts),
			)
			err := next(ctx, m)
			if err != nil {
				logger.WarnContext(ctx, "shoebox: handler error",
					slog.String("queue", m.Queue),
					slog.String("id", m.ID),
					slog.Int("attempt", m.Attempts),
					slog.Duration("elapsed", time.Since(start)),
					slog.Any("err", err),
				)
			} else {
				logger.InfoContext(ctx, "shoebox: handled",
					slog.String("queue", m.Queue),
					slog.String("id", m.ID),
					slog.Duration("elapsed", time.Since(start)),
				)
			}
			return err
		}
	}
}

// TimeoutMiddleware aborts the handler if it does not return within d. The
// handler receives a context with a deadline; cooperative handlers will
// observe it via ctx.Done().
func TimeoutMiddleware(d time.Duration) Middleware {
	return func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, m Message) error {
			ctx, cancel := context.WithTimeout(ctx, d)
			defer cancel()
			return next(ctx, m)
		}
	}
}

// RecoveryMiddleware catches panics from downstream handlers and converts
// them into errors so the retry/DLQ machinery still kicks in. Stack traces
// are logged via slog.Default().
func RecoveryMiddleware() Middleware {
	return RecoveryMiddlewareWith(slog.Default())
}

// RecoveryMiddlewareWith is RecoveryMiddleware with an explicit logger.
func RecoveryMiddlewareWith(logger *slog.Logger) Middleware {
	if logger == nil {
		logger = slog.Default()
	}
	return func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, m Message) (err error) {
			defer func() {
				if r := recover(); r != nil {
					logger.ErrorContext(ctx, "shoebox: handler panic",
						slog.String("queue", m.Queue),
						slog.String("id", m.ID),
						slog.Any("panic", r),
						slog.String("stack", string(debug.Stack())),
					)
					err = errPanic{value: r}
				}
			}()
			return next(ctx, m)
		}
	}
}

// MetricsMiddleware records Prometheus metrics for every handler invocation:
// processed/errors counters, handler duration histogram. Requires a non-nil
// *Metrics; panics if nil to catch wiring mistakes at registration time.
func MetricsMiddleware(metrics *Metrics) Middleware {
	if metrics == nil {
		panic("shoebox: MetricsMiddleware requires a non-nil *Metrics")
	}
	return func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, m Message) error {
			start := time.Now()
			err := next(ctx, m)

			metrics.Duration.WithLabelValues(m.Queue).
				Observe(time.Since(start).Seconds())

			if err != nil {
				metrics.Errors.WithLabelValues(m.Queue).Inc()
			} else {
				metrics.Processed.WithLabelValues(m.Queue).Inc()
			}
			return err
		}
	}
}

// errPanic is the error returned by RecoveryMiddleware. It is unexported
// because callers should rely on errors.Is/errors.As rather than string
// matching; it is kept distinct so a future structured logger can format it
// specially.
type errPanic struct{ value any }

func (e errPanic) Error() string { return "shoebox: handler panicked" }

func errOrNil(err error) any {
	if err == nil {
		return nil
	}
	return err
}
