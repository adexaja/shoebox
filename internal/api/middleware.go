package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"
)

// Middleware is an HTTP server middleware function.
type Middleware func(http.Handler) http.Handler

// Chain composes multiple HTTP middleware into a single middleware.
// The first element is outermost (runs first on the request, last on the
// response), matching the convention used by the broker's message-side
// middleware chain.
func Chain(mw ...Middleware) Middleware {
	return func(final http.Handler) http.Handler {
		h := final
		for i := len(mw) - 1; i >= 0; i-- {
			h = mw[i](h)
		}
		return h
	}
}

type contextKey string

// requestIDKey is the context key for the per-request correlation ID.
const requestIDKey contextKey = "request_id"

// RequestIDMiddleware injects a random hex request ID into the request
// context. The ID is also set on the X-Request-ID response header so
// callers can correlate logs with responses.
func RequestIDMiddleware() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			id := r.Header.Get("X-Request-ID")
			if id == "" {
				id = newCorrelationID()
			}
			w.Header().Set("X-Request-ID", id)
			ctx := context.WithValue(r.Context(), requestIDKey, id)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// LoggingMiddleware logs each request at Info level with method, path,
// status code, duration, and request ID.
func LoggingMiddleware(logger *slog.Logger) Middleware {
	if logger == nil {
		logger = slog.Default()
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ww := &statusWriter{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(ww, r)

			reqID, _ := r.Context().Value(requestIDKey).(string)
			logger.InfoContext(r.Context(), "http",
				slog.String("method", r.Method),
				slog.String("path", r.URL.Path),
				slog.Int("status", ww.status),
				slog.Duration("duration", time.Since(start)),
				slog.String("request_id", reqID),
			)
		})
	}
}

// AuthMiddleware validates a bearer token (or X-API-Key header) against the
// expected token. If the token is empty the middleware is a pass-through,
// allowing shoeboxd to run without auth in development.
func AuthMiddleware(token string) Middleware {
	if token == "" {
		return func(next http.Handler) http.Handler { return next }
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			provided := r.Header.Get("X-API-Key")
			if provided == "" {
				if auth := r.Header.Get("Authorization"); len(auth) > 7 && auth[:7] == "Bearer " {
					provided = auth[7:]
				}
			}
			if provided != token {
				writeError(w, http.StatusUnauthorized, "unauthorized")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// RecoveryMiddleware catches panics in HTTP handlers, logs the stack trace,
// and returns a 500 so the server doesn't crash.
func RecoveryMiddleware(logger *slog.Logger) Middleware {
	if logger == nil {
		logger = slog.Default()
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rv := recover(); rv != nil {
					logger.ErrorContext(r.Context(), "http: panic recovered",
						slog.Any("panic", rv),
						slog.String("stack", string(debug.Stack())),
					)
					writeError(w, http.StatusInternalServerError, "internal panic")
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

// --- helpers ---

// statusWriter wraps http.ResponseWriter to capture the status code for
// logging. It only intercepts WriteHeader; all other methods pass through.
type statusWriter struct {
	http.ResponseWriter
	status int
}

func (sw *statusWriter) WriteHeader(code int) {
	sw.status = code
	sw.ResponseWriter.WriteHeader(code)
}

// newCorrelationID generates a short random hex string for request
// correlation. Shorter than message IDs (8 bytes) since it only needs to
// be unique within a log window.
func newCorrelationID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "ts-" + hex.EncodeToString([]byte(time.Now().Format("150405.000000")))
	}
	return hex.EncodeToString(b[:])
}
