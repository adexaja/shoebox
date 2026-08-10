// Command shoeboxd is the standalone server binary for shoebox. It exposes
// the full HTTP API (enqueue, consume, ack, stats, DLQ), a web dashboard,
// and Prometheus metrics — all backed by the same engine the library uses
// in-process.
//
// Usage:
//
//	shoeboxd --addr=:8080 --storage=sqlite --path=/var/lib/shoebox.db
//	shoeboxd --storage=memory --auth-token=secret
//	shoeboxd --storage=postgres --dsn="host=localhost port=5432 ..."
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/adexaja/shoebox"
	"github.com/adexaja/shoebox/internal/api"
	"github.com/adexaja/shoebox/internal/dlq"
	"github.com/adexaja/shoebox/internal/dashboard"
)

func main() {
	var (
		addr      = flag.String("addr", ":8080", "address to listen on")
		storage   = flag.String("storage", "memory", "memory | sqlite | postgres")
		dbPath    = flag.String("path", "shoebox.db", "SQLite database path")
		dsn       = flag.String("dsn", "", "Postgres DSN (key=value format)")
		authToken = flag.String("auth-token", "", "bearer token / X-API-Key for API auth (empty = no auth)")
	)
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	opts := shoebox.Options{Logger: logger}
	switch *storage {
	case "memory":
		opts.Storage = shoebox.Memory
	case "sqlite":
		opts.Storage = shoebox.SQLite
		opts.Path = *dbPath
	case "postgres":
		opts.Storage = shoebox.Postgres
		opts.DSN = *dsn
	default:
		fmt.Fprintf(os.Stderr, "shoeboxd: unknown --storage=%q\n", *storage)
		os.Exit(2)
	}

	// --- build engine ---
	q, err := shoebox.New(opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "shoeboxd: failed to create queue: %v\n", err)
		os.Exit(1)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = q.Shutdown(ctx)
	}()

	dlqMgr := dlq.NewManager(q.Store())
	apiHandler := api.NewHandler(q.Store(), dlqMgr, logger)
	dash := dashboard.New(q.Store(), dlqMgr, q.Queues, logger)

	// --- wire HTTP routes ---
	mux := http.NewServeMux()

	// Dashboard (HTML) at root.
	dash.Register(mux)
	mux.HandleFunc("GET /api/stats.json", dash.JSONHandler)

	// API endpoints under /api.
	apiMux := http.NewServeMux()
	apiHandler.Register(apiMux)

	// Apply middleware chain: recovery → request-ID → logging → auth.
	apiMiddleware := api.Chain(
		api.RecoveryMiddleware(logger),
		api.RequestIDMiddleware(),
		api.LoggingMiddleware(logger),
		api.AuthMiddleware(*authToken),
	)
	mux.Handle("/api/", apiMiddleware(http.StripPrefix("/api", apiMux)))

	// Prometheus metrics.
	mux.Handle("GET /metrics", q.MetricsHandler())

	// --- start server ---
	srv := &http.Server{
		Addr:    *addr,
		Handler: mux,
		// Generous timeouts for slow DLQ operations.
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	// Background depth-gauge refresher.
	gaugeCtx, gaugeCancel := context.WithCancel(context.Background())
	defer gaugeCancel()
	go refreshDepthGauges(gaugeCtx, q, logger)

	// Graceful shutdown on SIGINT/SIGTERM.
	errCh := make(chan error, 1)
	go func() {
		logger.Info("shoeboxd starting",
			slog.String("addr", *addr),
			slog.String("storage", *storage),
		)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errCh:
		logger.Error("shoeboxd: server error", slog.Any("err", err))
		os.Exit(1)
	case sig := <-sigCh:
		logger.Info("shoeboxd: shutting down", slog.String("signal", sig.String()))
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("shoeboxd: forced shutdown", slog.Any("err", err))
	}
}

// refreshDepthGauges polls queue depths every 5 seconds so Prometheus
// has current values without the caller needing to hit UpdateDepthGauges.
func refreshDepthGauges(ctx context.Context, q *shoebox.Queue, logger *slog.Logger) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			q.UpdateDepthGauges(ctx)
		}
	}
}
