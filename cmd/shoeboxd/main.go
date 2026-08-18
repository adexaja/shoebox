// Command shoeboxd is the standalone server binary for shoebox. It exposes
// the full HTTP API (enqueue, consume, ack, stats, DLQ), a web dashboard,
// Prometheus metrics, and optional per-queue webhook push delivery — all
// backed by the same engine the library uses in-process.
//
// Usage:
//
//	shoeboxd --config=config.yaml
//	shoeboxd --addr=:8080 --storage=sqlite --path=/var/lib/shoebox.db
//	shoeboxd --storage=memory --auth-token=secret
//
// When --config is given, the config file provides server/storage/webhook
// settings. CLI flags override config-file values for the same field.
// Webhooks are declared only in the config file (see config.example.yaml).
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
	"github.com/adexaja/shoebox/internal/config"
	"github.com/adexaja/shoebox/internal/dashboard"
	"github.com/adexaja/shoebox/internal/dlq"
)

func main() {
	var (
		configPath = flag.String("config", "", "path to YAML config file (overrides flags)")
		addr       = flag.String("addr", "", "address to listen on (overrides config)")
		storage    = flag.String("storage", "", "memory | sqlite | postgres (overrides config)")
		dbPath     = flag.String("path", "", "SQLite database path (overrides config)")
		dsn        = flag.String("dsn", "", "Postgres DSN (overrides config)")
		dbSchema   = flag.String("schema", "", "Postgres schema (overrides config)")
		authToken  = flag.String("auth-token", "", "bearer token / X-API-Key (overrides config)")
	)
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// --- load config ---
	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "shoeboxd: config: %v\n", err)
		os.Exit(1)
	}

	// CLI flags override config-file values.
	if *addr != "" {
		cfg.Server.Addr = *addr
	}
	if *storage != "" {
		cfg.Storage.Kind = *storage
	}
	if *dbPath != "" {
		cfg.Storage.Path = *dbPath
	}
	if *dsn != "" {
		cfg.Storage.DSN = *dsn
	}
	if *dbSchema != "" {
		cfg.Storage.Schema = *dbSchema
	}
	if *authToken != "" {
		cfg.Server.AuthToken = *authToken
	}

	// --- build engine ---
	opts := shoebox.Options{Logger: logger}
	switch cfg.Storage.Kind {
	case "memory":
		opts.Storage = shoebox.Memory
	case "sqlite":
		opts.Storage = shoebox.SQLite
		opts.Path = cfg.Storage.Path
	case "postgres":
		opts.Storage = shoebox.Postgres
		opts.DSN = cfg.Storage.DSN
		opts.Schema = cfg.Storage.Schema
	default:
		fmt.Fprintf(os.Stderr, "shoeboxd: unknown storage %q\n", cfg.Storage.Kind)
		os.Exit(2)
	}

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

	// --- register push webhooks from config ---
	for queue, wh := range cfg.Webhooks {
		var whOpts []shoebox.WebhookOption
		if wh.Timeout > 0 {
			whOpts = append(whOpts, shoebox.WithWebhookTimeout(wh.Timeout))
		}
		if wh.ContentType != "" {
			whOpts = append(whOpts, shoebox.WithWebhookContentType(wh.ContentType))
		}
		if wh.Secret != "" {
			whOpts = append(whOpts, shoebox.WithWebhookSecret(wh.Secret))
		}
		q.Handle(queue, shoebox.WebhookHandler(wh.URL, nil, whOpts...))
		logger.Info("webhook registered",
			slog.String("queue", queue),
			slog.String("url", wh.URL),
			slog.Bool("signed", wh.Secret != ""),
		)
	}

	dlqMgr := dlq.NewManager(q.Store())
	apiHandler := api.NewHandler(q.Store(), dlqMgr, logger)
	dash := dashboard.New(q.Store(), dlqMgr, q.Queues, logger)

	// --- wire HTTP routes ---
	mux := http.NewServeMux()

	// Dashboard (HTML) at root — protected by Basic Auth so a human can
	// log in via the browser's native password prompt. Uses a separate
	// credential from the API token (dashboard_user / dashboard_password).
	dashMux := http.NewServeMux()
	dash.Register(dashMux)
	dashMux.HandleFunc("GET /api/stats.json", dash.JSONHandler)
	dashMiddleware := api.Chain(
		api.RecoveryMiddleware(logger),
		api.RequestIDMiddleware(),
		api.LoggingMiddleware(logger),
		api.BasicAuthMiddleware(cfg.Server.DashboardUser, cfg.Server.DashboardPassword),
	)
	mux.Handle("/", dashMiddleware(dashMux))

	// API endpoints under /api.
	apiMux := http.NewServeMux()
	apiHandler.Register(apiMux)

	// Apply middleware chain: recovery → request-ID → logging → auth.
	apiMiddleware := api.Chain(
		api.RecoveryMiddleware(logger),
		api.RequestIDMiddleware(),
		api.LoggingMiddleware(logger),
		api.AuthMiddleware(cfg.Server.AuthToken),
	)
	mux.Handle("/api/", apiMiddleware(http.StripPrefix("/api", apiMux)))

	// Prometheus metrics.
	mux.Handle("GET /metrics", q.MetricsHandler())

	// --- start server ---
	srv := &http.Server{
		Addr:              cfg.Server.Addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	// Background depth-gauge refresher.
	gaugeCtx, gaugeCancel := context.WithCancel(context.Background())
	defer gaugeCancel()
	go refreshDepthGauges(gaugeCtx, q)

	// Graceful shutdown on SIGINT/SIGTERM.
	errCh := make(chan error, 1)
	go func() {
		logger.Info("shoeboxd starting",
			slog.String("addr", cfg.Server.Addr),
			slog.String("storage", cfg.Storage.Kind),
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
func refreshDepthGauges(ctx context.Context, q *shoebox.Queue) {
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
