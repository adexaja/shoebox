// Package dashboard serves the minimal web UI for shoeboxd. It renders a
// single HTML page listing all registered queues with their depth and error
// counters, plus a DLQ browser.
//
// Tech: html/template (stdlib) + HTMX for auto-refresh and DLQ expansion.
// No JavaScript framework — the page is server-rendered and HTMX polls the
// same partial every few seconds.
package dashboard

import (
	"context"
	"embed"
	"encoding/json"
	"html/template"
	"log/slog"
	"net/http"
	"sort"
	"time"

	"github.com/adexaja/shoebox/internal/dlq"
	"github.com/adexaja/shoebox/internal/storage"
)

//go:embed templates/*.html
var templateFS embed.FS

// Dashboard holds the dependencies for the web UI: the storage backend (for
// live stats) and the DLQ manager (for the dead-letter browser).
type Dashboard struct {
	store    storage.Storage
	dlq      *dlq.Manager
	logger   *slog.Logger
	tmpl     *template.Template
	queuesFn func() []string // returns registered queue names
}

// New creates a Dashboard. queuesFn provides the list of registered queue
// names (so the dashboard shows only queues the broker knows about, not
// internal shadow queues).
func New(store storage.Storage, dlqMgr *dlq.Manager, queuesFn func() []string, logger *slog.Logger) *Dashboard {
	if logger == nil {
		logger = slog.Default()
	}
	tmpl := template.Must(template.New("").Funcs(template.FuncMap{
		"truncate": truncatePayload,
	}).ParseFS(templateFS, "templates/*.html"))
	return &Dashboard{
		store:    store,
		dlq:      dlqMgr,
		logger:   logger,
		tmpl:     tmpl,
		queuesFn: queuesFn,
	}
}

// Register mounts the dashboard routes on the given mux.
//
//	GET /                full HTML page (auto-refreshs via HTMX)
//	GET /partials/stats  HTMX partial — queue stats table only
//	GET /partials/dlq    HTMX partial — DLQ table for a single queue
func (d *Dashboard) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /{$}", d.index)
	mux.HandleFunc("GET /partials/stats", d.statsPartial)
	mux.HandleFunc("GET /partials/dlq", d.dlqPartial)
}

// index renders the full dashboard page.
func (d *Dashboard) index(w http.ResponseWriter, r *http.Request) {
	stats := d.collectStats(r.Context())
	d.render(w, "layout.html", stats)
}

// statsPartial returns just the queue stats table — used by HTMX polling.
func (d *Dashboard) statsPartial(w http.ResponseWriter, r *http.Request) {
	stats := d.collectStats(r.Context())
	d.render(w, "stats.html", stats)
}

// dlqPartial returns the DLQ table for a single queue — loaded on demand
// when the user expands a queue's DLQ section.
func (d *Dashboard) dlqPartial(w http.ResponseWriter, r *http.Request) {
	queue := r.URL.Query().Get("queue")
	if queue == "" {
		http.Error(w, "missing queue parameter", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	records, err := d.dlq.List(ctx, queue, 50)
	if err != nil {
		d.logger.ErrorContext(ctx, "dashboard: dlq list failed",
			slog.String("queue", queue), slog.Any("err", err))
		records = nil // render empty table on error
	}

	d.render(w, "dlq.html", map[string]any{
		"Queue":   queue,
		"Records": records,
	})
}

// queueStat is the per-queue data passed to the template.
type queueStat struct {
	Name      string
	Depth     int
	Processed uint64
	Errors    uint64
	Retries   uint64
	Dead      uint64
}

// collectStats queries storage for each registered queue's stats.
func (d *Dashboard) collectStats(ctx context.Context) map[string]any {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	queues := d.queuesFn()
	sort.Strings(queues)

	stats := make([]queueStat, 0, len(queues))
	for _, q := range queues {
		s, err := d.store.Stats(ctx, q)
		if err != nil {
			d.logger.ErrorContext(ctx, "dashboard: stats failed",
				slog.String("queue", q), slog.Any("err", err))
			continue
		}
		stats = append(stats, queueStat{
			Name:      q,
			Depth:     s.Depth,
			Processed: s.Processed,
			Errors:    s.Errors,
			Retries:   s.Retries,
			Dead:      s.Dead,
		})
	}

	return map[string]any{
		"Queues": stats,
	}
}

// render executes a named template with the given data and writes it to w.
func (d *Dashboard) render(w http.ResponseWriter, name string, data any) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := d.tmpl.ExecuteTemplate(w, name, data); err != nil {
		d.logger.Error("dashboard: template render failed", slog.Any("err", err))
	}
}

// --- JSON export (for programmatic consumers) ---

// JSONHandler returns queue stats as JSON — useful for curl-based monitoring.
func (d *Dashboard) JSONHandler(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	queues := d.queuesFn()
	sort.Strings(queues)

	result := make(map[string]storage.QueueStats, len(queues))
	for _, q := range queues {
		s, err := d.store.Stats(ctx, q)
		if err != nil {
			continue
		}
		result[q] = s
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}

// truncatePayload returns a string representation of a payload byte slice,
// truncated to maxLen characters with an ellipsis if it exceeds that length.
func truncatePayload(payload []byte, maxLen int) string {
	s := string(payload)
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "…"
}
