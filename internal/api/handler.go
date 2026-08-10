// Package api serves the HTTP API for shoebox's standalone-server mode
// (shoeboxd). The handlers are a thin adapter over the storage and dlq
// packages — they contain no queue logic of their own (ADR 0003).
//
// Endpoints (PRD §v0.2 §7):
//
//	POST   /queues/{name}/messages         enqueue a message
//	GET    /queues/{name}/messages/next    consume one message (pull-based)
//	GET    /queues/{name}/stats            queue depth + counters
//	GET    /queues/{name}/dlq              list dead-letter messages
//	POST   /queues/{name}/dlq/{id}/replay  replay a dead message
//	DELETE /queues/{name}/messages/{id}    acknowledge/delete a message
package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/adexaja/shoebox/internal/dlq"
	"github.com/adexaja/shoebox/internal/storage"
)

// Handler holds the dependencies shared by all HTTP endpoints: the storage
// backend (for enqueue/dequeue/ack/stats) and the DLQ manager (for list/replay).
type Handler struct {
	store   storage.Storage
	dlq     *dlq.Manager
	logger  *slog.Logger
}

// NewHandler creates an API Handler.
func NewHandler(store storage.Storage, dlqMgr *dlq.Manager, logger *slog.Logger) *Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return &Handler{store: store, dlq: dlqMgr, logger: logger}
}

// Register mounts all API routes on the given mux. Uses Go 1.22+ routing
// patterns so path parameters ({name}, {id}) are extracted by the mux.
func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /queues/{name}/messages", h.enqueue)
	mux.HandleFunc("GET /queues/{name}/messages/next", h.consume)
	mux.HandleFunc("GET /queues/{name}/stats", h.stats)
	mux.HandleFunc("GET /queues/{name}/dlq", h.listDLQ)
	mux.HandleFunc("POST /queues/{name}/dlq/{id}/replay", h.replayDLQ)
	mux.HandleFunc("DELETE /queues/{name}/messages/{id}", h.ack)
}

// enqueueRequest is the JSON body for POST /queues/{name}/messages.
// Payload is a string to keep the API user-friendly (JSON []byte would
// require base64 encoding); it is converted to []byte internally.
type enqueueRequest struct {
	Payload  string            `json:"payload"`
	Delay    string            `json:"delay,omitempty"`     // e.g. "5s", "1m"
	Metadata map[string]string `json:"metadata,omitempty"`
}

// enqueueResponse is the JSON body returned on successful enqueue.
type enqueueResponse struct {
	ID string `json:"id"`
}

// enqueue handles POST /queues/{name}/messages.
func (h *Handler) enqueue(w http.ResponseWriter, r *http.Request) {
	queue := r.PathValue("name")
	if queue == "" {
		writeError(w, http.StatusBadRequest, "missing queue name")
		return
	}

	var req enqueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	if req.Payload == "" {
		writeError(w, http.StatusBadRequest, "payload is required")
		return
	}

	msg := storage.Message{
		ID:       newRequestID(),
		Queue:    queue,
		Payload:  []byte(req.Payload),
		Metadata: req.Metadata,
	}

	// Parse optional delay.
	if req.Delay != "" {
		d, err := time.ParseDuration(req.Delay)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid delay: "+err.Error())
			return
		}
		if d > 0 {
			msg.ScheduledAt = time.Now().Add(d)
		}
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	if err := h.store.Enqueue(ctx, queue, msg); err != nil {
		h.logger.ErrorContext(ctx, "api: enqueue failed",
			slog.String("queue", queue), slog.Any("err", err))
		writeError(w, http.StatusInternalServerError, "enqueue failed")
		return
	}

	writeJSON(w, http.StatusCreated, enqueueResponse{ID: msg.ID})
}

// consume handles GET /queues/{name}/messages/next.
// It pulls one message from the queue (status pending → processing). The
// caller is responsible for DELETE-ing the message to ack it; if they don't,
// the message is reclaimed to pending on the next open (crash recovery).
func (h *Handler) consume(w http.ResponseWriter, r *http.Request) {
	queue := r.PathValue("name")

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	msgs, err := h.store.Dequeue(ctx, queue, 1)
	if err != nil {
		if errors.Is(err, storage.ErrEmpty) {
			writeError(w, http.StatusNotFound, "no messages available")
			return
		}
		h.logger.ErrorContext(ctx, "api: dequeue failed",
			slog.String("queue", queue), slog.Any("err", err))
		writeError(w, http.StatusInternalServerError, "dequeue failed")
		return
	}

	writeJSON(w, http.StatusOK, msgs[0])
}

// stats handles GET /queues/{name}/stats.
func (h *Handler) stats(w http.ResponseWriter, r *http.Request) {
	queue := r.PathValue("name")

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	stats, err := h.store.Stats(ctx, queue)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "stats failed")
		return
	}

	writeJSON(w, http.StatusOK, stats)
}

// listDLQ handles GET /queues/{name}/dlq.
func (h *Handler) listDLQ(w http.ResponseWriter, r *http.Request) {
	queue := r.PathValue("name")
	limit := 50
	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 {
			limit = parsed
		}
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	records, err := h.dlq.List(ctx, queue, limit)
	if err != nil {
		if errors.Is(err, storage.ErrEmpty) {
			writeJSON(w, http.StatusOK, []any{})
			return
		}
		writeError(w, http.StatusInternalServerError, "dlq list failed")
		return
	}

	writeJSON(w, http.StatusOK, records)
}

// replayDLQ handles POST /queues/{name}/dlq/{id}/replay.
func (h *Handler) replayDLQ(w http.ResponseWriter, r *http.Request) {
	queue := r.PathValue("name")
	id := r.PathValue("id")

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	if err := h.dlq.Replay(ctx, queue, id); err != nil {
		if errors.Is(err, storage.ErrEmpty) {
			writeError(w, http.StatusNotFound, "dlq message not found")
			return
		}
		h.logger.ErrorContext(ctx, "api: replay failed",
			slog.String("queue", queue), slog.String("id", id), slog.Any("err", err))
		writeError(w, http.StatusInternalServerError, "replay failed")
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "replayed"})
}

// ack handles DELETE /queues/{name}/messages/{id}.
func (h *Handler) ack(w http.ResponseWriter, r *http.Request) {
	queue := r.PathValue("name")
	id := r.PathValue("id")

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	if err := h.store.Ack(ctx, queue, id); err != nil {
		writeError(w, http.StatusInternalServerError, "ack failed")
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "acked"})
}

// --- helpers ---

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// newRequestID generates a unique ID for an enqueued message. Uses the same
// crypto/rand approach as the broker.
func newRequestID() string {
	return storage.NewMessageID()
}
