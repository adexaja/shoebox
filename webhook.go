package shoebox

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Webhook push delivery (PRD §v0.2 §7.4, E4-S3): per-queue delivery to an HTTP
// endpoint. It is implemented as a plain HandlerFunc, so the broker's
// retry/backoff/DLQ machinery applies unchanged — a non-2xx response returns an
// error, which the dispatcher turns into a Nack (backoff) and eventually a
// dead-letter. Register it per queue with:
//
//	q.Handle("orders", shoebox.WebhookHandler("https://hooks.example.com/orders"))

// WebhookOption configures a WebhookHandler.
type WebhookOption func(*webhookConfig)

// webhookConfig holds the tunables for a WebhookHandler.
type webhookConfig struct {
	timeout     time.Duration
	contentType string
}

// WithWebhookTimeout sets the per-request timeout used by the default HTTP
// client. Ignored if a client is passed to WebhookHandler. The broker's own
// per-handler Timeout (HandlerOptions.Timeout), when set, still takes
// precedence because it cancels the handler context the request runs under.
func WithWebhookTimeout(d time.Duration) WebhookOption {
	return func(c *webhookConfig) { c.timeout = d }
}

// WithWebhookContentType sets the Content-Type header on the POST. Defaults
// to "application/json" (most shoebox payloads are JSON).
func WithWebhookContentType(ct string) WebhookOption {
	return func(c *webhookConfig) { c.contentType = ct }
}

// webhookHandler delivers messages by POSTing their payload to a target URL.
type webhookHandler struct {
	target string
	client *http.Client
	config webhookConfig
}

// WebhookHandler returns a HandlerFunc that POSTs the message payload to
// target. A non-2xx response returns an error, triggering the broker's
// normal retry/backoff/DLQ path. The message travels in its payload; message
// context is attached as headers so the receiver can correlate:
//
//	X-Shoebox-Message-ID   the message ID (matches the API's DELETE path)
//	X-Shoebox-Queue        the source queue name
//	X-Shoebox-Attempt      attempt number (1 = first delivery)
//
// client is used for all requests; if nil, a default client with a
// 10-second timeout is used.
func WebhookHandler(target string, client *http.Client, opts ...WebhookOption) HandlerFunc {
	cfg := webhookConfig{
		timeout:     10 * time.Second,
		contentType: "application/json",
	}
	for _, o := range opts {
		o(&cfg)
	}
	if client == nil {
		client = &http.Client{Timeout: cfg.timeout}
	}

	h := &webhookHandler{
		target: target,
		client: client,
		config: cfg,
	}

	return func(ctx context.Context, m Message) error {
		return h.deliver(ctx, m)
	}
}

// deliver POSTs one message's payload to the target URL.
func (h *webhookHandler) deliver(ctx context.Context, m Message) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.target, bytes.NewReader(m.Payload))
	if err != nil {
		// A bad (unparseable) target URL never succeeds; skip retries.
		return fmt.Errorf("webhook: %s: build request: %w", h.target, err)
	}

	req.Header.Set("Content-Type", h.config.contentType)
	req.Header.Set("X-Shoebox-Message-ID", m.ID)
	req.Header.Set("X-Shoebox-Queue", m.Queue)
	req.Header.Set("X-Shoebox-Attempt", strconv.Itoa(m.Attempts+1))

	resp, err := h.client.Do(req)
	if err != nil {
		return fmt.Errorf("webhook: post %s: %w", h.target, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// Read a bounded snippet of the response body so the DLQ page
		// shows something actionable, then treat it as a delivery failure.
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return fmt.Errorf("webhook: %s returned %d: %s",
			h.target, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}
