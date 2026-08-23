package shoebox

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Webhook push delivery: each message on the queue is POSTed to a
// configured HTTP endpoint. It is implemented as a plain HandlerFunc, so
// the broker's retry/backoff/DLQ machinery applies unchanged — a non-2xx
// response returns an error, which the dispatcher turns into a Nack
// (backoff) and eventually a dead-letter. Register it per queue with:
//
//	q.Handle("orders", shoebox.WebhookHandler("https://hooks.example.com/orders"))

// WebhookOption configures a WebhookHandler.
type WebhookOption func(*webhookConfig)

// webhookConfig holds the tunables for a WebhookHandler.
type webhookConfig struct {
	timeout      time.Duration
	contentType  string
	secret       string // HMAC signing key (empty = no signature)
	maxIdleConns int    // idle keep-alive conns kept warm to the webhook target
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

// WithWebhookSecret sets an HMAC-SHA256 shared secret. When non-empty, every
// POST includes an X-Shoebox-Signature header containing the hex-encoded
// HMAC of the payload body. The receiver should verify this before trusting
// the delivery:
//
//	mac := hmac.New(sha256.New, []byte(secret))
//	mac.Write(body)
//	expected := hex.EncodeToString(mac.Sum(nil))
//	if !hmac.Equal([]byte(sig), []byte(expected)) { /* reject */ }
func WithWebhookSecret(secret string) WebhookOption {
	return func(c *webhookConfig) { c.secret = secret }
}

// WithWebhookMaxIdleConns sets the number of keep-alive connections kept warm
// to the webhook target. http.Client with a nil Transport defaults to 2 idle
// connections per host, so handler workers beyond the second pay a fresh
// TCP+TLS handshake on every delivery. Size this to the broker's
// Concurrency — concurrency workers can share maxIdle warm connections.
// Defaults to 16.
func WithWebhookMaxIdleConns(n int) WebhookOption {
	return func(c *webhookConfig) { c.maxIdleConns = n }
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
//	X-Shoebox-Signature    HMAC-SHA256 of the payload (only if a secret is set)
//
// client is used for all requests; if nil, a default client with a
// 10-second timeout and redirect-following disabled (SSRF protection) is used.
func WebhookHandler(target string, client *http.Client, opts ...WebhookOption) HandlerFunc {
	cfg := webhookConfig{
		timeout:      10 * time.Second,
		contentType:  "application/json",
		maxIdleConns: 16,
	}
	for _, o := range opts {
		o(&cfg)
	}
	if client == nil {
		client = newWebhookClient(cfg.timeout, cfg.maxIdleConns)
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

// newWebhookClient creates an HTTP client that does NOT follow redirects.
// This prevents SSRF: a malicious or compromised webhook target could
// redirect the POST to an internal service, leaking the payload. The
// caller gets the raw 3xx response and we treat it as a non-2xx failure.
//
// The Transport is cloned from http.DefaultTransport with MaxIdleConnsPerHost
// raised from the package default of 2 so handler workers beyond the second
// reuse warm connections instead of paying a TCP+TLS handshake per delivery.
// maxIdle is the number of keep-alive connections to keep open to the target.
func newWebhookClient(timeout time.Duration, maxIdle int) *http.Client {
	if maxIdle <= 0 {
		maxIdle = 16
	}
	var transport http.RoundTripper = http.DefaultTransport
	if base, ok := http.DefaultTransport.(*http.Transport); ok && base != nil {
		tr := base.Clone()
		tr.MaxIdleConnsPerHost = maxIdle
		tr.MaxIdleConns = maxIdle
		transport = tr
	}
	return &http.Client{
		Timeout:   timeout,
		Transport: transport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
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

	// HMAC signature so the receiver can authenticate the sender.
	if h.config.secret != "" {
		mac := hmac.New(sha256.New, []byte(h.config.secret))
		mac.Write(m.Payload)
		req.Header.Set("X-Shoebox-Signature", hex.EncodeToString(mac.Sum(nil)))
	}

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
