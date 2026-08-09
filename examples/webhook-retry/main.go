// Example: webhook delivery with exponential backoff.
//
// This is the PRD's second use case. A queue receives webhook events;
// the handler POSTs them to a target URL. The target is unreliable, so
// the handler is retried with exponential backoff up to one hour, with
// a hard cap of 10 attempts.
package main

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/adexaja/shoebox"
	"github.com/adexaja/shoebox/internal/retry"
)

func main() {
	logger := slog.Default()
	q, err := shoebox.New(shoebox.Options{
		Storage:     shoebox.Memory,
		Concurrency: 2,
		Logger:      logger,
	})
	if err != nil {
		logger.Error("new queue", "err", err)
		return
	}

	client := &http.Client{Timeout: 3 * time.Second}

	q.Handle("webhooks", func(_ context.Context, msg shoebox.Message) error {
		url := msg.Metadata["url"]
		req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(msg.Payload))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 500 {
			return fmt.Errorf("webhook %s -> %d", url, resp.StatusCode)
		}
		fmt.Printf("[webhook] %s -> %d OK (id=%s attempt=%d)\n", url, resp.StatusCode, msg.ID, msg.Attempts)
		return nil
	}, shoebox.HandlerOptions{
		MaxRetries: 10,
		// First retry: 1s. Second: 2s. ... capped at 1h.
		Backoff: retry.Exponential(1*time.Second, 1*time.Hour),
	})

	// Enqueue three webhooks, two of which point to a URL that 500s
	// and one that succeeds.
	for i, target := range []string{
		"http://httpbin.org/status/200", // succeeds
		"http://httpbin.org/status/500", // retries
		"http://httpbin.org/status/500", // retries
	} {
		_ = q.Enqueue("webhooks",
			[]byte(fmt.Sprintf(`{"event":"order.created","seq":%d}`, i)),
			shoebox.WithMetadata(map[string]string{"url": target}),
		)
	}

	// Drain. The two failing webhooks will retry with backoff up to 10
	// times, so this takes a while in a worst case — cap at 10s for
	// the example.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = q.Shutdown(ctx)
}
