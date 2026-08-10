// Example: email sender with exponential backoff.
//
// A common task queue use case: send transactional emails. The handler
// simulates an SMTP send that fails intermittently (20% of the time).
// shoebox retries with exponential backoff (1s → 2s → 4s, capped at 1m),
// dead-letters messages that exhaust 5 retries, and logs every attempt.
//
// Run:
//
//	go run ./examples/email-sender
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/adexaja/shoebox"
	"github.com/adexaja/shoebox/internal/retry"
)

type email struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	q, err := shoebox.New(shoebox.Options{
		Storage:     shoebox.Memory,
		Concurrency: 3,
		Logger:      logger,
	})
	if err != nil {
		logger.Error("new queue", "err", err)
		return
	}

	// The "emails" handler: simulates an SMTP send with 20% failure rate.
	// Retried with exponential backoff (1s, 2s, 4s, 8s, 16s), max 5 attempts.
	q.Handle("emails", func(_ context.Context, msg shoebox.Message) error {
		var e email
		if err := json.Unmarshal(msg.Payload, &e); err != nil {
			return err
		}

		// Simulate flaky SMTP: 20% chance of failure.
		if rand.Intn(5) == 0 {
			return fmt.Errorf("smtp: transient failure for %s (attempt %d)", e.To, msg.Attempts+1)
		}

		fmt.Printf("[email]  to=%s  subject=%q  attempt=%d  OK\n", e.To, e.Subject, msg.Attempts+1)
		return nil
	}, shoebox.HandlerOptions{
		MaxRetries: 5,
		Backoff:    retry.Exponential(1*time.Second, 1*time.Minute),
	})

	// Enqueue a batch of emails.
	for i := 1; i <= 10; i++ {
		e := email{
			To:      "user" + strconv.Itoa(i) + "@example.test",
			Subject: fmt.Sprintf("Welcome #%d", i),
			Body:    "Thanks for signing up!",
		}
		payload, _ := json.Marshal(e)
		_ = q.Enqueue("emails", payload)
	}

	// Give the dispatcher time to drain (retries with backoff take a few seconds).
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Shutdown drains the queue (waits for in-flight + retrying handlers),
	// THEN we read stats. Reading before Shutdown shows zeros because
	// handlers haven't finished yet.
	_ = q.Shutdown(ctx)

	stats, _ := q.Stats(context.Background(), "emails")
	fmt.Printf("\n--- results: processed=%d errors=%d retries=%d dead=%d ---\n",
		stats.Processed, stats.Errors, stats.Retries, stats.Dead)
}
