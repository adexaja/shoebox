// Example: lead assignment with WhatsApp notification.
//
// This is the PRD's first use case: 400 leads/night, round-robin
// assignment, reliable WhatsApp notification on the side. The handler
// that assigns the lead runs once (the assignment must not be retried),
// and the notification handler runs with its own retry policy so a
// flaky WhatsApp API doesn't poison the assignment step.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	"github.com/adexaja/shoebox"
)

type lead struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	WhatsApp   string `json:"whatsapp"`
	AssignedTo string `json:"assigned_to"`
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	q, err := shoebox.New(shoebox.Options{
		Storage:     shoebox.Memory,
		Concurrency: 2,
		Logger:      logger,
	})
	if err != nil {
		logger.Error("new queue", "err", err)
		return
	}

	var nextSalesman atomic.Uint64
	salesmen := []string{"Ani", "Budi", "Citra", "Dewi"}

	// The "leads" handler: idempotent, never retried (we want the
	// assignment to happen exactly once even if WhatsApp fails).
	q.Handle("leads", func(_ context.Context, msg shoebox.Message) error {
		var l lead
		if err := json.Unmarshal(msg.Payload, &l); err != nil {
			return err
		}
		idx := nextSalesman.Add(1) - 1
		l.AssignedTo = salesmen[int(idx)%len(salesmen)]
		fmt.Printf("[leads] %s -> %s\n", l.Name, l.AssignedTo)

		// Enqueue a notification. The idempotent part of the workflow
		// is now in the queue; the assignment is committed above.
		payload, _ := json.Marshal(l)
		return q.Enqueue("notifications", payload)
	})

	// The "notifications" handler: retried independently. If the
	// WhatsApp API is down, only the notification is retried — the
	// assignment is already done.
	q.Handle("notifications", func(_ context.Context, msg shoebox.Message) error {
		var l lead
		_ = json.Unmarshal(msg.Payload, &l)
		// Simulate a flaky sender: 30% of the time we fail.
		if time.Now().UnixNano()%10 < 3 {
			return fmt.Errorf("simulated WhatsApp failure for %s", l.Name)
		}
		fmt.Printf("[notify] WhatsApp to %s (assigned to %s) OK\n", l.WhatsApp, l.AssignedTo)
		return nil
	}, shoebox.HandlerOptions{
		MaxRetries: 3,
		// shoebox.ConstantBackoff / shoebox.ExponentialBackoff live in
		// the retry package; we use the literal here for the example.
	})

	// Seed 5 leads.
	for i := 1; i <= 5; i++ {
		l := lead{ID: fmt.Sprintf("L%03d", i), Name: fmt.Sprintf("Lead %d", i), WhatsApp: fmt.Sprintf("+62%d", 8000000+i)}
		payload, _ := json.Marshal(l)
		_ = q.Enqueue("leads", payload)
	}

	// Give the dispatcher time to drain.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = q.Shutdown(ctx)
}
