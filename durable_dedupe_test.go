package shoebox

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"
)

func TestDurableDedupePostgres(t *testing.T) {
	dsn := os.Getenv("SHOEBOX_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("SHOEBOX_TEST_POSTGRES_DSN is not set")
	}
	q, err := New(Options{Storage: Postgres, DSN: dsn, Dedupe: DedupeOptions{Policy: DedupePolicyDurable}})
	if err != nil {
		t.Skipf("Postgres unavailable: %v", err)
	}
	defer func() { _ = q.Shutdown(context.Background()) }()
	queue := "durable-dedupe-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	if err := q.Enqueue(queue, []byte("one"), DedupeKey("K")); err != nil {
		t.Fatal(err)
	}
	if err := q.Enqueue(queue, []byte("two"), DedupeKey("K")); err != nil {
		t.Fatal(err)
	}
	stats, err := q.Stats(context.Background(), queue)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Depth != 1 {
		t.Fatalf("durable duplicate depth = %d, want 1", stats.Depth)
	}
	q.Handle(queue, func(context.Context, Message) error { return nil })
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		stats, _ = q.Stats(context.Background(), queue)
		if stats.Processed >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if stats.Processed < 1 {
		t.Fatalf("message was not acknowledged before key reuse: %#v", stats)
	}
	if err := q.Enqueue(queue, []byte("three"), DedupeKey("K")); err != nil {
		t.Fatal(err)
	}
	stats, err = q.Stats(context.Background(), queue)
	if err != nil || stats.Depth != 1 {
		t.Fatalf("reused durable key depth = %d, err=%v", stats.Depth, err)
	}
}
