package shoebox

import (
	"context"
	"errors"
	"testing"
	"time"
)

type taskPayload struct {
	ID    int    `json:"id"`
	Label string `json:"label"`
}

type stringCodec struct{}

func (stringCodec) Marshal(v taskPayload) ([]byte, error) { return []byte(v.Label), nil }
func (stringCodec) Unmarshal(b []byte, v *taskPayload) error {
	v.Label = string(b)
	return nil
}

func TestTypedTaskJSONRoundTrip(t *testing.T) {
	q := newTestQueue(t)
	got := make(chan taskPayload, 1)
	HandleTask(q, "tasks", func(_ context.Context, task taskPayload) error {
		got <- task
		return nil
	})
	if err := EnqueueTask(q, "tasks", taskPayload{ID: 7, Label: "json"}); err != nil {
		t.Fatal(err)
	}
	select {
	case task := <-got:
		if task.ID != 7 || task.Label != "json" {
			t.Fatalf("decoded task = %#v", task)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("typed handler did not run")
	}
}

func TestTypedTaskCustomCodec(t *testing.T) {
	q := newTestQueue(t)
	got := make(chan string, 1)
	HandleTaskWithCodec(q, "tasks", stringCodec{}, func(_ context.Context, task taskPayload) error {
		got <- task.Label
		return nil
	})
	if err := EnqueueTaskWithCodec(q, "tasks", taskPayload{Label: "custom"}, stringCodec{}); err != nil {
		t.Fatal(err)
	}
	select {
	case value := <-got:
		if value != "custom" {
			t.Fatalf("decoded custom task = %q", value)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("custom typed handler did not run")
	}
}

func TestTypedTaskDecodeErrorUsesDLQ(t *testing.T) {
	q := newTestQueue(t)
	HandleTask(q, "tasks", func(context.Context, taskPayload) error { return nil }, HandlerOptions{MaxRetries: 0})
	if err := q.Enqueue("tasks", []byte("not-json")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		stats, _ := q.Stats(context.Background(), "tasks")
		return stats.Dead == 1
	}, 3*time.Second)
}

func TestTypedTaskNilCodec(t *testing.T) {
	q := newTestQueue(t)
	if err := EnqueueTaskWithCodec(q, "tasks", taskPayload{}, nil); !errors.Is(err, errNilTaskCodec) {
		t.Fatalf("nil codec error = %v", err)
	}
}
