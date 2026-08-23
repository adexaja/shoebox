package shoebox

import (
	"context"
	"encoding/json"
	"errors"
)

// TaskCodec converts a typed task to and from a message payload.
type TaskCodec[T any] interface {
	Marshal(T) ([]byte, error)
	Unmarshal([]byte, *T) error
}

type jsonTaskCodec[T any] struct{}

func (jsonTaskCodec[T]) Marshal(task T) ([]byte, error) { return json.Marshal(task) }
func (jsonTaskCodec[T]) Unmarshal(payload []byte, task *T) error {
	return json.Unmarshal(payload, task)
}

var errNilTaskCodec = errors.New("shoebox: task codec is nil")

// EnqueueTask enqueues task using the standard JSON codec.
func EnqueueTask[T any](q *Queue, queue string, task T, opts ...EnqueueOpt) error {
	return EnqueueTaskWithCodec(q, queue, task, jsonTaskCodec[T]{}, opts...)
}

// EnqueueTaskWithCodec enqueues task using codec.
func EnqueueTaskWithCodec[T any](q *Queue, queue string, task T, codec TaskCodec[T], opts ...EnqueueOpt) error {
	if codec == nil {
		return errNilTaskCodec
	}
	payload, err := codec.Marshal(task)
	if err != nil {
		return err
	}
	return q.Enqueue(queue, payload, opts...)
}

// HandleTask registers a handler for JSON-encoded tasks.
func HandleTask[T any](q *Queue, queue string, handler func(context.Context, T) error, opts ...HandlerOptions) {
	HandleTaskWithCodec(q, queue, jsonTaskCodec[T]{}, handler, opts...)
}

// HandleTaskWithCodec registers a handler using codec. Decode errors are
// returned as handler errors and therefore follow the normal retry/DLQ path.
// A nil codec produces a handler error when a message is delivered.
func HandleTaskWithCodec[T any](q *Queue, queue string, codec TaskCodec[T], handler func(context.Context, T) error, opts ...HandlerOptions) {
	q.Handle(queue, func(ctx context.Context, msg Message) error {
		if codec == nil {
			return errNilTaskCodec
		}
		var task T
		if err := codec.Unmarshal(msg.Payload, &task); err != nil {
			return err
		}
		return handler(ctx, task)
	}, opts...)
}
