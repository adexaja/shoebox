package shoebox

import "github.com/adexaja/shoebox/internal/storage"

// Message is the unit of work passed between Enqueue and a registered handler.
//
// IDs are assigned by the broker on Enqueue. Payload is opaque to the broker —
// encode it however you like (JSON, protobuf, gob). Metadata is for tracing,
// correlation IDs, and other handler-side concerns; it is not interpreted by
// the broker.
//
// ScheduledAt is the time at which the message becomes visible to a consumer.
// On Enqueue it defaults to CreatedAt; for retries it is pushed forward by the
// configured backoff.
//
// Message is an alias for storage.Message so the public API and the storage
// layer share a single struct definition. This avoids translation code in
// the hot path.
type Message = storage.Message

// QueueStats is what Stats returns. Counters are cumulative since the broker
// was started. Aliased for the same reason as Message.
type QueueStats = storage.QueueStats
