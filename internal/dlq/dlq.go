// Package dlq implements the dead-letter queue: a shadow queue per source
// queue where messages that exhaust their retries are stored with their
// original payload, the last error, and a timestamp.
//
// The Week-1 broker already routes dead messages to a `{queue}.dlq` shadow
// queue via the storage layer. This package will add (in Week 2):
//   - structured DLQ record (error message, retry count, original queue)
//   - list/inspect/replay APIs consumed by the HTTP layer in Week 4
//   - retention policy (default: never expire; configurable)
package dlq

// TODO(E2): implement the DLQ record type and the list/inspect/replay APIs.
