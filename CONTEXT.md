# Shoebox Context

Shoebox is an embedded queue for durable background work. It lets an application enqueue messages, process them through registered handlers, and expose the same work through in-process, persistent, and standalone-server use cases.

## Queue and message language

**Queue**:
A named stream of messages processed by a registered handler. A queue can be paused, resumed, drained, inspected, and measured.
_Avoid_: topic, channel (unless referring specifically to a Go channel)

**Message**:
One unit of background work, consisting of a payload and delivery metadata such as attempts, visibility time, priority, deduplication identity, and handler metadata.
_Avoid_: job, task (when referring to an individual queued work unit)

**Handler**:
The registered processor that attempts one message and reports success or failure.
_Avoid_: consumer, worker (a worker executes a handler; it is not the processing rule)

**Enqueue**:
The act of placing a new message into a queue for later delivery.
_Avoid_: publish, send (unless describing an external transport)

**Acknowledge**:
The confirmation that a handler successfully processed a message. A message is acknowledged only after successful handler completion.
_Avoid_: complete (too broad), delete (deleting is one possible storage action)

**Delivery attempt**:
One invocation of a handler for a message. Failed attempts can be retried according to the handler's retry policy.
_Avoid_: execution (too broad)

**Retry**:
A subsequent delivery attempt after a handler failure, optionally delayed by a backoff policy.
_Avoid_: redelivery (use only when discussing transport behavior)

**Dead letter**:
The terminal handling outcome for a message that has exhausted its retry policy or otherwise cannot continue through normal delivery.
_Avoid_: failed message (a failed attempt is not terminal)

**Dead-letter queue (DLQ)**:
The inspection and recovery destination associated with a source queue for dead-letter messages. Its public name is `{queue}.dlq`.
_Avoid_: error queue, poison queue

**Replay**:
The operation that returns a dead-letter message to its source queue for another delivery attempt.
_Avoid_: retry (replay is an explicit operator action; retry follows handler failure)

## Timing and recurring work

**Visibility time**:
The earliest time at which a message may be delivered. A message can be immediately visible, delayed, or scheduled for an absolute time.
_Avoid_: due date (reserved for periodic work)

**Priority**:
The relative delivery order of messages within one queue. Higher priority is delivered first; equal priority preserves creation order.
_Avoid_: urgency

**Dedupe key**:
An application-provided identity used to suppress repeated enqueue requests for the same queue during the configured deduplication window.
_Avoid_: idempotency key (deduplication suppresses enqueues; handler idempotency is the application's responsibility)

**Periodic job**:
A recurring instruction that enqueues a message for a queue at a fixed interval.
_Avoid_: cron job (there is no cron expression in the domain)

**Schedule**:
The cadence and next occurrence of a periodic job, including its enabled state and payload.
_Avoid_: timer (a timer is an implementation mechanism)

**Occurrence**:
One scheduled run of a periodic job. Missed occurrences advance to the first future cadence rather than creating an unbounded backlog.
_Avoid_: tick (too implementation-oriented)

## Queue control and guarantees

**Pause**:
A queue state in which new messages remain queued while in-flight handlers continue.
_Avoid_: stop (stop implies cancellation of in-flight work)

**Resume**:
The operation that allows a paused queue to deliver queued messages again.
_Avoid_: restart (resume does not recreate the queue)

**Drain**:
The operation that processes a queue to quiescence without stopping other queues.
_Avoid_: flush (flush does not describe handler completion)

**Quiescence**:
The state in which a queue has no pending messages and no in-flight handlers.
_Avoid_: empty (an empty due window can still contain scheduled messages)

**At-least-once delivery**:
The delivery guarantee that a message may be delivered more than once, but successful processing is acknowledged only after the handler returns success. Handlers must therefore tolerate retries.
_Avoid_: exactly-once delivery

**Crash recovery**:
The return of in-flight persistent messages to deliverable state after a process or machine failure.
_Avoid_: resume (resume is an intentional queue-control action)

## Payload forms and delivery modes

**Typed task**:
A message whose payload is encoded and decoded as an application type while using the same queue, retry, and dead-letter semantics as a byte-oriented message.
_Avoid_: typed message (the typed view applies to the payload and handler, not to the queue lifecycle)

**Webhook delivery**:
A handler mode that posts a message payload to an HTTP target. Non-success responses follow the normal retry and dead-letter rules.
_Avoid_: callback (the delivery is an outbound HTTP attempt)

**Standalone server**:
The `shoeboxd` deployment mode that exposes queue intake, pull delivery, statistics, dead-letter operations, dashboard views, metrics, and webhook configuration over HTTP.
_Avoid_: external broker (the queue still runs inside the server process)
