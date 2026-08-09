// Package api serves the HTTP API for shoebox's standalone-server mode
// (shoeboxd). Planned endpoints (Week 4):
//
//	POST   /queues/{name}/messages         enqueue
//	GET    /queues/{name}/messages/next    consume (pull-based)
//	GET    /queues/{name}/stats            depth, rate, errors
//	GET    /queues/{name}/dlq              list dead-letter messages
//	POST   /queues/{name}/dlq/{id}/replay  replay a dead message
//	DELETE /queues/{name}/messages/{id}    acknowledge/delete
package api

// TODO(E4): implement the HTTP handlers. They will share the broker
// and storage types from the internal packages above.
