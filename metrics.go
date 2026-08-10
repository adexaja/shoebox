package shoebox

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics holds the Prometheus collectors shoebox exposes. There is one set
// per Queue, labelled by queue name so multiple queues are distinguishable on
// the same dashboard.
//
// The collectors are:
//
//   shoebox_messages_processed_total{queue}   — counter, bumped on Ack
//   shoebox_messages_errors_total{queue}      — counter, bumped on Nack
//   shoebox_messages_retries_total{queue}     — counter, bumped on retry
//   shoebox_messages_dead_total{queue}        — counter, bumped on dead-letter
//   shoebox_queue_depth{queue}                — gauge, set from Stats().Depth
//   shoebox_handler_duration_seconds{queue}   — histogram, observed per invocation
type Metrics struct {
	Processed  *prometheus.CounterVec
	Errors     *prometheus.CounterVec
	Retries    *prometheus.CounterVec
	Dead       *prometheus.CounterVec
	Depth      *prometheus.GaugeVec
	Duration   *prometheus.HistogramVec
	registerer prometheus.Registerer
}

// NewMetrics creates and registers a Metrics set on the given registerer.
// If registerer is nil, prometheus.DefaultRegisterer is used. Calling
// NewMetrics twice on the same registerer with the same namespace will panic
// (duplicate registration); use a custom registry per Queue in tests.
func NewMetrics(namespace string, registerer prometheus.Registerer) *Metrics {
	if registerer == nil {
		registerer = prometheus.DefaultRegisterer
	}
	// Guard against typed-nil: DefaultRegisterer can wrap a nil *Registry
	// in rare init-order edge cases (Go 1.26 + dependency cycles). Fall
	// back to a fresh registry so the caller never sees a nil panic.
	if registerer == prometheus.Registerer((*prometheus.Registry)(nil)) {
		registerer = prometheus.NewRegistry()
	}
	if namespace == "" {
		namespace = "shoebox"
	}
	m := &Metrics{
		registerer: registerer,
		Processed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "messages_processed_total",
			Help:      "Total messages successfully processed (Acked).",
		}, []string{"queue"}),
		Errors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "messages_errors_total",
			Help:      "Total messages that returned an error (Nacked).",
		}, []string{"queue"}),
		Retries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "messages_retries_total",
			Help:      "Total message retry attempts.",
		}, []string{"queue"}),
		Dead: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "messages_dead_total",
			Help:      "Total messages moved to the dead-letter queue.",
		}, []string{"queue"}),
		Depth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "queue_depth",
			Help:      "Current number of pending messages in the queue.",
		}, []string{"queue"}),
		Duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "handler_duration_seconds",
			Help:      "Handler invocation duration in seconds.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"queue"}),
	}
	registerer.MustRegister(m.Processed)
	registerer.MustRegister(m.Errors)
	registerer.MustRegister(m.Retries)
	registerer.MustRegister(m.Dead)
	registerer.MustRegister(m.Depth)
	registerer.MustRegister(m.Duration)
	return m
}
