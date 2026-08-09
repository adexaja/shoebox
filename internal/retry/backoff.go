// Package retry computes the delay between handler attempts.
//
// The strategy is pluggable via the Backoff interface; the broker calls
// Next(attempt) where attempt is 1-indexed (the first retry is attempt=1).
package retry

import "time"

// Backoff returns the delay before the next attempt.
type Backoff interface {
	Next(attempt int) time.Duration
}

// Constant always returns d. Use it for fixed-cadence retries such as
// "try once a minute until it works."
func Constant(d time.Duration) Backoff {
	return constant(d)
}

type constant time.Duration

func (c constant) Next(attempt int) time.Duration { return time.Duration(c) }

// Exponential returns min(base * 2^(attempt-1), max). Attempt is 1-indexed,
// so the first retry waits `base`, the second `2*base`, and so on.
//
// This is the default backoff in v0.1.
func Exponential(base, max time.Duration) Backoff {
	return exponential{base: base, max: max}
}

type exponential struct {
	base, max time.Duration
}

func (e exponential) Next(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	// Guard against overflow for very large attempt counts.
	shift := attempt - 1
	if shift > 30 {
		shift = 30
	}
	d := e.base << shift
	if d <= 0 || d > e.max {
		return e.max
	}
	return d
}
