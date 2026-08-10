package retry

import (
	"testing"
	"time"
)

// TestConstant verifies Constant ignores the attempt number and always
// returns the same duration.
func TestConstant(t *testing.T) {
	c := Constant(250 * time.Millisecond)
	for _, attempt := range []int{1, 2, 5, 100} {
		if got := c.Next(attempt); got != 250*time.Millisecond {
			t.Errorf("Next(%d) = %v, want 250ms", attempt, got)
		}
	}
}

// TestExponential_Growth verifies base * 2^(attempt-1) for the first few
// attempts before the cap bites.
func TestExponential_Growth(t *testing.T) {
	b := Exponential(100*time.Millisecond, 60*time.Second)
	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{1, 100 * time.Millisecond},   // base * 2^0
		{2, 200 * time.Millisecond},   // base * 2^1
		{3, 400 * time.Millisecond},   // base * 2^2
		{4, 800 * time.Millisecond},   // base * 2^3
		{5, 1600 * time.Millisecond},  // base * 2^4
		{6, 3200 * time.Millisecond},  // base * 2^5
		{7, 6400 * time.Millisecond},  // base * 2^6
	}
	for _, tc := range cases {
		if got := b.Next(tc.attempt); got != tc.want {
			t.Errorf("Next(%d) = %v, want %v", tc.attempt, got, tc.want)
		}
	}
}

// TestExponential_CapsAtMax verifies the delay never exceeds max, even for
// absurdly large attempt counts (which would otherwise overflow).
func TestExponential_CapsAtMax(t *testing.T) {
	max := 10 * time.Second
	b := Exponential(100*time.Millisecond, max)
	for _, attempt := range []int{50, 100, 1000, 10_000} {
		if got := b.Next(attempt); got != max {
			t.Errorf("Next(%d) = %v, want cap %v", attempt, got, max)
		}
	}
}

// TestExponential_CapBoundary pinpoints the exact attempt where growth stops
// and the cap takes over.
func TestExponential_CapBoundary(t *testing.T) {
	// base=100ms, max=1.6s. Growth: 100,200,...,800 (attempt 4), 1600 (5).
	// Attempt 6 would be 3200ms > 1600ms, so the cap kicks in at attempt 6.
	max := 1600 * time.Millisecond
	b := Exponential(100*time.Millisecond, max)

	if got := b.Next(5); got != max {
		t.Errorf("Next(5) = %v, want %v (last growth step)", got, max)
	}
	if got := b.Next(6); got != max {
		t.Errorf("Next(6) = %v, want cap %v", got, max)
	}
}

// TestExponential_AttemptClampedToMin verifies attempt <= 0 is treated as 1
// (first retry), not a degenerate/negative shift.
func TestExponential_AttemptClampedToMin(t *testing.T) {
	b := Exponential(100*time.Millisecond, 60*time.Second)
	want := 100 * time.Millisecond
	for _, attempt := range []int{-5, -1, 0} {
		if got := b.Next(attempt); got != want {
			t.Errorf("Next(%d) = %v, want %v (clamped to attempt 1)", attempt, got, want)
		}
	}
}

// TestExponential_NoOverflowAtExtremeAttempt is a regression guard for the
// shift guard: an attempt of 100 must not overflow `base << shift` into a
// negative or tiny duration; it must return the cap.
func TestExponential_NoOverflowAtExtremeAttempt(t *testing.T) {
	b := Exponential(1*time.Second, 30*time.Second)
	got := b.Next(100)
	if got <= 0 || got > 30*time.Second {
		t.Errorf("Next(100) = %v, want cap 30s (no overflow/negative)", got)
	}
}
