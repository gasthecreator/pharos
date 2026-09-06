// Package clock provides a Clock abstraction so time-dependent logic (lease
// expiry, watermark advancement) can be driven by a controllable, seeded
// clock in tests instead of real wall-clock time (§2.4, PLAN.md Slice 22:
// property-based & deterministic simulation testing). Production code
// always uses Real, which is a zero-behavior-change wrapper around
// time.Now().UTC() -- this package only exists to give tests a second,
// controllable implementation to substitute in its place.
package clock

import (
	"sync"
	"time"
)

// Clock returns the current time. Production code depends on this
// interface, not on time.Now() directly, wherever a property or simulation
// test needs to control elapsed time without real sleeping.
type Clock interface {
	Now() time.Time
}

// Real is the production Clock: a thin wrapper around time.Now().UTC().
type Real struct{}

func (Real) Now() time.Time { return time.Now().UTC() }

// Simulated is a manually-advanced Clock for tests -- no real time ever
// passes; Now() only changes when Advance or Set is called. Safe for
// concurrent use, since property/simulation tests may drive it from
// multiple goroutines representing concurrent workers.
type Simulated struct {
	mu  sync.Mutex
	now time.Time
}

// NewSimulated constructs a Simulated clock starting at start.
func NewSimulated(start time.Time) *Simulated {
	return &Simulated{now: start}
}

func (c *Simulated) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance moves the clock forward by d and returns the new time.
func (c *Simulated) Advance(d time.Duration) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	return c.now
}

// Set moves the clock directly to t (which must not be before the current
// time -- this clock models forward-only wall-clock progression, not time
// travel; a simulation that needs "what if two events raced" models that as
// concurrent operations at the same or nearby simulated times, not by
// rewinding the clock).
func (c *Simulated) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if t.After(c.now) {
		c.now = t
	}
}
