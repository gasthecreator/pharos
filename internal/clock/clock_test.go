package clock

import (
	"testing"
	"time"
)

func TestReal_ReturnsUTC(t *testing.T) {
	now := Real{}.Now()
	if now.Location() != time.UTC {
		t.Errorf("expected Real clock to return UTC time, got location %v", now.Location())
	}
}

func TestSimulated_AdvanceMovesForward(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	c := NewSimulated(start)
	if !c.Now().Equal(start) {
		t.Fatalf("expected initial time %v, got %v", start, c.Now())
	}
	got := c.Advance(5 * time.Second)
	want := start.Add(5 * time.Second)
	if !got.Equal(want) || !c.Now().Equal(want) {
		t.Errorf("expected %v after Advance, got %v (Now()=%v)", want, got, c.Now())
	}
}

func TestSimulated_SetIgnoresBackwardMoves(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 10, 0, time.UTC)
	c := NewSimulated(start)
	c.Set(start.Add(-5 * time.Second))
	if !c.Now().Equal(start) {
		t.Errorf("expected Set to a past time to be a no-op, got %v", c.Now())
	}
	c.Set(start.Add(5 * time.Second))
	if !c.Now().Equal(start.Add(5 * time.Second)) {
		t.Errorf("expected Set to a future time to apply, got %v", c.Now())
	}
}
