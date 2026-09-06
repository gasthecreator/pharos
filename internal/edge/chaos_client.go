package edge

import (
	"errors"
	"net/http"
	"sync/atomic"
)

// ChaosClient wraps a real HTTPClient with a togglable simulated network
// partition (§2.4, PLAN.md Slice 23: Chaos Control Panel) -- when
// partitioned, Do returns a connection-refused-style error without ever
// reaching the real network, mirroring
// internal/faultinjection/network_partition_test.go's own
// partitionableTransport test double, but wired into this project's real
// running pharos-edge process (via ServeChaosAdmin, below) instead of only
// existing inside a test binary. Defaults to pass-through -- constructing
// one and never calling SetPartitioned is a zero-behavior-change wrapper
// around the real client, exercising this project's existing store-and-forward
// resilience (§2.1) for real rather than reimplementing it.
type ChaosClient struct {
	real        HTTPClient
	partitioned atomic.Bool
}

// NewChaosClient wraps real. real must not be nil.
func NewChaosClient(real HTTPClient) *ChaosClient {
	return &ChaosClient{real: real}
}

// SetPartitioned toggles the simulated partition on or off.
func (c *ChaosClient) SetPartitioned(v bool) {
	c.partitioned.Store(v)
}

// IsPartitioned reports the current simulated partition state.
func (c *ChaosClient) IsPartitioned() bool {
	return c.partitioned.Load()
}

// Do implements HTTPClient.
func (c *ChaosClient) Do(req *http.Request) (*http.Response, error) {
	if c.partitioned.Load() {
		return nil, errors.New("chaos control panel: simulated network partition (connection refused)")
	}
	return c.real.Do(req)
}
