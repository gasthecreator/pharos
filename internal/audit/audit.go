// Package audit implements a durable access-audit trail (§2.4, PLAN.md
// Slice 20: Compliance / access-audit logging) -- who queried what, and
// once Slice 10 (DLQ replay) exists, who replayed what, in the same
// trail. Named and shaped after the existing LateArrivalAudit pattern
// (internal/consumer/types.go, §2.4, Slice 13: "21 CFR Part 11" audit
// deduplication) rather than inventing a second audit mechanism -- one
// typed record struct per audit event, exposed the same way -- except
// this one is genuinely persisted: LateArrivalAudit lives only in a
// WatermarkTracker's own in-memory state, correct for what Slice 13
// needed (proving no late-arrival regression within a single test run,
// not a durable compliance record), but "who accessed a specific
// patient's adverse event data, and when" is exactly the kind of fact a
// real audit trail can't afford to lose on a process restart.
package audit

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gasthecreator/pharos/internal/tlsutil"
	"github.com/gocql/gocql"
)

// AccessAudit records one identified actor performing one action against
// one resource. "Operator" is deliberately generic, not "site_id" or
// "username": a query audit entry's actor is a human/system operator
// running pharos-cli (an internal investigation, not a trial site's own
// action), while a DLQ replay audit entry's actor is the authenticated
// site that owns the record it replayed (§2.1, §2.2, Slice 15) -- both
// are "who did this," recorded in the same trail, per PLAN.md's own
// instruction not to build a second mechanism once replay needed one too.
type AccessAudit struct {
	Operator   string
	Action     string
	Resource   string
	Outcome    string
	OccurredAt time.Time
}

// Store defines the persistence contract for access audit records.
type Store interface {
	RecordAccess(ctx context.Context, a AccessAudit) error
	// ListByOperator returns up to limit of operator's most recent audit
	// entries, newest first.
	ListByOperator(ctx context.Context, operator string, limit int) ([]AccessAudit, error)
	Close() error
}

// CassandraConfig specifies connection parameters for the audit store,
// mirroring the shape of this project's other Cassandra stores (§2.4).
type CassandraConfig struct {
	Hosts          []string
	Port           int
	Keyspace       string
	ConnectTimeout time.Duration
	TLS            *tlsutil.ClientConfig
}

// DefaultCassandraConfig returns connection defaults matching this
// project's other Cassandra stores.
func DefaultCassandraConfig() CassandraConfig {
	var tlsCfg *tlsutil.ClientConfig
	if caCert := tlsutil.DefaultCACertPath(); caCert != "" {
		tlsCfg = &tlsutil.ClientConfig{CACertPath: caCert, ServerName: "localhost"}
	}
	return CassandraConfig{
		Hosts:          []string{"127.0.0.1"},
		Port:           9042,
		Keyspace:       "pharos",
		ConnectTimeout: 10 * time.Second,
		TLS:            tlsCfg,
	}
}

// CassandraStore implements Store against Apache Cassandra.
type CassandraStore struct {
	session *gocql.Session
	mu      sync.RWMutex
	closed  bool
}

// NewCassandraStore connects to Cassandra and bootstraps the
// access_audit_log table.
func NewCassandraStore(cfg CassandraConfig) (*CassandraStore, error) {
	cluster := gocql.NewCluster(cfg.Hosts...)
	if cfg.Port > 0 {
		cluster.Port = cfg.Port
	}
	cluster.Timeout = cfg.ConnectTimeout
	cluster.DisableInitialHostLookup = true
	cluster.Keyspace = cfg.Keyspace
	if cfg.TLS != nil {
		sslOpts, err := cfg.TLS.GocqlSslOptions()
		if err != nil {
			return nil, fmt.Errorf("failed to build TLS config: %w", err)
		}
		cluster.SslOpts = sslOpts
	}

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Cassandra cluster: %w", err)
	}

	store := &CassandraStore{session: session}
	if err := store.ensureSchema(); err != nil {
		session.Close()
		return nil, fmt.Errorf("failed to bootstrap access_audit_log schema: %w", err)
	}
	return store, nil
}

func (s *CassandraStore) ensureSchema() error {
	// Partitioned by operator, clustered by audit_id (a timeuuid) DESC --
	// "who queried what" is fundamentally an operator-scoped question
	// ("show me operator X's recent activity"), and timeuuid gives
	// natural chronological ordering plus per-entry uniqueness without a
	// separate counter.
	return s.session.Query(`
		CREATE TABLE IF NOT EXISTS pharos.access_audit_log (
			operator text,
			audit_id timeuuid,
			action text,
			resource text,
			outcome text,
			occurred_at timestamp,
			PRIMARY KEY (operator, audit_id)
		) WITH CLUSTERING ORDER BY (audit_id DESC);
	`).Exec()
}

func (s *CassandraStore) RecordAccess(ctx context.Context, a AccessAudit) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return fmt.Errorf("audit store is closed")
	}
	occurredAt := a.OccurredAt
	if occurredAt.IsZero() {
		occurredAt = time.Now().UTC()
	}
	return s.session.Query(
		`INSERT INTO pharos.access_audit_log (operator, audit_id, action, resource, outcome, occurred_at) VALUES (?, ?, ?, ?, ?, ?);`,
		a.Operator, gocql.TimeUUID(), a.Action, a.Resource, a.Outcome, occurredAt,
	).WithContext(ctx).Exec()
}

func (s *CassandraStore) ListByOperator(ctx context.Context, operator string, limit int) ([]AccessAudit, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, fmt.Errorf("audit store is closed")
	}
	iter := s.session.Query(
		`SELECT action, resource, outcome, occurred_at FROM pharos.access_audit_log WHERE operator = ? LIMIT ?;`,
		operator, limit,
	).WithContext(ctx).Iter()

	var results []AccessAudit
	var action, resource, outcome string
	var occurredAt time.Time
	for iter.Scan(&action, &resource, &outcome, &occurredAt) {
		results = append(results, AccessAudit{
			Operator:   operator,
			Action:     action,
			Resource:   resource,
			Outcome:    outcome,
			OccurredAt: occurredAt,
		})
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("failed to list access audit for operator %s: %w", operator, err)
	}
	return results, nil
}

func (s *CassandraStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed && s.session != nil {
		s.session.Close()
		s.closed = true
	}
	return nil
}

// MemoryStore is an in-memory Store for --memory CLI mode and tests.
type MemoryStore struct {
	mu      sync.RWMutex
	entries map[string][]AccessAudit // keyed by operator, newest first
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{entries: make(map[string][]AccessAudit)}
}

func (m *MemoryStore) RecordAccess(ctx context.Context, a AccessAudit) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if a.OccurredAt.IsZero() {
		a.OccurredAt = time.Now().UTC()
	}
	// Prepend so the slice stays newest-first, matching CassandraStore's
	// own audit_id DESC clustering order.
	m.entries[a.Operator] = append([]AccessAudit{a}, m.entries[a.Operator]...)
	return nil
}

func (m *MemoryStore) ListByOperator(ctx context.Context, operator string, limit int) ([]AccessAudit, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	entries := m.entries[operator]
	if limit > 0 && len(entries) > limit {
		entries = entries[:limit]
	}
	result := make([]AccessAudit, len(entries))
	copy(result, entries)
	return result, nil
}

func (m *MemoryStore) Close() error { return nil }
