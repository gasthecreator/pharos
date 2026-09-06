package audit

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
)

func isPortOpen(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// TestCassandraStore_RealIntegration proves the actual persisted audit
// trail (§2.4, PLAN.md Slice 20: Compliance / access-audit logging) works
// against real Cassandra, not just the in-memory stand-in used for
// --memory CLI mode and other tests.
func TestCassandraStore_RealIntegration(t *testing.T) {
	if !isPortOpen("127.0.0.1:9042") {
		t.Skip("skipping: Cassandra port 9042 is not open on 127.0.0.1")
	}
	ctx := context.Background()
	store, err := NewCassandraStore(DefaultCassandraConfig())
	if err != nil {
		t.Fatalf("failed to connect to real Cassandra: %v", err)
	}
	defer store.Close()

	operator := "test-operator-" + uuid.New().String()[:8]

	if err := store.RecordAccess(ctx, AccessAudit{
		Operator: operator, Action: "QUERY_EVENT", Resource: "SITE-AUDIT-TEST:1", Outcome: "SUCCESS",
	}); err != nil {
		t.Fatalf("RecordAccess failed: %v", err)
	}
	if err := store.RecordAccess(ctx, AccessAudit{
		Operator: operator, Action: "QUERY_STUDY", Resource: "STUDY-AUDIT-TEST", Outcome: "ERROR: not found",
	}); err != nil {
		t.Fatalf("RecordAccess failed: %v", err)
	}

	entries, err := store.ListByOperator(ctx, operator, 10)
	if err != nil {
		t.Fatalf("ListByOperator failed: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 real, persisted audit entries for %s, got %d", operator, len(entries))
	}
	// Newest first (clustered by audit_id DESC).
	if entries[0].Action != "QUERY_STUDY" {
		t.Errorf("expected most recent entry first (QUERY_STUDY), got %s", entries[0].Action)
	}
	if entries[0].Outcome != "ERROR: not found" {
		t.Errorf("expected outcome to be persisted verbatim, got %q", entries[0].Outcome)
	}
	if entries[1].Resource != "SITE-AUDIT-TEST:1" {
		t.Errorf("expected the first-recorded entry's resource to be preserved, got %q", entries[1].Resource)
	}
}
