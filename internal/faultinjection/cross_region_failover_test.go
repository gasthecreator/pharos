package faultinjection

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/gasthecreator/pharos/internal/consumer"
	"github.com/google/uuid"
)

// waitForGossipStatusFromNode is waitForDCGossipStatus generalized to poll
// from an arbitrary observer node rather than always pharos-cassandra-1 --
// PLAN.md's Slice 18 cross-region failover test needs to confirm dc-eu's
// *own* gossip view of dc-us going down, not just dc-us's view of dc-eu
// (which is all regional_partition_test.go's own helper checks).
func waitForGossipStatusFromNode(t *testing.T, observer, targetDCHeader string, wantDown bool, deadline time.Duration) {
	t.Helper()
	marker := "UN"
	if wantDown {
		marker = "DN"
	}
	other := "DN"
	if wantDown {
		other = "UN"
	}
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		out, err := exec.Command("docker", "exec", "-i", observer, "nodetool", "status").CombinedOutput()
		if err == nil {
			text := string(out)
			section := text
			if idx := strings.Index(text, targetDCHeader); idx >= 0 {
				section = text[idx:]
				if end2 := strings.Index(section[len(targetDCHeader):], "Datacenter:"); end2 >= 0 {
					section = section[:len(targetDCHeader)+end2]
				}
			}
			if strings.Contains(section, marker) && !strings.Contains(section, other) {
				return
			}
		}
		time.Sleep(3 * time.Second)
	}
	t.Fatalf("timed out waiting for %s (observed from %s) to report gossip status %s within %s", targetDCHeader, observer, marker, deadline)
}

// TestCrossRegionFailover_DcEuServesLocalQuorumWhenDcUsUnreachable is the
// flagship fault-injection test for PLAN.md Slice 18 (Backup & disaster
// recovery): "a tested cross-region failover, not just a single-region
// restore." regional_partition_test.go already proves dc-us survives
// losing dc-eu; this proves the property that actually matters for
// disaster recovery -- that dc-eu is a *real* failover target, not just a
// replication sink that happens to hold a copy of the data. A client
// configured with LocalDC=dc-eu (exactly what an operator would
// reconfigure Central Ingestion/consumer to use during a real dc-us
// outage) is proven to genuinely coordinate LOCAL_QUORUM reads and writes
// against dc-eu alone, through cassandra-4's own directly-published CQL
// port (9043) -- not routed through dc-us at all -- while dc-us is
// completely unreachable.
func TestCrossRegionFailover_DcEuServesLocalQuorumWhenDcUsUnreachable(t *testing.T) {
	if !isPortOpen("127.0.0.1", 9042) || !isPortOpen("127.0.0.1", 9043) {
		t.Skip("skipping: Cassandra ports 9042 (dc-us) and/or 9043 (dc-eu) are not open on 127.0.0.1")
	}
	for _, c := range append(append([]string{}, dcUSCassandraContainers...), dcEUCassandraContainers...) {
		if out, err := exec.Command("docker", "inspect", "-f", "{{.State.Running}}", c).CombinedOutput(); err != nil || strings.TrimSpace(string(out)) != "true" {
			t.Skipf("skipping: container %s is not running (this test needs the full docker-compose.yml 2-region topology up)", c)
		}
	}

	ctx := context.Background()
	var usIPs, euIPs []string
	for _, c := range dcUSCassandraContainers {
		usIPs = append(usIPs, containerIP(t, c))
	}
	for _, c := range dcEUCassandraContainers {
		euIPs = append(euIPs, containerIP(t, c))
	}

	t.Cleanup(func() { healRegionalPartition(t) })

	// A store connecting directly to dc-eu's own published port (9043),
	// configured as if an operator had just reconfigured it to treat
	// dc-eu as local during a real dc-us outage.
	dcEUCfg := consumer.DefaultCassandraStoreConfig()
	dcEUCfg.Hosts = []string{"127.0.0.1"}
	dcEUCfg.Port = 9043
	dcEUCfg.LocalDC = "dc-eu"
	dcEUCfg.RemoteDCs = map[string]int{"dc-us": 3}
	// RestrictToLocalDC: a real operator failing over to dc-eu during a
	// genuine dc-us outage needs this too, not just this test -- without
	// it, gocql still tries (and can hang for minutes) reconnecting to
	// dc-us's now-dead hosts as part of its normal peer-awareness, found
	// by watching this exact test hang on store.Close() before this field
	// existed.
	dcEUCfg.RestrictToLocalDC = true
	store, err := consumer.NewCassandraCanonicalStore(dcEUCfg)
	if err != nil {
		t.Fatalf("failed to connect a dc-eu-local store via cassandra-4's own port: %v", err)
	}
	defer store.Close()

	uniqueID := uuid.New().String()[:8]
	siteID := fmt.Sprintf("SITE-FAILOVER-%s", uniqueID)
	studyID := fmt.Sprintf("STUDY-FAILOVER-%s", uniqueID)
	idKey := fmt.Sprintf("%s:1", siteID)
	eventTime := time.Now().UTC()

	record := &consumer.CanonicalRecord{
		IdempotencyKey: idKey,
		SiteID:         siteID,
		StudyID:        studyID,
		LocalSeq:       1,
		EventTime:      eventTime,
		RecordedTime:   eventTime,
		IngestionTime:  eventTime,
		Severity:       "severe",
		EventCode:      "10012345",
		Subject:        "SUBJ-FAILOVER-TEST",
		Payload:        `{"resourceType":"AdverseEvent"}`,
	}

	// 1. Sanity: the dc-eu-local store works normally before any partition.
	if err := store.SaveEvent(ctx, record); err != nil {
		t.Fatalf("pre-partition SaveEvent (dc-eu local) failed: %v", err)
	}
	if _, err := store.GetEvent(ctx, idKey); err != nil {
		t.Fatalf("pre-partition GetEvent (dc-eu local) failed: %v", err)
	}

	// 2. Apply the same real tc-induced partition regional_partition_test.go
	// uses, and wait for dc-eu's *own* gossip view (not dc-us's) to mark
	// dc-us down -- this is the direction that test doesn't check.
	applyRegionalPartition(t, usIPs, euIPs)
	waitForGossipStatusFromNode(t, dcEUCassandraContainers[0], "Datacenter: dc-us", true, 60*time.Second)

	// 3. THE CRITICAL ASSERTION: with dc-us completely unreachable, a
	// fresh write and read against dc-eu's own LOCAL_QUORUM must still
	// succeed -- proving dc-eu is a genuine failover target, not just a
	// passive replication sink.
	failoverKey := fmt.Sprintf("%s:2", siteID)
	failoverRecord := &consumer.CanonicalRecord{
		IdempotencyKey: failoverKey,
		SiteID:         siteID,
		StudyID:        studyID,
		LocalSeq:       2,
		EventTime:      eventTime.Add(time.Minute),
		RecordedTime:   eventTime.Add(time.Minute),
		IngestionTime:  eventTime.Add(time.Minute),
		Severity:       "severe",
		EventCode:      "10012345",
		Subject:        "SUBJ-FAILOVER-TEST",
		Payload:        `{"resourceType":"AdverseEvent","duringFailover":true}`,
	}
	saveCtx, saveCancel := context.WithTimeout(ctx, 15*time.Second)
	if err := store.SaveEvent(saveCtx, failoverRecord); err != nil {
		saveCancel()
		t.Fatalf("CRITICAL: SaveEvent (LOCAL_QUORUM against dc-eu) failed while dc-us was unreachable: %v -- dc-eu cannot serve as a failover region if this fails", err)
	}
	saveCancel()

	getCtx, getCancel := context.WithTimeout(ctx, 15*time.Second)
	got, err := store.GetEvent(getCtx, failoverKey)
	getCancel()
	if err != nil {
		t.Fatalf("CRITICAL: GetEvent (LOCAL_QUORUM against dc-eu) failed while dc-us was unreachable: %v", err)
	}
	if got.StudyID != studyID {
		t.Errorf("expected StudyID %s, got %s", studyID, got.StudyID)
	}

	// 4. Heal and confirm dc-us rejoins from dc-eu's own point of view too.
	healRegionalPartition(t)
	waitForGossipStatusFromNode(t, dcEUCassandraContainers[0], "Datacenter: dc-us", false, 60*time.Second)
}
