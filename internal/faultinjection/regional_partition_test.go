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

// dcUSCassandraContainers and dcEUCassandraContainers name the real
// containers docker-compose.yml stands up for each simulated datacenter
// (§2.4, Slice 14: Multi-Region Cassandra + Kafka). dc-eu is down to 1
// container as of Slice 15 (was 2, was originally planned as 3) -- see
// docker-compose.yml's Cassandra section comment for the full reasoning (a
// real, empirically-confirmed ~6.3GB Docker VM memory ceiling, tightened
// further once TLS was added).
var (
	dcUSCassandraContainers = []string{"pharos-cassandra-1", "pharos-cassandra-2", "pharos-cassandra-3"}
	dcEUCassandraContainers = []string{"pharos-cassandra-4"}
)

// containerIP returns a running container's IP address on pharos-net, looked
// up fresh rather than assumed/hardcoded -- Docker assigns these dynamically,
// and this project's containers aren't pinned to static IPs.
func containerIP(t *testing.T, container string) string {
	t.Helper()
	out, err := exec.Command("docker", "exec", container, "hostname", "-i").CombinedOutput()
	if err != nil {
		t.Fatalf("failed to look up IP for %s: %v (%s)", container, err, string(out))
	}
	return strings.TrimSpace(string(out))
}

// applyRegionalPartition induces a real, surgical network partition between
// dc-us and dc-eu's Cassandra containers using tc netem (§2.4, Slice 14) --
// the same standard, recognized mechanism PLAN.md names for simulating WAN
// failure without real geographic infrastructure. Only inter-DC traffic is
// blocked: each side gets a `prio` qdisc whose *default* band (priomap all
// zeros -> band 0 -> class 1:1, an untouched pfifo) carries same-DC traffic
// completely normally, while `tc filter` rules u32-match the *other* DC's
// specific peer IPs into a second band (class 1:3) whose child qdisc is
// `netem loss 100%`. Getting the priomap wrong here is exactly the bug this
// comment exists to warn against: a priomap that defaults traffic into the
// same band the filters target silently breaks intra-DC gossip too, not
// just the intended inter-DC link -- caught by hand while first exercising
// this mechanism, before automating it into this test.
func applyRegionalPartition(t *testing.T, usIPs, euIPs []string) {
	t.Helper()
	runOrFail := func(container string, peerIPs []string) {
		cmd := "tc qdisc add dev eth0 root handle 1: prio priomap 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 && " +
			"tc qdisc add dev eth0 parent 1:3 handle 30: netem loss 100%"
		for _, ip := range peerIPs {
			cmd += fmt.Sprintf(" && tc filter add dev eth0 protocol ip parent 1:0 prio 3 u32 match ip dst %s/32 flowid 1:3", ip)
		}
		out, err := exec.Command("docker", "exec", container, "sh", "-c", cmd).CombinedOutput()
		if err != nil {
			t.Fatalf("failed to apply partition on %s: %v (%s)", container, err, string(out))
		}
	}
	for _, c := range dcUSCassandraContainers {
		runOrFail(c, euIPs)
	}
	for _, c := range dcEUCassandraContainers {
		runOrFail(c, usIPs)
	}
}

// healRegionalPartition removes every tc qdisc this test added, restoring
// full connectivity between dc-us and dc-eu.
func healRegionalPartition(t *testing.T) {
	t.Helper()
	for _, c := range append(append([]string{}, dcUSCassandraContainers...), dcEUCassandraContainers...) {
		_ = exec.Command("docker", "exec", c, "tc", "qdisc", "del", "dev", "eth0", "root").Run()
	}
}

// waitForDCGossipStatus polls `nodetool status` on a dc-us node until every
// dc-eu node's reported status matches wantDown (true for "DN", false for
// "UN"), or the deadline passes. Real gossip failure detection takes several
// seconds even after the network is genuinely cut, so tests observe this
// directly rather than assuming a fixed sleep is enough.
func waitForDCGossipStatus(t *testing.T, wantDown bool, deadline time.Duration) {
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
		out, err := exec.Command("docker", "exec", "-i", "pharos-cassandra-1", "nodetool", "status").CombinedOutput()
		if err == nil {
			text := string(out)
			euSection := text
			if idx := strings.Index(text, "Datacenter: dc-eu"); idx >= 0 {
				euSection = text[idx:]
				if end2 := strings.Index(euSection, "Datacenter: dc-us"); end2 >= 0 {
					euSection = euSection[:end2]
				}
			}
			if strings.Contains(euSection, marker) && !strings.Contains(euSection, other) {
				return
			}
		}
		time.Sleep(3 * time.Second)
	}
	t.Fatalf("timed out waiting for dc-eu nodes to report gossip status %s within %s", marker, deadline)
}

// TestRegionalPartition_DcUsLocalQuorumSurvivesDcEuUnreachable is the
// flagship fault-injection test for Slice 14 (ARCHITECTURE_PROPOSALS.md
// "Multi-Region Cassandra + Kafka (Simulated)"): a real tc-induced network
// partition cuts all traffic between dc-us and dc-eu's Cassandra containers,
// and this proves the property PLAN.md's own Slice 7 reasoning was built
// around -- that LOCAL_QUORUM against dc-us's own 3-node replica set (RF=3)
// keeps working normally through the real application code path (DC-aware
// gocql host selection + NetworkTopologyStrategy), with zero dependency on
// dc-eu's reachability. The partition is then healed and the write made
// during the outage is confirmed to reach dc-eu too, proving Cassandra's own
// hinted handoff/repair genuinely catches the remote DC back up rather than
// silently losing it.
func TestRegionalPartition_DcUsLocalQuorumSurvivesDcEuUnreachable(t *testing.T) {
	if !isPortOpen("127.0.0.1", 9042) {
		t.Skip("skipping: Cassandra port 9042 is not open on 127.0.0.1")
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

	// Always heal, even if the test fails partway -- leaving a real network
	// partition applied to shared dev infrastructure after a failed test run
	// would be a nasty surprise for whatever runs next.
	t.Cleanup(func() { healRegionalPartition(t) })

	cfg := consumer.DefaultCassandraStoreConfig()
	store, err := consumer.NewCassandraCanonicalStore(cfg)
	if err != nil {
		t.Fatalf("failed to connect to real Cassandra: %v", err)
	}
	defer store.Close()

	uniqueID := uuid.New().String()[:8]
	siteID := fmt.Sprintf("SITE-PARTITION-%s", uniqueID)
	studyID := fmt.Sprintf("STUDY-PARTITION-%s", uniqueID)
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
		Subject:        "SUBJ-PARTITION-TEST",
		Payload:        `{"resourceType":"AdverseEvent"}`,
	}

	// 1. Sanity: a normal write/read succeeds before the partition.
	if err := store.SaveEvent(ctx, record); err != nil {
		t.Fatalf("pre-partition SaveEvent failed: %v", err)
	}
	if _, err := store.GetEvent(ctx, idKey); err != nil {
		t.Fatalf("pre-partition GetEvent failed: %v", err)
	}

	// 2. Apply the real partition and wait for gossip to actually detect it --
	// not a fixed sleep, an observed state change.
	applyRegionalPartition(t, usIPs, euIPs)
	waitForDCGossipStatus(t, true, 60*time.Second)

	// 3. THE CRITICAL ASSERTION: with dc-eu completely unreachable, a fresh
	// write and read against dc-us's own LOCAL_QUORUM must still succeed --
	// through the real application code path, not a raw cqlsh session.
	partitionKey := fmt.Sprintf("%s:2", siteID)
	partitionRecord := &consumer.CanonicalRecord{
		IdempotencyKey: partitionKey,
		SiteID:         siteID,
		StudyID:        studyID,
		LocalSeq:       2,
		EventTime:      eventTime.Add(time.Minute),
		RecordedTime:   eventTime.Add(time.Minute),
		IngestionTime:  eventTime.Add(time.Minute),
		Severity:       "severe",
		EventCode:      "10012345",
		Subject:        "SUBJ-PARTITION-TEST",
		Payload:        `{"resourceType":"AdverseEvent","duringPartition":true}`,
	}
	saveCtx, saveCancel := context.WithTimeout(ctx, 15*time.Second)
	if err := store.SaveEvent(saveCtx, partitionRecord); err != nil {
		saveCancel()
		t.Fatalf("CRITICAL: SaveEvent (LOCAL_QUORUM against dc-us) failed while dc-eu was unreachable: %v -- dc-us's own fault tolerance must not depend on dc-eu", err)
	}
	saveCancel()

	getCtx, getCancel := context.WithTimeout(ctx, 15*time.Second)
	got, err := store.GetEvent(getCtx, partitionKey)
	getCancel()
	if err != nil {
		t.Fatalf("CRITICAL: GetEvent (LOCAL_QUORUM against dc-us) failed while dc-eu was unreachable: %v", err)
	}
	if got.StudyID != studyID {
		t.Errorf("expected StudyID %s, got %s", studyID, got.StudyID)
	}

	// 4. Heal the partition and confirm dc-eu rejoins.
	healRegionalPartition(t)
	waitForDCGossipStatus(t, false, 60*time.Second)

	// 5. The write made entirely against dc-us during the outage must
	// eventually reach dc-eu too, once reachable again -- proving Cassandra's
	// hinted handoff genuinely catches the remote DC up rather than silently
	// losing replicas nothing ever explicitly repaired.
	deadline := time.Now().Add(60 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		// Client encryption is on as of Slice 15 (§2.4, ARCHITECTURE_PROPOSALS.md
		// "Slice 15: Auth & TLS") -- a plaintext cqlsh call against the CQL
		// port would fail outright, same as the docker-compose healthchecks.
		out, err := exec.Command("docker", "exec", "-i", "-e", "SSL_CERTFILE=/tls/ca-cert.pem", dcEUCassandraContainers[0], "cqlsh", "--ssl", "-e",
			fmt.Sprintf("CONSISTENCY ONE; SELECT idempotency_key FROM pharos.canonical_events WHERE idempotency_key = '%s';", partitionKey),
		).CombinedOutput()
		if err == nil && strings.Contains(string(out), partitionKey) {
			lastErr = nil
			break
		}
		lastErr = fmt.Errorf("not yet visible on dc-eu (output: %s)", string(out))
		time.Sleep(3 * time.Second)
	}
	if lastErr != nil {
		t.Fatalf("expected the during-partition write to eventually reach dc-eu via hinted handoff after healing: %v", lastErr)
	}
}
