package chaos

import (
	"context"
	"net"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/gasthecreator/pharos/internal/tlsutil"
	kafkaGo "github.com/segmentio/kafka-go"
)

func isPortOpen(host string, port string) bool {
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(host, port), 1*time.Second)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// waitForCassandraPort polls until Cassandra's client port is reachable
// again after a restart -- a necessary but not sufficient condition; see
// waitForClusterGossipStable for the real readiness check this package's
// tests actually rely on before returning.
func waitForCassandraPort(t *testing.T, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if isPortOpen("127.0.0.1", "9042") {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("Cassandra port 9042 did not become reachable again within %s", timeout)
}

// waitForClusterGossipStable polls `nodetool status` (run inside a
// container that's expected to still be up throughout) until every node in
// both datacenters reports UN (Up/Normal) and none report DN (Down).
// Necessary, but empirically NOT sufficient on its own (see
// waitForClusterReady): a node can pass this check while Cassandra's
// internal Paxos/LWT coordination -- which this project's outbox tables
// depend on for every single write -- is still catching up after a
// restart, causing real query timeouts elsewhere for a while even though
// gossip already looks healthy.
func waitForClusterGossipStable(t *testing.T, viaContainer string, expectedNodes int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		out, err := exec.Command("docker", "exec", viaContainer, "nodetool", "status").CombinedOutput()
		if err == nil {
			text := string(out)
			if !strings.Contains(text, "DN ") && strings.Count(text, "UN ") >= expectedNodes {
				return
			}
		}
		time.Sleep(1 * time.Second)
	}
	t.Fatalf("cluster gossip did not stabilize (all %d nodes UN, none DN) within %s", expectedNodes, timeout)
}

// waitForClusterReady combines waitForClusterGossipStable with a stronger
// signal from `nodetool describecluster` -- schema agreement (exactly one
// "Schema versions:" hash covering every live node, not split across
// multiple hashes) and "Unreachable: 0" -- plus a short fixed settle buffer
// afterward. This exists because a real, previously-observed flake in this
// exact test suite proved gossip-UN status alone isn't a sufficient
// readiness signal: internal/consumer's real end-to-end test hit genuine
// Cassandra write timeouts ("insert events_by_study failed: context
// deadline exceeded") immediately after this package's own disruptive
// tests reported the cluster gossip-stable, because LWT-heavy write
// latency (exactly what this project's outbox tables depend on) can stay
// degraded for a few more seconds after gossip itself has reconverged.
func waitForClusterReady(t *testing.T, viaContainer string, timeout time.Duration) {
	t.Helper()
	waitForClusterGossipStable(t, viaContainer, 4, timeout)

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		out, err := exec.Command("docker", "exec", viaContainer, "nodetool", "describecluster").CombinedOutput()
		if err == nil {
			text := string(out)
			schemaVersionLines := 0
			inSchemaSection := false
			for _, line := range strings.Split(text, "\n") {
				trimmed := strings.TrimSpace(line)
				if strings.HasPrefix(trimmed, "Schema versions:") {
					inSchemaSection = true
					continue
				}
				if inSchemaSection {
					if trimmed == "" || strings.HasPrefix(trimmed, "Stats for all nodes") {
						break
					}
					if strings.Contains(trimmed, ":") {
						schemaVersionLines++
					}
				}
			}
			if schemaVersionLines == 1 && strings.Contains(text, "Unreachable: 0") {
				break
			}
		}
		time.Sleep(1 * time.Second)
	}
	if time.Now().After(deadline) {
		t.Fatalf("cluster schema did not reach agreement within %s", timeout)
	}

	// Cassandra schema agreement doesn't say anything about Kafka -- and
	// empirically, restart/repair activity on this project's own
	// resource-constrained Docker VM (the whole multi-node topology shares
	// ~6.3GB) measurably spills over onto Kafka's own responsiveness too, a
	// second, real, previously-observed flake this exact check exists to
	// close: internal/consumer's real end-to-end test hitting "failed to
	// commit kafka offset: context deadline exceeded" immediately after
	// this package's own tests, even though nothing here touches Kafka
	// containers directly. A fixed sleep alone was tried first and proved
	// insufficient (still flaked at 30s); this checks Kafka's actual
	// responsiveness directly instead of guessing how long is enough.
	waitForKafkaReady(t, 90*time.Second)
}

// waitForKafkaReady polls a real Kafka metadata request (a TLS-authenticated
// kafka-go dial + ReadPartitions, exactly the client path
// internal/consumer's own real end-to-end test uses) against the topic
// this project's pipeline actually publishes to, until it succeeds -- proof
// the broker is genuinely answering requests again, not just that its port
// accepts a TCP connection. Deliberately NOT `docker exec ... kafka-topics.sh`:
// that CLI spawns its own separate JVM inside the broker's own container,
// which was empirically observed to fail outright with
// "OutOfMemoryError: Java heap space" on this same resource-constrained
// host during this exact investigation -- a real illustration of how tight
// this project's ~6.3GB shared budget is, and why a pure-Go client dial
// (no extra JVM) is the more robust check here.
func waitForKafkaReady(t *testing.T, timeout time.Duration) {
	t.Helper()
	tlsCfg, err := (tlsutil.ClientConfig{CACertPath: tlsutil.DefaultCACertPath(), ServerName: "localhost"}).StdTLSConfig()
	if err != nil {
		t.Fatalf("failed to build TLS config for Kafka readiness probe: %v", err)
	}
	dialer := &kafkaGo.Dialer{Timeout: 5 * time.Second, DualStack: true, TLS: tlsCfg}

	var lastErr error
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		lastErr = func() error {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			conn, dialErr := dialer.DialContext(ctx, "tcp", "127.0.0.1:9092")
			if dialErr != nil {
				return dialErr
			}
			defer conn.Close()
			_, readErr := conn.ReadPartitions("pharos.events.adverse")
			return readErr
		}()
		if lastErr == nil {
			return
		}
		time.Sleep(1 * time.Second)
	}
	t.Fatalf("Kafka did not become ready (ReadPartitions succeeding for pharos.events.adverse) within %s: %v", timeout, lastErr)
}

// TestStopStartContainer_RealDocker proves the §2.4, Slice 23 node
// kill/restart chaos action against a real running Cassandra container --
// stops pharos-cassandra-2 (a dc-us replica, not dc-eu's single node), then
// restarts it and waits for the cluster's client port to come back, so this
// test leaves the shared cluster in the same healthy state it found it in.
func TestStopStartContainer_RealDocker(t *testing.T) {
	if !isPortOpen("127.0.0.1", "9042") {
		t.Skip("skipping: Cassandra port 9042 is not open on 127.0.0.1 (no local Docker cluster)")
	}
	ctx := context.Background()
	const container = "pharos-cassandra-2"

	running, err := ContainerIsRunning(ctx, container)
	if err != nil {
		t.Fatalf("ContainerIsRunning failed: %v", err)
	}
	if !running {
		t.Skipf("skipping: %s is not currently running", container)
	}

	if err := StopContainer(ctx, container); err != nil {
		t.Fatalf("StopContainer failed: %v", err)
	}
	running, err = ContainerIsRunning(ctx, container)
	if err != nil {
		t.Fatalf("ContainerIsRunning after stop failed: %v", err)
	}
	if running {
		t.Fatalf("CRITICAL: %s still reports running after StopContainer", container)
	}

	if err := StartContainer(ctx, container); err != nil {
		t.Fatalf("StartContainer failed: %v", err)
	}
	running, err = ContainerIsRunning(ctx, container)
	if err != nil {
		t.Fatalf("ContainerIsRunning after start failed: %v", err)
	}
	if !running {
		t.Fatalf("CRITICAL: %s does not report running after StartContainer", container)
	}

	// Give Cassandra time to actually finish booting (JVM startup, not just
	// the container process existing) before checking gossip/schema.
	waitForCassandraPort(t, 90*time.Second)
	// Then wait for the cluster to actually be ready -- port-open and even
	// gossip-UN alone aren't enough (see waitForClusterReady's docs) --
	// before this test, and whatever real-infra test runs after it, tries
	// to use the cluster.
	waitForClusterReady(t, "pharos-cassandra-1", 90*time.Second)
}

// TestPartitionAndHealRegions_RealDocker proves the §2.4, Slice 23
// dc-us/dc-eu partition chaos action against the real cluster, reusing the
// exact tc-netem-via-docker-exec mechanism
// internal/faultinjection/regional_partition_test.go already proved
// correct -- applies a real partition, confirms it's actually blocking
// traffic isn't separately re-verified here (that's already
// internal/faultinjection's job), then heals it immediately so this test
// leaves the cluster in its normal, connected state.
func TestPartitionAndHealRegions_RealDocker(t *testing.T) {
	if !isPortOpen("127.0.0.1", "9042") {
		t.Skip("skipping: Cassandra port 9042 is not open on 127.0.0.1 (no local Docker cluster)")
	}
	ctx := context.Background()

	for _, c := range append(append([]string{}, DCUSContainers...), DCEUContainers...) {
		running, err := ContainerIsRunning(ctx, c)
		if err != nil || !running {
			t.Skipf("skipping: %s is not running (err=%v)", c, err)
		}
	}

	if err := PartitionRegions(ctx, DCUSContainers, DCEUContainers); err != nil {
		t.Fatalf("PartitionRegions failed: %v", err)
	}

	all := append(append([]string{}, DCUSContainers...), DCEUContainers...)
	if err := HealRegionPartition(ctx, all); err != nil {
		t.Fatalf("HealRegionPartition failed: %v", err)
	}

	// Applying again immediately after healing must succeed cleanly (no
	// "qdisc already exists" from a leftover partial state) -- proves
	// HealRegionPartition genuinely removed what PartitionRegions added.
	if err := PartitionRegions(ctx, DCUSContainers, DCEUContainers); err != nil {
		t.Fatalf("re-PartitionRegions after healing failed (heal may not have fully cleaned up): %v", err)
	}
	if err := HealRegionPartition(ctx, all); err != nil {
		t.Fatalf("final HealRegionPartition failed: %v", err)
	}

	// A tc-level heal is immediate (it's just a local qdisc removal, no
	// JVM/gossip restart involved), but the cluster still needs a moment to
	// reconfirm readiness -- wait for that before returning, for the same
	// reason TestStopStartContainer_RealDocker does: leave the cluster
	// genuinely settled for whatever real-infra test runs next in this
	// same `go test ./...` invocation.
	waitForClusterReady(t, "pharos-cassandra-1", 60*time.Second)
}
