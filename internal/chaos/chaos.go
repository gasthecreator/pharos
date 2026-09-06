// Package chaos implements real, infrastructure-mutating actions for the
// Slice 23 Chaos Control Panel (§2.4, PLAN.md) -- reusing the exact
// mechanisms internal/faultinjection's real-infra tests already proved
// correct (regional_partition_test.go's tc-netem-via-docker-exec pattern
// for a dc-us/dc-eu partition; plain `docker stop`/`restart` for a node
// kill/restart), adapted into reusable, non-test functions a running
// pharos-dashboard process can call. Every function here mutates real
// running Docker containers -- callers (cmd/pharos-dashboard) MUST gate
// access behind an explicit operator opt-in (--enable-chaos), never expose
// these unconditionally just because the dashboard happens to be running.
package chaos

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
)

// CassandraContainers lists every Cassandra container name in this
// project's docker-compose.yml topology (§2.4, Slice 14: multi-region).
var CassandraContainers = []string{"pharos-cassandra-1", "pharos-cassandra-2", "pharos-cassandra-3", "pharos-cassandra-4"}

// DCUSContainers and DCEUContainers mirror
// internal/faultinjection/regional_partition_test.go's own
// dcUSCassandraContainers/dcEUCassandraContainers exactly -- this project's
// 2-DC NetworkTopologyStrategy topology (dc-us: RF=3, dc-eu: RF=1).
var (
	DCUSContainers = []string{"pharos-cassandra-1", "pharos-cassandra-2", "pharos-cassandra-3"}
	DCEUContainers = []string{"pharos-cassandra-4"}
)

func runDocker(ctx context.Context, args ...string) (string, error) {
	out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("docker %s failed: %w (output: %s)", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return string(out), nil
}

// StopContainer runs `docker stop <name>` -- the exact command shape
// internal/faultinjection's own tests already use `docker exec`/`docker
// inspect` against these same container names for, just an unused verb.
func StopContainer(ctx context.Context, name string) error {
	_, err := runDocker(ctx, "stop", name)
	return err
}

// StartContainer runs `docker start <name>`.
func StartContainer(ctx context.Context, name string) error {
	_, err := runDocker(ctx, "start", name)
	return err
}

// RestartContainer runs `docker restart <name>`.
func RestartContainer(ctx context.Context, name string) error {
	_, err := runDocker(ctx, "restart", name)
	return err
}

// ContainerIsRunning mirrors
// internal/faultinjection/regional_partition_test.go's own containerIP-style
// `docker inspect` usage, checked via {{.State.Running}} instead of
// {{.NetworkSettings...}} -- used to confirm a stop/start/restart actually
// took effect before reporting success to the chaos panel.
func ContainerIsRunning(ctx context.Context, name string) (bool, error) {
	out, err := runDocker(ctx, "inspect", "-f", "{{.State.Running}}", name)
	if err != nil {
		return false, err
	}
	return strings.TrimSpace(out) == "true", nil
}

// containerIP mirrors internal/faultinjection/regional_partition_test.go's
// own containerIP helper: the container's IP on the docker-compose bridge
// network, needed to scope a tc filter to only the peer DC's traffic.
func containerIP(ctx context.Context, name string) (string, error) {
	out, err := runDocker(ctx, "exec", name, "hostname", "-i")
	if err != nil {
		return "", err
	}
	ip := strings.TrimSpace(out)
	if ip == "" {
		return "", fmt.Errorf("container %s returned an empty IP", name)
	}
	return ip, nil
}

// HealRegionPartition removes any tc qdisc previously applied by
// PartitionRegions from every container in containers -- idempotent and
// safe to call even if no partition is currently active (`tc qdisc del`
// against a container with no custom qdisc is a no-op success in practice;
// errors from containers that were never partitioned are tolerated, not
// fatal, since the caller may be healing a partial/partial-failure state).
func HealRegionPartition(ctx context.Context, containers []string) error {
	var errs []string
	for _, c := range containers {
		if _, err := runDocker(ctx, "exec", c, "tc", "qdisc", "del", "dev", "eth0", "root"); err != nil {
			// Not fatal: "no qdisc to delete" is the expected, harmless
			// outcome for a container that was never partitioned.
			errs = append(errs, err.Error())
		}
	}
	if len(errs) == len(containers) && len(containers) > 0 {
		return fmt.Errorf("failed to heal any of %d containers: %s", len(containers), strings.Join(errs, "; "))
	}
	return nil
}

// PartitionRegions applies a real, 100%-packet-loss `tc netem` partition
// between usContainers and euContainers -- the exact mechanism
// internal/faultinjection/regional_partition_test.go's applyRegionalPartition
// already proved correct against this project's real Cassandra cluster,
// adapted here to return an error instead of calling t.Fatalf so it's
// usable from a running process, not just a test binary. Always heals both
// container sets first (best-effort, errors ignored) before applying, so a
// stale qdisc left over from a previous run (e.g. the dashboard crashed
// mid-partition) can't make `tc qdisc add ... root` fail with "already
// exists."
func PartitionRegions(ctx context.Context, usContainers, euContainers []string) error {
	all := append(append([]string{}, usContainers...), euContainers...)
	_ = HealRegionPartition(ctx, all)

	usIPs, err := resolveIPs(ctx, usContainers)
	if err != nil {
		return fmt.Errorf("failed to resolve dc-us container IPs: %w", err)
	}
	euIPs, err := resolveIPs(ctx, euContainers)
	if err != nil {
		return fmt.Errorf("failed to resolve dc-eu container IPs: %w", err)
	}

	apply := func(container string, peerIPs []string) error {
		cmd := "tc qdisc add dev eth0 root handle 1: prio priomap 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 && " +
			"tc qdisc add dev eth0 parent 1:3 handle 30: netem loss 100%"
		for _, ip := range peerIPs {
			cmd += fmt.Sprintf(" && tc filter add dev eth0 protocol ip parent 1:0 prio 3 u32 match ip dst %s/32 flowid 1:3", ip)
		}
		_, err := runDocker(ctx, "exec", container, "sh", "-c", cmd)
		return err
	}

	for _, c := range usContainers {
		if err := apply(c, euIPs); err != nil {
			return fmt.Errorf("failed to partition %s from dc-eu: %w", c, err)
		}
	}
	for _, c := range euContainers {
		if err := apply(c, usIPs); err != nil {
			return fmt.Errorf("failed to partition %s from dc-us: %w", c, err)
		}
	}
	return nil
}

func resolveIPs(ctx context.Context, containers []string) ([]string, error) {
	ips := make([]string, 0, len(containers))
	for _, c := range containers {
		ip, err := containerIP(ctx, c)
		if err != nil {
			return nil, fmt.Errorf("container %s: %w", c, err)
		}
		ips = append(ips, ip)
	}
	return ips, nil
}
