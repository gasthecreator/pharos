// Package simulation implements deterministic simulation testing (§2.4,
// PLAN.md Slice 22 Stage B): a fake, in-memory Kafka broker plus the
// Producer/MessageReader adapters needed to run the REAL
// internal/ingestion.Handler and internal/consumer.Engine against it,
// instead of real Kafka -- so a single integer seed can deterministically
// reproduce one exact interleaving of message delivery delay and consumer
// fetch/commit timing, and thousands of seeds can run in a fraction of a
// second. This deliberately does not reimplement any pipeline logic: the
// harness (harness.go) drives the actual production Handler/Engine/Sweeper
// code, only their I/O boundaries are swapped for simulated ones.
package simulation

import (
	"context"
	"fmt"

	"github.com/gasthecreator/pharos/internal/kafka"
	kafkaGo "github.com/segmentio/kafka-go"
)

// pendingMessage is a published message not yet visible to readers --
// held back to simulate real network/broker delivery latency. Kafka's own
// per-partition ordering guarantee is preserved by construction: each
// partition has its own FIFO pending queue (see Broker.pending), so a
// later-published message can never become visible before an
// earlier-published one in the *same* partition, no matter what delay each
// one draws -- only cross-partition interleaving is allowed to vary, which
// is exactly what real Kafka allows too.
type pendingMessage struct {
	msg          kafkaGo.Message
	visibleAfter int // Broker.tick value at/after which this becomes visible
}

// Broker is a deterministic, single-goroutine, in-memory stand-in for a
// Kafka topic's partitions. All randomness (partition assignment via
// caller-supplied hashing, delivery delay) is drawn from one
// caller-supplied *rand.Rand (see harness.go), so an identical seed
// reproduces an identical sequence of broker decisions bit-for-bit. NOT
// safe for concurrent use -- the whole simulation runs from a single
// driving goroutine; "concurrent" scenarios are modeled as *interleaved*
// harness-level operations, not real parallel goroutines, since real
// goroutine scheduling order is not reproducible the way a PRNG sequence
// is.
type Broker struct {
	numPartitions int
	partitions    [][]kafkaGo.Message // committed, visible log per partition, in publish order
	pending       [][]pendingMessage  // one FIFO queue per partition
	offsets       []int64
	tick          int
	nextDelay     func() int // returns this message's visibility delay in ticks, drawn from the harness's shared rng
}

// NewBroker constructs a Broker with numPartitions partitions. nextDelay is
// called once per published message to determine how many ticks must pass
// before it becomes visible to readers -- callers pass a closure over their
// own seeded *rand.Rand so every random decision in a simulation run traces
// back to that one seed.
func NewBroker(numPartitions int, nextDelay func() int) *Broker {
	return &Broker{
		numPartitions: numPartitions,
		partitions:    make([][]kafkaGo.Message, numPartitions),
		pending:       make([][]pendingMessage, numPartitions),
		offsets:       make([]int64, numPartitions),
		nextDelay:     nextDelay,
	}
}

// partitionFor deterministically assigns a partition from key, mirroring
// real Kafka's own key-based partitioning (same key always routes to the
// same partition) using a simple, fast hash -- not cryptographic, doesn't
// need to be.
func (b *Broker) partitionFor(key []byte) int {
	if b.numPartitions <= 1 {
		return 0
	}
	var h uint32 = 2166136261
	for _, c := range key {
		h ^= uint32(c)
		h *= 16777619
	}
	return int(h % uint32(b.numPartitions))
}

// Publish implements kafka.Producer, appending to the assigned partition's
// pending queue with a randomized (but per-partition FIFO-respecting)
// visibility delay.
func (b *Broker) Publish(ctx context.Context, topic string, key []byte, value []byte, headers map[string]string) (kafka.KafkaMetadata, error) {
	partition := b.partitionFor(key)
	offset := b.offsets[partition]
	b.offsets[partition]++

	hdrs := make([]kafkaGo.Header, 0, len(headers))
	for k, v := range headers {
		hdrs = append(hdrs, kafkaGo.Header{Key: k, Value: []byte(v)})
	}
	msg := kafkaGo.Message{
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
		Key:       key,
		Value:     value,
		Headers:   hdrs,
	}
	delay := 0
	if b.nextDelay != nil {
		delay = b.nextDelay()
	}
	b.pending[partition] = append(b.pending[partition], pendingMessage{msg: msg, visibleAfter: b.tick + delay})

	return kafka.KafkaMetadata{Topic: topic, Partition: partition, Offset: offset}, nil
}

// Close implements kafka.Producer; the broker itself has nothing to
// release.
func (b *Broker) Close() error { return nil }

// Advance moves virtual time forward one tick, flushing the front of every
// partition's pending queue whose delay has elapsed into that partition's
// visible log. Each partition's own publish order is always preserved.
func (b *Broker) Advance() {
	b.tick++
	for p := range b.pending {
		for len(b.pending[p]) > 0 && b.pending[p][0].visibleAfter <= b.tick {
			b.partitions[p] = append(b.partitions[p], b.pending[p][0].msg)
			b.pending[p] = b.pending[p][1:]
		}
	}
}

// VisibleCount returns how many messages are currently visible (committed
// to a partition's log, whether or not a reader has fetched them yet)
// across every partition -- used by the harness to know when it's safe to
// conclude a simulation run has nothing left to deliver.
func (b *Broker) VisibleCount() int {
	total := 0
	for _, p := range b.partitions {
		total += len(p)
	}
	return total
}

// PendingCount returns how many published messages are still awaiting
// their simulated delivery delay.
func (b *Broker) PendingCount() int {
	total := 0
	for _, p := range b.pending {
		total += len(p)
	}
	return total
}

// NewReader constructs a SimulatedReader consuming every partition of this
// broker in round-robin order, standing in for one consumer instance
// reading the whole topic (§2.4, Slice 22 -- multi-instance rebalancing
// determinism is already covered by real-infra tests, Slice 19; this
// harness models exactly one consumer instance to keep the simulation's
// own state space tractable).
func (b *Broker) NewReader() *SimulatedReader {
	return &SimulatedReader{
		broker:     b,
		readCursor: make([]int64, b.numPartitions),
	}
}

// SimulatedReader implements consumer.MessageReader against a Broker.
// FetchMessage never advances past an uncommitted offset -- exactly
// mirroring real kafka-go Reader semantics with manual commits (this
// project's own CommitInterval: 0 configuration, engine.go) -- so a message
// fetched but never committed (a simulated crash before commit) is
// naturally refetched on the next FetchMessage call, without any special
// "redelivery" mechanism: this emerges for free from correctly modeling
// "the read cursor only moves on commit."
type SimulatedReader struct {
	broker     *Broker
	readCursor []int64 // next un-fetched offset per partition
	nextPartRR int     // round-robin starting point across FetchMessage calls
}

// FetchMessage returns the next available message across every partition
// (round-robin, starting from wherever the last call left off, so no
// partition is starved), or ErrNoMessage if nothing is currently visible.
func (r *SimulatedReader) FetchMessage(ctx context.Context) (kafkaGo.Message, error) {
	for i := 0; i < r.broker.numPartitions; i++ {
		p := (r.nextPartRR + i) % r.broker.numPartitions
		if r.readCursor[p] < int64(len(r.broker.partitions[p])) {
			msg := r.broker.partitions[p][r.readCursor[p]]
			r.nextPartRR = (p + 1) % r.broker.numPartitions
			return msg, nil
		}
	}
	return kafkaGo.Message{}, ErrNoMessage
}

// CommitMessages advances the read cursor past each given message's
// offset, on its partition.
func (r *SimulatedReader) CommitMessages(ctx context.Context, msgs ...kafkaGo.Message) error {
	for _, m := range msgs {
		if m.Offset+1 > r.readCursor[m.Partition] {
			r.readCursor[m.Partition] = m.Offset + 1
		}
	}
	return nil
}

// Close implements consumer.MessageReader; nothing to release.
func (r *SimulatedReader) Close() error { return nil }

// UnconsumedCount returns how many visible messages this reader has not
// yet committed, across every partition -- used by the harness to know
// when a drain is genuinely finished, since Broker.VisibleCount alone
// counts every message ever made visible, not just unconsumed ones (the
// broker never truncates a partition's log, matching real Kafka retaining
// committed messages rather than deleting them on consume).
func (r *SimulatedReader) UnconsumedCount() int {
	total := 0
	for p := range r.broker.partitions {
		total += len(r.broker.partitions[p]) - int(r.readCursor[p])
	}
	return total
}

// ErrNoMessage is returned by SimulatedReader.FetchMessage when nothing is
// currently visible on any partition -- the harness treats this as "wait
// for the next Advance," not a real error.
var ErrNoMessage = fmt.Errorf("simulation: no message currently available")
