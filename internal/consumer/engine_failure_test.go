package consumer

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	kafkaGo "github.com/segmentio/kafka-go"
)

// flakyReader is a MessageReader whose fetch and commit can be made to fail,
// so the engine's error paths are testable without Kafka.
type flakyReader struct {
	msgs      []kafkaGo.Message
	next      int
	fetchErr  error
	commitErr error
	committed []kafkaGo.Message
}

func (r *flakyReader) FetchMessage(ctx context.Context) (kafkaGo.Message, error) {
	if err := ctx.Err(); err != nil {
		return kafkaGo.Message{}, err
	}
	if r.fetchErr != nil {
		return kafkaGo.Message{}, r.fetchErr
	}
	if r.next >= len(r.msgs) {
		<-ctx.Done()
		return kafkaGo.Message{}, ctx.Err()
	}
	m := r.msgs[r.next]
	r.next++
	return m, nil
}

func (r *flakyReader) CommitMessages(_ context.Context, msgs ...kafkaGo.Message) error {
	if r.commitErr != nil {
		return r.commitErr
	}
	r.committed = append(r.committed, msgs...)
	return nil
}

func (r *flakyReader) Close() error { return nil }

// TestEngineStep_FailurePaths: for every way a single Step can fail, the
// offset must not be committed past an event that was not durably written,
// the error must be counted, and the failure must surface to the caller.
func TestEngineStep_FailurePaths(t *testing.T) {
	base := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	good := newTestKafkaMessage("SITE-01", 1, "STUDY-A", base, 100)
	poison := good
	poison.Value = []byte(`{"resourceType": "AdverseEvent", "date": 12345}`) // wrong type for date

	tests := []struct {
		name          string
		reader        func() *flakyReader
		saveErr       error
		wantErrSub    string
		wantCommitted int
		wantErrCount  uint64
		wantConsumed  uint64
		wantSaved     bool
	}{
		{
			name:       "kafka fetch error",
			reader:     func() *flakyReader { return &flakyReader{fetchErr: errors.New("broker unreachable")} },
			wantErrSub: "failed to fetch message", wantErrCount: 1,
		},
		{
			name:       "payload is not valid JSON for the event type",
			reader:     func() *flakyReader { return &flakyReader{msgs: []kafkaGo.Message{poison}} },
			wantErrSub: "failed to unmarshal", wantErrCount: 1,
		},
		{
			name:       "cassandra write fails: offset stays uncommitted",
			reader:     func() *flakyReader { return &flakyReader{msgs: []kafkaGo.Message{good}} },
			saveErr:    errors.New("write timeout"),
			wantErrSub: "offset uncommitted", wantErrCount: 1,
		},
		{
			name: "commit fails after a durable write: error surfaces, row is kept",
			reader: func() *flakyReader {
				return &flakyReader{msgs: []kafkaGo.Message{good}, commitErr: errors.New("coordinator not available")}
			},
			wantErrSub: "failed to commit kafka offset", wantErrCount: 1, wantSaved: true,
		},
		{
			name:          "healthy",
			reader:        func() *flakyReader { return &flakyReader{msgs: []kafkaGo.Message{good}} },
			wantCommitted: 1, wantConsumed: 1, wantSaved: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := NewMemoryCanonicalStore()
			if tt.saveErr != nil {
				store.SetSaveHook(func(*CanonicalRecord) error { return tt.saveErr })
			}
			reader := tt.reader()
			engine := NewEngine(reader, store, NewWatermarkTracker(10*time.Minute, 10*time.Minute), DefaultEngineConfig(nil))

			err := engine.Step(context.Background())
			switch {
			case tt.wantErrSub == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case tt.wantErrSub != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErrSub)):
				t.Fatalf("err = %v, want it to mention %q", err, tt.wantErrSub)
			}
			if got := len(reader.committed); got != tt.wantCommitted {
				t.Errorf("committed offsets = %d, want %d", got, tt.wantCommitted)
			}
			stats := engine.Stats()
			if stats.ErrorCount != tt.wantErrCount || stats.ConsumedCount != tt.wantConsumed {
				t.Errorf("stats errors=%d consumed=%d, want %d / %d", stats.ErrorCount, stats.ConsumedCount, tt.wantErrCount, tt.wantConsumed)
			}
			rec, _ := store.GetEvent(context.Background(), string(good.Headers[0].Value))
			if (rec != nil) != tt.wantSaved {
				t.Errorf("event persisted = %v, want %v", rec != nil, tt.wantSaved)
			}
		})
	}
}

// TestEngineStep_ContextCancellationIsNotAnError: shutting down mid-fetch is
// normal, so it must return the context error without inflating ErrorCount
// (which feeds the PharosConsumerErrors alert).
func TestEngineStep_ContextCancellationIsNotAnError(t *testing.T) {
	reader := &flakyReader{}
	engine := NewEngine(reader, NewMemoryCanonicalStore(), nil, DefaultEngineConfig(nil))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := engine.Step(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Step = %v, want context.Canceled", err)
	}
	if got := engine.Stats().ErrorCount; got != 0 {
		t.Fatalf("ErrorCount = %d after cancellation, want 0", got)
	}
}

// TestEngineRun_StopsPromptlyOnCancellationAndKeepsRetryingOnErrors: Run
// retries through transient errors but exits as soon as its context ends.
func TestEngineRun_StopsPromptlyOnCancellationAndKeepsRetryingOnErrors(t *testing.T) {
	reader := &flakyReader{fetchErr: errors.New("transient broker error")}
	engine := NewEngine(reader, NewMemoryCanonicalStore(), nil, DefaultEngineConfig(nil))

	ctx, cancel := context.WithTimeout(context.Background(), 350*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- engine.Run(ctx) }()

	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Run = %v, want context.DeadlineExceeded", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after its context expired")
	}
	if got := engine.Stats().ErrorCount; got < 2 {
		t.Fatalf("ErrorCount = %d, want at least 2 retries through the transient error", got)
	}
}
