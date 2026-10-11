package model

import (
	"encoding/json"
	"testing"
)

func benchEvent(b *testing.B) AdverseEvent {
	b.Helper()
	var ev AdverseEvent
	if err := json.Unmarshal([]byte(seedEvent), &ev); err != nil {
		b.Fatal(err)
	}
	if err := ev.Validate(); err != nil {
		b.Fatalf("benchmark event must be valid: %v", err)
	}
	return ev
}

func BenchmarkValidate(b *testing.B) {
	ev := benchEvent(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := ev.Validate(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDecodeAndValidate is the per-event cost on Central Ingestion's
// intake path (JSON decode plus profile validation).
func BenchmarkDecodeAndValidate(b *testing.B) {
	data := []byte(seedEvent)
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var ev AdverseEvent
		if err := json.Unmarshal(data, &ev); err != nil {
			b.Fatal(err)
		}
		if err := ev.Validate(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNewIdempotencyKey(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := NewIdempotencyKey("SITE-NG-01", uint64(i)+1); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkIdempotencyKeyStringAndParse(b *testing.B) {
	k, _ := NewIdempotencyKey("SITE-NG-01", 1<<40|12345)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := ParseIdempotencyKey(k.String()); err != nil {
			b.Fatal(err)
		}
	}
}
