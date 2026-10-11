package consumer

import (
	"strconv"
	"testing"
	"time"
)

// FuzzWatermarkTracker decodes arbitrary bytes into a sequence of events
// (partition, event time possibly far in the past/future/negative, wall-clock
// advance) and checks the tracker's core guarantees hold for every sequence:
// no panic, the emitted watermark never decreases, and a snapshot restored
// into a fresh tracker reports the same watermark.
//
//	go test -fuzz=FuzzWatermarkTracker -fuzztime=30s ./internal/consumer/
func FuzzWatermarkTracker(f *testing.F) {
	f.Add([]byte{0, 0, 0, 0, 10, 0, 5, 1, 0, 0, 0, 20, 0, 5})
	f.Add([]byte{0, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 1, 0x80, 0, 0, 0, 0, 0xff})
	f.Add([]byte{2, 0xff, 0xff, 0xff, 0x00, 0, 1, 2, 0xff, 0xff, 0xff, 0x00, 0, 1})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		base := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
		tracker := NewWatermarkTracker(15*time.Minute, 10*time.Minute)
		now := base
		var last time.Time

		const opSize = 7 // partition(1) | event offset seconds int32 (4) | wall-clock advance seconds uint16 (2)
		for i := 0; i+opSize <= len(data); i += opSize {
			partition := int(data[i] % 8)
			offset := int32(uint32(data[i+1])<<24 | uint32(data[i+2])<<16 | uint32(data[i+3])<<8 | uint32(data[i+4]))
			advance := time.Duration(uint16(data[i+5])<<8|uint16(data[i+6])) * time.Second
			now = now.Add(advance)

			eventTime := base.Add(time.Duration(offset) * time.Second)
			_, wm, _ := tracker.ProcessEvent(partition, "SITE-F:"+strconv.Itoa(i), eventTime, now)
			if wm.Before(last) {
				t.Fatalf("watermark regressed: %s -> %s after op %d", last, wm, i/opSize)
			}
			if cur := tracker.CurrentWatermark(now); cur.Before(last) {
				t.Fatalf("CurrentWatermark regressed: %s -> %s", last, cur)
			}
			last = wm
		}

		restored := NewWatermarkTracker(15*time.Minute, 10*time.Minute)
		restored.Restore(tracker.Snapshot())
		if got, want := restored.CurrentWatermark(now), tracker.CurrentWatermark(now); !got.Equal(want) {
			t.Fatalf("restored tracker disagrees: %s vs %s", got, want)
		}
	})
}
