package wire

import (
	"encoding/json"
	"testing"
)

// FuzzBatchRequestJSON feeds arbitrary bytes to the edge-to-ingestion
// envelope decoder. A decodable envelope must survive an encode/decode round
// trip with the same event count, and nothing may panic.
//
//	go test -fuzz=FuzzBatchRequestJSON -fuzztime=30s ./internal/wire/
func FuzzBatchRequestJSON(f *testing.F) {
	f.Add([]byte(`{"site_id":"SITE-1","events":[{"a":1},{"b":[1,2,3]}]}`))
	f.Add([]byte(`{"events":[]}`))
	f.Add([]byte(`{"events":null}`))
	f.Add([]byte(`[]`))
	f.Add([]byte(`{"site_id":"\u0000日本","events":[null,true,1e999,"x"]}`))
	f.Add([]byte(`{"events":[{"deep":` + string(deepJSON(2000)) + `}]}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		var req BatchRequest
		if json.Unmarshal(data, &req) != nil {
			return
		}
		out, err := json.Marshal(req)
		if err != nil {
			t.Fatalf("a decoded envelope cannot be re-encoded: %v", err)
		}
		var back BatchRequest
		if err := json.Unmarshal(out, &back); err != nil {
			t.Fatalf("re-encoded envelope does not decode: %v", err)
		}
		if len(back.Events) != len(req.Events) || back.SiteID != req.SiteID {
			t.Fatalf("round trip changed the envelope: %d events -> %d", len(req.Events), len(back.Events))
		}

		var resp BatchResponse
		_ = json.Unmarshal(data, &resp) // the response decoder shares the same untrusted-input risk on the edge
	})
}

func deepJSON(n int) []byte {
	b := make([]byte, 0, 2*n+1)
	for i := 0; i < n; i++ {
		b = append(b, '[')
	}
	b = append(b, '0')
	for i := 0; i < n; i++ {
		b = append(b, ']')
	}
	return b
}
