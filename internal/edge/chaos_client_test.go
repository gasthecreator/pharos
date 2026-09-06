package edge

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

type stubHTTPClient struct {
	calls int
}

func (s *stubHTTPClient) Do(req *http.Request) (*http.Response, error) {
	s.calls++
	rec := httptest.NewRecorder()
	rec.WriteHeader(http.StatusOK)
	return rec.Result(), nil
}

func TestChaosClient_DefaultsToPassThrough(t *testing.T) {
	stub := &stubHTTPClient{}
	client := NewChaosClient(stub)

	req := httptest.NewRequest(http.MethodPost, "http://example.invalid/x", nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("expected pass-through by default, got error: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 from the wrapped client, got %d", resp.StatusCode)
	}
	if stub.calls != 1 {
		t.Errorf("expected the real client to be called once, got %d", stub.calls)
	}
	if client.IsPartitioned() {
		t.Errorf("expected IsPartitioned() to default to false")
	}
}

func TestChaosClient_PartitionedBlocksRequests(t *testing.T) {
	stub := &stubHTTPClient{}
	client := NewChaosClient(stub)
	client.SetPartitioned(true)

	req := httptest.NewRequest(http.MethodPost, "http://example.invalid/x", nil)
	_, err := client.Do(req)
	if err == nil {
		t.Fatalf("expected an error while partitioned")
	}
	if stub.calls != 0 {
		t.Errorf("expected the real client to never be called while partitioned, got %d calls", stub.calls)
	}
	if !client.IsPartitioned() {
		t.Errorf("expected IsPartitioned() to report true")
	}
}

func TestChaosClient_HealRestoresPassThrough(t *testing.T) {
	stub := &stubHTTPClient{}
	client := NewChaosClient(stub)
	client.SetPartitioned(true)
	client.SetPartitioned(false)

	req := httptest.NewRequest(http.MethodPost, "http://example.invalid/x", nil)
	if _, err := client.Do(req); err != nil {
		t.Fatalf("expected pass-through after healing, got error: %v", err)
	}
	if stub.calls != 1 {
		t.Errorf("expected the real client to be called once after healing, got %d", stub.calls)
	}
}
