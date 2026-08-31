package model

import (
	"testing"
)

func TestNewIdempotencyKey(t *testing.T) {
	tests := []struct {
		name     string
		siteID   string
		seq      uint64
		wantErr  bool
		expected string
	}{
		{
			name:     "valid key",
			siteID:   "SITE-NG-01",
			seq:      42,
			wantErr:  false,
			expected: "SITE-NG-01:42",
		},
		{
			name:     "valid key with whitespace trimmed",
			siteID:   "  SITE-US-102  ",
			seq:      1,
			wantErr:  false,
			expected: "SITE-US-102:1",
		},
		{
			name:    "empty site id",
			siteID:  "",
			seq:     10,
			wantErr: true,
		},
		{
			name:    "whitespace-only site id",
			siteID:  "   ",
			seq:     10,
			wantErr: true,
		},
		{
			name:    "zero sequence number",
			siteID:  "SITE-01",
			seq:     0,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key, err := NewIdempotencyKey(tt.siteID, tt.seq)
			if (err != nil) != tt.wantErr {
				t.Fatalf("NewIdempotencyKey() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && key.String() != tt.expected {
				t.Errorf("key.String() = %s, want %s", key.String(), tt.expected)
			}
		})
	}
}

func TestParseIdempotencyKey(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		wantSiteID string
		wantSeq    uint64
		wantErr    bool
	}{
		{
			name:       "canonical string",
			input:      "SITE-NG-01:100",
			wantSiteID: "SITE-NG-01",
			wantSeq:    100,
			wantErr:    false,
		},
		{
			name:       "site id containing colons",
			input:      "ORG:SITE-42:500",
			wantSiteID: "ORG:SITE-42",
			wantSeq:    500,
			wantErr:    false,
		},
		{
			name:    "missing colon",
			input:   "SITE-NG-01-100",
			wantErr: true,
		},
		{
			name:    "non-numeric sequence",
			input:   "SITE-01:abc",
			wantErr: true,
		},
		{
			name:    "zero sequence",
			input:   "SITE-01:0",
			wantErr: true,
		},
		{
			name:    "negative sequence",
			input:   "SITE-01:-5",
			wantErr: true,
		},
		{
			name:    "empty site prefix",
			input:   ":100",
			wantErr: true,
		},
		{
			name:    "empty string",
			input:   "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key, err := ParseIdempotencyKey(tt.input)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseIdempotencyKey(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			}
			if !tt.wantErr {
				if key.SiteID != tt.wantSiteID {
					t.Errorf("key.SiteID = %s, want %s", key.SiteID, tt.wantSiteID)
				}
				if key.LocalSeq != tt.wantSeq {
					t.Errorf("key.LocalSeq = %d, want %d", key.LocalSeq, tt.wantSeq)
				}
			}
		})
	}
}
