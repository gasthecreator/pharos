package model

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var (
	ErrInvalidIdempotencyKey = errors.New("invalid idempotency key format; expected <site_id>:<local_seq>")
	ErrEmptySiteID           = errors.New("site_id cannot be empty")
	ErrZeroSequenceNumber    = errors.New("local_seq must be greater than zero")
)

// IdempotencyKey represents the client-assigned idempotency identifier
// established at the moment of adverse event capture at a clinical trial site (§2.2).
// Format: "<site_id>:<local_sequence_number>"
type IdempotencyKey struct {
	SiteID   string `json:"site_id"`
	LocalSeq uint64 `json:"local_seq"`
}

// NewIdempotencyKey constructs and validates an IdempotencyKey.
func NewIdempotencyKey(siteID string, localSeq uint64) (IdempotencyKey, error) {
	siteID = strings.TrimSpace(siteID)
	if siteID == "" {
		return IdempotencyKey{}, ErrEmptySiteID
	}
	if localSeq == 0 {
		return IdempotencyKey{}, ErrZeroSequenceNumber
	}
	return IdempotencyKey{
		SiteID:   siteID,
		LocalSeq: localSeq,
	}, nil
}

// String returns the canonical wire format: "<site_id>:<local_seq>".
func (k IdempotencyKey) String() string {
	return fmt.Sprintf("%s:%d", k.SiteID, k.LocalSeq)
}

// ParseIdempotencyKey parses and validates a canonical wire string into an IdempotencyKey.
func ParseIdempotencyKey(raw string) (IdempotencyKey, error) {
	raw = strings.TrimSpace(raw)
	parts := strings.Split(raw, ":")
	if len(parts) < 2 {
		return IdempotencyKey{}, ErrInvalidIdempotencyKey
	}

	// Handle site IDs that might have hyphens or underscores;
	// the last token is always the local sequence number.
	seqStr := parts[len(parts)-1]
	siteID := strings.Join(parts[:len(parts)-1], ":")

	if strings.TrimSpace(siteID) == "" {
		return IdempotencyKey{}, ErrEmptySiteID
	}

	seq, err := strconv.ParseUint(seqStr, 10, 64)
	if err != nil || seq == 0 {
		return IdempotencyKey{}, ErrInvalidIdempotencyKey
	}

	return IdempotencyKey{
		SiteID:   siteID,
		LocalSeq: seq,
	}, nil
}
