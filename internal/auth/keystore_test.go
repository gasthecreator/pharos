package auth

import (
	"context"
	"testing"
)

func TestMemoryKeyStore_CreateAndVerify(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryKeyStore()

	plaintext, err := store.CreateKey(ctx, "SITE-001")
	if err != nil {
		t.Fatalf("CreateKey failed: %v", err)
	}
	if plaintext == "" {
		t.Fatalf("expected a non-empty plaintext key")
	}

	ok, err := store.VerifyKey(ctx, "SITE-001", plaintext)
	if err != nil {
		t.Fatalf("VerifyKey failed: %v", err)
	}
	if !ok {
		t.Fatalf("expected the freshly created key to verify successfully")
	}
}

func TestMemoryKeyStore_WrongKeyRejected(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryKeyStore()

	if _, err := store.CreateKey(ctx, "SITE-001"); err != nil {
		t.Fatalf("CreateKey failed: %v", err)
	}

	ok, err := store.VerifyKey(ctx, "SITE-001", "definitely-not-the-real-key")
	if err != nil {
		t.Fatalf("VerifyKey failed: %v", err)
	}
	if ok {
		t.Fatalf("expected an incorrect key to be rejected")
	}
}

func TestMemoryKeyStore_UnknownSiteRejected(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryKeyStore()

	ok, err := store.VerifyKey(ctx, "SITE-NEVER-CREATED", "any-key")
	if err != nil {
		t.Fatalf("expected no error for an unknown site, got: %v", err)
	}
	if ok {
		t.Fatalf("expected verification to fail for a site with no key")
	}
}

func TestMemoryKeyStore_RevokedKeyRejected(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryKeyStore()

	plaintext, err := store.CreateKey(ctx, "SITE-001")
	if err != nil {
		t.Fatalf("CreateKey failed: %v", err)
	}

	if err := store.RevokeKey(ctx, "SITE-001"); err != nil {
		t.Fatalf("RevokeKey failed: %v", err)
	}

	ok, err := store.VerifyKey(ctx, "SITE-001", plaintext)
	if err != nil {
		t.Fatalf("VerifyKey failed: %v", err)
	}
	if ok {
		t.Fatalf("expected a revoked key to no longer verify, even though it was correct before revocation")
	}
}

// TestMemoryKeyStore_ReissueOverwritesPriorKey proves rotation (revoke then
// create-key again) actually invalidates the old plaintext, not just marks
// it revoked while still accepting it -- CreateKey overwrites the stored
// hash entirely.
func TestMemoryKeyStore_ReissueOverwritesPriorKey(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryKeyStore()

	firstKey, err := store.CreateKey(ctx, "SITE-001")
	if err != nil {
		t.Fatalf("first CreateKey failed: %v", err)
	}
	secondKey, err := store.CreateKey(ctx, "SITE-001")
	if err != nil {
		t.Fatalf("second CreateKey failed: %v", err)
	}
	if firstKey == secondKey {
		t.Fatalf("expected two distinct random keys, got the same value twice")
	}

	if ok, _ := store.VerifyKey(ctx, "SITE-001", firstKey); ok {
		t.Errorf("expected the first (superseded) key to no longer verify after reissuance")
	}
	ok, err := store.VerifyKey(ctx, "SITE-001", secondKey)
	if err != nil {
		t.Fatalf("VerifyKey failed: %v", err)
	}
	if !ok {
		t.Errorf("expected the newly issued key to verify")
	}
}

func TestMemoryKeyStore_KeysAreIsolatedPerSite(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryKeyStore()

	keyA, err := store.CreateKey(ctx, "SITE-A")
	if err != nil {
		t.Fatalf("CreateKey for SITE-A failed: %v", err)
	}
	if _, err := store.CreateKey(ctx, "SITE-B"); err != nil {
		t.Fatalf("CreateKey for SITE-B failed: %v", err)
	}

	// SITE-A's genuinely valid key must not verify against SITE-B -- proves
	// the lookup is keyed by the claimed site_id, not just "any known hash".
	ok, err := store.VerifyKey(ctx, "SITE-B", keyA)
	if err != nil {
		t.Fatalf("VerifyKey failed: %v", err)
	}
	if ok {
		t.Fatalf("expected SITE-A's key to be rejected when claiming to be SITE-B")
	}
}

func TestHashKey_Deterministic(t *testing.T) {
	input := "same-input"
	first := hashKey(input)
	second := hashKey(input)
	if first != second {
		t.Fatalf("expected hashKey to be deterministic for the same input")
	}
	if hashKey("input-one") == hashKey("input-two") {
		t.Fatalf("expected different inputs to hash differently")
	}
}

func TestGenerateKey_ProducesUniqueHighEntropyValues(t *testing.T) {
	seen := make(map[string]bool)
	for i := 0; i < 100; i++ {
		k, err := generateKey()
		if err != nil {
			t.Fatalf("generateKey failed: %v", err)
		}
		if seen[k] {
			t.Fatalf("generateKey produced a duplicate value: %s", k)
		}
		seen[k] = true
	}
}
