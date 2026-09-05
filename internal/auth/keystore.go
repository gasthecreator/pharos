// Package auth implements per-site API key authentication for Central
// Ingestion (§2.1, §2.2, ARCHITECTURE_PROPOSALS.md "Slice 15: Auth & TLS").
//
// A site proves its identity by sending its own key alongside the site_id
// it's claiming (X-Site-ID + X-API-Key), rather than the server needing a
// reverse index from key to site: KeyStore.VerifyKey looks up the single
// row for the claimed site_id and compares hashes, which is exactly the
// point-lookup shape Cassandra's site_id-keyed table already supports
// efficiently, with no secondary index needed.
package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/gasthecreator/pharos/internal/tlsutil"
	"github.com/gocql/gocql"
)

// KeyStore defines the persistence interface for per-site API keys.
type KeyStore interface {
	// CreateKey generates a new random key for siteID, stores only its
	// hash, and returns the plaintext key exactly once -- callers must save
	// it immediately, since it can never be retrieved again (mirrors how
	// Stripe/GitHub/AWS all handle API token issuance: the plaintext is
	// shown once, at creation, and never again).
	CreateKey(ctx context.Context, siteID string) (plaintextKey string, err error)
	// VerifyKey reports whether plaintextKey is the current, non-revoked
	// key for siteID. Returns (false, nil) for "no such site" or "wrong
	// key" alike -- the caller (the HTTP middleware) treats both as 401,
	// and distinguishing them in the response would just help an attacker
	// enumerate valid site IDs.
	VerifyKey(ctx context.Context, siteID, plaintextKey string) (bool, error)
	// RevokeKey immediately invalidates siteID's current key. A revoked
	// site can be issued a new key via CreateKey (which overwrites the
	// stored hash), so revoke-then-reissue is the rotation path.
	RevokeKey(ctx context.Context, siteID string) error
	Close() error
}

// hashKey returns the hex-encoded SHA-256 hash of a plaintext key. No
// per-key salt: the key itself is a 256-bit random value with no
// human-guessable structure, so there's no precomputation attack a salt
// would need to defeat (unlike a human-chosen password).
func hashKey(plaintextKey string) string {
	sum := sha256.Sum256([]byte(plaintextKey))
	return hex.EncodeToString(sum[:])
}

// generateKey returns a new random, high-entropy plaintext API key.
func generateKey() (string, error) {
	buf := make([]byte, 32) // 256 bits
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("failed to generate random key: %w", err)
	}
	return "phk_" + base64.RawURLEncoding.EncodeToString(buf), nil
}

// CassandraConfig specifies connection parameters for the API key store,
// mirroring the shape of dedup.CassandraConfig/consumer.CassandraStoreConfig
// (§2.4, Slice 14) -- including DC-aware routing and TLS, since this store
// talks to the same multi-region Cassandra cluster as everything else.
type CassandraConfig struct {
	Hosts             []string
	Port              int
	Keyspace          string
	Consistency       gocql.Consistency
	ConnectTimeout    time.Duration
	ReplicationFactor int
	LocalDC           string
	RemoteDCs         map[string]int
	TLS               *tlsutil.ClientConfig
}

// DefaultCassandraConfig returns connection defaults matching this
// project's other Cassandra stores.
func DefaultCassandraConfig() CassandraConfig {
	var tlsCfg *tlsutil.ClientConfig
	if caCert := tlsutil.DefaultCACertPath(); caCert != "" {
		// Real Cassandra now requires TLS on its client port (§2.4, Slice
		// 15) -- see dedup.DefaultCassandraConfig's docs for why this is a
		// default, not just an opt-in.
		tlsCfg = &tlsutil.ClientConfig{CACertPath: caCert, ServerName: "localhost"}
	}
	return CassandraConfig{
		Hosts:             []string{"127.0.0.1"},
		Port:              9042,
		Keyspace:          "pharos",
		Consistency:       gocql.LocalQuorum,
		ConnectTimeout:    10 * time.Second,
		ReplicationFactor: 3,
		LocalDC:           "dc-us",
		RemoteDCs:         map[string]int{"dc-eu": 1},
		TLS:               tlsCfg,
	}
}

// CassandraKeyStore implements KeyStore against Apache Cassandra.
type CassandraKeyStore struct {
	session *gocql.Session
	closed  bool
	mu      sync.RWMutex
}

// NewCassandraKeyStore connects to Cassandra and bootstraps the
// site_api_keys table.
func NewCassandraKeyStore(cfg CassandraConfig) (*CassandraKeyStore, error) {
	cluster := gocql.NewCluster(cfg.Hosts...)
	if cfg.Port > 0 {
		cluster.Port = cfg.Port
	}
	cluster.Timeout = cfg.ConnectTimeout
	cluster.Consistency = cfg.Consistency
	cluster.DisableInitialHostLookup = true
	if cfg.LocalDC != "" {
		cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.DCAwareRoundRobinPolicy(cfg.LocalDC))
	}
	if cfg.TLS != nil {
		sslOpts, err := cfg.TLS.GocqlSslOptions()
		if err != nil {
			return nil, fmt.Errorf("failed to build TLS config: %w", err)
		}
		cluster.SslOpts = sslOpts
	}
	cluster.Keyspace = cfg.Keyspace

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Cassandra cluster: %w", err)
	}

	store := &CassandraKeyStore{session: session}
	if err := store.ensureSchema(); err != nil {
		session.Close()
		return nil, fmt.Errorf("failed to bootstrap site_api_keys schema: %w", err)
	}
	return store, nil
}

func (s *CassandraKeyStore) ensureSchema() error {
	return s.session.Query(`
		CREATE TABLE IF NOT EXISTS pharos.site_api_keys (
			site_id text,
			key_hash text,
			created_at timestamp,
			revoked boolean,
			PRIMARY KEY (site_id)
		);
	`).Exec()
}

func (s *CassandraKeyStore) CreateKey(ctx context.Context, siteID string) (string, error) {
	plaintext, err := generateKey()
	if err != nil {
		return "", err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return "", fmt.Errorf("key store is closed")
	}
	err = s.session.Query(
		`INSERT INTO pharos.site_api_keys (site_id, key_hash, created_at, revoked) VALUES (?, ?, ?, false);`,
		siteID, hashKey(plaintext), time.Now().UTC(),
	).WithContext(ctx).Exec()
	if err != nil {
		return "", fmt.Errorf("failed to store key for site %s: %w", siteID, err)
	}
	return plaintext, nil
}

func (s *CassandraKeyStore) VerifyKey(ctx context.Context, siteID, plaintextKey string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return false, fmt.Errorf("key store is closed")
	}
	var storedHash string
	var revoked bool
	err := s.session.Query(
		`SELECT key_hash, revoked FROM pharos.site_api_keys WHERE site_id = ?;`, siteID,
	).WithContext(ctx).Scan(&storedHash, &revoked)
	if err == gocql.ErrNotFound {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("failed to look up key for site %s: %w", siteID, err)
	}
	if revoked {
		return false, nil
	}
	// Constant-time comparison: this is a credential check, not a
	// structural equality check -- a timing side-channel here would leak
	// how many leading bytes of the hash matched.
	provided := hashKey(plaintextKey)
	return subtle.ConstantTimeCompare([]byte(storedHash), []byte(provided)) == 1, nil
}

func (s *CassandraKeyStore) RevokeKey(ctx context.Context, siteID string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return fmt.Errorf("key store is closed")
	}
	return s.session.Query(
		`UPDATE pharos.site_api_keys SET revoked = true WHERE site_id = ?;`, siteID,
	).WithContext(ctx).Exec()
}

func (s *CassandraKeyStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed && s.session != nil {
		s.session.Close()
		s.closed = true
	}
	return nil
}

// MemoryKeyStore is an in-memory KeyStore for fast unit testing.
type MemoryKeyStore struct {
	mu   sync.RWMutex
	keys map[string]memoryKeyRecord
}

type memoryKeyRecord struct {
	hash    string
	revoked bool
}

func NewMemoryKeyStore() *MemoryKeyStore {
	return &MemoryKeyStore{keys: make(map[string]memoryKeyRecord)}
}

func (m *MemoryKeyStore) CreateKey(ctx context.Context, siteID string) (string, error) {
	plaintext, err := generateKey()
	if err != nil {
		return "", err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.keys[siteID] = memoryKeyRecord{hash: hashKey(plaintext)}
	return plaintext, nil
}

func (m *MemoryKeyStore) VerifyKey(ctx context.Context, siteID, plaintextKey string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	rec, ok := m.keys[siteID]
	if !ok || rec.revoked {
		return false, nil
	}
	return subtle.ConstantTimeCompare([]byte(rec.hash), []byte(hashKey(plaintextKey))) == 1, nil
}

func (m *MemoryKeyStore) RevokeKey(ctx context.Context, siteID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.keys[siteID]
	if !ok {
		return fmt.Errorf("no key for site %s", siteID)
	}
	rec.revoked = true
	m.keys[siteID] = rec
	return nil
}

func (m *MemoryKeyStore) Close() error { return nil }
