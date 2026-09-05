// Package tlsutil builds shared TLS configuration for every Pharos service
// (§2.1, §2.2, §2.4, ARCHITECTURE_PROPOSALS.md "Slice 15: Auth & TLS") from
// this project's own CA (scripts/generate_certs.sh) -- one place that knows
// how to turn a CA cert path (and, for a server, a cert/key pair) into the
// stdlib crypto/tls.Config, gocql.SslOptions, and kafka-go dialer/transport
// TLS settings every Cassandra/Kafka/HTTP connection in this project needs,
// rather than duplicating certificate-loading logic in each of them.
package tlsutil

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"path/filepath"

	"github.com/gocql/gocql"
)

// ClientConfig describes how a Pharos client (Central Ingestion talking to
// Cassandra/Kafka, the edge talking to Central Ingestion, a test connecting
// to any of them) should trust and optionally present certificates.
type ClientConfig struct {
	// CACertPath is the PEM file for this project's own CA
	// (certs/ca-cert.pem, from scripts/generate_certs.sh). Required.
	CACertPath string
	// ServerName overrides the hostname used for certificate verification,
	// for cases where the dial address (e.g. "127.0.0.1") doesn't match any
	// SAN on the server's certificate but a name that *is* on it (e.g.
	// "localhost") is known to be correct.
	ServerName string
}

// LoadCAPool reads CACertPath and returns a cert pool containing just that
// CA -- this project intentionally does not fall back to the system trust
// store, since nothing here should ever be trusted merely for chaining to a
// public root; every Pharos-internal connection must chain to this
// project's own CA specifically.
func LoadCAPool(caCertPath string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(caCertPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read CA cert %s: %w", caCertPath, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("failed to parse CA cert %s: no valid certificates found", caCertPath)
	}
	return pool, nil
}

// StdTLSConfig builds a crypto/tls.Config suitable for an HTTP client or a
// kafka-go dialer/transport: trusts only this project's CA, verifies the
// server's certificate and hostname normally (no InsecureSkipVerify --
// skipping verification would defeat the entire point of standing up a CA).
func (c ClientConfig) StdTLSConfig() (*tls.Config, error) {
	pool, err := LoadCAPool(c.CACertPath)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		RootCAs:    pool,
		ServerName: c.ServerName,
		MinVersion: tls.VersionTLS12,
	}, nil
}

// GocqlSslOptions builds gocql's SslOptions from the same CA, for
// Cassandra client connections (§2.4).
func (c ClientConfig) GocqlSslOptions() (*gocql.SslOptions, error) {
	pool, err := LoadCAPool(c.CACertPath)
	if err != nil {
		return nil, err
	}
	return &gocql.SslOptions{
		Config: &tls.Config{
			RootCAs:    pool,
			ServerName: c.ServerName,
			MinVersion: tls.VersionTLS12,
		},
		EnableHostVerification: true,
	}, nil
}

// DefaultCACertPath locates this project's own certs/ca-cert.pem (generated
// by scripts/generate_certs.sh) by walking up from the current working
// directory looking for a go.mod, then checking certs/ca-cert.pem relative
// to that root -- regardless of whether the caller is a `go test` run
// (whose working directory is its own package, not the repo root) or a
// binary invoked directly from the repo root. Returns "" (not an error) if
// no go.mod is found or the cert file doesn't exist there, so callers can
// gate on an empty result to mean "no default CA available here" -- e.g. a
// compiled binary running somewhere outside this project's own checkout,
// where an explicit --ca-cert flag is the correct way to point at a
// deployment-provisioned CA instead (§2.4, ARCHITECTURE_PROPOSALS.md
// "Slice 15: Auth & TLS"). This exists specifically so real Cassandra/Kafka
// connections default to using this project's own TLS the same way
// LocalDC/RemoteDCs became defaults in Slice 14 -- the real dev/CI
// infrastructure now requires TLS on Cassandra's client port, so a default
// config that produces a plaintext connection attempt is a default that
// simply can't connect, not a safer fallback.
func DefaultCACertPath() string {
	dir, err := os.Getwd()
	if err != nil {
		return ""
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			candidate := filepath.Join(dir, "certs", "ca-cert.pem")
			if _, err := os.Stat(candidate); err == nil {
				return candidate
			}
			return ""
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return ""
		}
		dir = parent
	}
}

// ServerConfig describes an HTTP server's own certificate (Central
// Ingestion's listener). The certificate must be signed by a CA that
// clients (the edge, in this project's case) are configured to trust via
// ClientConfig.CACertPath.
type ServerConfig struct {
	CertPath string
	KeyPath  string
}

// StdTLSConfig builds a crypto/tls.Config for an http.Server to serve with,
// loading the given certificate/key pair once at startup.
func (s ServerConfig) StdTLSConfig() (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(s.CertPath, s.KeyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load server certificate %s/%s: %w", s.CertPath, s.KeyPath, err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}, nil
}
