// Package discovery wraps a hoplock.Backend in an imperative
// acquire/renew/release API tuned to hop's existing tick loop.
//
// Mutual exclusion lives entirely in the backend: every claim is a
// conditional write keyed on the last observed handle (ETag). The
// in-memory Discovery only caches the handle so the next renew can prove
// it has not been displaced.
package discovery

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/xinix00/hoplock"
	"github.com/xinix00/hoplock/mem"
	"github.com/xinix00/hoplock/s3"
	"github.com/xinix00/hoplockserver/client"

	"github.com/xinix00/hop/pkg/config"
)

// minBackendTimeout is the floor for one lock-store round-trip. The actual
// per-call budget scales with the lease: see backendTimeoutFor.
const minBackendTimeout = 5 * time.Second

// backendTimeoutFor returns the per-call timeout for a lease of the given
// TTL: a third of the lease, never under minBackendTimeout. A renew is a
// Read plus a conditional Write; on a slow object store (Bunny Storage was
// measured at 4–20 s per PUT, 2026-09-08) a fixed 5 s made every renew and
// takeover time out, so the cluster lost its leader as soon as the lease
// expired. Tying the budget to the TTL lets an operator absorb a slow store
// by raising timeouts.leader_lease alone: with a 120 s lease each call may
// take 40 s and still leaves two more ticks before expiry.
func backendTimeoutFor(ttl time.Duration) time.Duration {
	if t := ttl / 3; t > minBackendTimeout {
		return t
	}
	return minBackendTimeout
}

// Discovery is the leader-election handle used by the agent main loop. It
// is safe to call from multiple goroutines.
type Discovery struct {
	backend hoplock.Backend
	owner   string
	ttl     time.Duration
	timeout time.Duration // per backend call, derived from ttl
	now     func() time.Time

	mu     sync.Mutex
	handle string
}

// New returns a Discovery bound to backend. The owner string is the
// "ip:port" address advertised as the leader address; ttl is how long
// each acquired or renewed lease is valid for.
func New(backend hoplock.Backend, nodeIP string, nodePort int, ttl time.Duration) *Discovery {
	return &Discovery{
		backend: backend,
		owner:   fmt.Sprintf("%s:%d", nodeIP, nodePort),
		ttl:     ttl,
		timeout: backendTimeoutFor(ttl),
		now:     time.Now,
	}
}

// HoplockServerBackend returns a hoplock.Backend that talks HTTP to a
// hoplockserver. The lease object is stored under "leases/<clusterName>".
func HoplockServerBackend(serverURL, apiKey, clusterName string) hoplock.Backend {
	return &client.Backend{
		URL:    serverURL,
		Key:    "leases/" + clusterName,
		APIKey: apiKey,
	}
}

// InMemoryBackend returns an in-process backend, used for standalone
// (single-node) mode and tests.
func InMemoryBackend() hoplock.Backend { return mem.New() }

// S3Backend returns a hoplock.Backend backed by an S3-compatible object
// store. The lease object lives at "leases/<clusterName>" within the
// supplied bucket.
type S3BackendConfig struct {
	Endpoint        string
	Bucket          string
	Region          string
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	UsePathStyle    bool
}

// S3Backend wires a hoplock/s3 backend for the given cluster.
func S3Backend(cfg S3BackendConfig, clusterName string) hoplock.Backend {
	return &s3.Backend{
		Endpoint:        cfg.Endpoint,
		Bucket:          cfg.Bucket,
		Key:             "leases/" + clusterName,
		Region:          cfg.Region,
		AccessKeyID:     cfg.AccessKeyID,
		SecretAccessKey: cfg.SecretAccessKey,
		SessionToken:    cfg.SessionToken,
		UsePathStyle:    cfg.UsePathStyle,
	}
}

// StatePersister is the committed-state contract used by the leader. It is
// defined here (structurally identical to leader.StatePersister) so
// discovery can build stores and hand them to the leader without importing
// the leader package.
type StatePersister interface {
	Save(ctx context.Context, snapshot []byte) error
	Load(ctx context.Context) (snapshot []byte, ok bool, err error)
}

// S3StateStore persists the leader's committed cluster state as a plain
// object at "state/<cluster>", next to the election lease — same bucket,
// same credentials, same signer (leader.StatePersister). Renaming or
// deleting that object in the bucket is the operator's "boot clean" switch.
type S3StateStore struct {
	b   *s3.Backend
	key string
}

// NewS3StateStore wires the state object for the given cluster.
func NewS3StateStore(cfg S3BackendConfig, clusterName string) *S3StateStore {
	return &S3StateStore{
		b: &s3.Backend{
			Endpoint:        cfg.Endpoint,
			Bucket:          cfg.Bucket,
			Key:             "state/" + clusterName, // alleen voor foutmeldingen; object-API krijgt de key expliciet
			Region:          cfg.Region,
			AccessKeyID:     cfg.AccessKeyID,
			SecretAccessKey: cfg.SecretAccessKey,
			SessionToken:    cfg.SessionToken,
			UsePathStyle:    cfg.UsePathStyle,
		},
		key: "state/" + clusterName,
	}
}

// StateStoreFromConfig geeft de committed-state-store voor deze cluster-
// config, of nil wanneer er geen durable backend is (standalone / mem). Dit
// is bewust de ENIGE gate zodat cmd/agent en agentboot niet uit elkaar lopen.
//
// De backend die de LEASE houdt, houdt ook de STAAT — één bron van waarheid:
//   - Een bruikbare S3-sectie ⇒ S3 (lease én state in dezelfde bucket).
//   - Anders een hoplockserver-URL ⇒ dezelfde server, object "state/<cluster>"
//     naast "leases/<cluster>". Zo krijgt de default (gratis, selfhosted)
//     modus óók durable desired state — een nieuwe leader verliest na failover
//     geen jobs meer die elders draaiden.
//   - mem / standalone ⇒ nil: in-process, geen netwerk-state nodig (de agent
//     bewaart z'n lokale state.json).
func StateStoreFromConfig(cfg *config.Config, standalone bool) StatePersister {
	// The backend that holds the lease holds the state. Mirror buildBackend:
	// a distributed lock (S3 / hoplockserver) → the matching remote store; an
	// in-process lock (standalone / mem / no remote) → a local crash-safe file.
	// Same StatePersister interface either way — only the backend differs, so
	// there is one write path and never a nil persister (the leader always has
	// a durable home for desired state).
	if !standalone {
		if s3c := cfg.Cluster.Lock.S3; s3c.Bucket != "" && s3c.Endpoint != "" {
			return NewS3StateStore(S3BackendConfig{
				Endpoint:        s3c.Endpoint,
				Bucket:          s3c.Bucket,
				Region:          s3c.Region,
				AccessKeyID:     s3c.AccessKeyID,
				SecretAccessKey: s3c.SecretAccessKey,
				SessionToken:    s3c.SessionToken,
				UsePathStyle:    s3c.UsePathStyle,
			}, cfg.Cluster.Name)
		}
		if lc := cfg.Cluster.Lock; (lc.Type == "" || lc.Type == "hoplockserver") && lc.URL != "" {
			return NewHoplockServerStateStore(lc.URL, lc.APIKey, cfg.Cluster.Name)
		}
	}
	return NewFileStateStore(cfg.Paths.StateFile)
}

// FileStateStore persists the committed cluster state to a local file. It is
// the StatePersister for single-node / standalone mode, where the lease lives
// in-process and the state has nowhere remote to go. Writes are crash-safe:
// tmp file + fsync + atomic rename + directory fsync, so a crash or a full
// disk never leaves a half-written or truncated state file (unlike a plain
// truncate-in-place write).
type FileStateStore struct {
	path string
}

// NewFileStateStore wires the state file at path.
func NewFileStateStore(path string) *FileStateStore {
	return &FileStateStore{path: path}
}

// Save atomically overwrites the state file.
func (s *FileStateStore) Save(_ context.Context, snapshot []byte) error {
	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("state file: mkdir %s: %w", dir, err)
	}
	// A fixed sibling cannot accumulate across crashes: the next save simply
	// truncates the one interrupted predecessor.
	tmpName := s.path + ".tmp"
	tmp, err := os.OpenFile(tmpName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("state file: temp: %w", err)
	}
	defer os.Remove(tmpName) // no-op once the rename succeeds
	if _, err := tmp.Write(snapshot); err != nil {
		tmp.Close()
		return fmt.Errorf("state file: write: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("state file: fsync: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("state file: close: %w", err)
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		return fmt.Errorf("state file: rename: %w", err)
	}
	// fsync the directory so the rename itself is durable across a crash.
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

// Load reads the state file; ok=false when it does not exist (clean boot).
func (s *FileStateStore) Load(_ context.Context) ([]byte, bool, error) {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("state file: read %s: %w", s.path, err)
	}
	return data, true, nil
}

// Save overschrijft de snapshot (enige schrijver: de leaseholder).
func (s *S3StateStore) Save(ctx context.Context, snapshot []byte) error {
	return s.b.PutObject(ctx, s.key, snapshot, "application/json")
}

// Load leest de snapshot; ok=false = geen object = schone boot.
func (s *S3StateStore) Load(ctx context.Context) ([]byte, bool, error) {
	return s.b.GetObject(ctx, s.key)
}

// HoplockServerStateStore persists the committed cluster state on the same
// hoplockserver that holds the election lease: a plain object at
// "state/<cluster>" next to "leases/<cluster>". No S3, no extra process —
// the default self-hosted backend now has durable desired state. Deleting
// that object on the server is the operator's "boot clean" switch, exactly
// like S3StateStore.
type HoplockServerStateStore struct {
	b   *client.Backend
	key string
}

// NewHoplockServerStateStore wires the state object for the given cluster on
// the hoplockserver at serverURL (same URL and API key as the lease backend).
func NewHoplockServerStateStore(serverURL, apiKey, clusterName string) *HoplockServerStateStore {
	key := "state/" + clusterName
	return &HoplockServerStateStore{
		b:   &client.Backend{URL: serverURL, Key: key, APIKey: apiKey},
		key: key,
	}
}

// Save overschrijft de snapshot (enige schrijver: de leaseholder).
func (s *HoplockServerStateStore) Save(ctx context.Context, snapshot []byte) error {
	return s.b.PutObject(ctx, s.key, snapshot, "application/json")
}

// Load leest de snapshot; ok=false = geen object = schone boot.
func (s *HoplockServerStateStore) Load(ctx context.Context) ([]byte, bool, error) {
	return s.b.GetObject(ctx, s.key)
}

// NodeAddr returns this node's owner identity.
func (d *Discovery) NodeAddr() string { return d.owner }

// GetLeader returns the current leader address, or "" if there is no
// active lease or the backend cannot be reached.
func (d *Discovery) GetLeader() string {
	if d.backend == nil {
		return ""
	}
	ctx, cancel := context.WithTimeout(context.Background(), d.timeout)
	defer cancel()
	state, _, err := d.backend.Read(ctx)
	if err != nil {
		return ""
	}
	if d.now().After(state.ExpiresAt) {
		return ""
	}
	return state.Owner
}

// tryClaim performs one acquire/renew attempt. A nil error means we now hold
// the lease. hoplock.ErrLeaseHeld means another node holds a live lease (we
// are displaced); any other error is a backend/transport problem.
func (d *Discovery) tryClaim(ctx context.Context) error {
	state, handle, err := d.backend.Read(ctx)
	switch {
	case errors.Is(err, hoplock.ErrNoLease):
		return d.refresh(ctx, "", 1)
	case err != nil:
		return err
	case state.Owner == d.owner:
		return d.refresh(ctx, handle, state.Generation)
	case d.now().After(state.ExpiresAt):
		return d.refresh(ctx, handle, state.Generation+1)
	default:
		return hoplock.ErrLeaseHeld // someone else holds a live lease
	}
}

// TryBecomeLeader attempts to claim the lease. Returns true if we now
// hold it. Three success paths: create from no-lease, refresh while we
// already own it, or take over an expired lease.
func (d *Discovery) TryBecomeLeader() bool {
	if d.backend == nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), d.timeout)
	defer cancel()
	return d.tryClaim(ctx) == nil
}

// RenewLease refreshes our hold on the lease. renewed is true on success.
// When renewed is false, displaced tells the two failure kinds apart:
//   - displaced=true: the store reports another owner (ErrLeaseHeld / 412) —
//     we have genuinely lost leadership and must step down now.
//   - displaced=false: the store was unreachable (a connectivity blip) — the
//     caller may keep leading while it still sees agents, so a working LAN
//     survives an internet/lock-store outage without losing the cluster
//     (no one else can take the lease while the store is unreachable to them).
func (d *Discovery) RenewLease() (renewed, displaced bool) {
	if d.backend == nil {
		return false, false
	}
	ctx, cancel := context.WithTimeout(context.Background(), d.timeout)
	defer cancel()
	err := d.tryClaim(ctx)
	if err == nil {
		return true, false
	}
	return false, errors.Is(err, hoplock.ErrLeaseHeld)
}

// ReleaseLeadership best-effort deletes the lease, allowing another
// candidate to take over immediately instead of waiting for TTL.
func (d *Discovery) ReleaseLeadership() {
	if d.backend == nil {
		return
	}
	d.mu.Lock()
	handle := d.handle
	d.handle = ""
	d.mu.Unlock()
	if handle == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), d.timeout)
	defer cancel()
	_ = d.backend.Delete(ctx, handle)
}

// IsLeader reports whether the current leader is this node.
func (d *Discovery) IsLeader() bool {
	return d.GetLeader() == d.owner
}

func (d *Discovery) refresh(ctx context.Context, prevHandle string, gen int64) error {
	state := &hoplock.State{
		Generation: gen,
		ExpiresAt:  d.now().Add(d.ttl),
		Owner:      d.owner,
	}
	newHandle, err := d.backend.Write(ctx, prevHandle, state)
	if err != nil {
		// Surface the backend error so callers can tell a transient network
		// blip from a real ErrLeaseHeld / 412 PreconditionFailed.
		log.Printf("lock refresh failed (prevHandle=%q, gen=%d): %v", prevHandle, gen, err)
		return err
	}
	d.mu.Lock()
	d.handle = newHandle
	d.mu.Unlock()
	return nil
}
