// Package store implements a file-backed key/value store with content-addressed
// ETags and compare-and-swap semantics. It is the storage layer underneath
// hoplockserver.
//
// One file per key lives under the configured data directory. ETags are
// SHA256 hashes of the stored body, which makes them deterministic and lets
// callers cache an ETag without trusting the server to remember it. CAS is
// enforced by an in-memory mutex, so the store is single-process — multiple
// hoplockserver replicas pointed at the same directory will race.
package store

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// Errors returned by Store operations.
var (
	// ErrNotFound is returned when the key does not exist.
	ErrNotFound = errors.New("hoplockserver/store: not found")

	// ErrPrecondition is returned when a conditional write fails:
	// If-None-Match: * on an existing key, or If-Match on a stale ETag.
	ErrPrecondition = errors.New("hoplockserver/store: precondition failed")

	// ErrBadKey is returned for keys that would escape the data directory
	// or contain disallowed characters.
	ErrBadKey = errors.New("hoplockserver/store: invalid key")
)

// Store persists opaque blobs under string keys. The zero value is not
// usable; construct with New.
type Store struct {
	dir string
	mu  sync.Mutex
}

// New returns a Store rooted at dir. The directory is created if it does
// not exist.
func New(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("hoplockserver/store: create dir: %w", err)
	}
	return &Store{dir: dir}, nil
}

// Object is the value returned by Get and produced by Put.
type Object struct {
	Body []byte
	ETag string
}

// Get returns the stored object or ErrNotFound.
func (s *Store) Get(key string) (Object, error) {
	path, err := s.path(key)
	if err != nil {
		return Object{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	body, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return Object{}, ErrNotFound
		}
		return Object{}, fmt.Errorf("hoplockserver/store: read %s: %w", key, err)
	}
	return Object{Body: body, ETag: etag(body)}, nil
}

// Put writes body under key. The condition controls whether the write
// proceeds:
//
//   - cond.IfNoneMatch == "*": succeed only if no object exists yet.
//   - cond.IfMatch != "": succeed only if the stored ETag equals IfMatch.
//   - otherwise: unconditional overwrite.
//
// Exactly one of IfNoneMatch and IfMatch should be set.
func (s *Store) Put(key string, body []byte, cond Condition) (Object, error) {
	path, err := s.path(key)
	if err != nil {
		return Object{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	existing, readErr := os.ReadFile(path)
	exists := readErr == nil

	switch {
	case cond.IfNoneMatch == "*":
		if exists {
			return Object{}, ErrPrecondition
		}
	case cond.IfMatch != "":
		if !exists {
			return Object{}, ErrPrecondition
		}
		if etag(existing) != cond.IfMatch {
			return Object{}, ErrPrecondition
		}
	}

	if !exists && readErr != nil && !os.IsNotExist(readErr) {
		return Object{}, fmt.Errorf("hoplockserver/store: read %s: %w", key, readErr)
	}

	if err := writeAtomic(path, body); err != nil {
		return Object{}, fmt.Errorf("hoplockserver/store: write %s: %w", key, err)
	}
	return Object{Body: body, ETag: etag(body)}, nil
}

// Delete removes key. If cond.IfMatch is set, the delete only succeeds if
// the stored ETag matches.
func (s *Store) Delete(key string, cond Condition) error {
	path, err := s.path(key)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	existing, readErr := os.ReadFile(path)
	if os.IsNotExist(readErr) {
		return ErrNotFound
	}
	if readErr != nil {
		return fmt.Errorf("hoplockserver/store: read %s: %w", key, readErr)
	}
	if cond.IfMatch != "" && etag(existing) != cond.IfMatch {
		return ErrPrecondition
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("hoplockserver/store: remove %s: %w", key, err)
	}
	return nil
}

// Condition expresses a conditional-write precondition. The zero value
// performs an unconditional operation.
type Condition struct {
	IfNoneMatch string // "*" means "must not exist"
	IfMatch     string // expected ETag
}

func etag(body []byte) string {
	sum := sha256.Sum256(body)
	return `"` + hex.EncodeToString(sum[:]) + `"`
}

// path turns a key into an absolute file path inside the data directory,
// rejecting anything that could traverse out.
func (s *Store) path(key string) (string, error) {
	if key == "" || strings.ContainsRune(key, 0) || strings.HasPrefix(key, "/") {
		return "", ErrBadKey
	}
	clean := filepath.Clean(key)
	if clean == "." || strings.HasPrefix(clean, "..") || strings.Contains(clean, ".."+string(filepath.Separator)) {
		return "", ErrBadKey
	}
	full := filepath.Join(s.dir, clean)
	rel, err := filepath.Rel(s.dir, full)
	if err != nil || strings.HasPrefix(rel, "..") {
		return "", ErrBadKey
	}
	if dir := filepath.Dir(full); dir != s.dir {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return "", fmt.Errorf("hoplockserver/store: mkdir %s: %w", dir, err)
		}
	}
	return full, nil
}

func writeAtomic(path string, body []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".write-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer func() {
		_ = os.Remove(tmpPath)
	}()
	if _, err := tmp.Write(body); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}
