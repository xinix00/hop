package discovery

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/xinix00/hop/pkg/config"
)

func TestStateStoreFromConfigSelection(t *testing.T) {
	// standalone=true → always the local file store, even with a remote config.
	stand := &config.Config{}
	stand.Cluster.Lock.URL = "http://lock:8090"
	if _, ok := StateStoreFromConfig(stand, true).(*FileStateStore); !ok {
		t.Fatalf("standalone: want *FileStateStore, got %T", StateStoreFromConfig(stand, true))
	}

	// Empty and explicit mem → local file store (in-process lock).
	if _, ok := StateStoreFromConfig(&config.Config{}, false).(*FileStateStore); !ok {
		t.Fatalf("empty config: want *FileStateStore, got %T", StateStoreFromConfig(&config.Config{}, false))
	}
	memCfg := &config.Config{}
	memCfg.Cluster.Lock.Type = "mem"
	if _, ok := StateStoreFromConfig(memCfg, false).(*FileStateStore); !ok {
		t.Fatalf("mem: want *FileStateStore, got %T", StateStoreFromConfig(memCfg, false))
	}

	// Explicit hoplockserver with a URL → HoplockServerStateStore.
	hls := &config.Config{}
	hls.Cluster.Name = "prod"
	hls.Cluster.Lock.Type = "hoplockserver"
	hls.Cluster.Lock.URL = "http://lock:8090"
	if _, ok := StateStoreFromConfig(hls, false).(*HoplockServerStateStore); !ok {
		t.Fatalf("hoplockserver: want *HoplockServerStateStore, got %T", StateStoreFromConfig(hls, false))
	}

	// Default type ("") with a URL also means hoplockserver.
	def := &config.Config{}
	def.Cluster.Lock.URL = "http://lock:8090"
	if _, ok := StateStoreFromConfig(def, false).(*HoplockServerStateStore); !ok {
		t.Fatalf("default type: want *HoplockServerStateStore, got %T", StateStoreFromConfig(def, false))
	}

	// S3 is the source of truth when usable, even if a URL is also set.
	s3 := &config.Config{}
	s3.Cluster.Lock.URL = "http://lock:8090"
	s3.Cluster.Lock.S3.Endpoint = "https://s3.example.com"
	s3.Cluster.Lock.S3.Bucket = "hop"
	if _, ok := StateStoreFromConfig(s3, false).(*S3StateStore); !ok {
		t.Fatalf("s3 should win over url, got %T", StateStoreFromConfig(s3, false))
	}
}

func TestFileStateStoreRoundtrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sub", "state.json")
	s := NewFileStateStore(path)
	ctx := context.Background()

	// Absent file = clean boot.
	if _, ok, err := s.Load(ctx); err != nil || ok {
		t.Fatalf("absent: ok=%v err=%v", ok, err)
	}

	snap := []byte(`{"jobs":[{"name":"web"}]}`)
	if err := s.Save(ctx, snap); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, ok, err := s.Load(ctx)
	if err != nil || !ok {
		t.Fatalf("load: ok=%v err=%v", ok, err)
	}
	if string(got) != string(snap) {
		t.Fatalf("roundtrip mismatch: %q vs %q", got, snap)
	}

	// Overwrite is atomic and leaves no stray temp files.
	if err := s.Save(ctx, []byte(`{"jobs":[]}`)); err != nil {
		t.Fatalf("overwrite: %v", err)
	}
	entries, _ := os.ReadDir(filepath.Dir(path))
	for _, e := range entries {
		if len(e.Name()) >= 6 && e.Name()[:6] == ".state" {
			t.Fatalf("stray temp file left behind: %s", e.Name())
		}
	}
}

func TestHoplockServerStateStoreSaveLoad(t *testing.T) {
	fake := newFakeObjectStore()
	srv := httptest.NewServer(fake)
	defer srv.Close()

	s := NewHoplockServerStateStore(srv.URL, "", "prod")
	ctx := context.Background()

	// Absent snapshot = clean boot.
	if _, ok, err := s.Load(ctx); err != nil || ok {
		t.Fatalf("absent load: ok=%v err=%v", ok, err)
	}

	snap := []byte(`{"jobs":[{"name":"web"}]}`)
	if err := s.Save(ctx, snap); err != nil {
		t.Fatalf("save: %v", err)
	}

	// The snapshot must land at state/<cluster>, next to (not on) the lease.
	if _, ok := fake.get("state/prod"); !ok {
		t.Fatalf("snapshot not stored at state/prod; keys=%v", fake.keys())
	}
	if _, ok := fake.get("leases/prod"); ok {
		t.Fatal("snapshot must not touch the lease object")
	}

	got, ok, err := s.Load(ctx)
	if err != nil || !ok {
		t.Fatalf("load after save: ok=%v err=%v", ok, err)
	}
	if string(got) != string(snap) {
		t.Fatalf("roundtrip mismatch: got %q want %q", got, snap)
	}
}

// fakeObjectStore is a minimal stand-in for hoplockserver's blob API: it
// exercises the real client HTTP path (the internal/server package cannot be
// imported across the module boundary; the full-server round-trip is covered
// by hoplockserver/client/object_test.go).
type fakeObjectStore struct {
	mu   sync.Mutex
	data map[string][]byte
}

func newFakeObjectStore() *fakeObjectStore {
	return &fakeObjectStore{data: make(map[string][]byte)}
}

func (f *fakeObjectStore) get(key string) ([]byte, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.data[key]
	return v, ok
}

func (f *fakeObjectStore) keys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	ks := make([]string, 0, len(f.data))
	for k := range f.data {
		ks = append(ks, k)
	}
	return ks
}

func (f *fakeObjectStore) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/")
	f.mu.Lock()
	defer f.mu.Unlock()
	switch r.Method {
	case http.MethodPut:
		body, _ := io.ReadAll(r.Body)
		f.data[key] = body
		w.Header().Set("ETag", `"v1"`)
		w.WriteHeader(http.StatusOK)
	case http.MethodGet:
		v, ok := f.data[key]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("ETag", `"v1"`)
		_, _ = w.Write(v)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}
