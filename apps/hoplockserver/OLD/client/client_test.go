package client

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/xinix00/hoplock"
	"github.com/xinix00/hoplockserver/internal/server"
	"github.com/xinix00/hoplockserver/internal/store"
)

func TestBackendRoundtrip(t *testing.T) {
	srv := newTestServer(t, "")
	b := &Backend{URL: srv.URL, Key: "lease/cluster"}
	ctx := context.Background()

	if _, _, err := b.Read(ctx); !errors.Is(err, hoplock.ErrNoLease) {
		t.Fatalf("expected ErrNoLease, got %v", err)
	}

	state := &hoplock.State{Generation: 1, ExpiresAt: time.Now().Add(time.Minute), Owner: "node-a"}
	handle, err := b.Write(ctx, "", state)
	if err != nil {
		t.Fatalf("first write: %v", err)
	}
	if handle == "" {
		t.Fatal("expected handle")
	}

	if _, err := b.Write(ctx, "", state); !errors.Is(err, hoplock.ErrLeaseHeld) {
		t.Fatalf("expected ErrLeaseHeld on second create, got %v", err)
	}

	got, gotHandle, err := b.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got.Owner != "node-a" || gotHandle != handle {
		t.Fatalf("read mismatch: %+v handle=%q", got, gotHandle)
	}

	next := &hoplock.State{Generation: 2, ExpiresAt: time.Now().Add(time.Minute), Owner: "node-a"}
	if _, err := b.Write(ctx, "stale", next); !errors.Is(err, hoplock.ErrLeaseHeld) {
		t.Fatalf("expected ErrLeaseHeld on stale handle, got %v", err)
	}
	handle2, err := b.Write(ctx, handle, next)
	if err != nil {
		t.Fatalf("renew: %v", err)
	}
	if handle2 == handle {
		t.Fatal("expected new handle after update")
	}

	if err := b.Delete(ctx, handle); !errors.Is(err, hoplock.ErrLeaseHeld) {
		t.Fatalf("expected ErrLeaseHeld deleting with stale handle, got %v", err)
	}
	if err := b.Delete(ctx, handle2); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := b.Delete(ctx, handle2); !errors.Is(err, hoplock.ErrNoLease) {
		t.Fatalf("expected ErrNoLease on second delete, got %v", err)
	}
}

func TestBackendAuth(t *testing.T) {
	srv := newTestServer(t, "secret")
	ctx := context.Background()

	noKey := &Backend{URL: srv.URL, Key: "lease/x"}
	if _, _, err := noKey.Read(ctx); err == nil || errors.Is(err, hoplock.ErrNoLease) {
		t.Fatalf("expected auth error without key, got %v", err)
	}

	good := &Backend{URL: srv.URL, Key: "lease/x", APIKey: "secret"}
	if _, _, err := good.Read(ctx); !errors.Is(err, hoplock.ErrNoLease) {
		t.Fatalf("expected ErrNoLease with valid key, got %v", err)
	}
}

func newTestServer(t *testing.T, apiKey string) *httptest.Server {
	t.Helper()
	st, err := store.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(server.New(st, apiKey))
	t.Cleanup(srv.Close)
	return srv
}
