package client

import (
	"bytes"
	"context"
	"testing"
)

func TestObjectRoundtrip(t *testing.T) {
	srv := newTestServer(t, "")
	b := &Backend{URL: srv.URL, Key: "state/cluster"}
	ctx := context.Background()

	// Absent object → ok=false, no error (clean boot).
	if _, ok, err := b.GetObject(ctx, "state/cluster"); err != nil || ok {
		t.Fatalf("expected absent (ok=false, err=nil), got ok=%v err=%v", ok, err)
	}

	first := []byte(`{"jobs":["a"]}`)
	if err := b.PutObject(ctx, "state/cluster", first, "application/json"); err != nil {
		t.Fatalf("put: %v", err)
	}
	got, ok, err := b.GetObject(ctx, "state/cluster")
	if err != nil || !ok {
		t.Fatalf("get after put: ok=%v err=%v", ok, err)
	}
	if !bytes.Equal(got, first) {
		t.Fatalf("roundtrip mismatch: got %q want %q", got, first)
	}

	// Unconditional overwrite (no CAS): a second Put must win.
	second := []byte(`{"jobs":["a","b"]}`)
	if err := b.PutObject(ctx, "state/cluster", second, "application/json"); err != nil {
		t.Fatalf("overwrite: %v", err)
	}
	got, _, err = b.GetObject(ctx, "state/cluster")
	if err != nil {
		t.Fatalf("get after overwrite: %v", err)
	}
	if !bytes.Equal(got, second) {
		t.Fatalf("overwrite mismatch: got %q want %q", got, second)
	}
}

func TestObjectAuth(t *testing.T) {
	srv := newTestServer(t, "secret")
	ctx := context.Background()

	noKey := &Backend{URL: srv.URL, Key: "state/x"}
	if err := noKey.PutObject(ctx, "state/x", []byte("{}"), "application/json"); err == nil {
		t.Fatal("expected auth error without key")
	}

	good := &Backend{URL: srv.URL, Key: "state/x", APIKey: "secret"}
	if err := good.PutObject(ctx, "state/x", []byte("{}"), "application/json"); err != nil {
		t.Fatalf("put with valid key: %v", err)
	}
	if _, ok, err := good.GetObject(ctx, "state/x"); err != nil || !ok {
		t.Fatalf("get with valid key: ok=%v err=%v", ok, err)
	}
}

func TestPutObjectMissingURL(t *testing.T) {
	b := &Backend{Key: "state/x"}
	if err := b.PutObject(context.Background(), "state/x", []byte("{}"), ""); err == nil {
		t.Fatal("expected error with empty URL")
	}
}
