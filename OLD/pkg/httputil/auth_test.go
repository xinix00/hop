package httputil

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/xinix00/lean/leanhttp"
)

// signed builds a request carrying the signature a client would have put on it.
// It signs through SignCall, so the test proves client and server agree rather
// than that the server agrees with itself.
func signed(t *testing.T, key, method, path string, body []byte) *leanhttp.Request {
	t.Helper()
	call := leanhttp.Call{Method: method, URL: "http://node" + path, Body: body}
	SignCall(&call, key)

	req := leanhttp.NewRequest(method, path, bytes.NewReader(body))
	if sig := call.Header.Get(AuthHeader); sig != "" {
		req.Header.Set(AuthHeader, sig)
	}
	return req
}

func okHandler(ran *bool) leanhttp.Handler {
	return func(w leanhttp.ResponseWriter, r *leanhttp.Request) {
		if ran != nil {
			*ran = true
		}
		w.WriteHeader(leanhttp.StatusOK)
	}
}

func TestRequireHMAC_EmptyKeyPassesThrough(t *testing.T) {
	handler := RequireHMAC("", okHandler(nil))

	rec := leanhttp.NewRecorder()
	handler(rec, leanhttp.NewRequest("GET", "/", nil))

	if rec.Code != leanhttp.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

func TestRequireHMAC_ValidSignature(t *testing.T) {
	handler := RequireHMAC("secret123", okHandler(nil))

	rec := leanhttp.NewRecorder()
	handler(rec, signed(t, "secret123", "GET", "/v1/jobs", nil))

	if rec.Code != leanhttp.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

func TestRequireHMAC_ValidSignatureWithBody(t *testing.T) {
	body := []byte(`{"name":"api","count":3}`)
	var got []byte
	handler := RequireHMAC("secret123", func(w leanhttp.ResponseWriter, r *leanhttp.Request) {
		got, _ = io.ReadAll(r.Body) // body must survive the middleware
		w.WriteHeader(leanhttp.StatusOK)
	})

	rec := leanhttp.NewRecorder()
	handler(rec, signed(t, "secret123", "POST", "/v1/jobs", body))

	if rec.Code != leanhttp.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if !bytes.Equal(got, body) {
		t.Errorf("body not preserved: got %q want %q", got, body)
	}
}

func TestRequireHMAC_BodyTooLarge(t *testing.T) {
	called := false
	handler := RequireHMAC("secret123", okHandler(&called))

	// Oversized body with no valid signature: the cap must trip during the
	// read, before auth is even checked — that is the pre-auth DoS guard.
	rec := leanhttp.NewRecorder()
	handler(rec, leanhttp.NewRequest("POST", "/v1/jobs", strings.NewReader(strings.Repeat("a", maxBodyBytes+1))))

	if rec.Code != leanhttp.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", rec.Code)
	}
	if called {
		t.Error("handler must not run for an oversized body")
	}
}

// TestRequireHMAC_BodyAtTheCap: exactly the cap still passes. The read asks for
// one byte more than the cap to tell "too large" from "just fits", and an
// off-by-one there would reject the largest legitimate job spec.
func TestRequireHMAC_BodyAtTheCap(t *testing.T) {
	body := bytes.Repeat([]byte("a"), maxBodyBytes)
	handler := RequireHMAC("secret123", okHandler(nil))

	rec := leanhttp.NewRecorder()
	handler(rec, signed(t, "secret123", "POST", "/v1/jobs", body))

	if rec.Code != leanhttp.StatusOK {
		t.Errorf("a body of exactly maxBodyBytes got %d", rec.Code)
	}
}

func TestRequireHMAC_MissingSignature(t *testing.T) {
	handler := RequireHMAC("secret123", okHandler(nil))

	rec := leanhttp.NewRecorder()
	handler(rec, leanhttp.NewRequest("GET", "/v1/jobs", nil))

	if rec.Code != leanhttp.StatusUnauthorized {
		t.Errorf("expected 401, got %d", rec.Code)
	}
}

func TestRequireHMAC_WrongKey(t *testing.T) {
	handler := RequireHMAC("secret123", okHandler(nil))

	rec := leanhttp.NewRecorder()
	handler(rec, signed(t, "wrongkey", "GET", "/v1/jobs", nil))

	if rec.Code != leanhttp.StatusUnauthorized {
		t.Errorf("expected 401, got %d", rec.Code)
	}
}

func TestRequireHMAC_TamperedPath(t *testing.T) {
	handler := RequireHMAC("secret123", okHandler(nil))

	// Signature made for /v1/jobs, replayed against a destructive endpoint.
	req := signed(t, "secret123", "GET", "/v1/jobs", nil)
	req.Path = "/v1/agents/node-1"
	req.Method = "DELETE"

	rec := leanhttp.NewRecorder()
	handler(rec, req)

	if rec.Code != leanhttp.StatusUnauthorized {
		t.Errorf("expected 401 for path/method tamper, got %d", rec.Code)
	}
}

func TestRequireHMAC_TamperedBody(t *testing.T) {
	handler := RequireHMAC("secret123", okHandler(nil))

	req := signed(t, "secret123", "POST", "/v1/jobs", []byte(`{"count":1}`))
	req.Body = strings.NewReader(`{"count":9999}`) // swap body after signing

	rec := leanhttp.NewRecorder()
	handler(rec, req)

	if rec.Code != leanhttp.StatusUnauthorized {
		t.Errorf("expected 401 for body tamper, got %d", rec.Code)
	}
}

// TestSignCallSignsWhatItSends is the property the old signature could not
// have: SignRequest took the body as a separate argument, so signing bytes that
// differed from the ones on the wire was a caller's mistake waiting to happen.
// SignCall reads Call.Body, so the two cannot disagree.
func TestSignCallSignsWhatItSends(t *testing.T) {
	call := leanhttp.Call{Method: "POST", URL: "http://node:7878/v1/jobs?dry=1", Body: []byte("spec")}
	SignCall(&call, "k")

	// The query string is deliberately NOT part of the signature (the canonical
	// string is method, path, body hash), so a signature stays valid across it.
	want := Sign("k", "POST", "/v1/jobs", []byte("spec"))
	if got := call.Header.Get(AuthHeader); got != want {
		t.Errorf("signature = %q, want %q", got, want)
	}

	// An empty key means unauthenticated mode: no header at all.
	plain := leanhttp.Call{Method: "POST", URL: "http://node/v1/jobs"}
	SignCall(&plain, "")
	if plain.Header.Get(AuthHeader) != "" {
		t.Error("an empty key must not sign")
	}
}
