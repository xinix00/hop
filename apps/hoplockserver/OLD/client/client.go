// Package client implements hoplock.Backend against a hoplockserver
// instance.
//
// This is the lightweight counterpart to hoplock/s3: same lease semantics,
// no AWS account, no sigv4. Authentication is a single shared X-API-Key
// header.
//
// The HTTP client is github.com/xinix00/lean (leanhttp, plus leanhttps for
// an https URL) rather than net/http, on a host as well as on bare metal.
// net/http links crypto/tls unconditionally, whether or not an https URL is
// ever opened, and this package is linked into the HopOS kernel image that
// boots on a 64MB node: there that cost about 2 MB.
package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/xinix00/hoplock"
	"github.com/xinix00/lean/leanhttp"
	"github.com/xinix00/lean/leanhttps"
	"github.com/xinix00/lean/leantls"
	"github.com/xinix00/lean/leantls/x509verify"
)

// HTTP methods used here. leanhttp has no constants for them.
const (
	methodGet    = "GET"
	methodPut    = "PUT"
	methodDelete = "DELETE"
)

// Status codes leanhttp does not name itself.
const (
	statusConflict           = 409
	statusPreconditionFailed = 412
)

// Backend talks to a hoplockserver over HTTP. The zero value is not
// usable; populate URL and Key, then pass to hoplock.Elector or use the
// Backend methods directly.
//
// A Backend owns a connection pool once it has been used, so use it by
// pointer and do not copy it.
type Backend struct {
	// URL is the base URL of the hoplockserver (e.g. http://lock:8090).
	// Required.
	URL string

	// Key is the object key for the lease record. Required.
	Key string

	// APIKey is sent as X-API-Key. Optional; omit when the server has
	// authentication disabled.
	APIKey string

	// Dial overrides how connections are made. nil — the normal case —
	// derives the transport from the URL scheme: https gets a TLS dialer
	// that validates the server's certificate chain, http dials plain TCP.
	// Set this for a proxy or a unix socket; note that doing so replaces the
	// TLS dialer, and with it the encryption.
	Dial func(ctx context.Context, network, addr string) (net.Conn, error)

	mu   sync.Mutex
	pool *leanhttp.Client
}

var _ hoplock.Backend = (*Backend)(nil)

// Read fetches the lease object. The server's ETag is returned as the
// hoplock.Backend handle.
func (b *Backend) Read(ctx context.Context) (*hoplock.State, string, error) {
	if err := b.validate(); err != nil {
		return nil, "", err
	}
	resp, err := b.do(ctx, methodGet, b.Key, nil, nil)
	if err != nil {
		return nil, "", fmt.Errorf("hoplockserver/client: GET %s: %w", b.Key, err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case leanhttp.StatusOK:
	case leanhttp.StatusNotFound:
		return nil, "", hoplock.ErrNoLease
	default:
		return nil, "", b.errFromResponse("GET", resp)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", fmt.Errorf("hoplockserver/client: read GET body: %w", err)
	}
	var state hoplock.State
	if err := json.Unmarshal(body, &state); err != nil {
		return nil, "", fmt.Errorf("hoplockserver/client: decode lease: %w", err)
	}
	etag := resp.Header.Get("ETag")
	if etag == "" {
		return nil, "", errors.New("hoplockserver/client: GET response missing ETag")
	}
	return &state, etag, nil
}

// Write commits state. An empty prevHandle creates the object only if it
// does not exist; otherwise prevHandle is used as If-Match. Returns
// hoplock.ErrLeaseHeld on 412 PreconditionFailed.
func (b *Backend) Write(ctx context.Context, prevHandle string, state *hoplock.State) (string, error) {
	if err := b.validate(); err != nil {
		return "", err
	}
	body, err := json.Marshal(state)
	if err != nil {
		return "", fmt.Errorf("hoplockserver/client: marshal state: %w", err)
	}
	hdr := leanhttp.Header{"Content-Type": "application/json"}
	if prevHandle == "" {
		hdr.Set("If-None-Match", "*")
	} else {
		hdr.Set("If-Match", prevHandle)
	}

	resp, err := b.do(ctx, methodPut, b.Key, body, hdr)
	if err != nil {
		return "", fmt.Errorf("hoplockserver/client: PUT %s: %w", b.Key, err)
	}
	defer resp.Body.Close()
	// Draining is what returns the connection to the pool: leanhttp only
	// reuses one whose body was read to the end.
	_, _ = io.Copy(io.Discard, resp.Body)

	switch resp.StatusCode {
	case leanhttp.StatusOK, leanhttp.StatusCreated:
	case statusPreconditionFailed, statusConflict:
		return "", hoplock.ErrLeaseHeld
	default:
		return "", b.errFromResponse("PUT", resp)
	}

	etag := resp.Header.Get("ETag")
	if etag == "" {
		return "", errors.New("hoplockserver/client: PUT response missing ETag")
	}
	return etag, nil
}

// Delete removes the lease iff handle still matches the server's ETag.
func (b *Backend) Delete(ctx context.Context, handle string) error {
	if err := b.validate(); err != nil {
		return err
	}
	if handle == "" {
		return hoplock.ErrLeaseHeld
	}
	resp, err := b.do(ctx, methodDelete, b.Key, nil, leanhttp.Header{"If-Match": handle})
	if err != nil {
		return fmt.Errorf("hoplockserver/client: DELETE %s: %w", b.Key, err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body) // drain: see Write

	switch resp.StatusCode {
	case leanhttp.StatusOK, leanhttp.StatusNoContent:
		return nil
	case leanhttp.StatusNotFound:
		return hoplock.ErrNoLease
	case statusPreconditionFailed:
		return hoplock.ErrLeaseHeld
	default:
		return b.errFromResponse("DELETE", resp)
	}
}

func (b *Backend) validate() error {
	if b.URL == "" {
		return errors.New("hoplockserver/client: URL is required")
	}
	if b.Key == "" {
		return errors.New("hoplockserver/client: Key is required")
	}
	return nil
}

// do sends one request for key over this Backend's connection pool. key is
// explicit rather than always b.Key: the lease methods pass b.Key, the
// generic object methods (object.go) pass the state key.
//
// Host, Content-Length, Connection and Accept-Encoding are deliberately not
// in hdr: leanhttp writes those itself and rejects a caller who sets them.
func (b *Backend) do(ctx context.Context, method, key string, body []byte, hdr leanhttp.Header) (*leanhttp.Response, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if hdr == nil {
		hdr = leanhttp.Header{}
	}
	if b.APIKey != "" {
		hdr.Set("X-API-Key", b.APIKey)
	}
	client, err := b.client()
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(leanhttp.Call{
		Method:  method,
		URL:     strings.TrimRight(b.URL, "/") + "/" + strings.TrimLeft(key, "/"),
		Header:  hdr,
		Body:    body,
		Timeout: timeoutFor(ctx),
	})
	if err != nil {
		return nil, err
	}
	// A 204 or 304 needs no handling here: leanhttp applies the bodyless rule
	// itself since 12-08 (RFC 9112 §6.3). This package is one of the two that
	// found that bug — a DELETE answers 204, and without the rule every delete
	// stalled on a read that could never yield a byte.
	return resp, nil
}

// client returns the Backend's connection pool, built on first use.
//
// One pool per Backend and not one per request: a lease is renewed every few
// seconds, and without keep-alive every renew pays a TCP handshake plus, over
// https, a key exchange.
func (b *Backend) client() (*leanhttp.Client, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.pool == nil {
		dial, err := b.dialer()
		if err != nil {
			return nil, err
		}
		b.pool = &leanhttp.Client{DialContext: dial}
	}
	return b.pool, nil
}

// dialer picks the transport from the URL scheme. A nil dialer is the answer
// for http: leanhttp then dials plain TCP itself, and links no TLS.
//
// The roots are nil, meaning x509.SystemCertPool: on a host that is the OS
// trust store, on a HopOS node it is the bundle the image baked in (import
// golang.org/x/crypto/x509roots/fallback in the main). Verification is never
// skipped — the lease server is the one peer where talking to an impostor is
// worse than not talking at all.
func (b *Backend) dialer() (func(ctx context.Context, network, addr string) (net.Conn, error), error) {
	if b.Dial != nil {
		return b.Dial, nil
	}
	u, err := url.Parse(b.URL)
	if err != nil {
		return nil, fmt.Errorf("hoplockserver/client: parse URL: %w", err)
	}
	switch u.Scheme {
	case "https":
		return leanhttps.DialerContext(&leantls.Config{
			VerifyPeer:          x509verify.Chain(nil),
			SignatureAlgorithms: x509verify.SignatureAlgorithms,
		}), nil
	case "http":
		return nil, nil
	default:
		return nil, fmt.Errorf("hoplockserver/client: URL scheme must be http or https, got %q", u.Scheme)
	}
}

// timeoutFor turns a context deadline into leanhttp's per-call timeout.
//
// leanhttp has no context: a deadline it can express (one connection
// deadline covering the body), a bare cancellation it cannot — that would
// cost a goroutine per call to watch, and the connection pool makes closing
// someone else's connection a real hazard. A context without a deadline
// therefore gets no timeout, which is what http.DefaultClient did too.
func timeoutFor(ctx context.Context) time.Duration {
	deadline, ok := ctx.Deadline()
	if !ok {
		return 0
	}
	if d := time.Until(deadline); d > 0 {
		return d
	}
	return time.Nanosecond // already past: fail on the first read, do not block
}

func (b *Backend) errFromResponse(op string, resp *leanhttp.Response) error {
	return b.errForKey(op, b.Key, resp)
}

func (b *Backend) errForKey(op, key string, resp *leanhttp.Response) error {
	const maxBody = 4 << 10
	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	// resp.Status is "403 Forbidden": the code plus the reason phrase the
	// server itself sent, so no status-text table is needed here.
	return fmt.Errorf("hoplockserver/client: %s %s: status %s: %s",
		op, key, resp.Status, strings.TrimSpace(string(body)))
}
