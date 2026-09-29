//go:build tamago

package hophttp

// client_tamago.go — leanhttp underneath, for hop inside the HopOS kernel.
//
// Keep-alive is on (leanhttp.Client pools per host) and that is not a nicety
// here: an agent heartbeats its leader every second, so a fresh TCP handshake
// per call would be the dominant cost of the whole conversation.
//
// There are TWO pools, and the split is the scheme. leanhttp links no TLS and
// refuses an https URL unless the caller hands it a dialer that returns an
// encrypted connection; a pool carries one dialer, so plain and encrypted
// cannot share one. Getting this wrong is not subtle in hindsight but it was
// invisible at build time: MEASURED on hardware 12-08, a node with a plain-only
// client failed every app placement within 0.16s, because a HopOS artifact comes
// from https://github.com/... and the runner never got past the refusal.

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/xinix00/lean/leanhttp"
	"github.com/xinix00/lean/leanhttps"
	"github.com/xinix00/lean/leantls"
	"github.com/xinix00/lean/leantls/x509verify"
)

// Client sends calls. The zero value works and is safe for concurrent use.
type Client struct {
	// Timeout covers a whole call unless the Call sets its own; 0 = none.
	Timeout time.Duration

	once  sync.Once
	plain leanhttp.Client
	tls   leanhttp.Client
}

// Do sends one call.
func (cl *Client) Do(call Call) (*Response, error) {
	return cl.DoContext(context.Background(), call)
}

// DoContext is Do with a lifetime: cancelling ctx aborts the call, matching the
// host transport and stopping a proxied stream when its own client walks away.
func (cl *Client) DoContext(ctx context.Context, call Call) (*Response, error) {
	timeout := call.Timeout
	if timeout == 0 {
		timeout = cl.Timeout
	}
	resp, err := cl.poolFor(call.URL).Do(leanhttp.Call{
		Context:       ctx,
		Method:        call.Method,
		URL:           call.URL,
		Header:        call.Header,
		Body:          call.Body,
		Timeout:       timeout,
		HeaderTimeout: call.HeaderTimeout,
	})
	if err != nil {
		return nil, err
	}
	return &Response{
		StatusCode: resp.StatusCode,
		Status:     resp.Status,
		Header:     resp.Header,
		Body:       resp.Body,
		Length:     resp.Length,
	}, nil
}

// poolFor picks the pool that matches the scheme, wiring the encrypted one on
// first use so a node that only talks plain http never links a handshake it
// does not run.
//
// The trust model is a real certificate chain against the system roots, because
// the peer is the public internet (GitHub releases, an object store). A node has
// no certificate store, so the BINARY has to carry the roots — HopOS' kernel
// does that by importing golang.org/x/crypto/x509roots/fallback. Without them
// this fails loudly at verification time, which is the right way round: a silent
// downgrade would be worse than no https at all.
func (cl *Client) poolFor(url string) *leanhttp.Client {
	if !strings.HasPrefix(url, "https://") {
		return &cl.plain
	}
	cl.once.Do(func() {
		cl.tls.DialContext = leanhttps.DialerContext(&leantls.Config{
			VerifyPeer:          x509verify.Chain(nil),
			SignatureAlgorithms: x509verify.SignatureAlgorithms,
		})
	})
	return &cl.tls
}

// CloseIdle drops the pooled connections of both pools. Worth calling when the
// network under the node changed (a new lease, a moved gateway): a pooled
// connection to the old path is a slower failure than a new connection.
func (cl *Client) CloseIdle() {
	cl.plain.CloseIdle()
	cl.tls.CloseIdle()
}
