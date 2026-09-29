package httputil

// client.go — hop's outbound HTTP client: leanhttp with keep-alive pools on
// every platform. Until 08-2026 this was pkg/hophttp's seam (net/http on a
// host, leanhttp on a node); leanhttp grew everything the seam existed for
// (Call.Context, HeaderTimeout, a mux, a Server), so now there is ONE
// transport and the host runs exactly the bytes the node runs.
//
// Keep-alive is not a nicety here: an agent heartbeats its leader every ten
// seconds and the satellites poll every five, so a fresh TCP handshake per
// call would be the dominant cost of the whole conversation.
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
func (cl *Client) Do(call leanhttp.Call) (*leanhttp.Response, error) {
	return cl.DoContext(context.Background(), call)
}

// DoContext is Do with a lifetime: cancelling ctx aborts the call, which is
// how a proxied stream stops when its own client walks away.
func (cl *Client) DoContext(ctx context.Context, call leanhttp.Call) (*leanhttp.Response, error) {
	if call.Timeout == 0 {
		call.Timeout = cl.Timeout
	}
	call.Context = ctx
	return cl.poolFor(call.URL).Do(call)
}

// poolFor picks the pool that matches the scheme, wiring the encrypted one on
// first use so a process that only talks plain http never runs a handshake it
// does not need.
//
// The trust model is a real certificate chain: Chain(nil) verifies against the
// system trust store — the OS store on a host, or the bare-metal store a HopOS
// kernel installs by importing golang.org/x/crypto/x509roots/fallback. Without
// roots this fails loudly at verification time, which is the right way round:
// a silent downgrade would be worse than no https at all.
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
// network underneath changed (a new lease, a moved gateway): a pooled
// connection to the old path is a slower failure than a new connection.
func (cl *Client) CloseIdle() {
	cl.plain.CloseIdle()
	cl.tls.CloseIdle()
}
