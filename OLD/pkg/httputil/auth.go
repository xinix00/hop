package httputil

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/url"

	"github.com/xinix00/lean/leanhttp"
)

// maxBodyBytes caps the request body RequireHMAC buffers to verify the
// signature. The body MUST be read before auth can be checked (the signature
// covers sha256(body) — there is no key on the wire to check first), so this
// is the guard against a pre-auth memory DoS. Set far above any real payload
// (a job spec is kilobytes); it only trips on an absurd body, returning 413.
//
// It MUST stay strictly below leanhttp's own 1 MiB server cap. That cap rejects
// an oversized body before this handler ever runs, so a value at or above it
// makes this guard unreachable in production and impossible to test at all:
// leanhttp.NewRequest refuses to build a body over the server limit (and
// refuses an unsized reader too), so there is no input that reaches here.
// Measured 21-08: at 8 MiB TestRequireHMAC_BodyTooLarge panicked in the
// recorder instead of asserting a 413.
const maxBodyBytes = 512 << 10 // 512 KiB — under leanhttp's 1 MiB, see above

// AuthHeader carries the request signature. The shared key never travels on
// the wire; only this HMAC does.
const AuthHeader = "X-Hop-Auth"

// SigningString builds the canonical string that gets signed:
//
//	METHOD \n PATH \n hex(sha256(body))
//
// PATH is the decoded URL path (no query string); body is the exact request
// body bytes (nil/empty hashes to sha256("")). Both client and server compute
// this identically so the HMAC matches.
func SigningString(method, path string, body []byte) string {
	sum := sha256.Sum256(body)
	return method + "\n" + path + "\n" + hex.EncodeToString(sum[:])
}

// Sign returns the hex HMAC-SHA256 of the canonical string, keyed by key.
func Sign(key, method, path string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(key))
	mac.Write([]byte(SigningString(method, path, body)))
	return hex.EncodeToString(mac.Sum(nil))
}

// SignCall sets the X-Hop-Auth header on call. No-op when key is empty, so
// empty-key mode keeps dev/standalone setups unauthenticated.
//
// It signs call.Body rather than taking the bytes as a separate argument (which
// is what the net/http version did): the signature covers what is actually
// sent, so the two can no longer disagree.
func SignCall(call *leanhttp.Call, key string) {
	if key == "" {
		return
	}
	method := call.Method
	if method == "" {
		method = leanhttp.MethodGet
	}
	path := call.URL
	if u, err := url.Parse(call.URL); err == nil {
		path = u.Path
	}
	call.SetHeader(AuthHeader, Sign(key, method, path, call.Body))
}

// RequireHMAC returns middleware that verifies the X-Hop-Auth signature.
// If key is empty, authentication is disabled (all requests pass through).
//
// The request body is buffered to hash it, then restored so the handler reads
// it normally. The signature binds method + path + body, so a captured request
// cannot be replayed against a different endpoint nor have its body tampered —
// and the key itself never appears on the wire, so it cannot be lifted and
// reused to forge new requests. (A verbatim replay of a captured request is
// still possible; see SECURITY.md for the threat model.)
func RequireHMAC(key string, next leanhttp.Handler) leanhttp.Handler {
	if key == "" {
		return next
	}
	return func(w leanhttp.ResponseWriter, r *leanhttp.Request) {
		var body []byte
		if r.Body != nil {
			// One byte past the cap, so a body that is exactly too large is
			// distinguishable from one that just fits. The cap is enforced here
			// rather than by the transport because the two transports do not
			// agree: leanhttp caps the body itself, net/http does not.
			b, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes+1))
			switch {
			case err != nil:
				WriteError(w, leanhttp.StatusBadRequest, "failed to read body")
				return
			case len(b) > maxBodyBytes:
				WriteError(w, leanhttp.StatusRequestEntityTooLarge, "request body too large")
				return
			}
			body = b
			r.Body = bytes.NewReader(body) // the handler reads it as if untouched
		}
		expected := Sign(key, r.Method, r.Path, body)
		got := r.Header.Get(AuthHeader)
		// Constant-time compare — avoids a timing side channel on the signature.
		if !hmac.Equal([]byte(expected), []byte(got)) {
			WriteError(w, leanhttp.StatusUnauthorized, "unauthorized")
			return
		}
		next(w, r)
	}
}
