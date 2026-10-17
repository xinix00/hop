package client

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/xinix00/lean/leanhttp"
)

// PutObject writes data to key, overwriting unconditionally — no lease CAS
// (no If-Match / If-None-Match). The hoplockserver stores arbitrary objects;
// the leader uses this to commit cluster state at "state/<cluster>" next to
// the election lease, giving hoplockserver-mode the same durable desired
// state that S3-mode already has.
//
// Single writer by contract: only the current leaseholder calls this, so the
// overwrite needs no conditional guard here (mirrors hoplock/s3's PutObject).
func (b *Backend) PutObject(ctx context.Context, key string, data []byte, contentType string) error {
	if b.URL == "" {
		return errors.New("hoplockserver/client: URL is required")
	}
	hdr := leanhttp.Header{}
	if contentType != "" {
		hdr.Set("Content-Type", contentType)
	}
	// An empty object still needs Content-Length: 0 on the wire, and leanhttp
	// writes that header only for a non-nil body.
	if data == nil {
		data = []byte{}
	}
	resp, err := b.do(ctx, methodPut, key, data, hdr)
	if err != nil {
		return fmt.Errorf("hoplockserver/client: PUT %s: %w", key, err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case leanhttp.StatusOK, leanhttp.StatusCreated:
		// Drain so leanhttp can pool the connection. Not before the switch:
		// the error path needs those bytes.
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	default:
		return b.errForKey("PUT", key, resp)
	}
}

// GetObject reads key. ok is false when the object does not exist — which is
// not an error: an absent state object simply means a clean boot.
func (b *Backend) GetObject(ctx context.Context, key string) (data []byte, ok bool, err error) {
	if b.URL == "" {
		return nil, false, errors.New("hoplockserver/client: URL is required")
	}
	resp, err := b.do(ctx, methodGet, key, nil, nil)
	if err != nil {
		return nil, false, fmt.Errorf("hoplockserver/client: GET %s: %w", key, err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case leanhttp.StatusOK:
	case leanhttp.StatusNotFound:
		return nil, false, nil
	default:
		return nil, false, b.errForKey("GET", key, resp)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, false, fmt.Errorf("hoplockserver/client: read GET %s body: %w", key, err)
	}
	return body, true, nil
}
