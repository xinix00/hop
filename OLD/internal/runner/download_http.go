package runner

import (
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/xinix00/hop/internal/types"
	"github.com/xinix00/hop/pkg/httputil"
	"github.com/xinix00/lean/leanhttp"
)

const downloadTimeout = 5 * time.Minute

// artifactClient is the runner's one outbound client, shared with the streamed
// image path (hopos_stream.go): pooled connections, so several artifacts from
// the same host cost one handshake instead of one each. The deadline sits on the
// call, because a streamed image is bounded by silence, not by total time.
var artifactClient httputil.Client

// downloadHTTP downloads from HTTP/HTTPS URL
func downloadHTTP(artifact *types.Artifact, destPath string) error {
	// Timeout on the call, not on a context: Call.Timeout covers the body read
	// too, on every platform (leanhttp arms the socket deadline itself).
	call := leanhttp.Call{Method: leanhttp.MethodGet, URL: artifact.URL, Timeout: downloadTimeout}

	// Add custom headers
	for k, v := range artifact.Headers {
		call.SetHeader(k, v)
	}

	// Helper: Basic Auth from username/password
	if artifact.Auth["username"] != "" && artifact.Auth["password"] != "" {
		auth := base64.StdEncoding.EncodeToString(
			[]byte(artifact.Auth["username"] + ":" + artifact.Auth["password"]),
		)
		call.SetHeader("Authorization", "Basic "+auth)
	}

	resp, err := artifactClient.Do(call)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != leanhttp.StatusOK {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, resp.Status)
	}

	out, err := os.Create(destPath)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, resp.Body)
	return err
}
