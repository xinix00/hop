//go:build !tamago

package runner

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/xinix00/hop/internal/types"
	"github.com/xinix00/lean/leanhttp"
)

// dockerFake vervangt de daemon op de do-seam: het krijgt de call zoals de
// runner hem zou versturen (method + volledige URL) en antwoordt in-memory.
type dockerFake func(call leanhttp.Call) (*leanhttp.Response, error)

func dockerResponse(status int, body string) *leanhttp.Response {
	return &leanhttp.Response{
		StatusCode: status,
		Status:     "", // reason is niet interessant voor de fakes
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     leanhttp.Header{},
		Length:     int64(len(body)),
	}
}

func dockerTestRunner(fn dockerFake) *DockerRunner {
	r := NewDockerRunner(nil, "", 4)
	r.do = fn
	return r
}

func TestDockerRunnerRequiresImage(t *testing.T) {
	r := NewDockerRunner(nil, "", 4)
	job := &types.Job{
		Name:    "no-image",
		Command: "echo hello",
	}

	task := &types.Task{ID: "test-task"}
	err := r.Run(job, task)
	if err == nil {
		t.Error("Run should fail without image")
	}
}

func TestDockerRunnerStatusNotFound(t *testing.T) {
	r := dockerTestRunner(func(leanhttp.Call) (*leanhttp.Response, error) {
		return dockerResponse(leanhttp.StatusNotFound, `{"message":"not found"}`), nil
	})
	task := &types.Task{
		ID:    "nonexistent-container-id",
		Image: "nginx:latest",
	}

	state, err := r.Status(task)
	if err != nil {
		t.Fatalf("Status should not error: %v", err)
	}
	if state != types.TaskFailed {
		t.Errorf("Status = %q, want %q for nonexistent container", state, types.TaskFailed)
	}
}

func TestDockerRunnerStopNonExistent(t *testing.T) {
	r := dockerTestRunner(func(leanhttp.Call) (*leanhttp.Response, error) {
		return dockerResponse(leanhttp.StatusNotFound, `{"message":"not found"}`), nil
	})
	task := &types.Task{
		ID:    "nonexistent-container-id",
		Image: "nginx:latest",
	}

	// Stop should not panic or return error for nonexistent container
	err := r.Stop(task)
	if err != nil {
		t.Errorf("Stop should not error for nonexistent container: %v", err)
	}
}

func TestDockerRunnerGetStdoutStderrNil(t *testing.T) {
	r := NewDockerRunner(nil, "", 4)

	if r.GetStdout("nonexistent") != nil {
		t.Error("GetStdout should return nil for unknown task")
	}
	if r.GetStderr("nonexistent") != nil {
		t.Error("GetStderr should return nil for unknown task")
	}
}

func TestDockerRunnerCleanup(t *testing.T) {
	r := dockerTestRunner(func(leanhttp.Call) (*leanhttp.Response, error) {
		return dockerResponse(leanhttp.StatusOK, `[]`), nil
	})

	// Een lege, bereikbare daemon heeft niets om op te ruimen.
	err := r.Cleanup()
	if err != nil {
		t.Errorf("Cleanup should not error: %v", err)
	}
}

func TestDockerRunSurfacesPullErrorFromSuccessfulHTTPStream(t *testing.T) {
	var created bool
	r := dockerTestRunner(func(call leanhttp.Call) (*leanhttp.Response, error) {
		if strings.Contains(call.URL, "/images/create") {
			return dockerResponse(leanhttp.StatusOK, `{"status":"Pulling"}
{"errorDetail":{"message":"manifest unknown"},"error":"manifest unknown"}
`), nil
		}
		created = true
		return dockerResponse(leanhttp.StatusCreated, `{}`), nil
	})
	err := r.Run(&types.Job{Name: "pull-error", Image: "missing:latest"}, &types.Task{ID: "pull-error"})
	if err == nil || !strings.Contains(err.Error(), "manifest unknown") {
		t.Fatalf("Run verborg Docker pull-error: %v", err)
	}
	if created {
		t.Fatal("Run probeerde een container te maken na een pull-error")
	}
}

func TestDockerStopReturnsDeleteFailure(t *testing.T) {
	r := dockerTestRunner(func(call leanhttp.Call) (*leanhttp.Response, error) {
		if call.Method == leanhttp.MethodDelete {
			return dockerResponse(leanhttp.StatusInternalServerError, `{"message":"busy"}`), nil
		}
		return dockerResponse(leanhttp.StatusNoContent, ""), nil
	})
	if err := r.Stop(&types.Task{ID: "busy"}); err == nil {
		t.Fatal("Stop rapporteerde succesvolle cleanup terwijl docker rm faalde")
	}
}

func TestDockerLogsRejectOversizedFrameWithoutAllocation(t *testing.T) {
	header := make([]byte, 8)
	header[0] = 1
	binary.BigEndian.PutUint32(header[4:], uint32(maxDockerLogFrame+1))
	r := dockerTestRunner(func(leanhttp.Call) (*leanhttp.Response, error) {
		return &leanhttp.Response{
			StatusCode: leanhttp.StatusOK,
			Status:     "200 OK",
			Body:       io.NopCloser(bytes.NewReader(header)),
			Header:     leanhttp.Header{},
			Length:     int64(len(header)),
		}, nil
	})
	r.startLogStreaming("large-frame", "hop-large-frame")

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if stderr := r.GetStderr("large-frame"); stderr != nil && strings.Contains(strings.Join(stderr.Tail(), ""), "frame too large") {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("oversized Docker-frame werd niet begrensd en gelogd")
}

func TestDockerUsageMeetViaStatsDeltas(t *testing.T) {
	// Twee stats-snapshots: het eerste heeft geen venster (geen vorige stand),
	// het tweede meet de CPU-delta. Geheugen is meteen een feit, mét de
	// page-cache-aftrek die `docker stats` ook doet.
	totals := []uint64{1_000_000_000, 3_000_000_000}
	call := 0
	r := dockerTestRunner(func(leanhttp.Call) (*leanhttp.Response, error) {
		body := fmt.Sprintf(`{"cpu_stats":{"cpu_usage":{"total_usage":%d}},`+
			`"memory_stats":{"usage":104857600,"stats":{"inactive_file":4857600}}}`, totals[call])
		if call < len(totals)-1 {
			call++
		}
		return dockerResponse(leanhttp.StatusOK, body), nil
	})
	task := &types.Task{ID: "usage", CPUShares: 2048}

	cpu, mem, ok := r.Usage(task)
	if cpu != -1 || !ok {
		t.Fatalf("eerste meting: cpu=%v ok=%v, wil cpu=-1 (geen venster) met gerapporteerd geheugen", cpu, ok)
	}
	if mem != 100000000 {
		t.Fatalf("mem=%d, wil 100000000 (usage minus inactive_file)", mem)
	}

	time.Sleep(20 * time.Millisecond)
	cpu, mem, ok = r.Usage(task)
	if !ok || cpu <= 0 {
		t.Fatalf("tweede meting: cpu=%v ok=%v, wil een positieve delta-meting", cpu, ok)
	}
	if mem != 100000000 {
		t.Fatalf("mem=%d, wil 100000000", mem)
	}
}
