package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/xinix00/hop/internal/types"
	"github.com/xinix00/lean/leanhttp"
)

// poolRunner is a hop runner that reports a largest placeable partition, the
// way HopOS does through hopos.PoolReporter.
type poolRunner struct {
	*MockRunner
	largest uint64
}

func (p poolRunner) PoolLargest() uint64 { return p.largest }

// A sum is not a hole. This is the case that made a LicheeRV fall over three
// times on 19-08: pool 222 MB with 126 and 36 placed, so 60 MB free — but in
// holes of 28 and 32. Admitting a 36 MB job on the sum means reserve, fail to
// place, hand back, and again five seconds later; that loop chokes the agent
// port and the node dies through missed watchdog pets (see the 17/18-08 note in
// handlers.go). The refusal has to happen here, before anything is reserved.
func TestHopJobLargerThanTheLargestHoleIsRefused(t *testing.T) {
	for _, c := range []struct {
		name    string
		limit   uint64
		largest uint64
		want    int
	}{
		{"larger than every hole", 36 << 20, 32 << 20, leanhttp.StatusServiceUnavailable},
		{"exactly the largest hole", 32 << 20, 32 << 20, leanhttp.StatusAccepted},
		{"smaller than the hole", 8 << 20, 32 << 20, leanhttp.StatusAccepted},
		{"node cannot answer", 36 << 20, 0, leanhttp.StatusAccepted},
	} {
		t.Run(c.name, func(t *testing.T) {
			cfg := testConfig()
			// Plenty on the sum, so only the hole can refuse.
			cfg.Capacity.Memory = 222 << 20
			cfg.Capacity.CPUShares = 4096
			cfg.Node.Attributes = map[string]string{"node.os": "hopos"}
			agent := New(cfg, "node-1", NewMockRunner())
			agent.WithHopRunner(poolRunner{MockRunner: NewMockRunner(), largest: c.largest})

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go agent.stateLoop(ctx)
			time.Sleep(10 * time.Millisecond)

			job := types.Job{
				Name:        "cloudflared",
				Driver:      types.DriverHop,
				Artifacts:   []types.Artifact{{URL: "http://example.test/app.elf"}},
				MemoryLimit: c.limit,
				CPUShares:   1024,
			}
			body, _ := json.Marshal(job)
			w := leanhttp.NewRecorder()
			agent.handleRun(w, leanhttp.NewRequest(leanhttp.MethodPost, "/run", bytes.NewReader(body)))

			if w.Code != c.want {
				t.Fatalf("status = %d, want %d (limit %d MB, largest hole %d MB)",
					w.Code, c.want, c.limit>>20, c.largest>>20)
			}
			// A refusal must leave no reservation behind: that phantom is what
			// made the reported capacity flap and made the node refuse OTHER
			// jobs that did fit.
			tasks := query(agent, func(s *agentState) int { return len(s.tasks) })
			if c.want == leanhttp.StatusServiceUnavailable && tasks != 0 {
				t.Errorf("refused job left %d task(s) in the state", tasks)
			}
			if c.want == leanhttp.StatusAccepted && tasks != 1 {
				t.Errorf("admitted job created %d task(s), want 1", tasks)
			}
		})
	}
}

// A rolling replace must not be blocked by the hole check: at admission the
// predecessor still holds its partition (it is stopped afterwards, on purpose),
// so the largest hole is legitimately too small.
func TestReplaceIsNotBlockedByTheHole(t *testing.T) {
	cfg := testConfig()
	cfg.Capacity.Memory = 222 << 20
	cfg.Capacity.CPUShares = 4096
	cfg.Node.Attributes = map[string]string{"node.os": "hopos"}
	agent := New(cfg, "node-1", NewMockRunner())
	agent.WithHopRunner(poolRunner{MockRunner: NewMockRunner(), largest: 32 << 20})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	job := types.Job{
		Name:        "stulp",
		Driver:      types.DriverHop,
		Artifacts:   []types.Artifact{{URL: "http://example.test/stulp.elf"}},
		MemoryLimit: 36 << 20, // larger than the hole, and that is correct here
		CPUShares:   1024,
	}
	// First placement: with the hole at 32 MB this one is refused, so seed the
	// state the way a running predecessor would.
	body, _ := json.Marshal(job)
	w := leanhttp.NewRecorder()
	agent.handleRun(w, leanhttp.NewRequest(leanhttp.MethodPost, "/run?replace=1", bytes.NewReader(body)))
	if w.Code != leanhttp.StatusAccepted {
		t.Fatalf("replace refused with %d — a rolling update of a job that fills its region would be impossible", w.Code)
	}
}
