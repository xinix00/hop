package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"time"

	"github.com/xinix00/lean/leanhttp"

	"github.com/xinix00/hop/internal/types"
)

// De replace-toelating (/run?replace=1): de opvolger hoeft niet naast zijn
// voorganger te passen — die reservering wordt weggedacht — maar de voorganger
// verdwijnt pas ná een geslaagde toelating. Dit is het update-pad op een node
// zonder headroom; vóór deze vorm was de enige uitweg de preemptie-pas, die op
// 01-08 een onschuldige buurman offerde (welcome-update → cloudflared weg).
func TestHandleRunReplaceVervangEigenTaakBinnenVolleNode(t *testing.T) {
	cfg := testConfig() // capaciteit: 1000 shares, 1GB
	agent := New(cfg, "node-1", NewMockRunner())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	// De node vol: welcome (600MB) + buurman (400MB) = precies de 1GB.
	run := func(job types.Job, replace bool) *leanhttp.Recorder {
		body, _ := json.Marshal(job)
		url := "/run"
		if replace {
			url += "?replace=1"
		}
		req := leanhttp.NewRequest(leanhttp.MethodPost, url, bytes.NewReader(body))
		w := leanhttp.NewRecorder()
		agent.handleRun(w, req)
		return w
	}
	if w := run(types.Job{Name: "welcome", Command: "v1", MemoryLimit: 600 << 20}, false); w.Code != leanhttp.StatusAccepted {
		t.Fatalf("welcome v1: status %d", w.Code)
	}
	if w := run(types.Job{Name: "buurman", Command: "b", MemoryLimit: 400 << 20}, false); w.Code != leanhttp.StatusAccepted {
		t.Fatalf("buurman: status %d", w.Code)
	}

	// Zonder replace is er geen plek voor een tweede welcome — het oude
	// gedrag, en precies wat de leader dan als errNoCapacity terugkrijgt.
	if w := run(types.Job{Name: "welcome", Command: "v2", MemoryLimit: 600 << 20}, false); w.Code != leanhttp.StatusServiceUnavailable {
		t.Fatalf("welcome v2 zonder replace: status %d, wil 503", w.Code)
	}

	// Mét replace past hij: de voorganger telt niet mee en wordt verruild.
	if w := run(types.Job{Name: "welcome", Command: "v2", MemoryLimit: 600 << 20}, true); w.Code != leanhttp.StatusAccepted {
		t.Fatalf("welcome v2 met replace: status %d, wil 202", w.Code)
	}
	time.Sleep(50 * time.Millisecond) // de achtergrond-swap (stop voorganger, start opvolger)

	kaart := query(agent, func(s *agentState) map[string]int {
		m := map[string]int{}
		for _, task := range s.tasks {
			m[task.JobName]++
		}
		return m
	})
	if kaart["welcome"] != 1 {
		t.Errorf("welcome: %d taken na replace, wil 1 (de opvolger)", kaart["welcome"])
	}
	if kaart["buurman"] != 1 {
		t.Errorf("buurman: %d taken, wil 1 — replace mag de buurman niet raken", kaart["buurman"])
	}

	// En een opvolger die écht niet past (groter dan de node minus de buurman)
	// wordt geweigerd MET de voorganger nog intact — de KeepsOld-invariant.
	if w := run(types.Job{Name: "welcome", Command: "v3", MemoryLimit: 700 << 20}, true); w.Code != leanhttp.StatusServiceUnavailable {
		t.Fatalf("welcome v3 (te groot) met replace: status %d, wil 503", w.Code)
	}
	nog := query(agent, func(s *agentState) int {
		n := 0
		for _, task := range s.tasks {
			if task.JobName == "welcome" {
				n++
			}
		}
		return n
	})
	if nog != 1 {
		t.Errorf("welcome na geweigerde replace: %d taken, wil 1 — een weigering mag niets stoppen", nog)
	}
}
