package leader

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/xinix00/hop/internal/types"
	"github.com/xinix00/hop/pkg/config"
)

// TestDecodeInitJobs: config-specs (JSON-veldnamen, zoals POST /v1/jobs)
// worden strikte, geldige Jobs — en typo's/halve specs zijn luide fouten.
func TestDecodeInitJobs(t *testing.T) {
	zero := 0
	jobs, err := DecodeInitJobs([]map[string]any{
		{
			"name":         "hopdns",
			"command":      "/usr/local/bin/hopdns",
			"count":        -1,
			"max_restarts": zero,
			"ports":        map[string]any{"dns": 5353},
			"tags":         map[string]any{"lb": "none"},
		},
		{"name": "redis", "image": "redis:7", "count": 2},
	})
	if err != nil {
		t.Fatalf("DecodeInitJobs: %v", err)
	}
	if len(jobs) != 2 {
		t.Fatalf("verwacht 2 jobs, kreeg %d", len(jobs))
	}
	dns := jobs[0]
	if dns.Count != -1 || dns.Ports["dns"] != 5353 || dns.Tags["lb"] != "none" {
		t.Fatalf("velden niet correct gedecodeerd: %+v", dns)
	}
	if dns.MaxRestarts == nil || *dns.MaxRestarts != 0 {
		t.Fatalf("max_restarts 0 moet expliciet 0 blijven (0 = geen restarts), kreeg %v", dns.MaxRestarts)
	}
	if jobs[1].Image != "redis:7" {
		t.Fatalf("image niet gedecodeerd: %+v", jobs[1])
	}

	cases := []struct {
		name string
		spec map[string]any
	}{
		{"onbekend veld", map[string]any{"name": "x", "command": "y", "comand": "typo"}},
		{"naam ontbreekt", map[string]any{"command": "y"}},
		{"niets om te draaien", map[string]any{"name": "x"}},
	}
	for _, c := range cases {
		if _, err := DecodeInitJobs([]map[string]any{c.spec}); err == nil {
			t.Errorf("%s: verwacht een fout, kreeg nil", c.name)
		}
	}
}

// TestDecodeInitJobs_FromConfig: het hele configpad — het bestand zoals de
// operator het schrijft, via config.ClusterConfig (map[string]any) naar Jobs.
// Dekt de aanname dat de configdecoder geneste maps met string-keys levert.
func TestDecodeInitJobs_FromConfig(t *testing.T) {
	src := `{
	  "cluster": {
	    "name": "dev",
	    "init_jobs": [
	      {
	        "name": "hopdns",
	        "command": "/usr/local/bin/hopdns",
	        "count": -1,
	        "ports": {"dns": 5353},
	        "env": {"HOP_MODE": "init"}
	      },
	      {
	        "name": "my-app",
	        "image": "myapp:v1",
	        "count": 2,
	        "tags": {"hoplb-urlprefix": "*.app.local"}
	      }
	    ]
	  }
	}`
	var cfg config.Config
	if err := json.Unmarshal([]byte(src), &cfg); err != nil {
		t.Fatalf("config: %v", err)
	}
	jobs, err := DecodeInitJobs(cfg.Cluster.InitJobs)
	if err != nil {
		t.Fatalf("DecodeInitJobs: %v", err)
	}
	if len(jobs) != 2 {
		t.Fatalf("verwacht 2 jobs, kreeg %d", len(jobs))
	}
	if jobs[0].Ports["dns"] != 5353 || jobs[0].Env["HOP_MODE"] != "init" {
		t.Fatalf("geneste maps niet gedecodeerd: %+v", jobs[0])
	}
	if jobs[1].Tags["hoplb-urlprefix"] != "*.app.local" {
		t.Fatalf("tags niet gedecodeerd: %+v", jobs[1])
	}
}

// TestSeedInitJobs: seeden stored de jobs (settle actief → dispatch komt
// later via reconcile), kent oplopende priorities toe en slaat bestaande
// namen over — een seed overschrijft nooit operator-state.
func TestSeedInitJobs(t *testing.T) {
	store := NewMockJobStore()
	store.StoreJob(&types.Job{Name: "bestaand", Command: "operator-versie"})

	l := New("local-agent", store, nil)
	l.EnableSettle()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.stateLoop(ctx)

	l.SeedInitJobs([]*types.Job{
		{Name: "hopdns", Command: "/usr/local/bin/hopdns", Count: -1},
		{Name: "bestaand", Command: "init-versie"},
		{Name: "app", Image: "myapp:v1", Count: 2},
	})

	if j := store.GetJob("hopdns"); j == nil || j.Count != -1 {
		t.Fatalf("hopdns niet (goed) geseed: %+v", j)
	}
	if j := store.GetJob("app"); j == nil || j.Image != "myapp:v1" {
		t.Fatalf("app niet (goed) geseed: %+v", j)
	}
	if j := store.GetJob("bestaand"); j.Command != "operator-versie" {
		t.Fatalf("seed heeft bestaande job overschreven: %+v", j)
	}

	// Priorities: toegekend en oplopend (hopdns vóór app).
	dns, app := store.GetJob("hopdns"), store.GetJob("app")
	if dns.Priority == nil || app.Priority == nil {
		t.Fatalf("seed hoort priorities toe te kennen: dns=%v app=%v", dns.Priority, app.Priority)
	}
	if *dns.Priority >= *app.Priority {
		t.Fatalf("priorities horen de configvolgorde te volgen: dns=%d app=%d", *dns.Priority, *app.Priority)
	}
}

// Eén init-job, meerdere artifacts: zo overspant één regel meerdere
// architecturen — elk artifact draagt een `match` op node-attributen en de
// agent kiest per node (resolveJobForRun), dus de runner ziet er alsnog precies
// één. De validatie mocht dat eerst niet en keurde de job af met "command or
// image required"; deze test is de wacht bij dat gat.
func TestDecodeInitJobs_MeerdereArtifactsPerArchitectuur(t *testing.T) {
	jobs, err := DecodeInitJobs([]map[string]any{{
		"name":   "welcome",
		"driver": "hop",
		"artifacts": []any{
			map[string]any{"url": "https://example.com/welcome-arm64.elf", "match": map[string]any{"node.arch": "arm64"}},
			map[string]any{"url": "https://example.com/welcome-riscv64.elf", "match": map[string]any{"node.arch": "riscv64"}},
		},
		"memory_limit": 67108864,
		"ports":        map[string]any{"http": 80},
	}})
	if err != nil {
		t.Fatalf("DecodeInitJobs: %v", err)
	}
	if len(jobs) != 1 || len(jobs[0].Artifacts) != 2 {
		t.Fatalf("verwacht 1 job met 2 artifacts, kreeg %+v", jobs)
	}
	if jobs[0].Artifacts[1].Match["node.arch"] != "riscv64" {
		t.Errorf("match niet gedecodeerd: %+v", jobs[0].Artifacts[1])
	}

	// Zonder driver blijft de shorthand gelden voor precies één artifact; met
	// twee moet de driver expliciet zijn, anders is het geen hop-job en heeft
	// hij een command of image nodig.
	if _, err := DecodeInitJobs([]map[string]any{{
		"name": "welcome",
		"artifacts": []any{
			map[string]any{"url": "https://example.com/a.elf"},
			map[string]any{"url": "https://example.com/b.elf"},
		},
	}}); err == nil {
		t.Error("twee artifacts zonder driver: verwacht een fout, kreeg nil")
	}
}
