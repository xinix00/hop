package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// schrijf legt een configuratiebestand in een tijdelijke map en geeft het pad.
func schrijf(t *testing.T, inhoud string) string {
	t.Helper()
	pad := filepath.Join(t.TempDir(), "hop.json")
	if err := os.WriteFile(pad, []byte(inhoud), 0o600); err != nil {
		t.Fatal(err)
	}
	return pad
}

func TestLoadZonderBestandGeeftDefaults(t *testing.T) {
	cfg, err := Load(filepath.Join(t.TempDir(), "bestaat-niet.json"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Node.Port != DefaultConfig().Node.Port || cfg.Cluster.Name != "default" {
		t.Errorf("kreeg %+v, wilde de defaults", cfg)
	}
	// Een leeg --config is hetzelfde geval, en dat is de gewone weg voor een
	// node die alles auto-detecteert.
	if _, err := Load(""); err != nil {
		t.Errorf("Load(\"\"): %v", err)
	}
}

func TestLoadOverschrijftAlleenWatErStaat(t *testing.T) {
	cfg, err := Load(schrijf(t, `{
	  "node": {"id": "node-1", "port": 9000, "attributes": {"core-class": "big"}},
	  "cluster": {"name": "dev", "lock": {"type": "s3", "s3": {"bucket": "hop-leases", "use_path_style": true}}},
	  "api_key": "geheim",
	  "timeouts": {"leader_lease": "45s"}
	}`))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Node.ID != "node-1" || cfg.Node.Port != 9000 {
		t.Errorf("node = %+v", cfg.Node)
	}
	if cfg.Node.Attributes["core-class"] != "big" {
		t.Errorf("attributes = %v", cfg.Node.Attributes)
	}
	if cfg.Cluster.Lock.Type != "s3" || cfg.Cluster.Lock.S3.Bucket != "hop-leases" || !cfg.Cluster.Lock.S3.UsePathStyle {
		t.Errorf("lock = %+v", cfg.Cluster.Lock)
	}
	if cfg.APIKey != "geheim" {
		t.Errorf("api_key = %q", cfg.APIKey)
	}
	if cfg.Timeouts.LeaderLease != 45*time.Second {
		t.Errorf("leader_lease = %v", cfg.Timeouts.LeaderLease)
	}
	// Niet genoemd = default blijft staan, ook binnen een blok dat wél genoemd
	// werd (timeouts noemde alleen leader_lease).
	if cfg.Timeouts.HealthCheckInterval != 5*time.Second {
		t.Errorf("health_check_interval = %v, wilde de default", cfg.Timeouts.HealthCheckInterval)
	}
	if cfg.Paths.StateFile != "./data/state.json" || !cfg.Runner.Isolate {
		t.Errorf("paths/runner defaults weg: %+v %+v", cfg.Paths, cfg.Runner)
	}
}

func TestLoadDuren(t *testing.T) {
	cfg, err := Load(schrijf(t, `{"timeouts": {
	  "health_check_interval": "10s", "health_check_timeout": "1500ms",
	  "node_dead_threshold": "1m30s", "leader_lease": "0s"}}`))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	wil := TimeoutsConfig{
		HealthCheckInterval: 10 * time.Second,
		HealthCheckTimeout:  1500 * time.Millisecond,
		NodeDeadThreshold:   90 * time.Second,
		LeaderLease:         0,
	}
	if cfg.Timeouts != wil {
		t.Errorf("timeouts = %+v, wilde %+v", cfg.Timeouts, wil)
	}
}

func TestLoadWeigert(t *testing.T) {
	cases := map[string]struct {
		inhoud string
		bevat  string
	}{
		"onbekende sleutel": {`{"node": {"prt": 8080}}`, "prt"},
		"onbekende sleutel in timeouts": {
			`{"timeouts": {"leader_leas": "30s"}}`, "leader_leas"},
		"duur als getal": {
			`{"timeouts": {"leader_lease": 30}}`, "durations are strings"},
		"duur zonder eenheid": {
			`{"timeouts": {"leader_lease": "30"}}`, "not a duration"},
		"negatieve duur": {
			`{"timeouts": {"leader_lease": "-30s"}}`, "negative"},
		"geen JSON":             {"node:\n  id: node-1\n", "config is JSON"},
		"YAML-comment bovenaan": {"# Hop config\nnode:\n  id: x\n", "config is JSON"},
		"twee documenten":       {`{"node": {"id": "a"}} {"node": {"id": "b"}}`, "more than one"},
		"stuk JSON":             {`{"node": {"id": `, ""},
		"verkeerd type":         {`{"node": {"port": "8080"}}`, ""},
	}
	for naam, c := range cases {
		t.Run(naam, func(t *testing.T) {
			_, err := Load(schrijf(t, c.inhoud))
			if err == nil {
				t.Fatal("geen fout")
			}
			if c.bevat != "" && !strings.Contains(err.Error(), c.bevat) {
				t.Errorf("fout %q noemt %q niet", err, c.bevat)
			}
		})
	}
}

// De YAML-hint is er voor precies één moment: iemand die na de wissel zijn oude
// bestand meegeeft. Die moet lezen wat er aan de hand is, niet "invalid
// character 'n'".
func TestYAMLHintNoemtHetFormaat(t *testing.T) {
	_, err := Load(schrijf(t, "cluster:\n  name: haas-prod\n"))
	if err == nil {
		t.Fatal("geen fout")
	}
	for _, woord := range []string{"JSON", "YAML", "cluster:"} {
		if !strings.Contains(err.Error(), woord) {
			t.Errorf("melding %q mist %q", err, woord)
		}
	}
}

// Wat Load leest moet Marshal weer kunnen schrijven: anders is een
// configuratie die hop zelf uitschrijft niet terug te lezen.
func TestRondjeMarshal(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Cluster.Name = "haas-prod"
	cfg.Timeouts.HealthCheckTimeout = 2500 * time.Millisecond

	b, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	terug, err := Load(schrijf(t, string(b)))
	if err != nil {
		t.Fatalf("Load van eigen output: %v", err)
	}
	if terug.Timeouts != cfg.Timeouts || terug.Cluster.Name != cfg.Cluster.Name {
		t.Errorf("rondje verloor iets:\n got  %+v\n want %+v", terug, cfg)
	}
}

func TestInitJobsBlijvenRuwJSON(t *testing.T) {
	// init_jobs volgen het job-schema van POST /v1/jobs; ze zijn dus bewust
	// []map[string]any en moeten copy-pasteable blijven.
	cfg, err := Load(schrijf(t, `{"cluster": {"init_jobs": [
	  {"name": "welcome", "driver": "hop", "count": -1, "artifacts": [{"url": "http://x/app.elf"}]}]}}`))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(cfg.Cluster.InitJobs) != 1 || cfg.Cluster.InitJobs[0]["name"] != "welcome" {
		t.Fatalf("init_jobs = %v", cfg.Cluster.InitJobs)
	}
	if got := cfg.Cluster.InitJobs[0]["count"]; got != float64(-1) {
		t.Errorf("count = %v (%T), wilde -1", got, got)
	}
}
