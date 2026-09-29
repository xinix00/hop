// Package config is de configuratie van de orkestrator: de structs, de
// defaults, en het lezen van één bestand.
//
// Het formaat is JSON. Dat was YAML, en de reden om te wisselen is niet
// smaak: dit pakket wordt geïmporteerd door HopOS' kern, en die kern parseert
// nooit een configuratiebestand — hij gebruikt alleen de types en
// DefaultConfig. gopkg.in/yaml.v3 kostte hem tóch 3.440 bytes symbolen én de
// hele regexp-machine erachteraan (62.576 bytes), omdat yaml's package-init
// regexp.MustCompile aanroept en een init nooit wegvalt in de deadcode-pas.
// Alles wat een job of een lease beschrijft was al JSON (jobs/*.json, de
// hop-API, de state-file), dus het formaat wisselt niet, het wordt alleen
// hetzelfde.
package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// Config holds all configuration for the orchestrator
type Config struct {
	Node     NodeConfig     `json:"node"`
	Cluster  ClusterConfig  `json:"cluster"`
	Capacity CapacityConfig `json:"capacity"`
	Paths    PathsConfig    `json:"paths"`
	Runner   RunnerConfig   `json:"runner"`
	Timeouts TimeoutsConfig `json:"timeouts"`
	APIKey   string         `json:"api_key"`
}

// NodeConfig holds node-specific configuration
type NodeConfig struct {
	ID   string `json:"id"`
	IP   string `json:"ip"`
	Port int    `json:"port"`
	// Network, when set, makes hop auto-pick the interface IP within this
	// CIDR (e.g. "10.0.0.0/24"). Used when IP is empty. Useful for clusters
	// that need to advertise on a specific LAN/VPN instead of the default
	// route. Ignored if IP is set explicitly.
	Network    string            `json:"network"`
	Attributes map[string]string `json:"attributes"` // user-defined node attributes (merged with auto-detected)
}

// ClusterConfig holds cluster-wide configuration.
type ClusterConfig struct {
	Name string     `json:"name"`
	Lock LockConfig `json:"lock"`

	// InitJobs seed the cluster on a clean boot: a leader that starts with
	// no committed snapshot and no local jobs schedules these once, through
	// the normal upsert path. They are NOT continuously enforced — deleting
	// a seeded job sticks until the next clean boot (deletion is absence).
	// Field names follow the job JSON schema (same as POST /v1/jobs), so a
	// spec is copy-pastable between config and API.
	InitJobs []map[string]any `json:"init_jobs"`
}

// LockConfig configures the hoplock backend used for leader election. The
// zero value means "no backend" (standalone mode); an in-memory backend is
// substituted at startup.
type LockConfig struct {
	// Type selects the backend implementation. Supported values:
	//   - "" or "hoplockserver": talk to a hoplockserver over HTTP.
	//   - "s3": talk to an S3-compatible object store via sigv4.
	//   - "mem": in-process memory (standalone / test).
	Type string `json:"type"`

	// URL is the hoplockserver base URL (e.g. http://lock:8090). Used
	// when Type is "hoplockserver".
	URL string `json:"url"`

	// Key is the lease object key. Defaults to "clusters/<cluster_name>/lease.json".
	Key string `json:"key"`

	// APIKey is the X-API-Key header for hoplockserver. Optional.
	APIKey string `json:"api_key"`

	// S3 holds the S3-backend configuration when Type is "s3".
	S3 S3LockConfig `json:"s3"`
}

// S3LockConfig configures hoplock/s3 for callers who already operate an
// S3-compatible object store and prefer not to run hoplockserver.
type S3LockConfig struct {
	Endpoint        string `json:"endpoint"`
	Bucket          string `json:"bucket"`
	Region          string `json:"region"`
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
	SessionToken    string `json:"session_token"`
	UsePathStyle    bool   `json:"use_path_style"`
}

// CapacityConfig caps how much CPU/memory hop will commit on this node.
// Both fields are optional: 0 means "use auto-detected hardware". Setting them
// to a positive value lower than the hardware tells hop to schedule fewer/
// smaller jobs — useful when the node is shared with non-hop workloads.
type CapacityConfig struct {
	CPUShares int    `json:"cpu_shares"`
	Memory    uint64 `json:"memory"`
}

// PathsConfig holds filesystem paths configuration
type PathsConfig struct {
	StateFile  string `json:"state_file"`
	RootfsBase string `json:"rootfs_base"`
}

// RunnerConfig holds runner configuration
type RunnerConfig struct {
	Isolate      bool   `json:"isolate"`       // Enable process isolation (chroot on Linux, sandbox on macOS). Default: true
	DockerSocket string `json:"docker_socket"` // Docker daemon socket path. Default: /var/run/docker.sock
	// LogTailLines is how many lines of stdout/stderr per task are kept in
	// memory (served first by /logs). Default 50 — small on purpose, hop runs
	// on boards with a few hundred MB.
	LogTailLines int `json:"log_tail_lines"`
	// LogKeepSeconds is how long the tail of a STOPPED task stays
	// retrievable, so a crash can be read after the fact. Default 300.
	LogKeepSeconds int `json:"log_keep_seconds"`
}

// TimeoutsConfig holds timeout configuration. Durations are written the way Go
// writes them ("30s", "1m30s"); see [TimeoutsConfig.UnmarshalJSON].
type TimeoutsConfig struct {
	HealthCheckInterval time.Duration `json:"health_check_interval"`
	HealthCheckTimeout  time.Duration `json:"health_check_timeout"`
	NodeDeadThreshold   time.Duration `json:"node_dead_threshold"`
	LeaderLease         time.Duration `json:"leader_lease"`
}

// UnmarshalJSON leest de timeouts met duren als string: "30s", "1m30s",
// "500ms" — dezelfde notatie die YAML hier had, en die time.ParseDuration
// leest.
//
// Een getal is bewust een fout en niet "dan zijn het nanoseconden": een
// leader_lease van 30 die stil 30ns wordt, is een cluster dat zijn leider
// duizenden keren per seconde verliest. Liever een melding bij het opstarten
// dan die zoektocht.
func (t *TimeoutsConfig) UnmarshalJSON(b []byte) error {
	// De schaduw-struct heeft dezelfde velden als string-pointers: nil
	// betekent "stond niet in het bestand", en dan blijft de default staan.
	var raw struct {
		HealthCheckInterval *string `json:"health_check_interval"`
		HealthCheckTimeout  *string `json:"health_check_timeout"`
		NodeDeadThreshold   *string `json:"node_dead_threshold"`
		LeaderLease         *string `json:"leader_lease"`
	}
	// Eigen decoder, want DisallowUnknownFields van de buitenste decoder geldt
	// niet in een eigen UnmarshalJSON: een typefout in dit blok zou anders
	// juist hier stil wegvallen.
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&raw); err != nil {
		return fmt.Errorf("timeouts: %w (durations are strings like \"30s\")", err)
	}

	for _, veld := range []struct {
		naam string
		bron *string
		doel *time.Duration
	}{
		{"health_check_interval", raw.HealthCheckInterval, &t.HealthCheckInterval},
		{"health_check_timeout", raw.HealthCheckTimeout, &t.HealthCheckTimeout},
		{"node_dead_threshold", raw.NodeDeadThreshold, &t.NodeDeadThreshold},
		{"leader_lease", raw.LeaderLease, &t.LeaderLease},
	} {
		if veld.bron == nil {
			continue
		}
		d, err := time.ParseDuration(*veld.bron)
		if err != nil {
			return fmt.Errorf("timeouts.%s: %q is not a duration (try \"30s\")", veld.naam, *veld.bron)
		}
		if d < 0 {
			return fmt.Errorf("timeouts.%s: %s is negative", veld.naam, d)
		}
		*veld.doel = d
	}
	return nil
}

// MarshalJSON schrijft de duren terug als string, zodat een geschreven
// configuratie weer te lezen is met dezelfde code.
func (t TimeoutsConfig) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		HealthCheckInterval string `json:"health_check_interval"`
		HealthCheckTimeout  string `json:"health_check_timeout"`
		NodeDeadThreshold   string `json:"node_dead_threshold"`
		LeaderLease         string `json:"leader_lease"`
	}{
		t.HealthCheckInterval.String(),
		t.HealthCheckTimeout.String(),
		t.NodeDeadThreshold.String(),
		t.LeaderLease.String(),
	})
}

// DefaultConfig returns a configuration with sensible defaults
func DefaultConfig() *Config {
	return &Config{
		Node: NodeConfig{
			ID:   "",
			IP:   "",
			Port: 8080,
		},
		Cluster: ClusterConfig{
			Name: "default",
			Lock: LockConfig{}, // empty = standalone
		},
		Capacity: CapacityConfig{
			// 0 = auto-detect; operators override to cap below hardware.
			CPUShares: 0,
			Memory:    0,
		},
		Paths: PathsConfig{
			StateFile:  "./data/state.json",
			RootfsBase: "/tmp/hop",
		},
		Runner: RunnerConfig{
			Isolate:        true,
			DockerSocket:   "/var/run/docker.sock",
			LogTailLines:   50,
			LogKeepSeconds: 300,
		},
		Timeouts: TimeoutsConfig{
			HealthCheckInterval: 5 * time.Second,
			HealthCheckTimeout:  5 * time.Second,
			NodeDeadThreshold:   30 * time.Second,
			LeaderLease:         30 * time.Second,
		},
	}
}

// Load leest een JSON-configuratiebestand bovenop [DefaultConfig]. Een bestand
// dat niet bestaat is geen fout: dan zijn de defaults de configuratie (dat is
// ook wat een lege --config doet).
//
// Onbekende sleutels zijn wél een fout. Een typefout in een sleutel is anders
// een instelling die er lijkt te staan en niets doet — en dat kost een avond,
// terwijl het bestand van de operator zelf is en één regel verderop staat.
func Load(path string) (*Config, error) {
	cfg := DefaultConfig()

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return nil, err
	}

	if err := hintYAML(data); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}

	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(cfg); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	// Eén document per bestand: wat er achter staat is niet gelezen, en stil
	// negeren is precies zo verwarrend als een genegeerde sleutel.
	if dec.More() {
		return nil, fmt.Errorf("%s: more than one JSON document in the file", path)
	}
	return cfg, nil
}

// hintYAML geeft de melding die iemand met een oud bestand nodig heeft. Zonder
// hem is de fout "invalid character 'n' looking for beginning of value" op regel
// 1 van een YAML-bestand, en dat leest niemand als "het formaat is gewisseld".
func hintYAML(data []byte) error {
	kop := bytes.TrimLeft(data, " \t\r\n")
	if len(kop) == 0 || kop[0] == '{' {
		return nil
	}
	return fmt.Errorf("config is JSON, and this is not (it starts with %q) — a YAML config converts to JSON one-to-one: the same keys, in braces, with durations quoted (\"30s\")",
		string(kop[:min(len(kop), 24)]))
}
