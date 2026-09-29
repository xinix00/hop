package types

import "time"

// TaskState represents the current state of a task
type TaskState string

const (
	// TaskQueued en TaskDownloading zijn de startfase, zichtbaar gemaakt:
	// een task heette vanaf zijn geboorte "running", en op HopOS kan de
	// image-download daarvoor minuten duren — tien minuten "running, 0%
	// cpu" op het scherm terwijl er niets draait ("staat ie running issie
	// dood", 07-08). Queued = aangenomen, capaciteit gereserveerd, wacht op
	// een downloadbeurt; Downloading = de bytes stromen (voortgang in
	// Task.Downloaded/ImageSize); Running = de app draait echt.
	//
	// ÉLKE state telt mee voor capaciteit — aanwezigheid is de maat, nooit
	// de state (vrijgeven = het record verwijderen).
	TaskQueued      TaskState = "queued"
	TaskDownloading TaskState = "downloading"
	TaskRunning     TaskState = "running"
	TaskStopping    TaskState = "stopping"
	TaskFailed      TaskState = "failed"
)

// Artifact describes where to download the application binary/assets
type Artifact struct {
	URL      string            `json:"url"`                // http://, https://, s3://
	Match    map[string]string `json:"match,omitempty"`    // node attribute constraints (agent picks first matching artifact)
	Headers  map[string]string `json:"headers,omitempty"`  // HTTP headers (Authorization, X-API-Key, etc.)
	Auth     map[string]string `json:"auth,omitempty"`     // other credentials (S3: access_key/secret_key/region)
	Extract  string            `json:"extract,omitempty"`  // "tar.gz", "zip", "" (empty = raw file, chmod +x, no extraction)
	Filename string            `json:"filename,omitempty"` // override filename for raw downloads (default: basename from URL)
}

// Health check types
const (
	CheckHTTP = "http"
	CheckTCP  = "tcp"
	CheckFile = "file"
)

// HealthCheck configuration for a job
type HealthCheck struct {
	Type             string        `json:"type,omitempty"`              // "http" (default), "tcp", "file"
	Path             string        `json:"path"`                        // http: endpoint path, file: absolute file path
	Port             string        `json:"port,omitempty"`              // http/tcp: named port (default "http")
	Interval         time.Duration `json:"interval,omitempty"`          // check interval (default 10s)
	Timeout          time.Duration `json:"timeout,omitempty"`           // http/tcp: per-request timeout (default 5s)
	InitialTimeout   time.Duration `json:"initial_timeout,omitempty"`   // max time after start to become healthy (default 30s)
	FailureThreshold int           `json:"failure_threshold,omitempty"` // consecutive failures before unhealthy (default 3)
}

// UpdatePolicy defines how job updates are handled
type UpdatePolicy string

const (
	// UpdateRolling replaces instances one at a time (zero downtime)
	UpdateRolling UpdatePolicy = "rolling"

	// UpdateRecreate stops all instances, then starts new version (downtime but simple)
	UpdateRecreate UpdatePolicy = "recreate"

	// UpdateBlueGreen starts new version alongside old, then switches (zero downtime, 2x resources)
	UpdateBlueGreen UpdatePolicy = "blue-green"
)

// Driver identifies which runner executes a job/task
const (
	DriverExec   = "exec"
	DriverDocker = "docker"
	DriverHop    = "hop" // native app image on a HopOS core slot
)

// DriverFor returns the driver for a given image (empty = exec, non-empty = docker)
func DriverFor(image string) string {
	if image != "" {
		return DriverDocker
	}
	return DriverExec
}

// Job defines what the user wants to run.
// Name is the unique key — no separate UUID.
type Job struct {
	Name          string            `json:"name"`                // unique identifier
	Affinity      map[string]string `json:"affinity,omitempty"`  // node attribute constraints (AND logic, equality)
	Driver        string            `json:"driver,omitempty"`    // "exec" (default) or "docker"
	Image         string            `json:"image,omitempty"`     // Docker image (only for driver=docker)
	Artifacts     []Artifact        `json:"artifacts,omitempty"` // binary/assets to download (agent picks first matching)
	User          string            `json:"user,omitempty"`      // run as this user (default: inherit from agent)
	Command       string            `json:"command,omitempty"`
	Count         int               `json:"count,omitempty"` // number of instances (default 1)
	Ports         map[string]int    `json:"ports,omitempty"` // process: port name -> host port (0 = dynamic), docker: port name -> container port
	CPUShares     int               `json:"cpu_shares,omitempty"`
	MemoryLimit   uint64            `json:"memory_limit,omitempty"`
	Env           map[string]string `json:"env,omitempty"`
	Tags          map[string]string `json:"tags,omitempty"`    // labels for discovery/grouping
	Volumes       map[string]string `json:"volumes,omitempty"` // host_path -> task_path (bind-mounted on Linux, symlinked on macOS)
	HealthCheck   *HealthCheck      `json:"health_check,omitempty"`
	MaxRestarts   *int              `json:"max_restarts,omitempty"`   // nil = default (unlimited, with backoff), 0 = no restarts, -1 = unlimited, N = give up after N in restart_window
	RestartWindow time.Duration     `json:"restart_window,omitempty"` // 0 = default (5m), reset restart count if last crash was longer ago
	UpdatePolicy  UpdatePolicy      `json:"update_policy,omitempty"`  // rolling (default) | recreate | blue-green
	Priority      *int              `json:"priority,omitempty"`       // nil=auto(end), 0=top, N=Nth position

	// Deploying is true while an update is rolling out, cleared when it
	// finishes. If the rollout fails — or the leader dies mid-way — it stays
	// set and rides along in the committed snapshot. That is the honest truth
	// ("rollout did not finish", the fleet may be mixed) instead of a false
	// "healthy". Nothing auto-heals it: re-applying the job clears it.
	Deploying bool `json:"deploying,omitempty"`
}

// Task represents a running instance of a Job
type Task struct {
	ID           string         `json:"id"`
	JobName      string         `json:"job_name"`
	Driver       string         `json:"driver"`          // "exec" or "docker"
	Image        string         `json:"image,omitempty"` // Docker image (only for driver=docker)
	Ports        map[string]int `json:"ports"`           // named port -> host port number
	Pid          int            `json:"pid"`
	State        TaskState      `json:"state"`
	StartedAt    time.Time      `json:"started_at"`
	RestartCount int            `json:"restart_count"`
	LastFailedAt time.Time      `json:"last_failed_at,omitempty"`
	// NextRestartAt is when the agent will try again after a crash (the
	// backoff is running); zero while running or when it gave up.
	NextRestartAt time.Time `json:"next_restart_at,omitempty"`
	CPUShares     int       `json:"cpu_shares,omitempty"`
	MemoryLimit   uint64    `json:"memory_limit,omitempty"`
	CPUPercent    float64   `json:"cpu_percent"`
	MemPercent    float64   `json:"mem_percent"`

	// Downloadvoortgang van de startfase (state "downloading"): bytes binnen
	// en de totale image-maat. Alleen gevuld door runners die streamen.
	Downloaded uint64 `json:"downloaded_bytes,omitempty"`
	ImageSize  uint64 `json:"image_size_bytes,omitempty"`
}

// Agent represents a registered agent
type Agent struct {
	ID       string    `json:"id"`
	Endpoint string    `json:"endpoint"` // http://ip:port
	Version  string    `json:"version"`  // Agent version (e.g., "v0.1.0")
	LastSeen time.Time `json:"last_seen"`

	// TempMilliC is de CPU-temperatuur van de node in milligraden Celsius,
	// meegestuurd met elke heartbeat. Eén getal per node, en dat is bewust:
	// wie meer sensoren heeft rapporteert de heetste (max), want dát is het
	// getal waarop je ingrijpt. 0 = onbekend (geen sensor).
	TempMilliC int `json:"temp_milli_c,omitempty"`
}
