package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/xinix00/hop/internal/types"
	"github.com/xinix00/hop/pkg/httputil"
	"github.com/xinix00/lean/leanhttp"
)

var (
	leaderAddr string
	apiKey     string
)

func main() {
	flag.StringVar(&leaderAddr, "leader", envOr("HOP_LEADER", "localhost:9080"), "Leader address")
	flag.StringVar(&apiKey, "api-key", os.Getenv("HOP_API_KEY"), "API key for authentication")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: run [flags] <command> [args]\n\nCommands:\n  apply    Create or update a job (upsert by name)\n  delete   Delete a job and all its tasks\n  status   Show cluster status\n  agents   List agents or show agent details\n  logs     Stream task logs\n\nFlags:\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		flag.Usage()
		os.Exit(1)
	}

	var err error
	switch args[0] {
	case "apply":
		err = runApply(args[1:])
	case "delete":
		err = runDelete(args[1:])
	case "status":
		err = runStatus()
	case "agents":
		err = runAgents(args[1:])
	case "logs":
		err = runLogs(args[1:])
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", args[0])
		flag.Usage()
		os.Exit(1)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// stringList collects a repeatable flag (flag.Value).
type stringList []string

func (s *stringList) String() string     { return strings.Join(*s, ",") }
func (s *stringList) Set(v string) error { *s = append(*s, v); return nil }

func runApply(args []string) error {
	fs := flag.NewFlagSet("apply", flag.ExitOnError)
	name := fs.String("name", "", "Job name (required)")
	command := fs.String("command", "", "Command to run")
	image := fs.String("image", "", "Docker image")
	driver := fs.String("driver", "", "Driver: exec (default), docker, or hop (HopOS slot; needs --artifact, no command/image)")
	count := fs.Int("count", 0, "Number of instances (default 1, -1 = all agents)")
	cpu := fs.Int("cpu", 0, "CPU shares")
	memory := fs.String("memory", "", "Memory limit (e.g., 512M, 1G)")
	priorityFlag := fs.Int("priority", -1, "Scheduling priority (0=highest, omit to append at end)")
	updatePolicy := fs.String("update-policy", "rolling", "Update policy if job exists: rolling, recreate, or blue-green")
	checkType := fs.String("check-type", "", "Health check type: http, tcp, or file")
	checkPath := fs.String("check-path", "", "Health check path")
	checkPort := fs.String("check-port", "", "Health check port name")
	checkFailures := fs.Int("check-failures", 0, "Consecutive failures before unhealthy")

	// Repeatable flags via flag.Value — the flag package parses them in place,
	// so no hand-rolled argument surgery is needed.
	var envFlags, artifactFlags, affinityFlags, tagFlags stringList
	fs.Var(&envFlags, "env", "Environment variable KEY=VALUE (repeatable; value may contain commas)")
	fs.Var(&artifactFlags, "artifact", "Artifact URL, optionally prefixed with match constraints: k=v[,k=v]::URL (repeatable)")
	fs.Var(&affinityFlags, "affinity", "Node affinity k=v[,k=v] (repeatable)")
	fs.Var(&tagFlags, "tag", "Job tag k=v[,k=v] (repeatable)")

	_ = fs.Parse(args)

	if *name == "" {
		return fmt.Errorf("--name is required")
	}
	// hop-driver-jobs (HopOS-slots) dragen een artifact i.p.v. command/image;
	// alle andere drivers eisen zoals vanouds één van beide.
	if *driver == "hop" {
		if len(artifactFlags) == 0 {
			return fmt.Errorf("--driver hop requires --artifact <url>")
		}
	} else if *command == "" && *image == "" {
		return fmt.Errorf("either --command or --image is required")
	}

	job := buildJob(*name, *command, *image, *driver, *count, *cpu, *memory, *priorityFlag,
		envFlags, artifactFlags, affinityFlags, tagFlags,
		*checkType, *checkPath, *checkPort, *checkFailures, *updatePolicy)

	resp, err := doRequest("POST", "/v1/jobs", job)
	if err != nil {
		return err
	}

	var result map[string]string
	if err := json.Unmarshal(resp, &result); err != nil {
		return err
	}

	switch result["status"] {
	case "updated":
		fmt.Printf("Job '%s' updated (policy=%s)\n", job.Name, result["policy"])
	case "pending":
		fmt.Printf("Job '%s' stored — pending dispatch: %s\n", job.Name, result["error"])
	default:
		fmt.Printf("Job '%s' dispatched\n", job.Name)
	}
	return nil
}

// buildJob constructs a Job from CLI flags
func buildJob(name, command, image, driver string, count, cpu int, memory string, priorityFlag int,
	envFlags, artifactFlags, affinityFlags, tagFlags []string,
	checkType, checkPath, checkPort string, checkFailures int,
	updatePolicy string) types.Job {

	job := types.Job{
		Name:         name,
		Command:      command,
		Image:        image,
		Driver:       driver,
		Count:        count,
		CPUShares:    cpu,
		UpdatePolicy: types.UpdatePolicy(updatePolicy),
	}

	if len(tagFlags) > 0 {
		job.Tags = parseKV(strings.Join(tagFlags, ","))
	}

	if priorityFlag >= 0 {
		p := priorityFlag
		job.Priority = &p
	}

	for _, art := range artifactFlags {
		a := types.Artifact{}
		if idx := strings.Index(art, "::"); idx > 0 {
			a.URL = art[idx+2:]
			a.Match = parseKV(art[:idx])
		} else {
			a.URL = art
		}
		job.Artifacts = append(job.Artifacts, a)
	}

	if memory != "" {
		memBytes, err := parseMemory(memory)
		if err == nil {
			job.MemoryLimit = memBytes
		}
	}

	if len(envFlags) > 0 {
		// One pair per --env flag: values keep their commas and equals signs
		// (the old comma-join silently split FOO=a,b into two half-pairs).
		job.Env = parsePairs(envFlags)
	}

	if len(affinityFlags) > 0 {
		job.Affinity = parseKV(strings.Join(affinityFlags, ","))
	}

	if checkType != "" || checkPath != "" {
		job.HealthCheck = &types.HealthCheck{
			Type:             checkType,
			Path:             checkPath,
			Port:             checkPort,
			FailureThreshold: checkFailures,
		}
	}

	return job
}

func runDelete(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("job name required")
	}
	_, err := doRequest("DELETE", "/v1/jobs/"+args[0], nil)
	if err != nil {
		return err
	}
	fmt.Printf("Job deleted (%s)\n", args[0])
	return nil
}

func runStatus() error {
	resp, err := doRequest("GET", "/v1/status", nil)
	if err != nil {
		return err
	}

	var status struct {
		Agents      int            `json:"agents"`
		Settling    bool           `json:"settling"`
		TotalPlaced int            `json:"total_placed"`
		Placed      map[string]int `json:"placed"`
	}
	if err := json.Unmarshal(resp, &status); err != nil {
		return err
	}

	jobsResp, err := doRequest("GET", "/v1/jobs", nil)
	if err != nil {
		return err
	}

	var jobs []*types.Job
	if err := json.Unmarshal(jobsResp, &jobs); err != nil {
		return err
	}

	fmt.Printf("Leader:  %s\n", leaderAddr)
	fmt.Printf("Agents:  %d\n", status.Agents)
	fmt.Printf("Placed:  %d total\n", status.TotalPlaced)
	if status.Settling {
		fmt.Println("Status:  settling...")
	}
	fmt.Println()

	if len(jobs) > 0 {
		// Eén cluster-brede /v1/tasks in plaats van één status-call per job:
		// de oude vorm liet de leader per regel alle agents afvragen, dus J
		// jobs kostten J fan-outs voor één statustabel.
		phases := startPhases()

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "NAME\tPLACED\tEXPECTED\tSTATUS")
		for _, job := range jobs {
			expected := job.Count
			if expected == -1 {
				expected = status.Agents
			}
			if expected == 0 {
				expected = 1
			}
			placed := status.Placed[job.Name]
			statusStr := "OK"
			if placed < expected {
				statusStr = "DEGRADED"
			}
			// De startfase zichtbaar: een geplaatste task die nog downloadt is
			// geen "OK, draait" — laat zien wat er gebeurt en hoe ver het is.
			if s := phases[job.Name]; s != "" {
				statusStr = s
			}
			expectedStr := fmt.Sprintf("%d", expected)
			if job.Count == -1 {
				expectedStr = fmt.Sprintf("all(%d)", status.Agents)
			}
			fmt.Fprintf(w, "%s\t%d\t%s\t%s\n", job.Name, placed, expectedStr, statusStr)
		}
		w.Flush()
		fmt.Println()
	}

	return nil
}

func runAgents(args []string) error {
	resp, err := doRequest("GET", "/v1/agents", nil)
	if err != nil {
		return err
	}

	var agents []*types.Agent
	if err := json.Unmarshal(resp, &agents); err != nil {
		return err
	}

	if len(args) > 0 {
		agentID := args[0]
		for _, a := range agents {
			if a.ID == agentID {
				return showAgentDetails(a)
			}
		}
		return fmt.Errorf("agent %s not found", agentID)
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tENDPOINT\tTEMP\tLAST SEEN")
	for _, agent := range agents {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", agent.ID, agent.Endpoint, fmtTemp(agent.TempMilliC), agent.LastSeen.Format("15:04:05"))
	}
	w.Flush()
	return nil
}

// fmtTemp toont de node-temperatuur uit de heartbeat (milligraden); 0 =
// geen sensor, en dan hoort er een streepje te staan — geen nep-nul.
func fmtTemp(milliC int) string {
	if milliC == 0 {
		return "-"
	}
	return fmt.Sprintf("%.1f°C", float64(milliC)/1000)
}

func showAgentDetails(agent *types.Agent) error {
	fmt.Printf("Agent:    %s\n", agent.ID)
	fmt.Printf("Endpoint: %s\n", agent.Endpoint)
	if agent.TempMilliC != 0 {
		fmt.Printf("CPU temp: %s\n", fmtTemp(agent.TempMilliC))
	}
	fmt.Printf("LastSeen: %s\n", agent.LastSeen.Format("15:04:05"))
	fmt.Println()

	// Signed, via the leader proxy — never an unsigned direct hit to the agent
	// (that 401s on any cluster with an API key).
	capBytes, err := doRequest("GET", "/v1/agents/"+agent.ID+"/capacity", nil)
	if err != nil {
		fmt.Printf("Capacity: (unavailable - %v)\n", err)
		return nil
	}

	var cap struct {
		CPUCores        int               `json:"cpu_cores"`
		MemoryBytes     uint64            `json:"memory_bytes"`
		CPUUsedShares   int               `json:"cpu_used_shares"`
		MemoryUsedBytes uint64            `json:"memory_used_bytes"`
		TasksRunning    int               `json:"tasks_running"`
		Attributes      map[string]string `json:"attributes"`
	}
	if err := json.Unmarshal(capBytes, &cap); err != nil {
		fmt.Printf("Capacity: (unavailable - %v)\n", err)
		return nil
	}

	usedCores := float64(cap.CPUUsedShares) / 1024
	totalShares := cap.CPUCores * 1024
	totalGB := float64(cap.MemoryBytes) / (1024 * 1024 * 1024)
	usedGB := float64(cap.MemoryUsedBytes) / (1024 * 1024 * 1024)

	fmt.Printf("Tasks:    %d running\n", cap.TasksRunning)
	fmt.Printf("CPU:      %.1f / %d cores (%.0f / %d shares)\n", usedCores, cap.CPUCores, float64(cap.CPUUsedShares), totalShares)
	fmt.Printf("Memory:   %.1f / %.0f GB\n", usedGB, totalGB)

	if len(cap.Attributes) > 0 {
		fmt.Println()
		fmt.Println("Attributes:")
		keys := make([]string, 0, len(cap.Attributes))
		for k := range cap.Attributes {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Printf("  %s = %s\n", k, cap.Attributes[k])
		}
	}
	return nil
}

func runLogs(args []string) error {
	fs := flag.NewFlagSet("logs", flag.ExitOnError)
	stream := fs.String("stream", "stdout", "Log stream (stdout or stderr)")
	_ = fs.Parse(args)

	taskID := fs.Arg(0)
	if taskID == "" {
		return fmt.Errorf("task ID required")
	}

	if *stream != "stdout" && *stream != "stderr" {
		return fmt.Errorf("stream must be stdout or stderr")
	}

	// Resolve which agent runs this task. tasks_by_agent lives in the per-job
	// status endpoint (/v1/jobs/<name>/status), NOT /v1/status — so scan jobs.
	agentID, err := findTaskAgent(taskID)
	if err != nil {
		return err
	}

	// Stream through the SIGNED leader proxy — never an unsigned direct hit.
	resp, err := signedGet(fmt.Sprintf("/v1/agents/%s/logs/%s/%s", agentID, taskID, *stream))
	if err != nil {
		return fmt.Errorf("failed to connect to leader: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != leanhttp.StatusOK {
		return fmt.Errorf("leader returned status %d", resp.StatusCode)
	}

	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "data: ") {
			fmt.Print(strings.TrimPrefix(line, "data: "))
		}
	}
	return scanner.Err()
}

// clusterTasks haalt alle tasks van het cluster op in één call
// (GET /v1/tasks: één parallelle fan-out op de leader).
func clusterTasks() (map[string][]*types.Task, error) {
	st, err := doRequest("GET", "/v1/tasks", nil)
	if err != nil {
		return nil, err
	}
	var status struct {
		TasksByAgent map[string][]*types.Task `json:"tasks_by_agent"`
	}
	if err := json.Unmarshal(st, &status); err != nil {
		return nil, err
	}
	return status.TasksByAgent, nil
}

// startPhases geeft per job de startfase als leesbare status ("QUEUED",
// "DOWNLOADING 42% (12.6/30.4 MB)"); jobs waarvan alle tasks voorbij de start
// zijn ontbreken. Best-effort: onbereikbaar = lege map, de tabel blijft heel.
func startPhases() map[string]string {
	tasksByAgent, err := clusterTasks()
	if err != nil {
		return nil
	}
	phases := make(map[string]string)
	for _, tasks := range tasksByAgent {
		for _, t := range tasks {
			switch t.State {
			case types.TaskQueued:
				if phases[t.JobName] == "" {
					phases[t.JobName] = "QUEUED"
				}
			case types.TaskDownloading:
				if t.ImageSize > 0 {
					phases[t.JobName] = fmt.Sprintf("DOWNLOADING %d%% (%.1f/%.1f MB)",
						t.Downloaded*100/t.ImageSize,
						float64(t.Downloaded)/(1<<20), float64(t.ImageSize)/(1<<20))
				} else {
					phases[t.JobName] = "DOWNLOADING"
				}
			}
		}
	}
	return phases
}

// findTaskAgent returns the ID of the agent running taskID — one /v1/tasks
// call instead of a per-job status scan.
func findTaskAgent(taskID string) (string, error) {
	tasksByAgent, err := clusterTasks()
	if err != nil {
		return "", err
	}
	for agentID, tasks := range tasksByAgent {
		for _, t := range tasks {
			if t.ID == taskID {
				return agentID, nil
			}
		}
	}
	return "", fmt.Errorf("task %s not found", taskID)
}

// client pools keep-alive connections over the handful of calls one CLI run
// makes. leanhttp everywhere: the CLI talks the same transport as the cluster.
var client = &httputil.Client{}

// signedGet issues a signed GET to the leader and returns the live response
// (the caller streams and closes the body). Used for log streaming, which
// doRequest cannot do because it buffers the whole body.
func signedGet(path string) (*leanhttp.Response, error) {
	call := leanhttp.Call{URL: fmt.Sprintf("http://%s%s", leaderAddr, path)}
	httputil.SignCall(&call, apiKey)
	return client.Do(call)
}

// parseKV parses "k=v,k2=v2" into a map. Only for values that are plain
// tokens (tags, affinity); env values may contain commas and take one pair
// per --env flag (see parsePairs).
func parseKV(s string) map[string]string {
	m := make(map[string]string)
	for _, pair := range strings.Split(s, ",") {
		if k, v, ok := strings.Cut(pair, "="); ok {
			m[k] = v
		}
	}
	return m
}

// parsePairs parses each element as ONE k=v pair, splitting on the first "="
// only — so values keep their commas and equals signs intact.
func parsePairs(pairs []string) map[string]string {
	m := make(map[string]string)
	for _, pair := range pairs {
		if k, v, ok := strings.Cut(pair, "="); ok {
			m[k] = v
		}
	}
	return m
}

func doRequest(method, path string, body any) ([]byte, error) {
	call := leanhttp.Call{Method: method, URL: fmt.Sprintf("http://%s%s", leaderAddr, path)}
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		call.Body = data
		call.SetHeader("Content-Type", "application/json")
	}
	httputil.SignCall(&call, apiKey)

	resp, err := client.Do(call)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to leader: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode >= 400 {
		var errResp map[string]string
		_ = json.Unmarshal(respBody, &errResp)
		if msg, ok := errResp["error"]; ok {
			return nil, fmt.Errorf("%s", msg)
		}
		return nil, fmt.Errorf("request failed: %s", resp.Status)
	}

	return respBody, nil
}

func parseMemory(s string) (uint64, error) {
	if len(s) == 0 {
		return 0, nil
	}
	unit := s[len(s)-1]
	numStr := s[:len(s)-1]

	num, err := strconv.ParseUint(numStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid memory value: %s", s)
	}

	switch unit {
	case 'K', 'k':
		return num * 1024, nil
	case 'M', 'm':
		return num * 1024 * 1024, nil
	case 'G', 'g':
		return num * 1024 * 1024 * 1024, nil
	default:
		return strconv.ParseUint(s, 10, 64)
	}
}
