//go:build !tamago

package runner

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/xinix00/hop/internal/types"
	"github.com/xinix00/lean/leanhttp"
)

const (
	containerPrefix      = "hop-"
	dockerStopTimeout    = 10 // seconds
	dockerCommandTimeout = 30 * time.Second
	dockerPullTimeout    = 30 * time.Minute
	dockerPingTimeout    = 2 * time.Second
	defaultDockerSocket  = "/var/run/docker.sock"
	maxDockerLogFrame    = 16 << 20
)

// DockerRunner runs jobs as Docker containers via the Docker API directly:
// leanhttp over the unix socket, no docker CLI needed on the host.
type DockerRunner struct {
	nodeAttrs map[string]string
	hostCores int
	client    *leanhttp.Client
	// do is the test seam: production uses client.Do (pooled unix
	// connections), tests substitute a fake daemon.
	do func(leanhttp.Call) (*leanhttp.Response, error)
	// logs zijn de broadcasters van de lopende tasks plus die van net-afgelopen
	// tasks (zie logStore): na een crash of in een restart-lus kun je zo nog even
	// zien wat de container zei — en Stop verwijdert 'm, dus `docker logs` kan het
	// dan ook niet meer navertellen. Eigen slot, buiten r.mu.
	logs      *logStore
	logCancel map[string]context.CancelFunc
	// cpuPrev is de vorige cumulatieve CPU-stand per task, voor de delta die
	// Usage rapporteert (zelfde ps-delta-truc als het exec-pad).
	cpuPrev map[string]dockerCPUSample
	mu      sync.RWMutex
}

type dockerCPUSample struct {
	totalNs uint64
	at      time.Time
}

// dockerDialer dials the daemon's unix socket regardless of the URL host.
func dockerDialer(socketPath string) func(context.Context, string, string) (net.Conn, error) {
	if socketPath == "" {
		socketPath = defaultDockerSocket
	}
	dialer := &net.Dialer{}
	return func(ctx context.Context, _, _ string) (net.Conn, error) {
		return dialer.DialContext(ctx, "unix", socketPath)
	}
}

// NewDockerRunner creates a new Docker runner. hostCores is the node's core
// count, used to express CPU usage of an unlimited container as a percentage
// of what it can actually claim.
func NewDockerRunner(nodeAttrs map[string]string, socketPath string, hostCores int) *DockerRunner {
	client := &leanhttp.Client{DialContext: dockerDialer(socketPath)}
	r := &DockerRunner{
		nodeAttrs: nodeAttrs,
		hostCores: hostCores,
		client:    client,
		logs:      newLogStore(),
		logCancel: make(map[string]context.CancelFunc),
		cpuPrev:   make(map[string]dockerCPUSample),
	}
	r.do = client.Do
	return r
}

// DockerPresent reports whether a Docker daemon answers on the socket. This is
// what node.docker means for affinity: a node that can actually RUN containers
// — a docker CLI on the path (the old test) proves neither the daemon nor the
// socket.
func DockerPresent(socketPath string) bool {
	resp, err := leanhttp.Do(leanhttp.Call{
		URL:         "http://docker/_ping",
		Timeout:     dockerPingTimeout,
		DialContext: dockerDialer(socketPath),
	})
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == leanhttp.StatusOK
}

// dockerAPIWithContext performs a Docker API request with context.
func (r *DockerRunner) dockerAPIWithContext(ctx context.Context, method, path string, body interface{}) (*leanhttp.Response, error) {
	call := leanhttp.Call{Method: method, URL: "http://docker" + path, Context: ctx}
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		call.Body = data
		call.SetHeader("Content-Type", "application/json")
	}
	return r.do(call)
}

// Run starts a Docker container for the job.
func (r *DockerRunner) Run(job *types.Job, task *types.Task) error {
	if job.Image == "" {
		return fmt.Errorf("image is required for docker runner")
	}

	containerName := containerPrefix + task.ID

	// Pull image first
	pullCtx, pullCancel := context.WithTimeout(context.Background(), dockerPullTimeout)
	resp, err := r.dockerAPIWithContext(pullCtx, "POST", fmt.Sprintf("/images/create?fromImage=%s", job.Image), nil)
	if err != nil {
		pullCancel()
		return fmt.Errorf("docker pull failed: %w", err)
	}
	if err := consumeDockerPullResponse(resp); err != nil {
		pullCancel()
		return err
	}
	pullCancel()

	// Build port bindings and exposed ports
	portBindings := map[string][]portBinding{}
	exposedPorts := map[string]struct{}{}
	for _, hostPort := range task.Ports {
		containerPort := hostPort // container port = host port (same convention)
		key := fmt.Sprintf("%d/tcp", containerPort)
		exposedPorts[key] = struct{}{}
		portBindings[key] = []portBinding{{HostPort: fmt.Sprintf("%d", hostPort)}}
	}

	// Build env
	var env []string
	for k, v := range job.Env {
		env = append(env, k+"="+v)
	}
	env = append(env, PortEnvVars(task.Ports)...)
	env = append(env, AttrEnvVars(r.nodeAttrs)...)

	// Build volumes
	var binds []string
	for hostPath, containerPath := range job.Volumes {
		binds = append(binds, hostPath+":"+containerPath)
	}

	// Build command
	var cmd []string
	if job.Command != "" {
		cmd = []string{"/bin/sh", "-c", job.Command}
	}

	// Create container
	createBody := createRequest{
		Image:        job.Image,
		Env:          env,
		Cmd:          cmd,
		ExposedPorts: exposedPorts,
		HostConfig: hostConfig{
			PortBindings: portBindings,
			Binds:        binds,
			Memory:       int64(job.MemoryLimit),
			CPUShares:    int64(job.CPUShares),
		},
	}

	createCtx, createCancel := context.WithTimeout(context.Background(), dockerCommandTimeout)
	resp, err = r.dockerAPIWithContext(createCtx, "POST", "/containers/create?name="+containerName, createBody)
	if err != nil {
		createCancel()
		return fmt.Errorf("docker create failed: %w", err)
	}
	if err := consumeDockerResponse(resp, "docker create", leanhttp.StatusCreated); err != nil {
		createCancel()
		return err
	}
	createCancel()

	// Start container
	startCtx, startCancel := context.WithTimeout(context.Background(), dockerCommandTimeout)
	resp, err = r.dockerAPIWithContext(startCtx, "POST", "/containers/"+containerName+"/start", nil)
	if err != nil {
		startCancel()
		return fmt.Errorf("docker start failed: %w", err)
	}
	// 304 Not Modified: the container was already running.
	if err := consumeDockerResponse(resp, "docker start", leanhttp.StatusNoContent, leanhttp.StatusOK, 304); err != nil {
		startCancel()
		return err
	}
	startCancel()

	// Start log streaming
	r.startLogStreaming(task.ID, containerName)

	return nil
}

// Stop stops and removes a Docker container
func (r *DockerRunner) Stop(task *types.Task) error {
	containerName := containerPrefix + task.ID
	ctx, cancel := context.WithTimeout(context.Background(), dockerCommandTimeout)
	// Stop container
	resp, err := r.dockerAPIWithContext(ctx, "POST", fmt.Sprintf("/containers/%s/stop?t=%d", containerName, dockerStopTimeout), nil)
	var stopErr error
	if err != nil {
		stopErr = fmt.Errorf("docker stop %s: %w", containerName, err)
	} else {
		// 304: already stopped.
		stopErr = consumeDockerResponse(resp, "docker stop", leanhttp.StatusNoContent, 304, leanhttp.StatusNotFound)
	}
	cancel()

	// Force-remove met een verse deadline: een trage stop mag de delete niet
	// met een reeds verlopen context laten beginnen.
	removeErr := r.removeContainer(containerName)

	// Stop log streaming only after graceful stop/remove, so the final SIGTERM
	// diagnostics are included in the retained tail.
	r.mu.Lock()
	if cancel := r.logCancel[task.ID]; cancel != nil {
		cancel()
	}
	delete(r.logCancel, task.ID)
	delete(r.cpuPrev, task.ID)
	r.mu.Unlock()

	// De container is verwijderd, de logs gaan met pensioen: nog logRetention
	// opvraagbaar. `docker logs` kan het na de rm niet meer navertellen, dus dit is
	// het enige dat na een crash nog weet wat de container zei.
	r.logs.retire(task.ID)

	if removeErr != nil {
		return errors.Join(stopErr, removeErr)
	}
	if stopErr != nil {
		log.Printf("%v (forced remove succeeded)", stopErr)
	}

	return nil
}

func (r *DockerRunner) removeContainer(containerName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), dockerCommandTimeout)
	defer cancel()
	resp, err := r.dockerAPIWithContext(ctx, "DELETE", "/containers/"+containerName+"?force=true", nil)
	if err != nil {
		return fmt.Errorf("docker rm %s: %w", containerName, err)
	}
	return consumeDockerResponse(resp, "docker rm", leanhttp.StatusNoContent, leanhttp.StatusNotFound)
}

func consumeDockerResponse(resp *leanhttp.Response, operation string, allowed ...int) error {
	defer resp.Body.Close()
	for _, status := range allowed {
		if resp.StatusCode == status {
			// Een succesvolle pull is pas klaar wanneer Docker zijn volledige
			// voortgangsbody sluit. Vroeg afkappen kan de image-download annuleren.
			if _, err := io.Copy(io.Discard, resp.Body); err != nil {
				return fmt.Errorf("%s response: %w", operation, err)
			}
			return nil
		}
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	return fmt.Errorf("%s failed (%d): %s", operation, resp.StatusCode, strings.TrimSpace(string(body)))
}

func consumeDockerPullResponse(resp *leanhttp.Response) error {
	defer resp.Body.Close()
	if resp.StatusCode != leanhttp.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		return fmt.Errorf("docker pull failed (%d): %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	decoder := json.NewDecoder(resp.Body)
	for {
		var progress struct {
			Error       string `json:"error"`
			ErrorDetail struct {
				Message string `json:"message"`
			} `json:"errorDetail"`
		}
		if err := decoder.Decode(&progress); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("docker pull response: %w", err)
		}
		message := progress.ErrorDetail.Message
		if message == "" {
			message = progress.Error
		}
		if message != "" {
			return fmt.Errorf("docker pull failed: %s", message)
		}
	}
}

// Status checks if a Docker container is running
func (r *DockerRunner) Status(task *types.Task) (types.TaskState, error) {
	containerName := containerPrefix + task.ID

	ctx, cancel := context.WithTimeout(context.Background(), dockerCommandTimeout)
	defer cancel()
	resp, err := r.dockerAPIWithContext(ctx, "GET", "/containers/"+containerName+"/json", nil)
	if err != nil {
		return types.TaskFailed, nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != leanhttp.StatusOK {
		return types.TaskFailed, nil
	}

	var info struct {
		State struct {
			Running bool `json:"Running"`
		} `json:"State"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return types.TaskFailed, nil
	}

	if info.State.Running {
		return types.TaskRunning, nil
	}
	return types.TaskFailed, nil
}

// Usage reports CPU as a percentage of the task's OWN cores (0-100; -1 while
// no delta window exists yet) and the container's memory usage in bytes —
// the same contract as HopRunner.Usage, so the monitor treats every
// self-reporting runner alike. One one-shot stats call on the socket per
// monitor tick; the old path forked `docker stats --no-stream` per container,
// which samples for 1-2s each and serialized the whole monitor loop.
func (r *DockerRunner) Usage(task *types.Task) (float64, uint64, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), dockerCommandTimeout)
	defer cancel()
	resp, err := r.dockerAPIWithContext(ctx, "GET",
		"/containers/"+containerPrefix+task.ID+"/stats?stream=false&one-shot=true", nil)
	if err != nil {
		return -1, 0, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != leanhttp.StatusOK {
		_, _ = io.Copy(io.Discard, resp.Body)
		return -1, 0, false
	}
	var st struct {
		CPUStats struct {
			CPUUsage struct {
				TotalUsage uint64 `json:"total_usage"` // cumulatieve ns over alle cores
			} `json:"cpu_usage"`
		} `json:"cpu_stats"`
		MemoryStats struct {
			Usage uint64            `json:"usage"`
			Stats map[string]uint64 `json:"stats"`
		} `json:"memory_stats"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		return -1, 0, false
	}

	// Zoals `docker stats` rekent: page cache telt niet als gebruik.
	// cgroup v2 rapporteert inactive_file, v1 total_inactive_file.
	mem := st.MemoryStats.Usage
	if inactive, ok := st.MemoryStats.Stats["inactive_file"]; ok && inactive < mem {
		mem -= inactive
	} else if inactive, ok := st.MemoryStats.Stats["total_inactive_file"]; ok && inactive < mem {
		mem -= inactive
	}

	now := time.Now()
	r.mu.Lock()
	prev, seen := r.cpuPrev[task.ID]
	r.cpuPrev[task.ID] = dockerCPUSample{totalNs: st.CPUStats.CPUUsage.TotalUsage, at: now}
	r.mu.Unlock()

	// Eerste meting (of een herstartte teller): geen venster, dus geen CPU —
	// het geheugen is wel al een feit.
	if !seen || !now.After(prev.at) || st.CPUStats.CPUUsage.TotalUsage < prev.totalNs {
		return -1, mem, true
	}
	cores := float64(task.CPUShares) / 1024
	if cores <= 0 {
		cores = float64(r.hostCores)
		if cores <= 0 {
			cores = 1
		}
	}
	usedCores := float64(st.CPUStats.CPUUsage.TotalUsage-prev.totalNs) /
		float64(now.Sub(prev.at).Nanoseconds())
	return usedCores / cores * 100, mem, true
}

// SetLogPolicy sets how much task output this runner keeps. Call before the
// first Start; it replaces the (still empty) log store.
func (r *DockerRunner) SetLogPolicy(p LogPolicy) { r.logs = newLogStoreWith(p) }

// GetStdout returns the stdout log broadcaster for a task, or the retired one of
// a task that finished less than logRetention ago (see logStore).
func (r *DockerRunner) GetStdout(taskID string) *LogBroadcaster { return r.logs.stdout(taskID) }

// GetStderr does the same for stderr.
func (r *DockerRunner) GetStderr(taskID string) *LogBroadcaster { return r.logs.stderr(taskID) }

// Cleanup removes all hop containers
func (r *DockerRunner) Cleanup() error {
	ctx, cancel := context.WithTimeout(context.Background(), dockerCommandTimeout)
	defer cancel()
	resp, err := r.dockerAPIWithContext(ctx, "GET", "/containers/json?all=true&filters="+`{"name":["hop-"]}`, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != leanhttp.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		return fmt.Errorf("docker cleanup list failed (%d): %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var containers []struct {
		ID    string   `json:"Id"`
		Names []string `json:"Names"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&containers); err != nil {
		return err
	}

	var cleanupErr error
	for _, c := range containers {
		if err := r.removeContainer(c.ID); err != nil {
			cleanupErr = errors.Join(cleanupErr, err)
		}
	}
	return cleanupErr
}

// startLogStreaming streams container logs via Docker API
func (r *DockerRunner) startLogStreaming(taskID, containerName string) {
	stdoutB, stderrB := r.logs.newPair()

	ctx, cancel := context.WithCancel(context.Background())

	r.logs.put(taskID, stdoutB, stderrB)

	r.mu.Lock()
	r.logCancel[taskID] = cancel
	r.mu.Unlock()

	go func() {
		resp, err := r.dockerAPIWithContext(ctx, "GET", "/containers/"+containerName+"/logs?follow=true&stdout=true&stderr=true", nil)
		if err != nil {
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode != leanhttp.StatusOK {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
			_, _ = stderrB.Write([]byte(fmt.Sprintf("docker logs failed (%d): %s\n", resp.StatusCode, strings.TrimSpace(string(body)))))
			return
		}

		// Docker multiplexed stream: 8-byte header per frame
		// [stream_type(1)][0][0][0][size(4)] then payload
		reader := bufio.NewReader(resp.Body)
		header := make([]byte, 8)
		for {
			if _, err := io.ReadFull(reader, header); err != nil {
				return
			}
			if (header[0] != 1 && header[0] != 2) || header[1] != 0 || header[2] != 0 || header[3] != 0 {
				_, _ = stderrB.Write([]byte("docker logs: invalid multiplex header\n"))
				return
			}
			size := binary.BigEndian.Uint32(header[4:])
			if size > maxDockerLogFrame {
				_, _ = stderrB.Write([]byte(fmt.Sprintf("docker logs: frame too large: %d bytes\n", size)))
				return
			}
			var dst io.Writer = stdoutB
			if header[0] == 2 {
				dst = stderrB
			}
			if _, err := io.CopyN(dst, reader, int64(size)); err != nil {
				return
			}
		}
	}()
}

// Docker API types (minimal, only what we need)

type portBinding struct {
	HostPort string `json:"HostPort"`
}

type hostConfig struct {
	PortBindings map[string][]portBinding `json:"PortBindings,omitempty"`
	Binds        []string                 `json:"Binds,omitempty"`
	Memory       int64                    `json:"Memory,omitempty"`
	CPUShares    int64                    `json:"CpuShares,omitempty"`
}

type createRequest struct {
	Image        string              `json:"Image"`
	Env          []string            `json:"Env,omitempty"`
	Cmd          []string            `json:"Cmd,omitempty"`
	ExposedPorts map[string]struct{} `json:"ExposedPorts,omitempty"`
	HostConfig   hostConfig          `json:"HostConfig"`
}
