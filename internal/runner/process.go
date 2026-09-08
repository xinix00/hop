// ExecRunner is POSIX-only: it needs a process model (fork/exec, signals,
// nice, chroot/cgroups). On other targets (HopOS/tamago) process_other.go
// provides a stub that fails with a clear error; tasks run via HopRunner.

//go:build darwin || linux

package runner

import (
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/xinix00/hop/internal/types"
)

const (
	gracefulShutdownTimeout = 10 * time.Second
	killTimeout             = 1 * time.Second // SIGKILL is enforced by the kernel; 1s is generous for reaping
	processExitPollInterval = 100 * time.Millisecond
	maxNiceValue            = 19
)

// ExecRunner runs processes with optional isolation
type ExecRunner struct {
	config    *Config
	processes map[string]*execProcess
	taskDirs  map[string]string   // taskID -> work directory
	mounts    map[string][]string // taskID -> bind mount targets we created, in setup order
	// logs zijn de broadcasters van de lopende tasks plus die van net-afgelopen
	// tasks (zie logStore): na een crash of in een restart-lus kun je zo nog even
	// zien wat de task zei. Eigen slot, buiten r.mu.
	logs *logStore
	mu   sync.RWMutex
}

// execProcess is the generation token for one started child. Unlike a numeric
// PID, the sticky groupGone observation cannot start referring to an unrelated,
// later process after the kernel reuses that number.
type execProcess struct {
	cmd *exec.Cmd

	mu        sync.Mutex
	groupGone bool // sticky ESRCH observation for this process-group generation
}

func (p *execProcess) signal(sig syscall.Signal) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.groupGone {
		return syscall.ESRCH
	}
	err := syscall.Kill(-p.cmd.Process.Pid, sig)
	if err == syscall.ESRCH {
		p.groupGone = true
	}
	return err
}

func (p *execProcess) isGroupGone() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.groupGone {
		return true
	}
	if err := syscall.Kill(-p.cmd.Process.Pid, 0); err == syscall.ESRCH {
		p.groupGone = true
	}
	return p.groupGone
}

// NewExecRunner creates a new process runner
func NewExecRunner(config *Config) *ExecRunner {
	return &ExecRunner{
		config:    config,
		processes: make(map[string]*execProcess),
		taskDirs:  make(map[string]string),
		mounts:    make(map[string][]string),
		logs:      newLogStoreWith(config.Logs),
	}
}

// Run starts a process for the job. The task is pre-created by the caller;
// Run fills in Pid and registers internal state (process handle, log broadcasters).
func (r *ExecRunner) Run(job *types.Job, task *types.Task) error {
	if job.Command == "" {
		return errors.New("command is required")
	}

	taskID := task.ID

	// Setup task directory
	taskDir, err := r.setupTaskDir(taskID, job)
	if err != nil {
		return fmt.Errorf("failed to setup task directory: %w", err)
	}

	// Download artifact if specified (agent already resolved to first matching entry)
	if len(job.Artifacts) > 0 {
		log.Printf("Downloading artifact for task %s: %s", taskID, job.Artifacts[0].URL)
		// Download directly to taskDir so commands like "./binary" work
		if err := downloadArtifact(&job.Artifacts[0], taskDir); err != nil {
			log.Printf("Artifact download failed for task %s: %v", taskID, err)
			return errors.Join(fmt.Errorf("failed to download artifact: %w", err), r.cleanupTaskDir(taskID))
		}
	}

	// Build port environment variables
	portEnvVars := PortEnvVars(task.Ports)

	// Pre-create the cgroup so the child is born inside it via
	// clone3(CLONE_INTO_CGROUP). Doing this before exec avoids a race where
	// the task reads /proc/self/cgroup (and memory.max) before we've moved
	// it into the per-task cgroup — RavenDB and other runtimes cache that
	// path once at startup and would otherwise see stale/wrong values.
	cgroupFD, err := r.prepareCgroup(taskID, job.MemoryLimit)
	if err != nil {
		log.Printf("Warning: cgroup setup for %s failed, starting without limit: %v", taskID, err)
		cgroupFD = -1
	}

	// Setup command with platform-specific isolation
	cmd := r.setupCommand(job, taskDir, portEnvVars)
	if cgroupFD >= 0 {
		r.attachCgroup(cmd, cgroupFD)
	}

	// Setup log broadcasting
	stdoutBroadcaster, stderrBroadcaster := r.logs.newPair()

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		if cgroupFD >= 0 {
			_ = syscall.Close(cgroupFD)
		}
		return errors.Join(fmt.Errorf("failed to create stdout pipe: %w", err), r.removeCgroup(taskID), r.cleanupTaskDir(taskID))
	}

	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		if cgroupFD >= 0 {
			_ = syscall.Close(cgroupFD)
		}
		return errors.Join(fmt.Errorf("failed to create stderr pipe: %w", err), r.removeCgroup(taskID), r.cleanupTaskDir(taskID))
	}

	// Start the process
	startErr := cmd.Start()
	if cgroupFD >= 0 {
		_ = syscall.Close(cgroupFD)
	}
	if startErr != nil {
		return errors.Join(fmt.Errorf("failed to start process: %w", startErr), r.removeCgroup(taskID), r.cleanupTaskDir(taskID))
	}

	// Start log broadcasting
	go PipeReader(stdoutBroadcaster, stdoutPipe)
	go PipeReader(stderrBroadcaster, stderrPipe)

	r.logs.put(taskID, stdoutBroadcaster, stderrBroadcaster)

	process := &execProcess{cmd: cmd}
	r.mu.Lock()
	r.processes[taskID] = process
	r.mu.Unlock()

	// Apply PID-based limits before Wait can reap a short-lived child and make
	// its numeric PID reusable. Stop itself is generation-safe via process.
	r.applyLimits(cmd.Process.Pid, job)
	task.Pid = cmd.Process.Pid

	go func() {
		_ = cmd.Wait()
		// Publish an observed ESRCH as a sticky generation barrier. Status and
		// Stop use the same flag, so a PGID proven gone can never be signalled
		// later merely because the kernel reused its number.
		process.isGroupGone()
	}()
	return nil
}

// Stop stops a running task
func (r *ExecRunner) Stop(task *types.Task) error {
	// The map is the ownership record; task.Pid is only historical/status data.
	// Falling back to that stale PID after a successful Stop can signal an
	// unrelated process group once the kernel reuses the number. Keep ownership
	// registered until the group is confirmed gone, so a failed Stop quarantines
	// the exact process generation instead of forgetting or mis-signalling it.
	r.mu.RLock()
	process := r.processes[task.ID]
	r.mu.RUnlock()
	if process == nil || process.cmd == nil || process.cmd.Process == nil || process.cmd.Process.Pid <= 0 {
		return errors.Join(r.cleanupTaskDir(task.ID), r.removeCgroup(task.ID))
	}
	pid := process.cmd.Process.Pid

	// SIGTERM the exact owned process-group generation, wait up to the graceful
	// budget, then SIGKILL. Status and the Wait goroutine make ESRCH sticky on
	// the record, preventing a later Stop from signalling a recycled PGID.
	if err := process.signal(syscall.SIGTERM); err != nil && err != syscall.ESRCH {
		log.Printf("Warning: failed to send SIGTERM to process group %d: %v", pid, err)
	}
	if !waitForPgroupExit(process, gracefulShutdownTimeout) {
		log.Printf("Process group %d did not exit within %s, sending SIGKILL", pid, gracefulShutdownTimeout)
		if err := process.signal(syscall.SIGKILL); err != nil && err != syscall.ESRCH {
			log.Printf("Warning: failed to send SIGKILL to process group %d: %v", pid, err)
		}
		if !waitForPgroupExit(process, killTimeout) {
			// Ownership is not released: deleting its taskdir/cgroup now would
			// forget a process that is demonstrably still alive. Keep it quarantined
			// inside the runner even though the agent removes the logical task.
			return fmt.Errorf("process group %d still alive after SIGKILL", pid)
		}
	}
	r.mu.Lock()
	if r.processes[task.ID] == process {
		delete(r.processes, task.ID)
	}
	r.mu.Unlock()

	cleanupErr := r.cleanupTaskDir(task.ID)
	cgroupErr := r.removeCgroup(task.ID)
	return errors.Join(cleanupErr, cgroupErr)
}

// waitForPgroupExit polls the generation record until ESRCH proves the process
// group is gone or budget elapses. EPERM and all other errors retain ownership.
func waitForPgroupExit(process *execProcess, budget time.Duration) bool {
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		if process.isGroupGone() {
			return true
		}
		time.Sleep(processExitPollInterval)
	}
	return false
}

// Status returns the current state of a task.
func (r *ExecRunner) Status(task *types.Task) (types.TaskState, error) {
	// PID not yet set → process still starting (artifact download, etc.)
	if task.Pid == 0 {
		return types.TaskRunning, nil
	}

	r.mu.RLock()
	process := r.processes[task.ID]
	r.mu.RUnlock()
	if process == nil {
		return types.TaskFailed, nil
	}
	if !process.isGroupGone() {
		return types.TaskRunning, nil
	}
	// A task record describes a service that should be running. Once its process
	// generation is gone it is failed, irrespective of exit code; an explicit
	// Stop removes the record and never asks Status again.
	return types.TaskFailed, nil
}

// applyLimits applies post-exec resource limits (nice only — memory is set
// pre-exec via the cgroup FD in Run so the child sees the right limit from
// /proc/self/cgroup at startup).
func (r *ExecRunner) applyLimits(pid int, job *types.Job) {
	if job.CPUShares > 0 {
		r.applyNice(pid, job.CPUShares)
	}
}

// applyNice sets process priority based on CPU shares. CFS weighs processes
// as weight = base / 1.25^nice, so to get an effective CPU share proportional
// to cpu_shares/max_shares we invert that: nice = log(max/shares) / log(1.25).
// Result: two jobs with shares 7000 and 1024 actually get ~86% / 14% of CPU
// under contention instead of the 94% / 6% the naive linear-to-nice mapping
// produced. Works the same on Linux and macOS — both use the nice 0..19 range.
func (r *ExecRunner) applyNice(pid int, cpuShares int) {
	maxShares := r.config.MaxCPUShares
	if maxShares <= 0 {
		maxShares = runtime.NumCPU() * 1024
	}

	nice := maxNiceValue
	if cpuShares >= maxShares {
		nice = 0
	} else if cpuShares > 0 {
		nice = int(math.Round(math.Log(float64(maxShares)/float64(cpuShares)) / math.Log(1.25)))
		if nice < 0 {
			nice = 0
		}
		if nice > maxNiceValue {
			nice = maxNiceValue
		}
	}

	if err := syscall.Setpriority(syscall.PRIO_PROCESS, pid, nice); err != nil {
		fmt.Printf("Warning: failed to set nice value: %v\n", err)
	}
}

// setupTaskDir creates an isolated directory for the task
func (r *ExecRunner) setupTaskDir(taskID string, job *types.Job) (result string, retErr error) {
	// Base directory for all tasks
	base := r.config.RootfsBase
	if base == "" {
		base = "/tmp/hop"
	}

	// Keep taskDir local: explicit error returns assign the named result before
	// defers run, while rollback must retain the directory it actually created.
	taskDir := filepath.Join(base, taskID)
	var mounts []string
	committed := false
	defer func() {
		if committed {
			return
		}
		// setupTaskDir is a transaction: none of its resources are visible in
		// the runner maps until all setup succeeded, so a local rollback owns
		// every partial bind and directory on every error return.
		if err := r.cleanupTaskResources(taskDir, mounts); err != nil {
			// De bind is mogelijk nog actief: bewaar ownership zodat een latere
			// Runner.Stop dezelfde quarantaine veilig opnieuw kan opruimen.
			r.mu.Lock()
			r.taskDirs[taskID] = taskDir
			r.mounts[taskID] = mounts
			r.mu.Unlock()
			retErr = errors.Join(retErr, fmt.Errorf("rollback task directory: %w", err))
		}
	}()

	// Resolve user for ownership
	uid, gid := -1, -1
	if job.User != "" {
		if cred, _, err := lookupCredential(job.User); err == nil {
			uid, gid = int(cred.Uid), int(cred.Gid)
		}
	}

	// Create directory structure (chown if user is set so child entries inherit)
	dirs := []string{
		taskDir,
		filepath.Join(taskDir, "tmp"),
	}
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return "", fmt.Errorf("failed to create %s: %w", dir, err)
		}
		if uid >= 0 {
			_ = os.Chown(dir, uid, gid)
		}
	}

	// Track every bind mount we create so cleanupTaskDir can unmount exactly
	// what was mounted — no /proc parsing needed. Job-script mounts live in
	// the child's mount namespace (CLONE_NEWNS) and die with the process, so
	// they're not our problem.
	volumePaths := make([]string, 0, len(job.Volumes))
	for hostPath := range job.Volumes {
		volumePaths = append(volumePaths, hostPath)
	}
	sort.Strings(volumePaths)
	for _, hostPath := range volumePaths {
		taskPath := job.Volumes[hostPath]
		if err := os.MkdirAll(hostPath, 0755); err != nil {
			return "", fmt.Errorf("failed to create volume host path %s: %w", hostPath, err)
		}

		// Create target path inside task dir (strip leading / to keep it relative)
		target := filepath.Join(taskDir, strings.TrimPrefix(taskPath, "/"))
		targetDir := filepath.Dir(target)
		if err := os.MkdirAll(targetDir, 0755); err != nil {
			return "", fmt.Errorf("failed to create volume target dir %s: %w", targetDir, err)
		}

		if err := r.mountVolume(hostPath, target); err != nil {
			return "", fmt.Errorf("failed to mount volume %s -> %s: %w", hostPath, target, err)
		}
		mounts = append(mounts, target)
	}

	if r.config.Isolate {
		mounts = append(mounts, r.setupIsolationEnv(taskDir)...)
		// Stage the fake /proc/meminfo at /.hop-meminfo inside taskDir. The
		// in-namespace wrapper script (procWrapperScript) bind-mounts it over
		// /proc/meminfo after mounting a fresh procfs. Just a regular file —
		// no mount tracking needed, RemoveAll cleans it up with taskDir.
		_ = r.fakeMeminfo(taskDir, job.MemoryLimit)
	}

	r.mu.Lock()
	r.taskDirs[taskID] = taskDir
	r.mounts[taskID] = mounts
	r.mu.Unlock()
	committed = true

	return taskDir, nil
}

// cleanupTaskDir removes the task directory. Unmounts exactly what we
// tracked in setupTaskDir — reverse order so nested binds drop before
// their parents. With every bind explicitly detached, RemoveAll can no
// longer descend into a bound /dev and unlink host /dev/null.
func (r *ExecRunner) cleanupTaskDir(taskID string) error {
	r.mu.RLock()
	taskDir := r.taskDirs[taskID]
	mounts := r.mounts[taskID]
	r.mu.RUnlock()

	// De taskdir gaat weg, de logs met pensioen: nog logRetention opvraagbaar, want
	// juist ná een crash wil je weten wat het proces zei — en zijn schijfsporen zijn
	// dan al opgeruimd.
	r.logs.retire(taskID)

	if err := r.cleanupTaskResources(taskDir, mounts); err != nil {
		return err
	}
	r.mu.Lock()
	if r.taskDirs[taskID] == taskDir {
		delete(r.taskDirs, taskID)
		delete(r.mounts, taskID)
	}
	r.mu.Unlock()
	return nil
}

func (r *ExecRunner) cleanupTaskResources(taskDir string, mounts []string) error {
	var cleanupErr error
	unsafeToRemove := false
	for i := len(mounts) - 1; i >= 0; i-- {
		if err := r.unmountVolume(mounts[i]); err != nil &&
			!errors.Is(err, os.ErrNotExist) && !errors.Is(err, syscall.EINVAL) {
			unsafeToRemove = true
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("unmount %s: %w", mounts[i], err))
		}
	}
	if unsafeToRemove {
		return cleanupErr
	}
	if taskDir != "" {
		if err := os.RemoveAll(taskDir); err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("remove task directory %s: %w", taskDir, err))
		}
	}
	return cleanupErr
}

// GetStdout returns the stdout broadcaster for a task, or the retired one of a
// task that finished less than logRetention ago (see logStore).
func (r *ExecRunner) GetStdout(taskID string) *LogBroadcaster { return r.logs.stdout(taskID) }

// GetStderr does the same for stderr — where a crashing process usually said why.
func (r *ExecRunner) GetStderr(taskID string) *LogBroadcaster { return r.logs.stderr(taskID) }

// Cleanup ensures the rootfs base exists and is writable. Stale taskdirs
// from a crashed predecessor (with bind mounts still attached) are NOT
// touched — RemoveAll into a bound /dev would nuke host /dev/null. After
// a hard crash, reboot or clean /tmp/hop manually with the agent stopped.
func (r *ExecRunner) Cleanup() error {
	base := r.config.RootfsBase
	if base == "" {
		base = "/tmp/hop"
	}
	if err := os.MkdirAll(base, 0777); err != nil {
		return err
	}
	if err := os.Chmod(base, 0777); err != nil {
		return err
	}
	ensureCgroupControllers()
	return nil
}

// lookupCredential resolves a username to UID/GID for process credential switching
func lookupCredential(username string) (*syscall.Credential, string, error) {
	u, err := user.Lookup(username)
	if err != nil {
		return nil, "", fmt.Errorf("user %q not found: %w", username, err)
	}
	uid, _ := strconv.Atoi(u.Uid)
	gid, _ := strconv.Atoi(u.Gid)
	return &syscall.Credential{Uid: uint32(uid), Gid: uint32(gid)}, u.HomeDir, nil
}
