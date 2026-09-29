//go:build darwin || linux

package runner

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/xinix00/hop/internal/types"
)

func TestExecProcessGoneBarrierNeverSignalsReusedGroup(t *testing.T) {
	cmd := exec.Command("sleep", "10")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		_ = cmd.Wait()
	}()

	process := &execProcess{
		cmd:       cmd,
		groupGone: true, // a prior Status/Wait observed ESRCH for the old generation
	}
	if err := process.signal(syscall.SIGTERM); err != syscall.ESRCH {
		t.Fatalf("signal after sticky ESRCH = %v, want ESRCH without a syscall", err)
	}
	if err := syscall.Kill(-cmd.Process.Pid, 0); err != nil {
		t.Fatalf("stand-in reused process group was signalled: %v", err)
	}
}

// Een gestopte exec-task blijft zijn logs houden: cleanupTaskDir gooit de taskdir
// weg (dus ook alles wat het proces op schijf zei), en juist ná een crash wil je
// weten wat er in de laatste regels stond.
func TestExecRunnerBewaartLogsNaDeStop(t *testing.T) {
	cfg := &Config{RootfsBase: t.TempDir(), Isolate: false}
	r := NewExecRunner(cfg)
	job := &types.Job{Name: "logbewaring", Command: "echo laatste-woorden"}
	task := &types.Task{ID: "t-exec-retire"}

	if err := r.Run(job, task); err != nil {
		t.Fatalf("Run: %v", err)
	}

	out := r.GetStdout(task.ID)
	if out == nil {
		t.Fatal("geen stdout-broadcaster voor een lopende task")
	}
	deadline := time.Now().Add(2 * time.Second)
	for len(out.Tail()) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if len(out.Tail()) == 0 {
		t.Fatal("geen logregel vóór de stop")
	}

	_ = r.Stop(task)

	after := r.GetStdout(task.ID)
	if after == nil {
		t.Fatal("logs zijn weg direct na de stop — de retentie doet niets")
	}
	if got := strings.Join(after.Tail(), ""); !strings.Contains(got, "laatste-woorden") {
		t.Fatalf("bewaarde tail = %q, mist de laatste regel van het proces", got)
	}
	if r.GetStderr(task.ID) == nil {
		t.Fatal("stderr is weg — juist daar staat waarom een proces viel")
	}

	// En na de termijn is het wél weg (geen groei op een node in een restart-lus).
	backdateRetired(r.logs, task.ID)
	if r.GetStdout(task.ID) != nil {
		t.Fatal("logs zijn na de retentietermijn nog opvraagbaar")
	}
}

func TestExecRunnerRollsBackTaskDirWhenArtifactFails(t *testing.T) {
	base := t.TempDir()
	r := NewExecRunner(&Config{RootfsBase: base})
	task := &types.Task{ID: "artifact-fails"}
	job := &types.Job{
		Name:      "artifact-fails",
		Command:   "true",
		Artifacts: []types.Artifact{{URL: "ftp://example.invalid/app"}},
	}

	if err := r.Run(job, task); err == nil {
		t.Fatal("Run accepteerde een unsupported artifact URL")
	}
	if _, err := os.Stat(filepath.Join(base, task.ID)); !os.IsNotExist(err) {
		t.Fatalf("taskdir bleef achter na artifactfout: %v", err)
	}
	r.mu.RLock()
	_, hasDir := r.taskDirs[task.ID]
	_, hasMounts := r.mounts[task.ID]
	r.mu.RUnlock()
	if hasDir || hasMounts {
		t.Fatalf("runner-bookkeeping bleef achter: taskDir=%v mounts=%v", hasDir, hasMounts)
	}
}

func TestSetupTaskDirRollsBackPartialSetup(t *testing.T) {
	base := t.TempDir()
	hosts := t.TempDir()
	goodHost := filepath.Join(hosts, "a-good")
	badParent := filepath.Join(hosts, "z-bad")
	if err := os.Mkdir(goodHost, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(badParent, []byte("not a directory"), 0644); err != nil {
		t.Fatal(err)
	}

	r := NewExecRunner(&Config{RootfsBase: base})
	job := &types.Job{Volumes: map[string]string{
		goodHost:                      "/data",
		filepath.Join(badParent, "x"): "/bad",
	}}
	if _, err := r.setupTaskDir("partial", job); err == nil {
		t.Fatal("setupTaskDir hoorde op het ongeldige tweede volume te falen")
	}
	if _, err := os.Stat(filepath.Join(base, "partial")); !os.IsNotExist(err) {
		t.Fatalf("partiële taskdir bleef achter: %v", err)
	}
}
