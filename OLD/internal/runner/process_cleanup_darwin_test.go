//go:build darwin

package runner

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCleanupTaskDirQuarantinesOnUnmountFailure(t *testing.T) {
	base := t.TempDir()
	taskDir := filepath.Join(base, "quarantine")
	mountTarget := filepath.Join(taskDir, "data")
	if err := os.MkdirAll(mountTarget, 0755); err != nil {
		t.Fatal(err)
	}
	child := filepath.Join(mountTarget, "still-busy")
	if err := os.WriteFile(child, []byte("keep"), 0644); err != nil {
		t.Fatal(err)
	}

	r := NewExecRunner(&Config{RootfsBase: base})
	r.taskDirs["quarantine"] = taskDir
	r.mounts["quarantine"] = []string{mountTarget}
	if err := r.cleanupTaskDir("quarantine"); err == nil {
		t.Fatal("cleanup slikte de unmountfout")
	}
	if _, err := os.Stat(child); err != nil {
		t.Fatalf("taskdir werd ondanks onbevestigde unmount verwijderd: %v", err)
	}
	if r.taskDirs["quarantine"] == "" {
		t.Fatal("quarantaine verloor cleanup-ownership")
	}

	if err := os.Remove(child); err != nil {
		t.Fatal(err)
	}
	if err := r.cleanupTaskDir("quarantine"); err != nil {
		t.Fatalf("cleanup retry: %v", err)
	}
	if _, err := os.Stat(taskDir); !os.IsNotExist(err) {
		t.Fatalf("taskdir bleef na bevestigde retry bestaan: %v", err)
	}
}
