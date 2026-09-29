package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/xinix00/hop/pkg/config"
)

func TestGetOrCreateNodeID_FromConfig(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Node.ID = "my-custom-id"

	id := getOrCreateNodeID(cfg)

	if id != "my-custom-id" {
		t.Errorf("expected 'my-custom-id', got %s", id)
	}
}

func TestGetOrCreateNodeID_Persistence(t *testing.T) {
	tempDir := t.TempDir()
	cfg := config.DefaultConfig()
	cfg.Paths.StateFile = filepath.Join(tempDir, "state.json")
	cfg.Node.ID = "" // Force generation

	// First call - generates and persists
	id1 := getOrCreateNodeID(cfg)
	if id1 == "" {
		t.Fatal("expected non-empty ID")
	}
	if len(id1) != 8 {
		t.Errorf("expected 8 char ID, got %d: %s", len(id1), id1)
	}

	// Verify file exists
	idFile := filepath.Join(tempDir, "node-id")
	if _, err := os.Stat(idFile); err != nil {
		t.Fatalf("node-id file should exist: %v", err)
	}

	// Second call - should return same ID
	id2 := getOrCreateNodeID(cfg)
	if id1 != id2 {
		t.Errorf("expected stable ID: first=%s, second=%s", id1, id2)
	}
}

func TestGetOrCreateNodeID_ConfigOverridesPersisted(t *testing.T) {
	tempDir := t.TempDir()
	cfg := config.DefaultConfig()
	cfg.Paths.StateFile = filepath.Join(tempDir, "state.json")

	// First: generate and persist
	cfg.Node.ID = ""
	id1 := getOrCreateNodeID(cfg)

	// Second: config overrides
	cfg.Node.ID = "override-id"
	id2 := getOrCreateNodeID(cfg)

	if id2 != "override-id" {
		t.Errorf("config should override persisted: got %s", id2)
	}

	if id1 == id2 {
		t.Error("expected different IDs (override should work)")
	}
}
