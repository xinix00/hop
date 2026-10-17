package dns

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNewCNAMEsExact(t *testing.T) {
	c := NewCNAMEs(map[string]string{
		"mail.hop.local": "mailserver.example.com",
		"GIT.hop.local":  "gitea.prod-eu.hop.local.",
	})

	tests := []struct {
		query  string
		want   string
		wantOk bool
	}{
		{"mail.hop.local", "mailserver.example.com.", true},
		{"mail.hop.local.", "mailserver.example.com.", true},
		{"MAIL.HOP.LOCAL", "mailserver.example.com.", true},
		{"git.hop.local", "gitea.prod-eu.hop.local.", true},
		{"unknown.hop.local", "", false},
	}

	for _, tt := range tests {
		got, ok := c.Lookup(tt.query)
		if ok != tt.wantOk || got != tt.want {
			t.Errorf("Lookup(%q) = (%q, %v), want (%q, %v)", tt.query, got, ok, tt.want, tt.wantOk)
		}
	}
}

func TestNewCNAMEsWildcard(t *testing.T) {
	c := NewCNAMEs(map[string]string{
		"*.apps.hop.local": "ingress.prod-eu.hop.local",
	})

	tests := []struct {
		query  string
		want   string
		wantOk bool
	}{
		{"foo.apps.hop.local", "ingress.prod-eu.hop.local.", true},
		{"bar.apps.hop.local.", "ingress.prod-eu.hop.local.", true},
		{"deep.nested.apps.hop.local", "ingress.prod-eu.hop.local.", true},
		{"apps.hop.local", "", false}, // wildcard doesn't match parent itself
		{"other.hop.local", "", false},
	}

	for _, tt := range tests {
		got, ok := c.Lookup(tt.query)
		if ok != tt.wantOk || got != tt.want {
			t.Errorf("Lookup(%q) = (%q, %v), want (%q, %v)", tt.query, got, ok, tt.want, tt.wantOk)
		}
	}
}

func TestCNAMEsExactBeatsWildcard(t *testing.T) {
	c := NewCNAMEs(map[string]string{
		"*.apps.hop.local": "ingress.hop.local",
		"web.apps.hop.local": "special.hop.local",
	})

	got, ok := c.Lookup("web.apps.hop.local")
	if !ok || got != "special.hop.local." {
		t.Errorf("exact should beat wildcard: got (%q, %v), want (special.hop.local., true)", got, ok)
	}

	got, ok = c.Lookup("other.apps.hop.local")
	if !ok || got != "ingress.hop.local." {
		t.Errorf("wildcard should match: got (%q, %v), want (ingress.hop.local., true)", got, ok)
	}
}

func TestCNAMEsNilSafe(t *testing.T) {
	var c *CNAMEs
	if _, ok := c.Lookup("anything.hop.local"); ok {
		t.Error("nil CNAMEs.Lookup should return false")
	}
	if n := c.Len(); n != 0 {
		t.Errorf("nil CNAMEs.Len = %d, want 0", n)
	}
}

func TestCNAMEsIgnoresEmpty(t *testing.T) {
	c := NewCNAMEs(map[string]string{
		"":              "target",
		"alias":         "",
		"  ":            "target",
		"keep.hop.local": "target.example.com",
	})
	if c.Len() != 1 {
		t.Errorf("expected 1 entry after dropping empties, got %d", c.Len())
	}
}

func TestCNAMEsZones(t *testing.T) {
	c := NewCNAMEs(map[string]string{
		"mail.hop.local":                                                "mail.example.com",
		"git.hop.local":                                                 "gitea.example.com",
		"database-cluster01-server00.production.svc.cluster.local":      "bridge-cluster01-server00.ts.net",
		"*.apps.hop.local":                                              "ingress.hop.local",
	})

	got := c.Zones()
	want := map[string]bool{
		"hop.local.":                       true,
		"production.svc.cluster.local.":    true,
		"apps.hop.local.":                  true,
	}

	if len(got) != len(want) {
		t.Errorf("Zones() len = %d, want %d (got %v)", len(got), len(want), got)
	}
	for _, z := range got {
		if !want[z] {
			t.Errorf("unexpected zone %q in Zones()", z)
		}
	}
}

func TestLoadConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "hopdns.yaml")

	content := []byte(`cnames:
  mail.hop.local: mailserver.example.com
  "*.apps.hop.local": ingress.prod-eu.hop.local
`)
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if got := cfg.CNAMEs["mail.hop.local"]; got != "mailserver.example.com" {
		t.Errorf("mail.hop.local = %q", got)
	}
	if got := cfg.CNAMEs["*.apps.hop.local"]; got != "ingress.prod-eu.hop.local" {
		t.Errorf("*.apps.hop.local = %q", got)
	}
}

func TestLoadConfigMissing(t *testing.T) {
	if _, err := LoadConfig("/nonexistent/path/hopdns.yaml"); err == nil {
		t.Error("expected error for missing file")
	}
}
