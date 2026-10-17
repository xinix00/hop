package dns

import (
	"net"
	"testing"
)

func TestCacheSetGetCluster(t *testing.T) {
	c := NewCache()

	ips := []net.IP{net.ParseIP("192.168.1.10"), net.ParseIP("192.168.1.20")}
	c.Set("prod", "myapp", ips)

	got := c.GetCluster("prod", "myapp")
	if len(got) != 2 {
		t.Errorf("GetCluster() returned %d IPs, want 2", len(got))
	}
}

func TestCacheGetClusterEmpty(t *testing.T) {
	c := NewCache()

	got := c.GetCluster("prod", "nonexistent")
	if got != nil {
		t.Errorf("GetCluster() returned %v, want nil", got)
	}
}

func TestCacheGetClusterIsolation(t *testing.T) {
	c := NewCache()

	c.Set("prod-eu", "myapp", []net.IP{net.ParseIP("10.0.0.1")})
	c.Set("prod-us", "myapp", []net.IP{net.ParseIP("10.0.1.1")})

	got := c.GetCluster("prod-eu", "myapp")
	if len(got) != 1 {
		t.Errorf("GetCluster(prod-eu) returned %d IPs, want 1", len(got))
	}
	if !got[0].Equal(net.ParseIP("10.0.0.1")) {
		t.Errorf("GetCluster(prod-eu) returned %s, want 10.0.0.1", got[0])
	}

	got = c.GetCluster("nonexistent", "myapp")
	if got != nil {
		t.Errorf("GetCluster(nonexistent) returned %v, want nil", got)
	}
}

func TestCacheUpdate(t *testing.T) {
	c := NewCache()

	c.Set("prod", "app1", []net.IP{net.ParseIP("10.0.0.1")})

	newData := map[string][]net.IP{
		"app2": {net.ParseIP("10.0.0.2")},
		"app3": {net.ParseIP("10.0.0.3")},
	}
	c.Update("prod", newData)

	if got := c.GetCluster("prod", "app1"); got != nil {
		t.Error("app1 should be gone after Update")
	}
	if got := c.GetCluster("prod", "app2"); len(got) != 1 {
		t.Error("app2 should have 1 IP")
	}
	if got := c.GetCluster("prod", "app3"); len(got) != 1 {
		t.Error("app3 should have 1 IP")
	}
}

func TestCacheClear(t *testing.T) {
	c := NewCache()

	c.Set("prod", "myapp", []net.IP{net.ParseIP("10.0.0.1")})
	c.Set("staging", "myapp", []net.IP{net.ParseIP("10.0.1.1")})

	c.Clear("prod")

	if got := c.GetCluster("prod", "myapp"); got != nil {
		t.Error("prod cluster should be cleared")
	}
	if got := c.GetCluster("staging", "myapp"); len(got) != 1 {
		t.Error("staging cluster should still have data")
	}
}

func TestCacheGetMergedDedupes(t *testing.T) {
	c := NewCache()
	c.Set("a", "web", []net.IP{net.ParseIP("10.0.0.1"), net.ParseIP("10.0.0.2")})
	c.Set("b", "web", []net.IP{net.ParseIP("10.0.0.2"), net.ParseIP("10.0.0.3")}) // .2 is a dup
	c.Set("a", "other", []net.IP{net.ParseIP("10.9.9.9")})                        // different job

	merged := c.GetMerged("web")
	if len(merged) != 3 {
		t.Fatalf("expected 3 deduped IPs across clusters, got %d (%v)", len(merged), merged)
	}
}
