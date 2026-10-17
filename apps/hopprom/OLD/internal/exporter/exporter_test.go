package exporter

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/xinix00/hopprom/internal/collector"
)

type fixedMetrics struct{ m *collector.Metrics }

func (f fixedMetrics) GetMetrics() *collector.Metrics { return f.m }

func TestLeaderAndLeaseMetrics(t *testing.T) {
	m := &collector.Metrics{
		TasksByState:      map[string]int{},
		TasksByJob:        map[string]int{},
		TaskRestartsByJob: map[string]int{},
		JobInstances:      map[string]*collector.JobMetric{},
		TaskStartsTotal:   map[string]int{},
		TaskFailuresTotal: map[string]int{},
		TaskRestartsTotal: map[string]int{},
		AgentLastSeen:     map[string]time.Time{},
		AgentCapacity:     map[string]*collector.CapacityResponse{},
		AgentCPUUsed:      map[string]float64{},
		AgentMemoryUsed:   map[string]uint64{},
		Leader:            "10.0.0.2:9080",
		LeaseExpiresAt:    time.Now().Add(90 * time.Second),
		LeaderChanges:     3,
		JobsDeploying:     []string{"api"},
		Settling:          true,
	}
	e := New(fixedMetrics{m})
	w := httptest.NewRecorder()
	e.ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	out := w.Body.String()
	for _, want := range []string{
		`hop_leader_info{leader="10.0.0.2:9080"} 1`,
		"hop_leader_changes_total 3",
		"hop_cluster_settling 1",
		"hop_jobs_deploying 1",
		`hop_job_deploying{job="api"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("metrics output lacks %q\n%s", want, out)
		}
	}
	if !strings.Contains(out, "hop_leader_lease_seconds 8") && !strings.Contains(out, "hop_leader_lease_seconds 9") {
		t.Errorf("hop_leader_lease_seconds not ~90: %s", out)
	}
}
