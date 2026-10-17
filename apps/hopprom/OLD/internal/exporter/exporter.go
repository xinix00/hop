package exporter

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/xinix00/hopprom/internal/collector"
)

// Exporter exposes metrics in Prometheus format
type Exporter struct {
	collector MetricsSource
}

// New creates a new Prometheus exporter
// MetricsSource is what the exporter renders: the collector, or a fixed
// set in tests.
type MetricsSource interface {
	GetMetrics() *collector.Metrics
}

func New(c MetricsSource) *Exporter {
	return &Exporter{
		collector: c,
	}
}

// metric writes a single Prometheus metric line
func metric(b *strings.Builder, name, help, mtype string) {
	fmt.Fprintf(b, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, mtype)
}

// sortedKeys returns sorted keys from a map
func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// ServeHTTP handles /metrics requests
func (e *Exporter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m := e.collector.GetMetrics()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")

	var b strings.Builder

	// Agent metrics
	metric(&b, "hop_agents_total", "Total number of registered agents", "gauge")
	fmt.Fprintf(&b, "hop_agents_total %d\n\n", m.AgentsTotal)

	metric(&b, "hop_agents_healthy", "Number of healthy agents (seen in last 60s)", "gauge")
	fmt.Fprintf(&b, "hop_agents_healthy %d\n\n", m.AgentsHealthy)

	// Agent capacity metrics
	if len(m.AgentCapacity) > 0 {
		metric(&b, "hop_agent_cpu_cores", "Total CPU cores per agent", "gauge")
		for id, cap := range m.AgentCapacity {
			fmt.Fprintf(&b, "hop_agent_cpu_cores{agent=%q} %d\n", id, cap.CPUCores)
		}
		b.WriteString("\n")

		metric(&b, "hop_agent_cpu_used_cores", "Used CPU cores per agent", "gauge")
		for id, used := range m.AgentCPUUsed {
			fmt.Fprintf(&b, "hop_agent_cpu_used_cores{agent=%q} %.2f\n", id, used)
		}
		b.WriteString("\n")

		metric(&b, "hop_agent_memory_bytes", "Total memory bytes per agent", "gauge")
		for id, cap := range m.AgentCapacity {
			fmt.Fprintf(&b, "hop_agent_memory_bytes{agent=%q} %d\n", id, cap.MemoryBytes)
		}
		b.WriteString("\n")

		metric(&b, "hop_agent_memory_used_bytes", "Used memory bytes per agent", "gauge")
		for id, used := range m.AgentMemoryUsed {
			fmt.Fprintf(&b, "hop_agent_memory_used_bytes{agent=%q} %d\n", id, used)
		}
		b.WriteString("\n")
	}

	// Task metrics by state
	metric(&b, "hop_tasks_total", "Number of tasks by state", "gauge")
	for state, count := range m.TasksByState {
		fmt.Fprintf(&b, "hop_tasks_total{state=%q} %d\n", state, count)
	}
	b.WriteString("\n")

	// Task metrics by job. "placed", not "running": the cluster knows a task
	// was placed and the node reports it started — real liveness is the node's
	// (and the health check's) business, not something the cluster asserts.
	if len(m.TasksByJob) > 0 {
		metric(&b, "hop_tasks_placed", "Placed tasks per job", "gauge")
		for job, count := range m.TasksByJob {
			fmt.Fprintf(&b, "hop_tasks_placed{job=%q} %d\n", job, count)
		}
		b.WriteString("\n")
	}

	// Task restart metrics
	if len(m.TaskRestartsByJob) > 0 {
		metric(&b, "hop_task_restarts", "Current restart count per job", "gauge")
		for job, count := range m.TaskRestartsByJob {
			fmt.Fprintf(&b, "hop_task_restarts{job=%q} %d\n", job, count)
		}
		b.WriteString("\n")
	}

	// Job metrics
	metric(&b, "hop_jobs_total", "Total number of jobs", "gauge")
	fmt.Fprintf(&b, "hop_jobs_total %d\n\n", m.JobsTotal)

	// Job instances (sorted for consistent output)
	if len(m.JobInstances) > 0 {
		jobs := sortedKeys(m.JobInstances)

		metric(&b, "hop_job_instances_placed", "Placed instances per job", "gauge")
		for _, job := range jobs {
			fmt.Fprintf(&b, "hop_job_instances_placed{job=%q} %d\n", job, m.JobInstances[job].Placed)
		}
		b.WriteString("\n")

		metric(&b, "hop_job_instances_expected", "Expected instances per job", "gauge")
		for _, job := range jobs {
			fmt.Fprintf(&b, "hop_job_instances_expected{job=%q} %d\n", job, m.JobInstances[job].Expected)
		}
		b.WriteString("\n")

		metric(&b, "hop_job_healthy", "Job is healthy (placed >= expected)", "gauge")
		for _, job := range jobs {
			health := 0
			if m.JobInstances[job].Healthy {
				health = 1
			}
			fmt.Fprintf(&b, "hop_job_healthy{job=%q} %d\n", job, health)
		}
		b.WriteString("\n")

		metric(&b, "hop_job_cpu_percent", "Average CPU usage percent per job (relative to allocation)", "gauge")
		for _, job := range jobs {
			fmt.Fprintf(&b, "hop_job_cpu_percent{job=%q} %.1f\n", job, m.JobInstances[job].CPUPercent)
		}
		b.WriteString("\n")

		metric(&b, "hop_job_mem_percent", "Average memory usage percent per job (relative to allocation)", "gauge")
		for _, job := range jobs {
			fmt.Fprintf(&b, "hop_job_mem_percent{job=%q} %.1f\n", job, m.JobInstances[job].MemPercent)
		}
		b.WriteString("\n")
	}

	// Counters (monotonic) — use shared sorted keys helper
	writeCounter := func(name, help string, data map[string]int) {
		if len(data) == 0 {
			return
		}
		metric(&b, name, help, "counter")
		for _, job := range sortedKeys(data) {
			fmt.Fprintf(&b, "%s{job=%q} %d\n", name, job, data[job])
		}
		b.WriteString("\n")
	}

	writeCounter("hop_task_starts_total", "Total task starts per job", m.TaskStartsTotal)
	writeCounter("hop_task_failures_total", "Total task failures per job", m.TaskFailuresTotal)
	writeCounter("hop_task_restarts_total", "Total task restarts per job", m.TaskRestartsTotal)

	// Leader and lease — the cluster's heartbeat as seen from this node.
	metric(&b, "hop_leader_info", "Current leader address as a label (1 = a leader is known)", "gauge")
	if m.Leader != "" {
		fmt.Fprintf(&b, "hop_leader_info{leader=%q} 1\n\n", m.Leader)
	} else {
		b.WriteString("hop_leader_info{leader=\"\"} 0\n\n")
	}
	metric(&b, "hop_leader_lease_seconds", "Seconds until the local leader's lease lapses unless renewed (0 when this node does not lead)", "gauge")
	lease := 0.0
	if !m.LeaseExpiresAt.IsZero() {
		if d := time.Until(m.LeaseExpiresAt).Seconds(); d > 0 {
			lease = d
		}
	}
	fmt.Fprintf(&b, "hop_leader_lease_seconds %.1f\n\n", lease)
	metric(&b, "hop_leader_changes_total", "Leader switches observed since hopprom started", "counter")
	fmt.Fprintf(&b, "hop_leader_changes_total %d\n\n", m.LeaderChanges)
	metric(&b, "hop_cluster_settling", "1 while a new leader waits before its first reconcile", "gauge")
	fmt.Fprintf(&b, "hop_cluster_settling %d\n\n", boolInt(m.Settling))
	metric(&b, "hop_jobs_deploying", "Jobs whose last rollout did not finish", "gauge")
	fmt.Fprintf(&b, "hop_jobs_deploying %d\n", len(m.JobsDeploying))
	for _, job := range m.JobsDeploying {
		fmt.Fprintf(&b, "hop_job_deploying{job=%q} 1\n", job)
	}
	b.WriteString("\n")

	_, _ = w.Write([]byte(b.String()))
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
