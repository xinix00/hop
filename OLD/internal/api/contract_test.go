package api

import (
	"strings"
	"testing"
)

// Contract tests pin the documented API shape so docs and code can't drift
// apart silently. If someone changes a response, the matching doc must change
// too — or this build goes red.

// TestContract_StatusShape: /v1/status carries exactly the fields the CLI and
// satellites read, and NOT tasks_by_agent. The docs once claimed tasks_by_agent
// lived here; it never did, so `run logs` searched /v1/status and never found a
// task. tasks_by_agent lives in the per-job status endpoint (below).
func TestContract_StatusShape(t *testing.T) {
	server, _, cancel := setupTestServer(t)
	defer cancel()

	w := doRequest(server, "GET", "/v1/status", nil)
	if w.Code != 200 {
		t.Fatalf("/v1/status: got %d", w.Code)
	}
	body := w.Body.String()
	for _, field := range []string{"cluster_name", "agents", "jobs", "total_placed", "placed", "settling"} {
		if !strings.Contains(body, `"`+field+`"`) {
			t.Errorf("/v1/status missing documented field %q: %s", field, body)
		}
	}
	if strings.Contains(body, "tasks_by_agent") {
		t.Errorf("/v1/status must NOT contain tasks_by_agent (it lives in /v1/jobs/<name>/status): %s", body)
	}
}

// TestContract_JobStatusHasTasksByAgent: tasks_by_agent lives here — the
// endpoint `run logs` uses to map a task ID to its agent.
func TestContract_JobStatusHasTasksByAgent(t *testing.T) {
	server, _, cancel := setupTestServer(t)
	defer cancel()

	w := doRequest(server, "GET", "/v1/jobs/anything/status", nil)
	if w.Code != 200 {
		t.Fatalf("/v1/jobs/<name>/status: got %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "tasks_by_agent") {
		t.Errorf("/v1/jobs/<name>/status must expose tasks_by_agent: %s", w.Body.String())
	}
}
