package leader

import (
	"testing"
	"time"

	"github.com/xinix00/hop/internal/types"
	"github.com/xinix00/hop/pkg/httputil"
)

// ============== BASIC LEADER TESTS ==============

func TestLeaderNew(t *testing.T) {
	store := NewMockJobStore()
	leader := New("agent-1", store, nil)

	if leader == nil {
		t.Fatal("New returned nil")
	}
	if leader.localAgentID != "agent-1" {
		t.Errorf("localAgentID = %q, want %q", leader.localAgentID, "agent-1")
	}
}

func TestLeaderNewWithCustomClient(t *testing.T) {
	store := NewMockJobStore()
	customClient := &httputil.Client{Timeout: 30 * time.Second}
	leader := New("agent-1", store, customClient)

	if leader.httpClient != customClient {
		t.Error("httpClient should be the custom client")
	}
}

func TestLeaderGetJobs(t *testing.T) {
	store := NewMockJobStore()
	store.StoreJob(&types.Job{Name: "job1"})
	store.StoreJob(&types.Job{Name: "job2"})

	leader := New("local-agent", store, nil)

	jobs := leader.GetJobs()
	if len(jobs) != 2 {
		t.Errorf("GetJobs() returned %d jobs, want 2", len(jobs))
	}
}
