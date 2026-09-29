package leader

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/xinix00/hop/internal/types"
)

// TestDeleteJobByName verifies delete by name works (API compatibility)
func TestDeleteJobByName(t *testing.T) {
	store := NewMockJobStore()
	leader := New("leader", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	// Create job (Name is the unique key)
	job := &types.Job{
		Name:    "test-app",
		Command: "./test",
		Count:   1,
	}

	// Store via jobStore
	leader.jobStore.StoreJob(job)

	// Verify it exists via GetJob
	found := store.GetJob("test-app")
	if found == nil {
		t.Fatal("GetJob should find the job")
	}

	// Delete by name (like GUI does)
	leader.DeleteJobByName("test-app")
	time.Sleep(10 * time.Millisecond)

	// Verify deleted
	found = store.GetJob("test-app")
	if found != nil {
		t.Errorf("Job should be deleted, but found: Name=%s", found.Name)
	}

	jobs := store.GetJobs()
	if len(jobs) != 0 {
		t.Errorf("Store should be empty, got %d jobs", len(jobs))
	}
}

// TestDeleteSweepSpaartHersubmit: een legitieme her-submit tijdens een
// lopende delete licht de grafsteen (DispatchJob, synchroon) — de
// naveeg-sweep moet daar dan vanaf blijven. Regressie: de sweep checkte
// alleen GetJob != nil en veegde daarmee ook de nieuwe job van de
// gebruiker weg, die al "dispatched" als antwoord had gekregen.
func TestDeleteSweepSpaartHersubmit(t *testing.T) {
	store := NewMockJobStore()
	leader := New("leader", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	// Trage agent: de delete blokkeert ~300ms op de agent-stop — precies
	// het venster waarin de her-submit binnenkomt.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			time.Sleep(300 * time.Millisecond)
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	leader.RegisterAgent("agent-1", srv.URL, "", nil)
	leader.Heartbeat("agent-1", "", 0)
	time.Sleep(10 * time.Millisecond)

	store.StoreJob(&types.Job{Name: "app", Command: "./v1", Count: 1})
	leader.do(func(s *leaderState) {
		s.placed["agent-1"] = map[string]int{"app": 1}
	})
	time.Sleep(10 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		leader.DeleteJobByName("app")
		close(done)
	}()

	// Her-submit terwijl de delete nog op de trage agent wacht.
	time.Sleep(50 * time.Millisecond)
	if err := leader.DispatchJob(&types.Job{Name: "app", Command: "./v2", Count: 1}); err != nil {
		t.Fatalf("DispatchJob: %v", err)
	}

	<-done
	got := store.GetJob("app")
	if got == nil {
		t.Fatal("her-submit is door de delete-sweep weggeveegd")
	}
	if got.Command != "./v2" {
		t.Fatalf("verwachtte de her-submit (./v2), kreeg %q", got.Command)
	}
}

// TestGetJobByName verifies jobStore.GetJob works with name-indexed store
func TestGetJobByName(t *testing.T) {
	store := NewMockJobStore()
	leader := New("leader", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	// Store multiple jobs
	jobs := []*types.Job{
		{Name: "app-1", Command: "echo 1"},
		{Name: "app-2", Command: "echo 2"},
		{Name: "app-3", Command: "echo 3"},
	}

	for _, j := range jobs {
		store.StoreJob(j)
	}
	time.Sleep(10 * time.Millisecond)

	// GetJob should work
	found := store.GetJob("app-2")
	if found == nil {
		t.Fatal("GetJob should find app-2")
	}
	if found.Name != "app-2" {
		t.Errorf("Found job Name = %s, want app-2", found.Name)
	}

	// Non-existent name should return nil
	notFound := store.GetJob("nonexistent")
	if notFound != nil {
		t.Error("GetJob should return nil for nonexistent job")
	}
}
