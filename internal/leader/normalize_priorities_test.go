package leader

import (
	"testing"

	"github.com/xinix00/hop/internal/types"
)

// Priority renumbering works on a snapshot. It must touch ONLY the priority
// in the store: a whole-job write from the snapshot clobbered the
// Deploying=false an update had just stored (traqqr, 2026-09-08).
func TestNormalizePrioritiesOnlyTouchesPriority(t *testing.T) {
	store := NewMockJobStore()
	leader := New("leader", store, nil)

	p9 := 9
	store.StoreJob(&types.Job{Name: "api", Command: "./api", Priority: &p9, Deploying: true})
	store.StoreJob(&types.Job{Name: "db", Command: "./db"}) // no priority yet

	snapshot := store.GetJobs() // reconcile takes its snapshot ...

	// ... and meanwhile the update finishes and clears Deploying.
	done := *store.GetJob("api")
	done.Deploying = false
	store.StoreJob(&done)

	leader.normalizePriorities(snapshot)

	api := store.GetJob("api")
	if api.Deploying {
		t.Fatal("api: Deploying=false written by the update was clobbered by priority renumbering")
	}
	if api.Priority == nil || *api.Priority != 0 {
		t.Fatalf("api: priority = %v, want 0", api.Priority)
	}
	db := store.GetJob("db")
	if db.Priority == nil || *db.Priority != 1 {
		t.Fatalf("db: priority = %v, want 1", db.Priority)
	}
	if db.Command != "./db" {
		t.Fatalf("db: command changed to %q", db.Command)
	}
}

// A job deleted after the snapshot must stay deleted.
func TestNormalizePrioritiesDoesNotResurrectDeletedJob(t *testing.T) {
	store := NewMockJobStore()
	leader := New("leader", store, nil)
	store.StoreJob(&types.Job{Name: "gone", Command: "./gone"})
	store.StoreJob(&types.Job{Name: "kept", Command: "./kept"})

	snapshot := store.GetJobs()
	store.DeleteJob("gone")

	leader.normalizePriorities(snapshot)

	if store.GetJob("gone") != nil {
		t.Fatal("deleted job resurrected by priority renumbering")
	}
	if kept := store.GetJob("kept"); kept.Priority == nil {
		t.Fatal("kept job did not get a priority")
	}
}

// Update clears Deploying through the store, never on the stored pointer.
func TestSetJobDeployingIsCopyOnWrite(t *testing.T) {
	store := NewMockJobStore()
	job := &types.Job{Name: "api", Command: "./api", Deploying: true}
	store.StoreJob(job)

	if !store.SetJobDeploying("api", false) {
		t.Fatal("SetJobDeploying returned false for an existing job")
	}
	if job.Deploying != true {
		t.Fatal("the caller's pointer was mutated; store must copy on write")
	}
	if cur := store.GetJob("api"); cur.Deploying || cur.Command != "./api" {
		t.Fatalf("stored job = %+v, want Deploying=false with fields intact", cur)
	}
	if store.SetJobDeploying("nope", false) {
		t.Fatal("SetJobDeploying returned true for an unknown job")
	}
}
