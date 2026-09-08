package leader

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/xinix00/hop/internal/types"
)

// fakePersister vangt Save-snapshots en serveert een voorgekookte Load.
type fakePersister struct {
	mu       sync.Mutex
	saves    [][]byte
	loadData []byte
	loadOK   bool
}

func (f *fakePersister) Save(_ context.Context, b []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := make([]byte, len(b))
	copy(cp, b)
	f.saves = append(f.saves, cp)
	return nil
}

func (f *fakePersister) Load(_ context.Context) ([]byte, bool, error) {
	return f.loadData, f.loadOK, nil
}

func (f *fakePersister) last(t *testing.T) persistedState {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.saves) == 0 {
		t.Fatalf("geen snapshot geschreven")
	}
	var st persistedState
	if err := json.Unmarshal(f.saves[len(f.saves)-1], &st); err != nil {
		t.Fatalf("snapshot unmarshal: %v", err)
	}
	return st
}

// waitSaves wacht tot er minstens n snapshots zijn (debounce is 1s).
func (f *fakePersister) waitSaves(t *testing.T, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		f.mu.Lock()
		got := len(f.saves)
		f.mu.Unlock()
		if got >= n {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("geen %d snapshots binnen de tijd", n)
}

// TestCommittedState_MutatiesLandenInSnapshot: elke mutatie via de gewrapte
// store markeert dirty; de flusher schrijft één gecoalescede snapshot; een
// delete is afwezigheid in de volgende snapshot.
func TestCommittedState_MutatiesLandenInSnapshot(t *testing.T) {
	store := NewMockJobStore()
	l := New("n1", store, nil)
	f := &fakePersister{}
	l.SetStatePersister(f)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.stateLoop(ctx)
	go l.persistLoop(ctx)

	l.jobStore.StoreJob(&types.Job{Name: "aap"})
	l.jobStore.StoreJob(&types.Job{Name: "noot"})
	f.waitSaves(t, 1)
	st := f.last(t)
	if len(st.Jobs) != 2 {
		t.Fatalf("snapshot heeft %d jobs, verwacht 2", len(st.Jobs))
	}

	l.jobStore.DeleteJob("aap")
	f.waitSaves(t, 2)
	st = f.last(t)
	if len(st.Jobs) != 1 || st.Jobs[0].Name != "noot" {
		t.Fatalf("na delete verwacht alleen 'noot', kreeg %+v", st.Jobs)
	}
}

// TestCommittedState_BootLoad: een bestaande snapshot laadt de store; geen
// snapshot = schone boot; en de load zelf triggert geen zinloze her-save-lus.
func TestCommittedState_BootLoad(t *testing.T) {
	snap, _ := json.Marshal(persistedState{
		Updated: time.Now(),
		Jobs:    []*types.Job{{Name: "terug"}},
	})

	store := NewMockJobStore()
	// De store is bij boot NIET leeg: de agent laadde al zijn lokale
	// state.json. "spook" is elders verwijderd terwijl deze node down was
	// (staat niet in de snapshot) en moet bij de load verdwijnen — de
	// snapshot is de enige waarheid, niet meer, niet minder.
	store.StoreJob(&types.Job{Name: "spook"})
	l := New("n1", store, nil)
	f := &fakePersister{loadData: snap, loadOK: true}
	l.SetStatePersister(f)
	loaded, err := l.LoadCommittedState(context.Background())
	if err != nil {
		t.Fatalf("LoadCommittedState: %v", err)
	}
	if !loaded {
		t.Fatalf("snapshot bestond — loaded hoort true te zijn")
	}
	if l.jobStore.GetJob("terug") == nil {
		t.Fatalf("job 'terug' niet geladen uit de snapshot")
	}
	if l.jobStore.GetJob("spook") != nil {
		t.Fatalf("job 'spook' staat niet in de snapshot en had moeten verdwijnen (deletion is absence)")
	}

	// Schone boot: geen object.
	l2 := New("n2", NewMockJobStore(), nil)
	f2 := &fakePersister{loadOK: false}
	l2.SetStatePersister(f2)
	loaded2, err := l2.LoadCommittedState(context.Background())
	if err != nil {
		t.Fatalf("schone boot hoort geen fout te geven: %v", err)
	}
	if loaded2 {
		t.Fatalf("geen snapshot — loaded hoort false te zijn (clean boot)")
	}
	if jobs := l2.jobStore.GetJobs(); len(jobs) != 0 {
		t.Fatalf("schone boot hoort leeg te zijn, kreeg %d jobs", len(jobs))
	}
}

func TestCommitTimeoutIsHalfTheLease(t *testing.T) {
	l := New("n1", nil, nil)
	if got := l.commitTimeout(); got != defaultPersistTimeout {
		t.Fatalf("default = %v, want %v", got, defaultPersistTimeout)
	}
	l.SetLeaseTTL(120 * time.Second)
	if got := l.commitTimeout(); got != 60*time.Second {
		t.Fatalf("after SetLeaseTTL(120s) = %v, want 60s", got)
	}
	l.SetLeaseTTL(0) // no TTL wired: keep what we have
	if got := l.commitTimeout(); got != 60*time.Second {
		t.Fatalf("after SetLeaseTTL(0) = %v, want 60s", got)
	}
}
