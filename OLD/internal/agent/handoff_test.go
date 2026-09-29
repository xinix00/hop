package agent

import (
	"context"
	"testing"

	"github.com/xinix00/hop/internal/types"
	"github.com/xinix00/hop/pkg/config"
)

// newHandoffAgent geeft een agent met een draaiende state-loop en verder niets
// — genoeg om de overdracht te toetsen zonder HTTP-server of runner.
func newHandoffAgent(t *testing.T) *Agent {
	t.Helper()
	a := New(&config.Config{}, "test-agent", &mockRunner{tasks: make(map[string]*types.Task)})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go a.stateLoop(ctx)
	return a
}

// De state-overdracht van de kern-flip: wat de vertrekkende agent opschrijft
// moet de nieuwe agent exact terugkrijgen. Gaat er één taak verloren, dan wil
// die agent hem plaatsen op een slot dat hij zelf al bezet — daarom is dit een
// round-trip-test en geen "het marshalt zonder fout"-test.
func TestHandoffRoundTrip(t *testing.T) {
	a := newHandoffAgent(t)
	a.do(func(s *agentState) {
		s.jobs["web"] = &types.Job{Name: "web", Driver: "hop"}
		s.tasks["T1"] = &types.Task{
			ID: "T1", JobName: "web", Driver: "hop",
			Pid: 3, State: types.TaskRunning, // Pid = slotnummer op HopOS
			Ports: map[string]int{"http": 8080}, MemoryLimit: 64 << 20,
		}
	})

	b, err := a.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	// Een verse agent, zoals na een kernwissel.
	b2 := newHandoffAgent(t)
	if err := b2.Restore(b); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	got := query(b2, func(s *agentState) *types.Task { return s.tasks["T1"] })
	if got == nil {
		t.Fatal("taak T1 kwam niet terug — de nieuwe agent zou hem opnieuw plaatsen")
	}
	if got.Pid != 3 || got.JobName != "web" || got.State != types.TaskRunning || got.Ports["http"] != 8080 {
		t.Errorf("taak kwam verminkt terug: %+v", got)
	}
	if j := query(b2, func(s *agentState) *types.Job { return s.jobs["web"] }); j == nil {
		t.Error("job web kwam niet terug")
	}
}

// Een blob dat niet klopt moet een FOUT zijn, geen halve state: de aanroeper
// kiest dan bewust voor een lege start (de taken draaien nog, ze zijn alleen
// even onbekend) in plaats van te denken dat de overdracht slaagde.
func TestHandoffWeigertOnzin(t *testing.T) {
	a := newHandoffAgent(t)
	if err := a.Restore([]byte("{niet eens json")); err == nil {
		t.Error("kapotte JSON werd geaccepteerd")
	}
	if err := a.Restore([]byte(`{"version":99,"jobs":{},"tasks":{}}`)); err == nil {
		t.Error("onbekende versie werd geaccepteerd")
	}
	if err := a.Restore(nil); err != nil {
		t.Errorf("een lege overdracht (gewone start) hoort geen fout te zijn: %v", err)
	}
}
