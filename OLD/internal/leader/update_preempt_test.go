package leader

import (
	"testing"
	"time"

	"github.com/xinix00/hop/internal/types"
)

func intp(v int) *int { return &v }

// De les van 01-08 op de LicheeRV (1 core, 2 kooien): een re-apply van welcome
// telde oud+nieuw even samen, de node meldde "vol", en de leader offerde
// cloudflared (prio 1) via de preemptie-pas — voor een update die vijf tellen
// later vanzelf gepast had. Een update mag NOOIT een buurman preempten: de
// terugval is stop-oud-eerst, en de onderbreking hoort bij de job die
// geüpdatet wordt.
func TestUpdateRollingPreemptGeenBuurman(t *testing.T) {
	agent := newMockAgent()
	defer agent.Close()
	agent.maxCapacity = 2 // de node: plek voor precies twee taken

	store := NewMockJobStore()
	leader := New("local", store, nil)
	ctx, cancel := newTestContext()
	defer cancel()
	go leader.Run(ctx)
	time.Sleep(10 * time.Millisecond)

	leader.RegisterAgent("node-1", agent.URL(), "", nil)
	leader.Heartbeat("node-1", "", 0)

	// De buurman (minder belangrijk — prio 1 in nice-semantiek) en de job die
	// straks geüpdatet wordt. Samen is de node vol.
	buurman := &types.Job{Name: "cloudflared", Command: "./tunnel", Priority: intp(1)}
	if err := leader.DispatchJob(buurman); err != nil {
		t.Fatalf("buurman plaatsen: %v", err)
	}
	oud := &types.Job{Name: "welcome", Command: "./welcome-v1", Priority: intp(0)}
	if err := leader.DispatchJob(oud); err != nil {
		t.Fatalf("welcome plaatsen: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	// De update: oud+nieuw passen niet naast elkaar (maxCapacity=2), dus het
	// oude pad preemptte hier de buurman. Het nieuwe pad stopt eerst zijn
	// EIGEN oude exemplaar en plaatst dan.
	nieuw := &types.Job{Name: "welcome", Command: "./welcome-v2", Priority: intp(0), UpdatePolicy: types.UpdateRolling}
	if err := leader.UpdateJob(nieuw); err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	// De buurman leeft nog, en welcome draait in de nieuwe versie.
	agent.mu.Lock()
	var cfd, v2 int
	for _, task := range agent.tasks {
		switch {
		case task.JobName == "cloudflared":
			cfd++
		case task.JobName == "welcome":
			v2++
		}
	}
	agent.mu.Unlock()
	if cfd != 1 {
		t.Errorf("buurman cloudflared: %d taken, wil 1 — de update heeft hem gepreempt", cfd)
	}
	if v2 != 1 {
		t.Errorf("welcome: %d taken na de update, wil 1", v2)
	}
	if got := store.GetJob("welcome").Command; got != "./welcome-v2" {
		t.Errorf("welcome-definitie = %q, wil ./welcome-v2", got)
	}
}

// De hand-back moet de plaatsing afboeken: de agent verwijderde de taak
// (onplaatsbaar), dus placed-1 en meteen reconcilen — anders bleef de teller
// op 1 staan en zag reconcile voorgoed een gezonde job waar niets draaide
// (gemeten 01-08: cloudflared eeuwig pending naast vrije ruimte).
func TestMarkUnplacedBoektAfEnReconcilet(t *testing.T) {
	agent := newMockAgent()
	defer agent.Close()

	store := NewMockJobStore()
	leader := New("local", store, nil)
	ctx, cancel := newTestContext()
	defer cancel()
	go leader.Run(ctx)
	time.Sleep(10 * time.Millisecond)

	leader.RegisterAgent("node-1", agent.URL(), "", nil)
	leader.Heartbeat("node-1", "", 0)

	job := &types.Job{Name: "cloudflared", Command: "./tunnel"}
	if err := leader.DispatchJob(job); err != nil {
		t.Fatalf("plaatsen: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	if got := len(leader.GetPlaced("cloudflared")); got != 1 {
		t.Fatalf("placed vooraf = %d, wil 1", got)
	}

	// De agent geeft de taak terug (bij hem verwijderd) en meldt unplaceable.
	agent.mu.Lock()
	agent.tasks = nil
	agent.mu.Unlock()
	leader.MarkUnplaced("node-1", "cloudflared")

	// De afboeking is direct; de reconcile die erachteraan komt plaatst hem
	// opnieuw zodra de agent weer wil (hier: meteen).
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		agent.mu.Lock()
		n := len(agent.tasks)
		agent.mu.Unlock()
		if n == 1 {
			return // opnieuw geplaatst — de lus is rond
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("job kwam na de hand-back nooit terug (reconcile bleef uit)")
}
