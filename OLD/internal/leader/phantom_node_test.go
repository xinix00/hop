package leader

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/xinix00/hop/internal/types"
	"github.com/xinix00/hop/pkg/httputil"
)

// TestNetworkBlipNewNodeJoins test het scenario:
//
// 1. 20 tasks verdeeld over 2 nodes (agent-a: 10, agent-b: 10)
// 2. agent-a heeft een korte netwerk timeout (< agentTimeout)
// 3. agent-c komt online binnen 2 sec
// 4. agent-a's tasks moeten NIET worden overgenomen (agent-a is niet dead!)
// 5. agent-a komt terug — alles is nog intact, geen duplicates
//
// De lean filosofie: niet redistribueren zolang de node niet getimeout is.
// De node die de task had, houdt die gewoon totdat de timeout verloopt.
func TestNetworkBlipNewNodeJoins(t *testing.T) {
	agentA := newMockAgent()
	agentB := newMockAgent()
	agentC := newMockAgent()
	defer agentA.Close()
	defer agentB.Close()
	defer agentC.Close()

	store := NewMockJobStore()
	leader := New("leader", store, &httputil.Client{Timeout: 100 * time.Millisecond})
	// agentTimeout = 2s: agent-a's blip van ~300ms triggert geen dead detection
	leader.agentTimeout = 2 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// ===== STAP 1: 20 tasks over 2 nodes =====
	leader.RegisterAgent("agent-a", agentA.URL(), "", nil)
	leader.RegisterAgent("agent-b", agentB.URL(), "", nil)
	leader.Heartbeat("agent-a", "", 0)
	leader.Heartbeat("agent-b", "", 0)
	time.Sleep(20 * time.Millisecond)

	job := &types.Job{
		Name:    "counter",
		Command: "sh -c 'i=0; while true; do echo counter: $i; i=$((i+1)); sleep 1; done'",
		Count:   20,
	}
	if err := leader.DispatchJob(job); err != nil {
		t.Fatalf("Dispatch failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	aTasks := agentA.TaskCount()
	bTasks := agentB.TaskCount()
	t.Logf("STAP 1 - Initieel: agent-a=%d, agent-b=%d (totaal=%d)", aTasks, bTasks, aTasks+bTasks)

	if aTasks+bTasks != 20 {
		t.Fatalf("Verwacht 20 tasks totaal, got %d", aTasks+bTasks)
	}

	// Verse heartbeats zodat LastSeen recent is
	leader.Heartbeat("agent-a", "", 0)
	leader.Heartbeat("agent-b", "", 0)
	time.Sleep(20 * time.Millisecond)

	aRunsBefore := agentA.RunCallCount()
	bRunsBefore := agentB.RunCallCount()

	// ===== STAP 2: agent-a heeft netwerk blip (~300ms) =====
	t.Log("STAP 2 - Agent A netwerk blip (mist heartbeats, maar < timeout)")

	// Agent-a stuurt geen heartbeats, agent-b wel
	time.Sleep(100 * time.Millisecond)
	leader.Heartbeat("agent-b", "", 0)
	time.Sleep(100 * time.Millisecond)
	leader.Heartbeat("agent-b", "", 0)

	// Dead agent check: agent-a is ~300ms zonder heartbeat, timeout is 2s → NIET dead
	leader.checkDeadAgents()
	time.Sleep(20 * time.Millisecond)

	// Verify agent-a is NIET verwijderd
	agents := leader.GetAgents()
	foundA := false
	for _, a := range agents {
		if a.ID == "agent-a" {
			foundA = true
		}
	}
	if !foundA {
		t.Fatal("agent-a zou NIET verwijderd moeten zijn (timeout niet bereikt)")
	}

	// ===== STAP 3: agent-c komt online binnen 2 sec =====
	t.Log("STAP 3 - Agent C komt online")
	leader.RegisterAgent("agent-c", agentC.URL(), "", nil)
	leader.Heartbeat("agent-c", "", 0)
	time.Sleep(100 * time.Millisecond)

	// Agent-c mag GEEN taken overnemen van agent-a!
	// reconcileJobs draait (isNew=true), maar GetClusterStatus ziet:
	// - agent-a: 10 running (agent is nog in agents map, endpoint bereikbaar)
	// - agent-b: 10 running
	// → 20 running, 20 desired → geen actie
	cTasks := agentC.TaskCount()
	if cTasks != 0 {
		t.Errorf("Agent-c kreeg %d tasks, verwacht 0 (agent-a is nog niet dead!)", cTasks)
	}

	// ===== STAP 4: Geen tasks verplaatst =====
	aRunsAfter := agentA.RunCallCount()
	bRunsAfter := agentB.RunCallCount()

	if aRunsAfter != aRunsBefore {
		t.Errorf("Agent-a kreeg %d nieuwe /run calls (verwacht 0)", aRunsAfter-aRunsBefore)
	}
	if bRunsAfter != bRunsBefore {
		t.Errorf("Agent-b kreeg %d nieuwe /run calls (verwacht 0)", bRunsAfter-bRunsBefore)
	}

	// ===== STAP 5: agent-a komt terug, alles intact =====
	t.Log("STAP 5 - Agent A netwerk hersteld")
	leader.Heartbeat("agent-a", "", 0)
	time.Sleep(50 * time.Millisecond)

	aFinal := agentA.TaskCount()
	bFinal := agentB.TaskCount()
	cFinal := agentC.TaskCount()
	total := aFinal + bFinal + cFinal

	t.Logf("RESULTAAT: agent-a=%d, agent-b=%d, agent-c=%d → totaal=%d (verwacht=20)",
		aFinal, bFinal, cFinal, total)

	if total != 20 {
		t.Errorf("Verwacht exact 20 tasks, got %d", total)
	}
	if aFinal != aTasks {
		t.Errorf("Agent-a tasks veranderd: was %d, nu %d", aTasks, aFinal)
	}
	if bFinal != bTasks {
		t.Errorf("Agent-b tasks veranderd: was %d, nu %d", bTasks, bFinal)
	}
	if cFinal != 0 {
		t.Errorf("Agent-c had geen tasks moeten krijgen, got %d", cFinal)
	}
}

// TestNetworkBlipAgentUnreachable test hetzelfde scenario maar waarbij
// agent-a's HTTP endpoint tijdelijk onbereikbaar is tijdens de blip.
//
// GetClusterStatus kan agent-a niet bereiken → ziet maar 10 tasks op agent-b.
// Maar agent-a staat NOG STEEDS in de agents map (timeout niet bereikt).
//
// Kritieke vraag: dispatch reconcileJobs dan 10 extra tasks?
// Antwoord: NEEN, want agent-a is niet dead. De leader moet wachten.
func TestNetworkBlipAgentUnreachable(t *testing.T) {
	agentA := newMockAgent()
	agentB := newMockAgent()
	agentC := newMockAgent()
	defer agentB.Close()
	defer agentC.Close()

	store := NewMockJobStore()
	leader := New("leader", store, &httputil.Client{Timeout: 100 * time.Millisecond})
	leader.agentTimeout = 2 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// 20 tasks over 2 nodes
	leader.RegisterAgent("agent-a", agentA.URL(), "", nil)
	leader.RegisterAgent("agent-b", agentB.URL(), "", nil)
	leader.Heartbeat("agent-a", "", 0)
	leader.Heartbeat("agent-b", "", 0)
	time.Sleep(20 * time.Millisecond)

	job := &types.Job{
		Name:    "counter",
		Command: "sh -c 'i=0; while true; do echo counter: $i; i=$((i+1)); sleep 1; done'",
		Count:   20,
	}
	if err := leader.DispatchJob(job); err != nil {
		t.Fatalf("Dispatch failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	aTasks := agentA.TaskCount()
	bTasks := agentB.TaskCount()
	t.Logf("Initieel: agent-a=%d, agent-b=%d", aTasks, bTasks)

	// Verse heartbeats
	leader.Heartbeat("agent-a", "", 0)
	leader.Heartbeat("agent-b", "", 0)
	time.Sleep(20 * time.Millisecond)

	// Agent-a wordt onbereikbaar (HTTP endpoint down)
	agentA.Close()
	t.Log("Agent A HTTP endpoint down (maar nog niet getimeout)")

	// Agent-b heartbeat normaal
	leader.Heartbeat("agent-b", "", 0)
	time.Sleep(50 * time.Millisecond)

	// checkDeadAgents: agent-a is ~100ms zonder heartbeat, timeout=2s → NIET dead
	leader.checkDeadAgents()
	time.Sleep(20 * time.Millisecond)

	// Agent-c komt online
	t.Log("Agent C komt online terwijl agent-a onbereikbaar is")
	leader.RegisterAgent("agent-c", agentC.URL(), "", nil)
	leader.Heartbeat("agent-c", "", 0)
	time.Sleep(100 * time.Millisecond)

	// KRITIEK: GetClusterStatus kan agent-a niet bereiken → ziet maar 10 tasks.
	// reconcileJobs draait (isNew=true voor agent-c) en ziet: 10/20 running.
	// MAAR: reconcileJobs mag NIET 10 extra dispatchen want agent-a is niet dead!
	//
	// DIT IS DE BUG: reconcileJobs kijkt alleen naar GetClusterStatus (wat 10 ziet)
	// en dispatcht 10 extra, terwijl agent-a ze nog gewoon draait.
	cTasks := agentC.TaskCount()
	bTasksAfter := agentB.TaskCount()
	newDispatches := (bTasksAfter - bTasks) + cTasks

	t.Logf("Na agent-c join: agent-b=%d (+%d), agent-c=%d",
		bTasksAfter, bTasksAfter-bTasks, cTasks)

	if newDispatches > 0 {
		t.Errorf("BUG: %d tasks onterecht gedispatched! Agent-a is niet dead (timeout niet bereikt),"+
			" maar reconcileJobs zag maar %d/%d via GetClusterStatus en dispatched het verschil.",
			newDispatches, bTasks, job.Count)
		t.Log("")
		t.Log("=== ROOT CAUSE ===")
		t.Log("reconcileJobs() vertrouwt blindelings op GetClusterStatus().")
		t.Log("Als een agent onbereikbaar is maar NIET dead (timeout niet bereikt),")
		t.Log("worden zijn tasks als 'missing' gezien en opnieuw gedispatched → duplicates!")
		t.Log("")
		t.Log("=== FIX ===")
		t.Log("reconcileJob moet het aantal VERWACHTE tasks per agent meenemen.")
		t.Log("Als agents.count * expected_per_agent >= desired, NIET redistribueren.")
		t.Log("Of: alleen redistribueren voor agents die ECHT dead zijn (uit agents map).")
	}

	// Agent-a komt terug
	agentARecovered := newMockAgent()
	defer agentARecovered.Close()

	// Agent-a had zijn tasks nog steeds draaien
	agentARecovered.mu.Lock()
	for i := 0; i < aTasks; i++ {
		agentARecovered.tasks = append(agentARecovered.tasks, &types.Task{
			ID:      fmt.Sprintf("original-task-%d", i),
			JobName: "counter",
			State:   types.TaskRunning,
		})
	}
	agentARecovered.mu.Unlock()

	leader.Heartbeat("agent-a", "", 0)
	time.Sleep(100 * time.Millisecond)

	aFinal := agentARecovered.TaskCount()
	bFinal := agentB.TaskCount()
	cFinal := agentC.TaskCount()
	total := aFinal + bFinal + cFinal

	t.Logf("Na recovery: agent-a=%d, agent-b=%d, agent-c=%d → totaal=%d (verwacht=20)",
		aFinal, bFinal, cFinal, total)

	if total != 20 {
		t.Errorf("Verwacht 20 tasks, got %d (%d overtollig)", total, total-20)
	}
}

// TestNetworkBlipNoRedistribution verifieert dat korte network blips
// (<agentTimeout) GEEN onnodige task redistributie veroorzaken.
// Basis case zonder nieuwe nodes.
func TestNetworkBlipNoRedistribution(t *testing.T) {
	agentA := newMockAgent()
	agentB := newMockAgent()
	defer agentA.Close()
	defer agentB.Close()

	store := NewMockJobStore()
	leader := New("leader", store, &httputil.Client{Timeout: 100 * time.Millisecond})
	leader.agentTimeout = 1 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	leader.RegisterAgent("agent-a", agentA.URL(), "", nil)
	leader.RegisterAgent("agent-b", agentB.URL(), "", nil)
	leader.Heartbeat("agent-a", "", 0)
	leader.Heartbeat("agent-b", "", 0)
	time.Sleep(20 * time.Millisecond)

	job := &types.Job{
		Name:    "counter",
		Command: "sh -c 'i=0; while true; do echo counter: $i; i=$((i+1)); sleep 1; done'",
		Count:   20,
	}
	if err := leader.DispatchJob(job); err != nil {
		t.Fatalf("Dispatch failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	aTasks := agentA.TaskCount()
	bTasks := agentB.TaskCount()
	t.Logf("Initial: agent-a=%d, agent-b=%d", aTasks, bTasks)

	// Verse heartbeats
	leader.Heartbeat("agent-a", "", 0)
	leader.Heartbeat("agent-b", "", 0)
	time.Sleep(20 * time.Millisecond)

	aRunsBefore := agentA.RunCallCount()
	bRunsBefore := agentB.RunCallCount()

	// Korte blip: agent-a mist heartbeats, agent-b blijft alive
	time.Sleep(100 * time.Millisecond)
	leader.Heartbeat("agent-b", "", 0)
	time.Sleep(100 * time.Millisecond)
	leader.Heartbeat("agent-b", "", 0)
	time.Sleep(100 * time.Millisecond)

	// agent-a is ~300ms zonder heartbeat, timeout=1s → NIET dead
	leader.checkDeadAgents()
	time.Sleep(20 * time.Millisecond)

	// Agent-a hersteld
	leader.Heartbeat("agent-a", "", 0)
	leader.Heartbeat("agent-b", "", 0)
	time.Sleep(50 * time.Millisecond)

	aRunsAfter := agentA.RunCallCount()
	bRunsAfter := agentB.RunCallCount()

	if aRunsAfter != aRunsBefore {
		t.Errorf("Agent-a kreeg %d nieuwe /run calls na blip (expected 0)", aRunsAfter-aRunsBefore)
	}
	if bRunsAfter != bRunsBefore {
		t.Errorf("Agent-b kreeg %d nieuwe /run calls na blip (expected 0)", bRunsAfter-bRunsBefore)
	}
	if agentA.TaskCount() != aTasks {
		t.Errorf("Agent-a tasks veranderd: was %d, nu %d", aTasks, agentA.TaskCount())
	}
	if agentB.TaskCount() != bTasks {
		t.Errorf("Agent-b tasks veranderd: was %d, nu %d", bTasks, agentB.TaskCount())
	}

	t.Log("OK: Korte netwerk blip veroorzaakte geen onnodige redistributie")
}

// TestPendingTasksScheduledOnNewNode test dat PENDING tasks (die niet geplaatst
// konden worden wegens gebrek aan capaciteit) WEL direct naar een nieuwe node gaan.
//
// Scenario:
// 1. Job count=20 maar agent-a kan max 10, agent-b kan max 5 → 15 geplaatst, 5 pending
// 2. Agent-c komt online
// 3. De 5 pending tasks moeten DIRECT naar agent-c
//
// Dit is ANDERS dan het netwerk blip scenario: hier zijn tasks ECHT pending,
// niet "tijdelijk onbereikbaar". Pending = nooit gestart = geen phantom risico.
func TestPendingTasksScheduledOnNewNode(t *testing.T) {
	agentA := newMockAgent()
	agentB := newMockAgent()
	defer agentA.Close()
	defer agentB.Close()

	store := NewMockJobStore()
	leader := New("leader", store, &httputil.Client{Timeout: 100 * time.Millisecond})
	leader.agentTimeout = 2 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// Register 2 agents
	leader.RegisterAgent("agent-a", agentA.URL(), "", nil)
	leader.RegisterAgent("agent-b", agentB.URL(), "", nil)
	time.Sleep(20 * time.Millisecond)

	// Dispatch 20 tasks → beide agents krijgen 10
	job := &types.Job{
		Name:    "counter",
		Command: "sh -c 'i=0; while true; do echo counter: $i; i=$((i+1)); sleep 1; done'",
		Count:   20,
	}
	if err := leader.DispatchJob(job); err != nil {
		t.Fatalf("Dispatch failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	aTasks := agentA.TaskCount()
	bTasks := agentB.TaskCount()
	total := aTasks + bTasks
	t.Logf("Initieel: agent-a=%d, agent-b=%d (totaal=%d)", aTasks, bTasks, total)

	if total != 20 {
		t.Fatalf("Verwacht 20 tasks, got %d", total)
	}

	// Simuleer dat agent-b 5 tasks verliest (crasht en herstart, verliest state)
	// Dit maakt 5 tasks "pending" vanuit de leader's perspectief
	agentB.mu.Lock()
	agentB.tasks = agentB.tasks[:5] // Houd maar 5 van de 10 over
	agentB.mu.Unlock()

	// Agent-b re-registers with reduced placed count → triggers reconcile → dispatches 5 missing
	leader.RegisterAgent("agent-a", agentA.URL(), "", map[string]int{"counter": aTasks})
	leader.RegisterAgent("agent-b", agentB.URL(), "", map[string]int{"counter": 5})
	time.Sleep(100 * time.Millisecond)

	// RegisterAgent triggers reconcile: placed=10+5=15, desired=20, dispatches 5
	aAfter := agentA.TaskCount()
	bAfter := agentB.TaskCount()
	totalAfter := aAfter + bAfter

	t.Logf("Na re-register: agent-a=%d, agent-b=%d (totaal=%d, verwacht=20)",
		aAfter, bAfter, totalAfter)

	if totalAfter != 20 {
		t.Errorf("Verwacht 20 tasks na reconcile, got %d", totalAfter)
	}

	// Agent-c joins → should not dispatch more (already at 20)
	agentC := newMockAgent()
	defer agentC.Close()

	leader.RegisterAgent("agent-c", agentC.URL(), "", nil)
	time.Sleep(100 * time.Millisecond)

	cTasks := agentC.TaskCount()
	t.Logf("Na agent-c join: agent-c=%d (verwacht 0, al op 20)", cTasks)

	if cTasks != 0 {
		t.Errorf("Agent-c should get 0 tasks (already at 20), got %d", cTasks)
	}
}
