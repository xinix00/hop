package runner

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/xinix00/hop/internal/types"
	"github.com/xinix00/hop/pkg/hopos"
)

// HopRunner runs tasks on HopOS: each task is a native Go image started on a
// dedicated CPU core (a "slot") with its own memory partition. HopOS enforces
// isolation and MemoryLimit in hardware, so — like DockerRunner and unlike
// ExecRunner — this runner only passes image + env + limits along and does no
// isolation plumbing of its own.
//
// Ports: every task gets its own network stack on HopOS' internal net; the
// node publishes each allocated port (name -> port) via stateless DNAT to
// the task's stack. The task binds the same port number it is published on
// (read from ER_PORT_<NAME>, injected below), matching the other runners.
//
// Volumes (shared path -> local path) pass through to HopOS: the task gets
// its own empty root plus exactly the mounted shared dirs, served by HOP's
// NVMe-backed storage layer over the hop-ABI. No memory is ever shared.

// hopStopTimeout is the cooperative window before Stop escalates to the
// stage-2 revoke. A healthy app parks within ~100ms of the kill-flag (the
// watch loop polls every 50ms), so 3s is generous; the old 10s made every
// uncooperative stop in a delete-storm burn 10 serialized seconds (measured
// 15-07: 127 deletes took tens of minutes).
const hopStopTimeout = 3 * time.Second

// HopRunner implements Runner against a SlotManager.
type HopRunner struct {
	sm        hopos.SlotManager
	nodeAttrs map[string]string

	// logs zijn de broadcasters van de lopende tasks plus die van net-afgelopen
	// tasks (zie logStore): na een crash of in een restart-lus kun je zo nog even
	// zien wat de task zei. Eigen slot, buiten r.mu.
	logs *logStore

	mu    sync.RWMutex
	slots map[string]int // taskID -> slot
	inUse map[int]string // slot -> taskID
	// faultLogged guards the once-per-task failure diagnostic in Status:
	// Status is polled continuously, the WHY of a failure should be logged
	// exactly once. Cleared in release alongside the other task maps.
	faultLogged map[string]bool
	// stopping markeert tasks waarvoor Stop is begonnen. runViaStream leest
	// dit (onder r.mu) i.p.v. task.State: dat veld muteert in de
	// agent-state-loop en hier raw pollen was een data race. Gewist in
	// release, samen met de andere task-maps.
	stopping map[string]bool

	// Het one-phase startpad (hopos_stream.go): voor iedere toegewezen slot
	// bestaan cancel/done/armed al VOORDAT de slot zichtbaar wordt. Daardoor
	// kan Stop nooit een half-gepubliceerde start voor een draaiende app aanzien
	// en de slot vrijgeven terwijl StartStream er nog in schrijft.
	// progress is de agent-sink voor queued→downloading; downloads is de
	// beurt-semafoor (maxConcurrentDownloads).
	progress  ProgressSink
	cancels   map[string]func()
	dones     map[string]chan struct{}
	armed     map[string]bool
	downloads chan struct{}
}

// PoolLargest reports the largest partition the node can still place, or 0 when
// the node cannot answer. It forwards hopos.PoolReporter, which is optional on
// purpose: a driver that does not know its own fragmentation says nothing and
// admission keeps using the sum.
func (r *HopRunner) PoolLargest() uint64 {
	if pr, ok := r.sm.(hopos.PoolReporter); ok {
		return pr.PoolLargest()
	}
	return 0
}

// NewHopRunner creates a runner on top of a SlotManager. nodeAttrs are
// injected as ER_ATTR_* env vars, matching the other runners.
func NewHopRunner(sm hopos.SlotManager, nodeAttrs map[string]string) *HopRunner {
	return &HopRunner{
		sm:          sm,
		nodeAttrs:   nodeAttrs,
		logs:        newLogStore(),
		slots:       make(map[string]int),
		inUse:       make(map[int]string),
		faultLogged: make(map[string]bool),
		stopping:    make(map[string]bool),
		cancels:     make(map[string]func()),
		dones:       make(map[string]chan struct{}),
		armed:       make(map[string]bool),
		downloads:   make(chan struct{}, maxConcurrentDownloads),
	}
}

// Run loads the job's artifact (the native app image) and starts it on a free
// slot. The slot number is recorded as task.Pid ("process id" = core index).
//
// Whatever goes wrong on the way lands in THIS task's log (see runLogged): on a
// node there is no console to fall back on, and a start that fails has no app
// yet to tell you why. MEASURED 12-08 on a LicheeRV: a job sat at "failed,
// restarts 5" and the reason ("no free cage of class …") existed only as text on
// a serial line nobody was reading — `hop logs <task>` said "not found".
func (r *HopRunner) Run(job *types.Job, task *types.Task) error {
	// De log van deze task bestaat vóór de eerste faalbare stap, want anders is
	// er geen plek om de fout in te schrijven. De slot-pomp die er later regels
	// in giet komt bij de start van de app (zie onder) — die hoort NIET hier,
	// want zonder slot is er niets te pompen.
	stdout := r.logs.stdout(task.ID)
	if stdout == nil {
		var stderr *LogBroadcaster
		stdout, stderr = r.logs.newPair()
		// hop apps have a single log ring; stderr stays empty
		r.logs.put(task.ID, stdout, stderr)
	}
	err := r.run(job, task)
	if err != nil {
		_, _ = stdout.Write([]byte("hop: this task did not start: " + err.Error() + "\n"))
		// Meteen met pensioen: er komt hierna geen regel meer bij, en zo blijft de
		// reden nog logRetention lang opvraagbaar zonder dat een restart-lus een
		// broadcaster per poging laat staan. Zonder dit zou élke mislukte start
		// een levende entry achterlaten die niemand meer sluit.
		r.logs.retire(task.ID)
	}
	return err
}

func (r *HopRunner) run(job *types.Job, task *types.Task) error {
	if job.Image != "" {
		return errors.New("hop driver: containers are not supported on HopOS")
	}
	if len(job.Artifacts) != 1 {
		return errors.New("hop driver: exactly one artifact (the app image) is required")
	}
	if job.Artifacts[0].Extract != "" {
		return errors.New("hop driver: artifact must be a raw app image (no extract)")
	}

	env := make(map[string]string, len(job.Env)+len(r.nodeAttrs)+len(task.Ports))
	for k, v := range job.Env {
		env[k] = v
	}
	for _, kv := range AttrEnvVars(r.nodeAttrs) {
		if i := strings.IndexByte(kv, '='); i > 0 {
			env[kv[:i]] = kv[i+1:]
		}
	}
	for _, kv := range PortEnvVars(task.Ports) {
		if i := strings.IndexByte(kv, '='); i > 0 {
			env[kv[:i]] = kv[i+1:]
		}
	}

	// cores = CPUShares/1024 (Docker/Nomad-conventie: 1024 shares = 1 core),
	// minimaal 1. Met cores > 1 draait de app SMP op één primair slot plus de
	// volgende cores-1 cores (gedeelde heap); HopOS brengt die transparant op.
	cores := job.CPUShares / 1024
	if cores < 1 {
		cores = 1
	}

	// Sharegroup (tag): coöperatieve core-deling op HopOS. Zonder de tag is elke
	// app een eigen SMP-eenheid die `cores` aaneengesloten cores dedicated pakt
	// (het bestaande gedrag). Mét de tag deelt de app een POOL van `cores` hele
	// cores met gelijk-getagde apps: dan reserveren we hier maar één kooi (HopOS
	// stapelt ze op de pool), draait de app zelf op één core, en is `cores` de
	// poolgrootte. Andere drivers (exec/docker) negeren de tag.
	sharegroup := job.Tags["sharegroup"]
	appCores, poolCores, allocCages := cores, 1, cores
	if sharegroup != "" {
		appCores, poolCores, allocCages = 1, cores, 1
	}

	// Kooi EERST alloceren, dán pas downloaden: op een volle node reject dit
	// meteen (geen vrije kooi) zonder één byte te trekken. Zo kan een storm
	// van jobs nooit meer images tegelijk laten downloaden dan er kooien zijn
	// — en met StartStream landt elke download rechtstreeks in de eigen
	// partitie i.p.v. de HOP-kern (geen core-0-OOM meer, gemeten 14-07).
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	r.mu.Lock()
	slot, err := r.allocateSlotLocked(job.Tags["core-class"], allocCages, sharegroup != "")
	if err != nil {
		r.mu.Unlock()
		cancel()
		return err
	}
	// De kooi(en) van deze app als bezet vastleggen (SMP: primair + secundairen;
	// sharegroup: één kooi — de pool-cores beheert HopOS, niet HOP's kooi-tabel).
	for c := slot; c < slot+allocCages; c++ {
		r.inUse[c] = task.ID
	}
	r.slots[task.ID] = slot
	r.cancels[task.ID] = cancel
	r.dones[task.ID] = done
	r.armed[task.ID] = false
	r.mu.Unlock()

	// One-phase start: de node streamt het image zélf de partitie in — de
	// partitie draagt dan alleen de app, en de startfase is zichtbaar als
	// queued→downloading. runViaStream geeft de slot bij een fout al vrij.
	started, err := r.runViaStream(ctx, cancel, done, job, task, slot, appCores, sharegroup, poolCores, env)
	if err != nil {
		return err
	}
	if !started {
		// Task tijdens staging gestopt; Stop() ruimt de slot op (of deed
		// dat al). HIER stoppen: broadcasters registreren of task.Pid
		// zetten zou naar een vrijgegeven (en mogelijk hergebruikte) slot
		// wijzen — de ghost-guard's Stop(task) killde daarmee via de
		// task.Pid-fallback de task van een ANDER die de slot al had.
		return nil
	}

	// Publiceer de draaiende task atomair t.o.v. Stop. Als Stop na het armen maar
	// vóór deze stap begon, laat die de app stoppen en registreren wij geen ghost
	// die naar een inmiddels vrijgegeven slot wijst.
	r.mu.Lock()
	if r.stopping[task.ID] || r.slots[task.ID] != slot {
		r.mu.Unlock()
		return nil
	}
	task.Pid = slot
	logLines := r.sm.Logs(slot)
	r.mu.Unlock()

	// De log van deze task staat er al (Run zette hem klaar voordat er iets kon
	// falen); hier komt alleen de slot-pomp erbij.
	stdout := r.logs.stdout(task.ID)

	// What kind of cage did this task land in? The node knows, and on a headless
	// board its serial console is the only other place it would say so — a line
	// that drops bytes at 115200. Putting it first in the task's own log makes it
	// arrive intact, and puts it where an operator already looks (`hop logs`).
	// Empty on architectures where the cage is a stage-2 map: there is nothing
	// there that the Fault* fields do not already carry.
	if cage := r.sm.Status(slot).Cage; cage != "" {
		_, _ = stdout.Write([]byte(cage))
	}

	go func() {
		for line := range logLines {
			_, _ = stdout.Write([]byte(line))
		}
	}()
	return nil
}

// placementErr maps HopOS' "cannot place the cage" (hopos.ErrNoCapacity, raised
// by the node's PlaceCage) onto the driver-agnostic runner.ErrNoCapacity, so the
// agent can act on it without importing the HopOS contract: this driver knows
// HopOS, the agent only knows runners. Anything else passes through untouched.
func placementErr(err error) error {
	if errors.Is(err, hopos.ErrNoCapacity) {
		return fmt.Errorf("%w: %v", ErrNoCapacity, err)
	}
	return err
}

// Stop asks the slot to shut down and frees it.
func (r *HopRunner) Stop(task *types.Task) error {
	r.mu.Lock()
	slot, ok := r.slots[task.ID]
	if !ok {
		// Onbekende taak = niet van ons. Dat blijft zo, en met opzet: een
		// STALE task-record mag nooit de nieuwe bewoner van dat kooinummer
		// omleggen (TestRepeatedHopStopCannotKillReusedSlot). Kooien die deze
		// runner niet zelf startte maar wél toebehoren — de bewoners die een
		// kern-flip overleefden — komen via AdoptRunning binnen; dát is een
		// expliciete eigendomsoverdracht, geen gok op een kooinummer.
		r.mu.Unlock()
		return nil
	}
	// Vlag vóór de (blokkerende) sm.Stop: een runViaStream die nog moet
	// beginnen ziet 'm en stapt er graceful uit — Stop ruimt op.
	r.stopping[task.ID] = true
	cancel, done := r.cancels[task.ID], r.dones[task.ID]
	r.mu.Unlock()
	return r.stopOwned(task.ID, slot, cancel, done)
}

func (r *HopRunner) stopOwned(taskID string, slot int, cancel func(), done chan struct{}) error {
	// Loopt er nog een stream-download, breek hem dan af en wacht tot de
	// node-kant klaar is: StartStream ruimt zijn eigen allocaties op zijn
	// foutpad op, en pas als dat gebeurd is weten we of er iets gearmd is.
	// Zonder dit wachten zou sm.Stop hieronder een partitie kunnen vrijgeven
	// waar de stream nog in schrijft.
	if cancel != nil {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * downloadStallTimeout):
			// StartStream kan nog in de partitie schrijven. Vrijgeven maakt de
			// slot dan direct herbruikbaar en corrumpeert de volgende task. Houd
			// ownership daarom in de runner vast als quarantaine.
			return fmt.Errorf("hop driver: task %.8s: stream did not wind down after cancel; slot %d quarantined", taskID, slot)
		}
	}
	armed := true
	if cancel != nil {
		r.mu.RLock()
		armed = r.armed[taskID]
		r.mu.RUnlock()
	}
	var err error
	if armed {
		err = r.sm.Stop(slot, hopStopTimeout)
	}
	if err != nil {
		// De driver kon release niet bevestigen. Laat de slot geclaimd zodat
		// niemand hem kan hergebruiken op basis van een onbevestigde release.
		return err
	}
	r.release(taskID)
	return nil
}

// Status maps slot state onto task state.
func (r *HopRunner) Status(task *types.Task) (types.TaskState, error) {
	// task.Pid wordt pas gezet als Run volledig klaar is. Zolang het 0 is, is
	// de task nog in zijn startfase (queued/downloading) — en die states
	// bevraagt de monitor niet, dus hier hoort dan niemand te komen. Toch
	// binnen? Dan is Running het eerlijke antwoord: de start loopt nog.
	if task.Pid == 0 {
		return types.TaskRunning, nil
	}
	r.mu.RLock()
	slot, ok := r.slots[task.ID]
	r.mu.RUnlock()
	if !ok {
		slot = task.Pid
	}

	s := r.sm.Status(slot)
	switch {
	case s.CoreOn:
		return types.TaskRunning, nil
	case s.App == hopos.SlotExited && s.ExitCode == 0:
		return types.TaskFailed, nil
	default:
		// The WHY, once: a stage-2 fault (cage violation / hard-kill, with
		// ESR/FAR from the EL2 vectors) or a nonzero exit. Without this the
		// reason lives only on the node's console — invisible on a headless
		// Altra. Diagnostics only; the state machine stays CoreOn/Exited.
		r.mu.Lock()
		if !r.faultLogged[task.ID] {
			r.faultLogged[task.ID] = true
			switch {
			case s.FaultVec != 0:
				log.Printf("hop task %s failed: stage-2 fault on slot %d (vec %d, ESR %#x, FAR %#x)",
					task.ID, slot, s.FaultVec-1, s.FaultESR, s.FaultFAR)
			case s.Cage != "":
				log.Printf("hop task %s failed: exit code %d (slot %d) — %s", task.ID, s.ExitCode, slot, s.Cage)
			case s.ExitCode != 0:
				log.Printf("hop task %s failed: exit code %d (slot %d)", task.ID, s.ExitCode, slot)
			}
		}
		r.mu.Unlock()
		return types.TaskFailed, nil
	}
}

// Usage returns the task's CPU usage and actual memory draw, both reported
// by the node/app themselves: cpuPct is the percentage of the task's OWN
// cores (0-100, from the slot's idle-tick counter — an idle core ticks at
// event-stream tempo, a computing core doesn't; negative while no sample
// window has completed yet), memBytes is Go MemStats.Sys (refreshed ~2s over
// the control page next to the heartbeat). ok is false while no memory has
// been reported yet (task still starting, or a pre-MemSys image). The agent
// monitor feeds this into the same CPUPercent/MemPercent pipeline the
// exec/docker drivers use — so HOP knows what a task USES, not just what it
// was allotted.
func (r *HopRunner) Usage(task *types.Task) (cpuPct float64, memBytes uint64, ok bool) {
	if task.Pid == 0 {
		return -1, 0, false // still starting (loader/staging phase)
	}
	r.mu.RLock()
	slot, found := r.slots[task.ID]
	r.mu.RUnlock()
	if !found {
		slot = task.Pid
	}
	s := r.sm.Status(slot)
	return float64(s.CPUPct), s.MemSys, s.MemSys != 0
}

// GetStdout returns the log broadcaster fed by the slot's hop-ABI log ring, or
// the retired one of a task that has already finished (see logStore) — asking a
// crashed task what it said is the whole reason those are kept.
func (r *HopRunner) GetStdout(taskID string) *LogBroadcaster { return r.logs.stdout(taskID) }

// GetStderr returns an empty broadcaster (hop apps have a single log stream),
// retired ones included — same reason as GetStdout.
func (r *HopRunner) GetStderr(taskID string) *LogBroadcaster { return r.logs.stderr(taskID) }

// Cleanup is a no-op: every HopOS start loads a fresh image into the slot
// partition, and after a node reboot all cores are off by construction.
func (r *HopRunner) Cleanup() error { return nil }

// allocateSlotLocked finds a run of `cores` contiguous free slots (each
// honoring an optional core-class tag) and returns the first (the primary).
// For cores == 1 this is just the first free slot; for an SMP app the run is
// the primary plus its secondary cores. Caller holds r.mu.
func (r *HopRunner) allocateSlotLocked(coreClass string, cores int, shared bool) (int, error) {
	// Shared cage IDs live above the physical range, leaving low IDs available
	// for SMP (whose primary cage must equal its primary core). Core class is
	// checked by the node against physical pool cores, never these cage IDs.
	first, limit := 1, -1
	if shared {
		first = r.sm.NumCores() + 1
	} else if coreClass != "" || cores > 1 {
		limit = r.sm.NumCores()
	}
	for slot := first; limit < 0 || slot+cores-1 <= limit; slot++ {
		if !shared && slot+cores-1 <= r.sm.NumCores() {
			if placement, ok := r.sm.(hopos.DedicatedPlacement); ok && !placement.CanPlaceDedicated(slot, cores) {
				continue
			}
		}
		ok := true
		for c := slot; c < slot+cores; c++ {
			// Twee vragen, en maar één ervan is van ons. r.inUse is puur een
			// RESERVERING voor starts die nog onderweg zijn (tussen deze
			// toewijzing en StartStream weet de node nog van niets, en twee
			// gelijktijdige dispatches mogen niet dezelfde kooi pakken).
			// Wat er ECHT draait weet alleen HopOS, en dat vragen we hem —
			// in plaats van een eigen kopie bij te houden die kan afdrijven.
			//
			// Dat afdrijven is geen theorie: na een kern-flip adopteert de
			// nieuwe kern de bewoners en krijgt de agent zijn taken terug,
			// maar deze map is dan een verse map in geheugen. Hij deelde
			// vrolijk kooi 1 opnieuw uit terwijl welcome erin draaide, en de
			// node weigerde dat terecht met "slot 1 still live" (GEMETEN
			// 02-09, M4, eerste job ná een geslaagde flip). Derek: "als HopOS
			// het weet en HOP weet wat er draait, waarom is er dan nog een
			// derde afhankelijkheid?"
			if _, busy := r.inUse[c]; busy {
				ok = false
				break
			}
			if r.sm.Status(c).CoreOn {
				ok = false
				break
			}
			if !shared && coreClass != "" && r.sm.CoreClass(c) != coreClass {
				ok = false
				break
			}
		}
		if ok {
			return slot, nil
		}
	}
	return 0, fmt.Errorf("%w: hop driver: no %d contiguous free %q cores", ErrNoCapacity, cores, coreClass)
}

// AdoptRunning draagt de kooien van al-draaiende taken over aan deze runner:
// na een KERN-FLIP adopteert de nieuwe kern de bewoners en krijgt de agent
// zijn taken terug uit de handoff, maar de lifecycle-plumbing van deze runner
// (welke task welke kooi houdt) is een verse map in geheugen. Zonder deze stap
// is zo'n bewoner niet meer te stoppen: Stop kent zijn kooi niet, geeft stil
// nil terug, en de kooi draait door op de node terwijl HOP de task opruimt
// (GEMETEN 02-09 op de M4: welcome verwijderd, node meldde hem nog live).
//
// Dit is géén tweede waarheid over wat er draait — dat vraagt de toewijzer aan
// de node (zie allocateSlotLocked). Het is puur EIGENDOM: welke task mag deze
// kooi straks stoppen. Idempotent; het pakt nooit een kooi af die deze runner
// zelf al uitdeelde.
//
// slots is task-ID → kooinummer (types.Task.Pid, door de node gevuld); cores
// hoeveel kooien die taak houdt (SMP-apps houden er meer).
func (r *HopRunner) AdoptRunning(slots map[string]int, cores map[string]int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, slot := range slots {
		if slot < 1 {
			continue
		}
		if _, known := r.slots[id]; known {
			continue
		}
		n := cores[id]
		if n < 1 {
			n = 1
		}
		r.slots[id] = slot
		for c := slot; c < slot+n; c++ {
			if _, busy := r.inUse[c]; !busy {
				r.inUse[c] = id
			}
		}
		// The old kernel's log pump and broadcasters did not survive the
		// flip. Reconnect the adopted task to its existing slot log stream.
		stdout, stderr := r.logs.newPair()
		r.logs.put(id, stdout, stderr)
		logLines := r.sm.Logs(slot)
		go func() {
			for line := range logLines {
				_, _ = stdout.Write([]byte(line))
			}
		}()
	}
}

func (r *HopRunner) release(taskID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if cancel := r.cancels[taskID]; cancel != nil {
		cancel()
	}
	// Free every core held by this task (an SMP app holds several).
	for slot, id := range r.inUse {
		if id == taskID {
			delete(r.inUse, slot)
		}
	}
	delete(r.slots, taskID)
	delete(r.faultLogged, taskID)
	delete(r.stopping, taskID)
	delete(r.cancels, taskID)
	delete(r.dones, taskID)
	delete(r.armed, taskID)
	// De logs gaan niet weg maar met pensioen: nog logRetention opvraagbaar.
	r.logs.retire(taskID)
}
