package runner

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xinix00/hop/internal/types"
	"github.com/xinix00/hop/pkg/hopos"
)

// fakeSlotManager is an in-memory SlotManager for lifecycle tests.
type fakeSlotManager struct {
	mu    sync.Mutex
	slots map[int]*fakeSlot

	num     int
	classes map[int]string

	// cage is wat de node over de kooi van élk slot meldt (hopos.SlotStatus.Cage).
	cage string

	// Laatste sharegroup-plaatsing (voor assertions in de core-deling-test).
	lastCoreClass  string
	lastSharegroup string
	lastPoolCores  int
}

type fakeSlot struct {
	image    []byte
	memLimit uint64
	cores    int
	env      map[string]string
	mounts   map[string]string
	ports    map[string]int
	coreOn   bool
	app      uint64
	exitCode uint64
	cage     string
	logs     chan string
}

func newFakeSlotManager(num int) *fakeSlotManager {
	classes := make(map[int]string, num)
	for i := 1; i <= num; i++ {
		switch {
		case i <= 3:
			classes[i] = "small"
		case i <= 7:
			classes[i] = "mid"
		default:
			classes[i] = "big"
		}
	}
	return &fakeSlotManager{
		slots:   make(map[int]*fakeSlot),
		num:     num,
		classes: classes,
	}
}

func (f *fakeSlotManager) NumCores() int             { return f.num }
func (f *fakeSlotManager) CoreClass(slot int) string { return f.classes[slot] }

// StartStream is de one-phase start: de runner heeft de GET al gedaan en
// geeft de stroom door; de fake leest hem leeg en markeert het slot draaiend —
// het spiegelbeeld van kern/slots.StartStreamOn.
func (f *fakeSlotManager) StartStream(slot int, image io.Reader, size int64, spec hopos.StartSpec) error {
	if spec.Cores > f.num {
		return fmt.Errorf("%w: insufficient physical cores", hopos.ErrNoCapacity)
	}
	if spec.CoreClass != "" {
		found := false
		for _, class := range f.classes {
			found = found || class == spec.CoreClass
		}
		if !found {
			return fmt.Errorf("%w: no physical cores of class %q", hopos.ErrNoCapacity, spec.CoreClass)
		}
	}
	img, err := io.ReadAll(image)
	if err != nil {
		return err
	}
	if int64(len(img)) != size {
		return fmt.Errorf("fake: %d van %d bytes — afgekapte stroom", len(img), size)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastSharegroup, f.lastPoolCores = spec.Sharegroup, spec.PoolCores
	f.lastCoreClass = spec.CoreClass
	f.slots[slot] = &fakeSlot{
		image: img, memLimit: spec.MemLimit, cores: spec.Cores,
		env: spec.Env, mounts: spec.Mounts, ports: spec.Ports,
		coreOn: true, app: hopos.SlotReady, logs: make(chan string, 16),
	}
	f.slots[slot].logs <- "app leeft"
	return nil
}

func (f *fakeSlotManager) Stop(slot int, _ time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if s, ok := f.slots[slot]; ok && s.coreOn {
		s.coreOn = false
		s.app = hopos.SlotExited
		s.exitCode = 0
		close(s.logs)
	}
	return nil
}

func (f *fakeSlotManager) Status(slot int) hopos.SlotStatus {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, ok := f.slots[slot]
	if !ok {
		return hopos.SlotStatus{App: hopos.SlotEmpty}
	}
	cage := s.cage
	if cage == "" {
		cage = f.cage
	}
	return hopos.SlotStatus{CoreOn: s.coreOn, App: s.app, ExitCode: s.exitCode, Cage: cage}
}

func (f *fakeSlotManager) Logs(slot int) <-chan string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.slots[slot].logs
}

func (f *fakeSlotManager) slot(i int) *fakeSlot {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.slots[i]
}

// imageServer serves fake app-image bytes over HTTP for the artifact path.
func imageServer(t *testing.T, body []byte) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func hopJob(url string) *types.Job {
	return &types.Job{
		Name:        "demo",
		Driver:      types.DriverHop,
		Artifacts:   []types.Artifact{{URL: url + "/app.img"}},
		MemoryLimit: 64 << 20,
		Env:         map[string]string{"BUCKET": "hop-apps"},
	}
}

func TestHopRunnerLifecycle(t *testing.T) {
	image := []byte("ELF-achtige bytes")
	srv := imageServer(t, image)
	sm := newFakeSlotManager(11)
	r := NewHopRunner(sm, map[string]string{"node.os": "hopos"})
	job := hopJob(srv.URL)
	job.Volumes = map[string]string{"/data": "/data"}
	task := &types.Task{ID: "t1", JobName: "demo", Driver: types.DriverHop}

	if err := r.Run(job, task); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if task.Pid == 0 {
		t.Fatal("task.Pid (slot) not set")
	}

	s := sm.slot(task.Pid)
	if string(s.image) != string(image) {
		t.Fatal("image bytes not passed to slot")
	}
	if s.memLimit != 64<<20 {
		t.Fatalf("memLimit = %d, want %d (job.MemoryLimit moet doorkomen)", s.memLimit, 64<<20)
	}
	if s.env["BUCKET"] != "hop-apps" {
		t.Fatal("job env not passed")
	}
	if s.env["ER_ATTR_NODE_OS"] != "hopos" {
		t.Fatalf("node attrs not injected as ER_ATTR_*: %v", s.env)
	}
	if s.mounts["/data"] != "/data" {
		t.Fatalf("job.Volumes not passed to slot: %v", s.mounts)
	}

	if st, _ := r.Status(task); st != types.TaskRunning {
		t.Fatalf("Status = %v, want running", st)
	}

	// Log line must reach the stdout broadcaster.
	out := r.GetStdout(task.ID)
	if out == nil {
		t.Fatal("no stdout broadcaster")
	}
	deadline := time.Now().Add(2 * time.Second)
	for len(out.Tail()) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if tail := out.Tail(); len(tail) == 0 || tail[0] != "app leeft" {
		t.Fatalf("log ring not bridged to broadcaster: %v", tail)
	}

	if err := r.Stop(task); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if st, _ := r.Status(task); st != types.TaskFailed {
		t.Fatalf("Status after stop = %v, want failed/non-running", st)
	}
}

// De kooi-regel van de node moet in de eigen log van de task belanden, en als
// eerste regel. Dat is de hele reden dat het veld bestaat: op een headless board
// is de andere plek waar de node dit zou zeggen zijn seriële console, en die
// verliest bytes op 115200 — een gehavend hex-getal is erger dan geen.
func TestHopRunnerCageLineReachesTaskLog(t *testing.T) {
	srv := imageServer(t, []byte("ELF-achtige bytes"))
	sm := newFakeSlotManager(11)
	sm.cage = "hart of slot 1: misa 0x800000000094112d (rv64 acdfimsux, S-mode present)"
	r := NewHopRunner(sm, map[string]string{"node.os": "hopos"})
	task := &types.Task{ID: "t-cage", JobName: "demo", Driver: types.DriverHop}

	if err := r.Run(hopJob(srv.URL), task); err != nil {
		t.Fatalf("Run: %v", err)
	}
	out := r.GetStdout(task.ID)
	if out == nil {
		t.Fatal("no stdout broadcaster")
	}
	tail := out.Tail()
	if len(tail) == 0 || tail[0] != sm.cage {
		t.Fatalf("kooi-regel staat niet vooraan in de task-log: %v", tail)
	}
}

// Zonder kooi-regel (de architecturen waar de kooi een stage-2-map is) mag er
// geen lege regel vooraan komen: dan is de eerste regel die van de app.
func TestHopRunnerNoCageLineWhenEmpty(t *testing.T) {
	srv := imageServer(t, []byte("ELF-achtige bytes"))
	sm := newFakeSlotManager(11)
	r := NewHopRunner(sm, map[string]string{"node.os": "hopos"})
	task := &types.Task{ID: "t-nocage", JobName: "demo", Driver: types.DriverHop}

	if err := r.Run(hopJob(srv.URL), task); err != nil {
		t.Fatalf("Run: %v", err)
	}
	out := r.GetStdout(task.ID)
	deadline := time.Now().Add(2 * time.Second)
	for len(out.Tail()) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if tail := out.Tail(); len(tail) == 0 || tail[0] != "app leeft" {
		t.Fatalf("eerste regel moet van de app zijn: %v", tail)
	}
}

func TestHopRunnerPorts(t *testing.T) {
	srv := imageServer(t, []byte("img"))
	sm := newFakeSlotManager(11)
	r := NewHopRunner(sm, nil)
	job := hopJob(srv.URL)
	job.Ports = map[string]int{"http": 0} // 0 = dynamic; agent allocates
	task := &types.Task{ID: "t-ports", Ports: map[string]int{"http": 18080}}

	if err := r.Run(job, task); err != nil {
		t.Fatalf("Run: %v", err)
	}
	s := sm.slot(task.Pid)
	if s.ports["http"] != 18080 {
		t.Fatalf("task.Ports not passed to slot: %v", s.ports)
	}
	if s.env["ER_PORT_HTTP"] != "18080" {
		t.Fatalf("ER_PORT_HTTP not injected: %v", s.env)
	}
}

// TestHopRunnerDeletedDuringStaging: wordt de task tijdens staging gestopt
// (delete/preemptie → Stop), dan moet Run met nil eindigen ZONDER iets aan de
// (door Stop vrijgegeven) slot te koppelen — geen task.Pid, geen
// slots/inUse-entry, geen log-broadcaster. Anders stopte de ghost-guard via
// de task.Pid-fallback de slot van een ANDERE task die 'm intussen had.
func TestHopRunnerDeletedDuringStaging(t *testing.T) {
	// Trage download: Stop komt binnen terwijl de apploader nog fetcht.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(300 * time.Millisecond)
		_, _ = w.Write([]byte("img"))
	}))
	t.Cleanup(srv.Close)

	sm := newFakeSlotManager(11)
	r := NewHopRunner(sm, nil)
	job := hopJob(srv.URL)
	task := &types.Task{ID: "t-del", JobName: "demo", Driver: types.DriverHop}

	errCh := make(chan error, 1)
	go func() { errCh <- r.Run(job, task) }()

	time.Sleep(50 * time.Millisecond) // Run zit midden in de staging
	if err := r.Stop(task); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	if err := <-errCh; err != nil {
		t.Fatalf("Run hoort nil te geven bij stop-tijdens-staging, kreeg: %v", err)
	}
	if task.Pid != 0 {
		t.Fatalf("task.Pid = %d, hoort 0 te blijven (slot is vrijgegeven)", task.Pid)
	}
	r.mu.RLock()
	_, hasSlot := r.slots[task.ID]
	busy := len(r.inUse)
	stopping := len(r.stopping)
	r.mu.RUnlock()
	if hasSlot || busy != 0 || stopping != 0 {
		t.Fatalf("runner-state niet schoon: slot=%v inUse=%d stopping=%d", hasSlot, busy, stopping)
	}
	// De log van deze task mág bestaan (Run zet hem klaar vóór de eerste
	// faalbare stap, anders is er geen plek voor de faalreden) — maar hij is
	// met pensioen en leeg: er is niets gebeurd om te melden, en er hangt geen
	// pomp aan een slot dat alweer van iemand anders kan zijn.
	if lg := r.logs.stdout(task.ID); lg != nil && len(lg.Tail()) != 0 {
		t.Fatalf("log van een tijdens staging gestopte task draagt regels: %q", lg.Tail())
	}
}

func TestHopRunnerCoreClassAllocation(t *testing.T) {
	srv := imageServer(t, []byte("img"))
	sm := newFakeSlotManager(11)
	r := NewHopRunner(sm, nil)
	job := hopJob(srv.URL)
	job.Tags = map[string]string{"core-class": "big"}
	task := &types.Task{ID: "t-big"}

	if err := r.Run(job, task); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if task.Pid != 1 || sm.lastCoreClass != "big" {
		t.Fatalf("cage=%d core class=%q; want first cage and big cores", task.Pid, sm.lastCoreClass)
	}
}

func TestHopRunnerSMPCores(t *testing.T) {
	srv := imageServer(t, []byte("img"))
	sm := newFakeSlotManager(11)
	r := NewHopRunner(sm, nil)
	// CPUShares 3072 = 3 cores → één SMP-task op 3 aaneengesloten cores.
	job := hopJob(srv.URL)
	job.CPUShares = 3072
	task := &types.Task{ID: "t-smp"}
	if err := r.Run(job, task); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if s := sm.slot(task.Pid); s.cores != 3 {
		t.Fatalf("cores doorgegeven aan slot = %d, want 3", s.cores)
	}
	if len(r.inUse) != 1 || r.inUse[task.Pid] != task.ID {
		t.Fatalf("SMP must own one cage: %v", r.inUse)
	}
	neighbor := &types.Task{ID: "neighbor"}
	if err := r.Run(hopJob(srv.URL), neighbor); err != nil {
		t.Fatal(err)
	}
	if neighbor.Pid != 2 {
		t.Fatalf("neighbor cage=%d; want 2", neighbor.Pid)
	}
	if err := r.Stop(task); err != nil {
		t.Fatal(err)
	}
	if len(r.inUse) != 1 || r.inUse[2] != neighbor.ID || !sm.Status(2).CoreOn {
		t.Fatalf("SMP stop disturbed neighbor: %v", r.inUse)
	}

}

func TestHopRunnerSharegroup(t *testing.T) {
	srv := imageServer(t, []byte("img"))
	sm := newFakeSlotManager(11)
	r := NewHopRunner(sm, nil)
	// sharegroup-tag + CPUShares 2048: een POOL van 2 hele cores, gedeeld door
	// gelijk-getagde apps. HOP reserveert voor iedere app één kooi
	// (HopOS stapelt de apps op de pool) en draait de app zelf op één core.
	job := hopJob(srv.URL)
	job.CPUShares = 2048
	job.Tags = map[string]string{"sharegroup": "web"}
	task := &types.Task{ID: "t-sg"}
	if err := r.Run(job, task); err != nil {
		t.Fatalf("Run: %v", err)
	}
	// Naam + poolgrootte doorgegeven aan de SlotManager (uit tag + CPUShares).
	if sm.lastSharegroup != "web" || sm.lastPoolCores != 2 {
		t.Fatalf("StartLoader kreeg sharegroup=%q poolCores=%d, want web/2", sm.lastSharegroup, sm.lastPoolCores)
	}
	// De app draait op ÉÉN core (de 2 is de poolgrootte, niet de app-breedte).
	if s := sm.slot(task.Pid); s.cores != 1 {
		t.Fatalf("app-cores = %d, want 1", s.cores)
	}
	// Slechts één kooi bezet: er blijft ruimte
	// voor een mede-bewoner.
	busy := 0
	for _, id := range r.inUse {
		if id == task.ID {
			busy++
		}
	}
	if busy != 1 {
		t.Fatalf("sharegroup-app bezet %d kooien, want 1", busy)
	}
}

func TestHopRunnerRejections(t *testing.T) {
	srv := imageServer(t, []byte("img"))
	sm := newFakeSlotManager(2)
	r := NewHopRunner(sm, nil)
	cases := map[string]*types.Job{
		"container":   {Image: "nginx", Artifacts: []types.Artifact{{URL: srv.URL}}},
		"no artifact": {Command: "./app"},
		"extract":     {Artifacts: []types.Artifact{{URL: srv.URL, Extract: "tar.gz"}}},
	}
	for name, job := range cases {
		if err := r.Run(job, &types.Task{ID: name}); err == nil {
			t.Fatalf("%s: expected rejection", name)
		}
	}

	// Cages are not capped to the core count: HopOS stacks more cages than it
	// has cores (sharegroups), and the node — not the runner — rejects an
	// unplaceable job. So on a 2-core fake a third unconstrained Run still
	// allocates; capacity is enforced by admission + PlaceCage, not here.
	for i := 0; i < 3; i++ {
		if err := r.Run(hopJob(srv.URL), &types.Task{ID: string(rune('a' + i))}); err != nil {
			t.Fatalf("Run %d: %v", i, err)
		}
	}

	// A class-constrained job stays bounded by real cores: the 2-core fake has
	// only "small" cores, so a "big" request has nowhere to land.
	big := hopJob(srv.URL)
	big.Tags = map[string]string{"core-class": "big"}
	if err := r.Run(big, &types.Task{ID: "big"}); err == nil {
		t.Fatal("expected rejection: no big core on a small-only node")
	}
}

// De logs van een AFGELOPEN task blijven nog opvraagbaar: dat is het hele punt
// van de retentie. Zonder dit was de log weg op het moment dat je hem nodig had
// — release() gooide de broadcaster meteen weg, dus wie een gevallen task om zijn
// logs vroeg kreeg "task not found".
func TestHopRunnerLogsBlijvenNaHetEinde(t *testing.T) {
	srv := imageServer(t, []byte("ELF-achtige bytes"))
	sm := newFakeSlotManager(11)
	r := NewHopRunner(sm, map[string]string{"node.os": "hopos"})
	task := &types.Task{ID: "t-retire", JobName: "demo", Driver: types.DriverHop}

	if err := r.Run(hopJob(srv.URL), task); err != nil {
		t.Fatalf("Run: %v", err)
	}
	out := r.GetStdout(task.ID)
	deadline := time.Now().Add(2 * time.Second)
	for len(out.Tail()) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	before := out.Tail()
	if len(before) == 0 {
		t.Fatal("geen logregel vóór de stop")
	}

	if err := r.Stop(task); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	// Nog opvraagbaar, met dezelfde inhoud.
	after := r.GetStdout(task.ID)
	if after == nil {
		t.Fatal("logs zijn weg direct na de stop — de retentie doet niets")
	}
	if got, want := len(after.Tail()), len(before); got != want {
		t.Fatalf("tail na de stop = %d regels, want %d", got, want)
	}

	// En een lezer die nu abonneert krijgt de geschiedenis én een dicht kanaal:
	// de task is voorbij, dus zijn logstroom is voorbij. Zonder dat zou opvragen
	// blijven hangen, en dat leest als een vastgelopen node.
	ch := after.Subscribe()
	n := 0
	for range ch {
		n++
	}
	if n != len(before) {
		t.Fatalf("Subscribe gaf %d regels, want %d — en het kanaal moet sluiten", n, len(before))
	}
}

// Voorbij de retentietermijn is de geschiedenis wél weg: een node die dagen
// restart-lussen draait mag geen logs opstapelen.
func TestHopRunnerLogsVerlopenNaDeTermijn(t *testing.T) {
	srv := imageServer(t, []byte("ELF-achtige bytes"))
	sm := newFakeSlotManager(11)
	r := NewHopRunner(sm, map[string]string{"node.os": "hopos"})
	task := &types.Task{ID: "t-expire", JobName: "demo", Driver: types.DriverHop}

	if err := r.Run(hopJob(srv.URL), task); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if err := r.Stop(task); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	// De klok terugzetten i.p.v. vijf minuten wachten.
	backdateRetired(r.logs, task.ID)

	if r.GetStdout(task.ID) != nil {
		t.Fatal("logs zijn na de retentietermijn nog opvraagbaar")
	}
}

// TestHopRunnerFailureLandsInTaskLog — de reden waarom een task NIET startte
// hoort in de log van díe task te staan. Op een node is er geen console om op
// terug te vallen, en er is nog geen app die het zelf kan zeggen: zonder deze
// regel is een mislukte plaatsing van buiten alleen het woord "failed".
func TestHopRunnerFailureLandsInTaskLog(t *testing.T) {
	srv := imageServer(t, []byte("img"))
	r := NewHopRunner(newFakeSlotManager(3), nil)
	job := hopJob(srv.URL)
	job.Tags = map[string]string{"core-class": "big"} // deze node heeft alleen small
	task := &types.Task{ID: "kanniet", JobName: job.Name, Driver: types.DriverHop}

	err := r.Run(job, task)
	if err == nil {
		t.Fatal("Run hoort te falen: geen kooi van klasse big op deze node")
	}
	lg := r.logs.stdout(task.ID)
	if lg == nil {
		t.Fatal("de task heeft geen log, dus staat de faalreden nergens")
	}
	var got string
	for _, line := range lg.Tail() {
		got += line
	}
	if !strings.Contains(got, err.Error()) {
		t.Fatalf("log draagt de reden niet: %q (fout was %q)", got, err)
	}
}

// Na een kern-flip draaien er kooien die déze runner nooit uitdeelde (de
// nieuwe kern adopteerde ze, de agent kreeg zijn taken terug uit de handoff).
// Zonder AdoptRunning deelt de eerste nieuwe job er een opnieuw uit en weigert
// de node dat met "slot N still live" — gemeten 02-09 op de M4.
func TestAllocateSkipsCagesTheNodeReportsLive(t *testing.T) {
	sm := newFakeSlotManager(9)
	r := NewHopRunner(sm, map[string]string{"node.os": "hopos"})

	// De node meldt kooi 1 en 3 als draaiend; de runner deelde ze nooit uit
	// (verse map, precies de situatie ná een kern-flip).
	sm.mu.Lock()
	for _, c := range []int{1, 3} {
		if sm.slots[c] == nil {
			sm.slots[c] = &fakeSlot{}
		}
		sm.slots[c].coreOn = true
	}
	sm.mu.Unlock()

	r.mu.Lock()
	defer r.mu.Unlock()
	if got := r.allocateSlotLocked(); got != 2 {
		t.Fatalf("cage=%d; want free cage 2 between live cages 1 and 3", got)
	}
}

func TestHopRunnerSharedAndSMPUseConsecutiveCages(t *testing.T) {
	srv := imageServer(t, []byte("img"))
	sm := newFakeSlotManager(9)
	r := NewHopRunner(sm, nil)
	for i := 0; i < 2; i++ {
		job := hopJob(srv.URL)
		job.Tags = map[string]string{"sharegroup": "trusted", "core-class": "big"}
		task := &types.Task{ID: fmt.Sprintf("shared-%d", i)}
		if err := r.Run(job, task); err != nil {
			t.Fatal(err)
		}
		if task.Pid != 1+i {
			t.Fatalf("shared cage=%d, want %d", task.Pid, 1+i)
		}
	}
	job := hopJob(srv.URL)
	job.CPUShares = 2048
	job.Tags = map[string]string{"core-class": "big"}
	task := &types.Task{ID: "smp"}
	if err := r.Run(job, task); err != nil {
		t.Fatal(err)
	}
	if task.Pid != 3 || len(r.inUse) != 3 {
		t.Fatalf("SMP cage=%d reservations=%v; want cage 3 only", task.Pid, r.inUse)
	}
}
func TestHopRunnerOneCoreClassSharing(t *testing.T) {
	srv := imageServer(t, []byte("img"))
	sm := newFakeSlotManager(1)
	sm.classes[1] = "big"
	r := NewHopRunner(sm, nil)
	for i := 0; i < 6; i++ {
		job := hopJob(srv.URL)
		job.Tags = map[string]string{"sharegroup": "trusted", "core-class": "big"}
		task := &types.Task{ID: fmt.Sprintf("shared-%d", i)}
		if err := r.Run(job, task); err != nil {
			t.Fatal(err)
		}
		if sm.lastCoreClass != "big" {
			t.Fatalf("lost physical class: %q", sm.lastCoreClass)
		}
		if task.Pid != 1+i {
			t.Fatalf("cage=%d", task.Pid)
		}
	}
}

// Adoption restores cage ownership, never reservations for the app's cores.
func TestHopRunnerAdoptionAndReuseKeepNeighbor(t *testing.T) {
	sm := newFakeSlotManager(3)
	sm.slots[1] = &fakeSlot{coreOn: true, logs: make(chan string)}
	sm.slots[2] = &fakeSlot{coreOn: true, logs: make(chan string)}
	r := NewHopRunner(sm, nil)
	r.AdoptRunning(map[string]int{"smp": 1, "shared": 2})
	if got := r.allocateSlotLocked(); got != 3 {
		t.Fatalf("next cage=%d; want 3", got)
	}
	if err := r.Stop(&types.Task{ID: "smp", Pid: 1}); err != nil {
		t.Fatal(err)
	}
	if got := r.allocateSlotLocked(); got != 1 {
		t.Fatalf("released cage=%d; want 1", got)
	}
	if r.inUse[2] != "shared" || !sm.Status(2).CoreOn {
		t.Fatal("neighbor lost when SMP cage was released")
	}
}

func TestHopRunnerCapacityFailureReleasesCage(t *testing.T) {
	srv := imageServer(t, []byte("img"))
	sm := newFakeSlotManager(1)
	r := NewHopRunner(sm, nil)
	job := hopJob(srv.URL)
	job.CPUShares = 2048
	if err := r.Run(job, &types.Task{ID: "too-wide"}); !errors.Is(err, ErrNoCapacity) {
		t.Fatalf("node capacity error=%v", err)
	}
	if len(r.inUse) != 0 || len(r.slots) != 0 {
		t.Fatalf("failed start retained a cage: %v", r.inUse)
	}
	task := &types.Task{ID: "fits"}
	if err := r.Run(hopJob(srv.URL), task); err != nil || task.Pid != 1 {
		t.Fatalf("next start cage=%d err=%v; want 1", task.Pid, err)
	}
}
