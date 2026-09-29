// Package hopos defines the public contract between HOP and HopOS: the
// SlotManager interface that HopOS' slot primitives implement and HOP's
// HopRunner consumes. It lives outside internal/ so the HopOS repository can
// implement it (hop-os/metal/slotmgr wraps metal/slots).
package hopos

import (
	"errors"
	"io"
	"time"
)

// ErrNoCapacity is what StartStream wraps when the node cannot PLACE the
// cage: no free app core for a dedicated job, or not enough free cores to
// open the requested sharegroup pool. It is emphatically not a crash — the
// job is fine and will run as soon as a core frees up (or right away on
// another node), so HOP must reject it back to pending instead of restart-
// looping it. Restarting an unplaceable job is a storm: every retry re-downloads
// the image and re-fails, and the churn starves the tasks that DO run.
var ErrNoCapacity = errors.New("hopos: no free core to place the cage")

// App status values, mirroring hop-os/metal/layout control-page states.
const (
	SlotEmpty   = 0
	SlotBooting = 1
	SlotReady   = 2
	SlotExited  = 3
)

// SlotStatus is a point-in-time view of one slot.
type SlotStatus struct {
	CoreOn    bool
	App       uint64 // Slot* constants
	ExitCode  uint64
	Heartbeat uint64
	RAMSize   uint64
	MemSys    uint64 // actual memory draw reported by the app (Go MemStats.Sys); 0 = not reported yet

	// CPUPct is the slot's CPU usage as a percentage of its OWN cores
	// (0-100, SMP-normalized), derived from the idle-tick counter every app
	// publishes on its control page: an idle core ticks at event-stream
	// tempo, a computing core doesn't — usage = 1 - measured/expected.
	// -1 = unknown (slot starting, first sample window, or no counter).
	CPUPct int

	// Diagnostics for an involuntary end, written by HopOS' EL2 vectors: a
	// stage-2 fault (cage violation) or HOP's hard-kill. FaultVec 0 = no
	// fault seen; nonzero = vector index + 1, with ESR/FAR then valid. Not
	// state-machine input — the runner logs it so operators see WHY a task
	// failed, which is otherwise invisible on a headless node.
	FaultVec uint64
	FaultESR uint64
	FaultFAR uint64

	// Cage is the node's own one-line account of the slot's cage, or empty when
	// there is nothing to say. Same reason the Fault* fields exist — a headless
	// node is otherwise silent — but for the architectures where the cage is not
	// a stage-2 map: what the loader stub reached, and what kind of hart is
	// underneath. It travels over the network, which the node's serial console
	// cannot be trusted to do: at 115200 that line drops bytes, and a mangled
	// hex value is worse than none.
	//
	// Diagnostics only, like Fault*: never state-machine input.
	Cage string
}

// StartSpec carries everything a one-phase start needs. It is the union of
// what StartLoader and StartStaged each took, because a streaming start IS
// both phases at once.
type StartSpec struct {
	CoreClass  string            // optional physical core class, including sharegroup pools
	MemLimit   uint64            // partition size (the job's memory_limit)
	Cores      int               // SMP cores for the app itself (>=1)
	Sharegroup string            // "" = dedicated core; else cooperative pool
	PoolCores  int               // pool size in whole cores (sharegroup only)
	Env        map[string]string // job env (ER_* included)
	Mounts     map[string]string // shared path -> local path
	Ports      map[string]int    // published ports (name -> port)
	Job        string            // job name = object-store namespace ("" = none)
}

// SlotManager abstracts HopOS' slot primitives (hop-os/metal/slots). The
// bare-metal implementation calls that package directly; tests use a fake.
type SlotManager interface {
	// NumCores is the node's usable app-core count — the only capacity HOP
	// schedules and reports against (a 4-core Pi with core 0 running HOP => 3).
	// The number of cages (slot IDs) a node can hold is deliberately NOT in
	// this contract: sharegroups stack more cages than cores, HopOS enforces
	// its own hard ceiling, and HOP simply tries to place a job — the node
	// accepts it or it doesn't.
	NumCores() int
	// CoreClass returns a physical core's class ("big", "mid" or "small").
	CoreClass(core int) string
	// StartStream is the one-phase start: the node streams the image from r
	// STRAIGHT into the slot's partition — every byte lands on the address it
	// will run from. No staged copy, so the partition only ever holds the app
	// itself: image + heap. size is the image size (Content-Length) and is
	// mandatory: placement is validated against it and a short read is a loud
	// failure.
	//
	// The download happens on the CALLER's network stack (HOP, core 0). That
	// used to be forbidden — 127 whole images buffering through the kernel
	// heap OOM'd it (14-07) — but streaming buffers only one read-chunk per
	// download, which is what makes this path possible at all. The caller
	// bounds how many streams run at once.
	//
	// spec.Sharegroup/PoolCores drive cooperative core-sharing: "" places the
	// slot on its own dedicated core; a name packs it onto a pool of PoolCores
	// whole cores shared with same-named slots. spec.Job is the JOB name (not
	// the task ID): the app's namespace in the node's object store, so a
	// restart or failover sees the same directory.
	//
	// Implementations clean up their own allocations on error (partition,
	// core placement): after a failed StartStream the slot is free again and
	// the caller only releases its own bookkeeping.
	StartStream(slot int, image io.Reader, size int64, spec StartSpec) error
	// Stop asks the app to exit (kill flag) and waits until the core is off.
	Stop(slot int, timeout time.Duration) error
	// Status reports the slot's current state.
	Status(slot int) SlotStatus
	// Logs returns the slot's log stream (hop-ABI outbox). The channel is
	// closed when the slot stops.
	Logs(slot int) <-chan string
}

// PoolReporter is an optional extra on a SlotManager: the largest partition the
// node can still place, right now.
//
// It exists because a sum is the wrong question. A node whose pool is several
// regions can have 60 MB free with no 36 MB anywhere in one piece, and an
// admission that adds up bytes then says yes to a job that can never be placed.
// The task is admitted, the placement fails, the leader takes it back, and five
// seconds later it happens again — MEASURED 19-08 on a LicheeRV: the node's
// reported capacity flapped between 162 and 198 MB of 222, and inside that
// window it refused a different 28 MB job that did fit.
//
// A driver that cannot answer simply does not implement it, and admission falls
// back to the sum.
type PoolReporter interface {
	// PoolLargest is the largest single partition that would fit now, in bytes.
	PoolLargest() uint64
}
