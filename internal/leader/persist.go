// Committed cluster state (Derek, 15-07): the leader is the single author
// of desired state and commits it as one snapshot to external storage
// (S3, next to the election lease). Followers never author job state —
// which retires the entire bidirectional job-gossip (and the zombie family
// it bred). Semantics in one line: the snapshot says what should run, the
// leader makes it so.
//
//   - Deletion is absence: a job missing from the snapshot is gone; no
//     tombstone bookkeeping is needed at the storage level.
//   - The snapshot lags mutations by up to persistDebounce. That is fine:
//     nothing is real-real-time (Derek), and a leader crash inside that
//     window loses at most the newest mutations — declaratively visible
//     (the submitter's job is absent) and re-submittable.
//   - Renaming or deleting the state object in the bucket IS the operator's
//     "boot clean" switch. No flag, no config.
//   - Split-brain needs no extra machinery here: leader election runs over
//     the same storage, so a displaced leader has already lost its lease
//     before its stale snapshot could matter.
package leader

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/xinix00/hop/internal/types"
)

// StatePersister stores and retrieves the committed cluster state. The
// leader treats it as a dumb byte sink; the S3 implementation lives in
// internal/discovery (same bucket, credentials and signer as the lease).
type StatePersister interface {
	// Save overwrites the committed snapshot (single writer: the leader).
	Save(ctx context.Context, snapshot []byte) error
	// Load reads the committed snapshot; ok=false means none exists
	// (clean boot).
	Load(ctx context.Context) (snapshot []byte, ok bool, err error)
}

// persistDebounce is how long the flusher coalesces mutations before one
// snapshot write. A 127-job storm becomes a handful of PUTs instead of 127.
const persistDebounce = time.Second

// persistedState is the snapshot format: one JSON object, small enough to
// read with the naked eye in the bucket. Bucket versioning (if enabled)
// gives a free audit trail of "what was allowed to run" over time.
type persistedState struct {
	Updated time.Time    `json:"updated"`
	Jobs    []*types.Job `json:"jobs"`
}

// SetStatePersister wires external state commitment. Call before Run; the
// flusher goroutine starts with Run. Also wraps the job store so every
// mutation path (submit, delete, priority renumbering, sync) marks the
// state dirty — no call site can forget to.
func (l *Leader) SetStatePersister(p StatePersister) {
	l.persister = p
	l.stateDirty = make(chan struct{}, 1)
	l.jobStore = &dirtyTrackingStore{inner: l.jobStore, dirty: l.markStateDirty}
}

// LoadCommittedState populates the job store from the committed snapshot.
// Call once at boot, before Run/reconciliation. An absent object is a
// clean boot; a corrupt object is an error (better loud than half-loaded).
// Returns whether a snapshot was found — callers use !loaded (with a nil
// error) to detect a clean boot for init-job seeding (see init.go).
func (l *Leader) LoadCommittedState(ctx context.Context) (loaded bool, err error) {
	if l.persister == nil {
		return false, nil
	}
	data, ok, err := l.persister.Load(ctx)
	if err != nil {
		return false, err
	}
	if !ok {
		log.Printf("No committed state found — clean boot")
		return false, nil
	}
	var st persistedState
	if err := json.Unmarshal(data, &st); err != nil {
		return false, err
	}
	// De snapshot is de ENIGE waarheid — niet meer, niet minder (Derek,
	// 18-07). De store is bij boot niet per se leeg: de agent laadde al zijn
	// lokale state.json. Een job die dáár nog staat maar niet in de snapshot,
	// is elders verwijderd terwijl deze node down was — SyncJobs merget
	// alleen en zou hem laten herrijzen. Dus: eerst alles weg wat de
	// snapshot niet kent, dan pas laden.
	inSnapshot := make(map[string]bool, len(st.Jobs))
	for _, j := range st.Jobs {
		inSnapshot[j.Name] = true
	}
	for _, j := range l.jobStore.GetJobs() {
		if !inSnapshot[j.Name] {
			log.Printf("Dropping job %s: not in committed state (deleted while this node was away)", j.Name)
			l.jobStore.DeleteJob(j.Name)
		}
	}
	l.jobStore.SyncJobs(st.Jobs, st.Updated)
	log.Printf("Loaded %d jobs from committed state (updated %s)", len(st.Jobs), st.Updated.Format(time.RFC3339))
	return true, nil
}

func (l *Leader) markStateDirty() {
	if l.stateDirty == nil {
		return
	}
	select {
	case l.stateDirty <- struct{}{}:
	default: // al gemarkeerd — de flusher pakt alles in één snapshot mee
	}
}

// persistLoop flushes the committed state at most once per persistDebounce
// while mutations keep arriving, and immediately goes quiet when they stop.
func (l *Leader) persistLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-l.stateDirty:
		}
		// Coalesce: laat de mutatiestorm even uitrazen, één PUT voor de golf.
		timer := time.NewTimer(persistDebounce)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		snapshot, err := json.Marshal(persistedState{Updated: time.Now(), Jobs: l.jobStore.GetJobs()})
		if err != nil {
			log.Printf("state commit: marshal: %v", err)
			continue
		}
		sctx, cancel := context.WithTimeout(ctx, l.commitTimeout())
		err = l.persister.Save(sctx, snapshot)
		cancel()
		if err != nil {
			// Niet fataal: de cluster draait door op de in-memory waarheid;
			// opnieuw markeren zodat de volgende ronde het alsnog probeert.
			log.Printf("state commit failed (will retry on next change): %v", err)
			l.markStateDirty()
		}
	}
}

// defaultPersistTimeout bounds one state PUT when no lease TTL was wired
// (tests, standalone). With a TTL it is half the lease — see
// SetLeaseTTL — so the same slow store that gets a longer lock budget
// (discovery.backendTimeoutFor) also gets a longer commit budget: the two
// hit the same bucket. Half of the default 30 s lease is the 15 s this
// constant always was.
const defaultPersistTimeout = 15 * time.Second

// SetLeaseTTL derives the state-commit timeout from the leader lease: half
// of it. The snapshot is tens of kilobytes and a slow object store (Bunny:
// 4–20 s per PUT, measured 2026-09-08) blew a fixed 15 s, after which the
// state was only retried on the NEXT job change — a failover in between
// would have started from an empty store.
func (l *Leader) SetLeaseTTL(ttl time.Duration) {
	if ttl > 0 {
		l.persistTimeout = ttl / 2
	}
}

func (l *Leader) commitTimeout() time.Duration {
	if l.persistTimeout > 0 {
		return l.persistTimeout
	}
	return defaultPersistTimeout
}

// dirtyTrackingStore decorates a JobStore so every mutation marks the
// committed state dirty. Reads pass through untouched.
type dirtyTrackingStore struct {
	inner JobStore
	dirty func()
}

func (d *dirtyTrackingStore) GetJobs() []*types.Job         { return d.inner.GetJobs() }
func (d *dirtyTrackingStore) GetJob(name string) *types.Job { return d.inner.GetJob(name) }
func (d *dirtyTrackingStore) GetStateTime() time.Time       { return d.inner.GetStateTime() }
func (d *dirtyTrackingStore) StoreJob(job *types.Job)       { d.inner.StoreJob(job); d.dirty() }
func (d *dirtyTrackingStore) DeleteJob(name string)         { d.inner.DeleteJob(name); d.dirty() }
func (d *dirtyTrackingStore) UpdateJob(job *types.Job) bool {
	ok := d.inner.UpdateJob(job)
	if ok {
		d.dirty()
	}
	return ok
}
func (d *dirtyTrackingStore) SyncJobs(jobs []*types.Job, updated time.Time) {
	d.inner.SyncJobs(jobs, updated)
	d.dirty()
}
