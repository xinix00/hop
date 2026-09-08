package leader

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/xinix00/hop/internal/types"
)

var (
	// RollingUpdateDelay can be overridden in tests for faster execution
	RollingUpdateDelay = 2 * time.Second

	// ErrJobLocked is returned when an update or dispatch is already in progress
	// for the given job. Callers can use errors.Is to map this to 409 Conflict.
	ErrJobLocked = errors.New("job is already being updated or dispatched")
)

// lockJob atomically sets the dispatching flag for a job.
// Returns false if the job is already being dispatched or updated.
func (l *Leader) lockJob(name string) bool {
	if l.stopped() {
		return false
	}
	alreadyLocked := query(l, func(s *leaderState) bool {
		if s.dispatching[name] {
			return true
		}
		s.dispatching[name] = true
		return false
	})
	return !alreadyLocked && !l.stopped()
}

// unlockJob clears the dispatching flag for a job.
func (l *Leader) unlockJob(name string) {
	l.do(func(s *leaderState) { delete(s.dispatching, name) })
}

// UpdateJob updates an existing job (found by name) with a new definition.
// The update policy is taken from newJob.UpdatePolicy (default: rolling).
// Returns an error if the job is already being updated or dispatched.
func (l *Leader) UpdateJob(newJob *types.Job) error {
	oldJob := l.jobStore.GetJob(newJob.Name)
	if oldJob == nil {
		return fmt.Errorf("job %s not found", newJob.Name)
	}

	// Preserve priority from old job if not explicitly set
	if newJob.Priority == nil {
		newJob.Priority = oldJob.Priority
	}

	policy := newJob.UpdatePolicy
	if policy == "" {
		policy = types.UpdateRolling
	}
	// Validate before acquiring dispatching[name]. Returning from the switch's
	// default after lockJob used to strand the lock forever, causing every later
	// update to return 409 and every reconcile to skip this job.
	switch policy {
	case types.UpdateRolling, types.UpdateRecreate, types.UpdateBlueGreen:
	default:
		return fmt.Errorf("unknown update policy: %s", policy)
	}

	if !l.lockJob(newJob.Name) {
		return fmt.Errorf("%w: %s", ErrJobLocked, newJob.Name)
	}

	log.Printf("Updating job %s with policy=%s", newJob.Name, policy)

	// Mark the rollout in progress. Each policy stores newJob, so this rides
	// into the committed snapshot: cleared on success below, but left set on
	// failure or a mid-rollout leader death — so a new leader (and any status
	// reader) sees the honest truth instead of a false "healthy".
	newJob.Deploying = true

	var err error
	switch policy {
	case types.UpdateRolling:
		err = l.updateRolling(newJob)
	case types.UpdateRecreate:
		err = l.updateRecreate(newJob)
	case types.UpdateBlueGreen:
		err = l.updateBlueGreen(newJob)
	}

	if err == nil {
		// Only the flag, in the store — newJob itself is the pointer the
		// policy stored, and handlers may be encoding it right now.
		l.jobStore.SetJobDeploying(newJob.Name, false)
	}

	l.normalizePriorities(l.jobStore.GetJobs())
	return err
}

// updateRolling: for each old task, dispatch new (if within count) → stop old.
// After the loop, reconcileJobs dispatches extra instances for scale-up.
func (l *Leader) updateRolling(job *types.Job) error {
	count := job.Count
	if count <= 0 {
		count = 1
	}

	oldTasks := l.snapshotJobTasks(job.Name)
	l.jobStore.StoreJob(job)

	var loopErr error
	for i, oldTask := range oldTasks {
		replaced := false
		if i < count {
			// Replace: dispatch new version before stopping old (zero
			// downtime) — maar NOOIT via preemptie: de tijdelijke
			// oud+nieuw-overlap van een update mag geen buurman kosten
			// (gemeten 01-08: welcome-update preemptte cloudflared, en de
			// uitgeweken partitie fragmenteerde de pool er nog bij).
			err := l.dispatchWithoutPreemption(job)
			if errors.Is(err, errNoCapacity) {
				// Geen ruimte voor oud+nieuw naast elkaar: replace-in-place
				// op de agent van het oude exemplaar. Die stopt zijn eigen
				// voorganger pas ná een geslaagde toelating — weigert hij,
				// dan draait het oude gewoon door (de KeepsOld-invariant).
				// Korte eigen onderbreking, buurman ongemoeid, en de
				// opvolger krijgt de vrijgekomen partitie terug (geen
				// fragmentatie).
				if agent := l.agentForTask(oldTask.agentID); agent != nil {
					if rerr := l.sendReplaceToAgent(agent, job); rerr == nil {
						// Oud is door de agent verruild voor nieuw: plaatsing
						// per saldo ongewijzigd, en hieronder niets meer te
						// stoppen.
						replaced = true
						err = nil
					} else {
						err = rerr
					}
				}
			}
			if err != nil {
				loopErr = fmt.Errorf("rolling update failed at instance %d: %w", i+1, err)
				break
			}
		}

		// Stop old task (replacement or scale-down excess)
		if !replaced {
			l.stopOldTask(job.Name, oldTask)
		}

		l.eventBus.Notify("job:" + job.Name)

		if i < count-1 {
			timer := time.NewTimer(RollingUpdateDelay)
			select {
			case <-timer.C:
			case <-l.context().Done():
				timer.Stop()
				l.unlockJob(job.Name)
				return l.context().Err()
			}
		}
	}

	// Unlock before reconcile so the post-update reconcile actually sees the job unlocked.
	// Using defer would race with `go reconcileJobs()` via the shared ops channel.
	l.unlockJob(job.Name)

	if loopErr != nil {
		return loopErr
	}

	log.Printf("Rolling update for job %s complete (%d old, %d desired)", job.Name, len(oldTasks), count)
	go l.reconcileJobs()
	return nil
}

// updateRecreate: stop all → store new definition → dispatch all new.
func (l *Leader) updateRecreate(job *types.Job) error {
	agents := query(l, func(s *leaderState) []*types.Agent {
		var result []*types.Agent
		for agentID, jobs := range s.placed {
			if jobs[job.Name] > 0 {
				if a := s.agents[agentID]; a != nil {
					result = append(result, a)
				}
				delete(jobs, job.Name)
			}
		}
		return result
	})

	for _, agent := range agents {
		_ = l.stopTasksOnAgent(agent, job.Name)
	}

	l.unlockJob(job.Name)
	return l.DispatchJob(job)
}

// updateBlueGreen: snapshot old tasks, dispatch all new, then stop all old.
func (l *Leader) updateBlueGreen(job *types.Job) error {
	defer l.unlockJob(job.Name)
	oldTasks := l.snapshotJobTasks(job.Name)

	count := job.Count
	if count <= 0 {
		count = 1
	}

	// Store new definition and dispatch all new instances. Zonder preemptie,
	// zelfde principe als rolling: blue-green EIST oud+nieuw naast elkaar, en
	// als dat niet past hoort de update te falen mét de oude versie intact —
	// niet te slagen over het lijk van een buurman.
	l.jobStore.StoreJob(job)
	for i := 0; i < count; i++ {
		if err := l.dispatchWithoutPreemption(job); err != nil {
			return fmt.Errorf("blue-green: failed to dispatch instance %d/%d: %w", i+1, count, err)
		}
	}

	// Stop all old instances by task ID.
	for _, oldTask := range oldTasks {
		agent := l.agentForTask(oldTask.agentID)
		if agent != nil {
			l.stopTaskByID(agent, oldTask.taskID)
			l.do(func(s *leaderState) {
				if s.placed[oldTask.agentID] != nil && s.placed[oldTask.agentID][job.Name] > 0 {
					s.placed[oldTask.agentID][job.Name]--
				}
			})
		}
	}

	l.eventBus.Notify("job:" + job.Name)
	return nil
}

// stopOldTask stopt één oud exemplaar en boekt de plaatsing af — het
// stop-gedeelte van elke update-stap, op één plek zodat de volgorde (eerst
// stoppen of eerst plaatsen) de boekhouding niet kan laten verlopen.
func (l *Leader) stopOldTask(jobName string, ref taskRef) {
	agent := l.agentForTask(ref.agentID)
	if agent == nil {
		return
	}
	l.stopTaskByID(agent, ref.taskID)
	l.do(func(s *leaderState) {
		if s.placed[ref.agentID] != nil && s.placed[ref.agentID][jobName] > 0 {
			s.placed[ref.agentID][jobName]--
		}
	})
}

// taskRef holds the agent ID and task ID of a running task.
type taskRef struct {
	agentID string
	taskID  string
}

// snapshotJobTasks fetches all currently running task IDs for a job from all
// agents. Used by rolling and blue-green updates to know which tasks to stop.
// GetJobStatus already resolves the placed agents, so its second return value
// IS the agent list — querying placed again here would just do the same work
// twice.
func (l *Leader) snapshotJobTasks(jobName string) []taskRef {
	tasksByAgent, agents := l.GetJobStatus(jobName)

	var refs []taskRef
	for _, agent := range agents {
		for _, task := range tasksByAgent[agent.ID] {
			refs = append(refs, taskRef{agentID: agent.ID, taskID: task.ID})
		}
	}
	return refs
}

// agentForTask returns the agent with the given ID (or nil if not registered).
func (l *Leader) agentForTask(agentID string) *types.Agent {
	return query(l, func(s *leaderState) *types.Agent {
		return s.agents[agentID]
	})
}
