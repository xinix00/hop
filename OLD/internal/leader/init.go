// Init jobs (Derek, 18-07): a cluster that boots with nothing gets a
// baseline. "Clean boot" means: no committed snapshot exists (or no state
// store is configured) AND the job store is empty. Only then are the
// configured init jobs seeded — through the normal dispatch path, so
// tombstones, priorities, persistence and reconciliation all behave as if
// an operator submitted them. A store that is unreachable is NOT a clean
// boot: never seed on a storage error, or an S3 outage would reset the
// cluster to its baseline.
package leader

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"

	"github.com/xinix00/hop/internal/types"
)

// DecodeInitJobs converts raw config specs (YAML maps using the job JSON
// field names) into Jobs. Strict: unknown fields, a missing name or a job
// with nothing to run are config errors — loud at boot beats a silently
// ignored typo.
func DecodeInitJobs(specs []map[string]any) ([]*types.Job, error) {
	jobs := make([]*types.Job, 0, len(specs))
	for i, spec := range specs {
		raw, err := json.Marshal(spec)
		if err != nil {
			return nil, fmt.Errorf("init job %d: %w", i, err)
		}
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.DisallowUnknownFields()
		var job types.Job
		if err := dec.Decode(&job); err != nil {
			return nil, fmt.Errorf("init job %d: %w", i, err)
		}
		if job.Name == "" {
			return nil, fmt.Errorf("init job %d: name required", i)
		}
		// Boot-config shorthand: exactly one artifact and no command/image can
		// only mean the hop driver — defaulting it saves 15 bytes per entry,
		// and the firmware's bootargs buffer is a hard ~1K (measured 19-07:
		// entry four fell off the end of every cmdline).
		if job.Driver == "" && job.Command == "" && job.Image == "" && len(job.Artifacts) == 1 {
			job.Driver = types.DriverHop
		}
		// Same validity rule as the API's apply handler. One artifact is the
		// common case; several is how one job spans architectures — each gets a
		// `match` on node attributes and the agent resolves it per node
		// (resolveJobForRun), so a hop job with two artifacts is valid here and
		// arrives at every runner as exactly the one that fits that node.
		hopImage := job.Driver == types.DriverHop && len(job.Artifacts) >= 1
		if job.Command == "" && job.Image == "" && !hopImage {
			return nil, fmt.Errorf("init job %q: command or image required (or driver \"hop\" with at least one artifact)", job.Name)
		}
		jobs = append(jobs, &job)
	}
	return jobs, nil
}

// SeedInitJobs schedules the given jobs. Call only on a clean boot and
// after Run has started (DispatchJob talks to the state loop). A name that
// already exists is skipped — a seed never overwrites operator state.
func (l *Leader) SeedInitJobs(jobs []*types.Job) {
	for _, job := range jobs {
		if l.GetJob(job.Name) != nil {
			log.Printf("Init job %s: already exists, skipping", job.Name)
			continue
		}
		if job.Priority == nil {
			n := l.NextPriority()
			job.Priority = &n
		}
		if err := l.DispatchJob(job); err != nil {
			log.Printf("Init job %s: %v (will retry on reconciliation)", job.Name, err)
			continue
		}
		log.Printf("Init job %s seeded", job.Name)
	}
}
