package agent

import (
	"testing"

	"github.com/xinix00/hop/internal/types"
)

// A job received from the leader must not overwrite the store's Deploying
// flag: the leader's Update is its only author.
func TestKeepRolloutFlagFollowsTheStore(t *testing.T) {
	s := &agentState{jobs: map[string]*types.Job{}}

	// Rollout finished in the store; the payload still says deploying.
	s.jobs["api"] = &types.Job{Name: "api", Deploying: false}
	got := &types.Job{Name: "api", Deploying: true}
	keepRolloutFlag(s, got)
	if got.Deploying {
		t.Fatal("payload flag overwrote the store's finished rollout")
	}

	// Rollout in progress in the store: stays in progress.
	s.jobs["api"] = &types.Job{Name: "api", Deploying: true}
	got = &types.Job{Name: "api", Deploying: false}
	keepRolloutFlag(s, got)
	if !got.Deploying {
		t.Fatal("in-progress rollout cleared by a payload")
	}

	// Unknown job (a follower receiving its first dispatch): never deploying.
	got = &types.Job{Name: "new", Deploying: true}
	keepRolloutFlag(s, got)
	if got.Deploying {
		t.Fatal("follower stored a leader-only flag")
	}
}
