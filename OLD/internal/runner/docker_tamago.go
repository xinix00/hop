//go:build tamago

package runner

// docker_tamago.go — there is no Docker on a HopOS node, and there never will
// be: a node runs slot apps in a cage, not containers on a kernel that HopOS
// replaced. The real runner (docker.go) is host-only.
//
// This is not only about the ~2 MB that docker.go's HTTP client drags into the
// kernel image. It is that the agent constructs both runners unconditionally
// and picks per job, so the Docker path has to EXIST on a node — it just has to
// exist as a refusal. Refusing loudly at Run is right: a job with a Docker
// driver that lands on a node is a scheduling mistake, and the operator needs to
// see that rather than a task that quietly never starts.

import (
	"errors"

	"github.com/xinix00/hop/internal/types"
)

// DockerRunner exists on a node so the agent can hold one, and refuses
// everything it is asked to do.
type DockerRunner struct{}

// NewDockerRunner returns the refusing stub. The arguments are the host
// runner's, kept so the caller reads the same on both platforms.
func NewDockerRunner(_ map[string]string, _ string, _ int) *DockerRunner { return &DockerRunner{} }

// DockerPresent is op bare metal altijd false: het node-attribuut node.docker
// hoort daar nooit true te zijn.
func DockerPresent(string) bool { return false }

var errNoDocker = errors.New("this node runs HopOS: there is no container runtime, so a docker job cannot run here")

func (*DockerRunner) Run(*types.Job, *types.Task) error { return errNoDocker }
func (*DockerRunner) Stop(*types.Task) error            { return nil } // nothing ever started
func (*DockerRunner) Cleanup() error                    { return nil }

func (*DockerRunner) Status(*types.Task) (types.TaskState, error) {
	return types.TaskFailed, errNoDocker
}

func (*DockerRunner) SetLogPolicy(LogPolicy)           {}
func (*DockerRunner) GetStdout(string) *LogBroadcaster { return nil }
func (*DockerRunner) GetStderr(string) *LogBroadcaster { return nil }
