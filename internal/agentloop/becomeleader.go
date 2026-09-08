package agentloop

// becomeleader.go — de leader-helft van een node, gedeeld door cmd/agent
// (Linux/macOS) en pkg/agentboot (HopOS). Vóór deze extractie waren dit twee
// ~55-regel-kopieën van dezelfde choreografie (leader + persister + API-server
// + zelf-registratie + init-seed) die alleen per ongeluk gelijk konden blijven.

import (
	"context"
	"fmt"
	"log"

	"github.com/xinix00/hop/internal/api"
	"github.com/xinix00/hop/internal/discovery"
	"github.com/xinix00/hop/internal/leader"
	"github.com/xinix00/hop/pkg/config"
)

// LeaderHost is wat BecomeLeader van de lokale agent nodig heeft: de job
// store (de leader gebruikt de agent als store) plus identiteit en placed
// counts voor de zelf-registratie.
type LeaderHost interface {
	leader.JobStore
	ID() string
	Endpoint() string
	GetPlacedTaskCounts() map[string]int
}

// BecomeLeader start de leader-helft op deze node: state loop, committed
// state, API-server op agentpoort+1000, zelf-registratie en (op een schone
// boot) de init-jobs. standalone kiest de state-store (lokale file i.p.v. de
// remote naast de lease); settle laat de leader agentTimeout wachten vóór de
// eerste reconcile (cluster-modus — een verse standalone node heeft niets te
// settlen). De stop-functie geeft :<poort+1000> synchroon vrij en stopt de
// state loop.
func BecomeLeader(ctx context.Context, cfg *config.Config, host LeaderHost, version string, standalone, settle bool) (stop func(), l *leader.Leader) {
	leaderCtx, cancel := context.WithCancel(ctx)

	l = leader.New(host.ID(), host, nil)
	l.SetAPIKey(cfg.APIKey)
	if d := cfg.Timeouts.NodeDeadThreshold; d > 0 {
		l.SetAgentTimeout(d)
	}
	if settle {
		l.EnableSettle()
	}
	l.SetLeaseTTL(cfg.Timeouts.LeaderLease)

	// Gecommitte clusterstaat: de leader commit zijn gewenste staat naast de
	// lease en een verse leader laadt hem terug — failover zonder
	// state-merging (discovery.StateStoreFromConfig is bewust de enige gate).
	cleanBoot := true
	if st := discovery.StateStoreFromConfig(cfg, standalone); st != nil {
		l.SetStatePersister(st)
		loaded, err := l.LoadCommittedState(leaderCtx)
		if err != nil {
			// Luid maar niet fataal; store onbereikbaar ≠ store leeg: nooit
			// init jobs seeden op een storing.
			log.Printf("committed state not loaded: %v", err)
			cleanBoot = false
		} else {
			cleanBoot = !loaded
		}
	}

	// Start leader state loop + health checker BEFORE any state operations.
	// Without this, RegisterAgent deadlocks (query waits on ops channel that
	// nobody reads).
	go l.Run(leaderCtx)

	// Start API server so other agents can register/heartbeat.
	srv := api.NewServer(l, fmt.Sprintf(":%d", cfg.Node.Port+1000), cfg.APIKey, cfg.Cluster.Name)
	go func() {
		if err := srv.Run(leaderCtx); err != nil {
			log.Printf("Leader API error: %v", err)
		}
	}()

	// Register self with placed counts (during settle, no reconcile yet).
	l.RegisterAgent(host.ID(), host.Endpoint(), version, host.GetPlacedTaskCounts())

	// Clean boot (geen snapshot én lege job store) → seed de init jobs.
	// Tijdens settle worden ze alleen gestored; reconcile dispatcht daarna.
	if cleanBoot && len(cfg.Cluster.InitJobs) > 0 && len(l.GetJobs()) == 0 {
		jobs, err := leader.DecodeInitJobs(cfg.Cluster.InitJobs)
		if err != nil {
			log.Printf("cluster.init_jobs: %v", err) // al gevalideerd bij boot; hooguit theoretisch
		} else {
			l.SeedInitJobs(jobs)
		}
	}

	return func() {
		srv.Stop() // release the leader port synchronously
		cancel()   // stop leader state loop
	}, l
}
