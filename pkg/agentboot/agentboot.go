// Package agentboot boots a complete single-node HOP agent for HopOS. It is
// the public entry point that hop-os links (internal/agent is not importable
// across modules): wire a hopos.SlotManager in, and out comes a running agent
// with leader API — the same bytes that cmd/agent runs on Linux/macOS.
//
// Sinds 20-07 draait hier de gedeelde election-lus (internal/agentloop, de
// fase-2-extractie uit cmd/agent): zonder S3-config is het gedrag het oude
// standalone (in-memory lock, deze node wordt meteen leader); mét
// hopos.s3.*-config doet de node echte leader-election via de S3-lock —
// zelfde S3 + eigen naam + zelfde API key = meedoen aan het cluster, precies
// zoals de Linux-agent.
package agentboot

import (
	"context"
	"errors"
	"fmt"
	"log"
	"runtime"
	"strconv"
	"time"

	"github.com/xinix00/hop/internal/agent"
	"github.com/xinix00/hop/internal/agentloop"
	"github.com/xinix00/hop/internal/discovery"
	"github.com/xinix00/hop/internal/runner"
	"github.com/xinix00/hop/pkg/config"
	"github.com/xinix00/hop/pkg/hopos"
)

// Version is reported to the leader on register/heartbeat; hop-os may
// override it at build time.
var Version = "hopos-dev"

// Options configures the HopOS agent node.
type Options struct {
	Config *config.Config // cluster name, node IP/port, API key
	NodeID string         // stable node ID (HopOS has no disk to persist one)
	Slots  hopos.SlotManager

	// MemoryBytes is the slot memory this node offers; bare metal cannot
	// auto-detect it the way sysinfo_{linux,darwin} do.
	MemoryBytes uint64

	// Temp reports the node's CPU temperature in milli-°C, sent along with
	// every heartbeat and shown by `hop agents`. One number per node — the
	// hottest sensor if there are several. nil or 0 = no sensor.
	Temp func() int

	// RestoreState is een eerder gemaakte agent-snapshot (agent.Snapshot):
	// jobs en taken van de vórige agent op deze node. Bedoeld voor de
	// kern-flip van HopOS, waar het OS zichzelf vervangt terwijl de taken
	// gewoon doordraaien — zonder dit kent de nieuwe agent zijn eigen
	// draaiende taken niet meer en wil hij ze plaatsen op slots die ze al
	// bezetten. Leeg = gewone start.
	//
	// Werkt óók zonder lock-backend, en dat is de reden dat het hier zit en
	// niet in de persister: een standalone node heeft geen gecommitte staat
	// om uit te herstellen.
	RestoreState []byte

	// OnSnapshot krijgt, als hij gezet is, een functie waarmee de aanroeper
	// op elk moment de agent-state kan uitlezen. HopOS gebruikt hem vlak vóór
	// een kernwissel; wie hem niet zet merkt er niets van.
	OnSnapshot func(snap func() ([]byte, error))

	// OnFlip voert de kern-flip uit: haal de bundel op url, controleer de
	// sha256 en spring erin (HopOS' kernflip.FlipFromURL). Gezet = de agent
	// opent POST /flip achter de gewone HMAC — de enige weg waarlangs een
	// draaiende node om een nieuwe kern gevraagd kan worden. nil = het
	// endpoint antwoordt eerlijk 501.
	OnFlip func(url, sha256 string) error
}

// Run boots the agent (+ leader zodra deze node de election wint) and blocks
// until ctx is cancelled or the agent's HTTP server stops. Init is
// intentionally skipped: HopOS boots from a clean slate ("niets is
// persistent") and the exec/docker cleanup paths assume a POSIX filesystem.
// Desired state, when durable, lives in the leader's StatePersister.
func Run(ctx context.Context, o Options) error {
	cfg, sm := o.Config, o.Slots
	if cfg == nil || sm == nil || o.NodeID == "" {
		return errors.New("agentboot: Config, NodeID en Slots zijn verplicht")
	}

	// Node attributes: HopOS identity + core classes, so affinity can target
	// e.g. node.cores.big. User-configured attributes win.
	attrs := map[string]string{
		"node.id":     o.NodeID,
		"node.arch":   runtime.GOARCH,
		"node.os":     "hopos",
		"node.docker": "false",
	}
	classes := map[string]int{}
	for i := 1; i <= sm.NumCores(); i++ {
		classes[sm.CoreClass(i)]++
	}
	for class, n := range classes {
		attrs["node.cores."+class] = strconv.Itoa(n)
	}
	for k, v := range cfg.Node.Attributes {
		attrs[k] = v
	}
	cfg.Node.Attributes = attrs

	// The HopRunner is both the hop driver and the default runner: on a
	// HopOS node every task is a slot task, and its errors ("exactly one
	// artifact required") beat ExecRunner's runtime failures on a missing
	// POSIX layer.
	hr := runner.NewHopRunner(sm, attrs)
	ag := agent.New(cfg, o.NodeID, hr).WithHopRunner(hr)
	if o.OnFlip != nil {
		ag.SetFlipFunc(o.OnFlip)
	}
	ag.SetSysInfo(agent.SystemInfo{
		CPUCores:    sm.NumCores(),
		MemoryBytes: o.MemoryBytes,
	})

	// Lock-backend: S3 als de boot-config hem draagt (echte election, zelfde
	// mechaniek als cmd/agent), anders het oude standalone in-memory gedrag.
	backend := discovery.InMemoryBackend()
	clustered := cfg.Cluster.Lock.Type == "s3" &&
		cfg.Cluster.Lock.S3.Endpoint != "" && cfg.Cluster.Lock.S3.Bucket != ""
	if clustered {
		// De cluster-naam is de namespace op de gedeelde store: lease op
		// "leases/<naam>", committed state op "state/<naam>". Leeg zou twee
		// naamloze clusters op één bucket dezelfde lease én snapshot laten
		// delen — stille clobber. cmd/agent weigert dit ook (bij boot).
		if cfg.Cluster.Name == "" {
			return fmt.Errorf("agentboot: cluster name required with an s3 lock backend (it namespaces leases/<name> and state/<name> on the shared bucket)")
		}
		s3 := cfg.Cluster.Lock.S3
		backend = discovery.S3Backend(discovery.S3BackendConfig{
			Endpoint:        s3.Endpoint,
			Bucket:          s3.Bucket,
			Region:          s3.Region,
			AccessKeyID:     s3.AccessKeyID,
			SecretAccessKey: s3.SecretAccessKey,
			UsePathStyle:    s3.UsePathStyle,
		}, cfg.Cluster.Name)
		log.Printf("agentboot: leader-election via s3 (%s/%s), cluster %q",
			s3.Endpoint, s3.Bucket, cfg.Cluster.Name)
	}
	disc := discovery.New(backend, cfg.Node.IP, cfg.Node.Port+1000, cfg.Timeouts.LeaderLease)

	// De agent (state-loop + agent-API) moet draaien vóór registratie:
	// RegisterAgent reconciliet en dat bevraagt de agent-state.
	runErr := make(chan error, 1)
	go func() { runErr <- ag.Run(ctx) }()

	// State van de vórige agent op deze node terugzetten (kern-flip). Vóór de
	// election-lus hieronder, want die registreert en reconciliet meteen —
	// dan moeten de draaiende taken al bekend zijn, anders plaatst hij ze
	// opnieuw bovenop zichzelf. Een onbruikbaar blob is geen reden om niet te
	// starten: de taken draaien nog, ze zijn alleen even onbekend, en de
	// leader corrigeert dat bij de eerste synchronisatie.
	if len(o.RestoreState) > 0 {
		if err := ag.Restore(o.RestoreState); err != nil {
			log.Printf("agentboot: restoring the previous agent state failed (%v) — continuing with an empty state", err)
		} else {
			log.Printf("agentboot: agent state restored from the previous kernel")
		}
	}
	if o.OnSnapshot != nil {
		o.OnSnapshot(ag.Snapshot)
	}

	// becomeLeader — de leader-helft, alleen actief op de election-winnaar.
	// De choreografie (leader + persister + API-server + zelf-registratie +
	// init-seed) is gedeeld met cmd/agent: agentloop.BecomeLeader. Settle
	// alleen in cluster-modus (op een standalone boot-verse node valt er
	// niets te settlen — oude gedrag). De persister volgt de lock: S3 in
	// cluster-modus, anders een lokale crash-safe file (!clustered) —
	// agentboot doet alleen in-memory of S3-election, dus een
	// hoplockserver-URL in de config mag hier géén gedeelde remote store
	// worden (onafhankelijke in-memory leaders zouden hem clobberen).
	becomeLeader := func() (func(), agentloop.LeaderAPI) {
		stop, l := agentloop.BecomeLeader(ctx, cfg, ag, Version, !clustered, clustered)
		log.Printf("agentboot: node %s is leader (%s), %d app cores", o.NodeID, cfg.Cluster.Name, sm.NumCores())
		return stop, l
	}

	// De gedeelde election/heartbeat-lus (internal/agentloop): word leader
	// via de lock, of registreer/heartbeat bij de gevonden leader; takeover
	// en isolatie-gedrag zitten erin. Standalone (in-memory) wint de eerste
	// tick de election — het oude "deze node is altijd leader".
	loop := &agentloop.Loop{
		Cfg:  cfg,
		Ag:   ag,
		Disc: disc,
		DoRegister: func(leaderAddr, id, ep string, placed map[string]int, key string) error {
			return agentloop.Register(leaderAddr, id, ep, Version, placed, key)
		},
		DoHeartbeat: func(leaderAddr, id, ep, key string) error {
			temp := 0
			if o.Temp != nil {
				temp = o.Temp()
			}
			return agentloop.Heartbeat(leaderAddr, id, ep, Version, key, temp)
		},
		DoBecomeLeader: becomeLeader,
	}
	// Eén directe poging bij boot: lock vrij (standalone, of de eerste van
	// een verse cluster) = meteen leader — geen 30s takeover-drempel voor de
	// init-desktop; lock bezet = tick 1 registreert bij de zittende leader.
	loop.BecomeLeaderNow()
	// Cluster calls on the agent API proxy to the leader the loop knows —
	// never a lock-store read per request (Bunny: seconds per GET).
	ag.SetLeaderFunc(loop.LeaderAddr)
	go loop.Run(ctx.Done(), 10*time.Second)

	return <-runErr
}
