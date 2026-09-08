// Package agentloop is de gedeelde election/heartbeat-lus van een hop-node
// (geëxtraheerd uit cmd/agent — de fase-2-stap uit de agentboot-doc):
// probeer leader te worden via het lock-backend, of vind de leader en
// registreer/heartbeat daar. cmd/agent (Linux) en pkg/agentboot (HopOS)
// draaien hierdoor exact dezelfde lus — "zelfde S3, eigen naam, zelfde
// API key" is alles wat een node nodig heeft om mee te doen.
package agentloop

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/xinix00/hop/pkg/httputil"
	"github.com/xinix00/lean/leanhttp"

	"github.com/xinix00/hop/internal/types"
	"github.com/xinix00/hop/pkg/config"
)

// Discoverer handles leader election queries.
type Discoverer interface {
	GetLeader() string
	TryBecomeLeader() bool
	RenewLease() (renewed, displaced bool)
	ReleaseLeadership()
}

// AgentAPI is the subset of agent.Agent used in the tick loop.
type AgentAPI interface {
	ID() string
	Endpoint() string
	GetPlacedTaskCounts() map[string]int
	StopAllTasks()
}

// LeaderAPI is the subset of leader.Leader used in the tick loop.
type LeaderAPI interface {
	GetAgents() []*types.Agent
}

// Loop holds all mutable state and injected dependencies for the tick loop.
type Loop struct {
	Cfg  *config.Config
	Ag   AgentAPI
	Disc Discoverer

	// injectable for testing
	DoRegister     func(leaderAddr, agentID, agentEndpoint string, placed map[string]int, apiKey string) error
	DoHeartbeat    func(leaderAddr, agentID, agentEndpoint, apiKey string) error
	DoBecomeLeader func() (stop func(), l LeaderAPI)

	// mutable state
	l              LeaderAPI
	stopLeader     func()
	failCount      int
	selfBeatFails  int // opeenvolgende mislukte self-heartbeats (zie Tick)
	registered     bool
	lastLeaderAddr string

	// leaderAddr is what the agent's API proxies to: our own leader API
	// while we hold the lease, otherwise the leader we register/heartbeat
	// with. Published from the tick, read from HTTP handlers. Nobody asks
	// the lock store "who leads?" per request — the lease is the timer
	// and the loop already keeps this answer for its own heartbeats.
	leaderAddr atomic.Value // string
}

// LeaderAddr returns the leader API address ("ip:port") the agent should
// proxy cluster calls to, or "" while no leader is known. Wire it into
// agent.SetLeaderFunc.
func (s *Loop) LeaderAddr() string {
	v, _ := s.leaderAddr.Load().(string)
	return v
}

func (s *Loop) publishLeader(addr string) { s.leaderAddr.Store(addr) }

// ownLeaderAddr is this node's leader API (agent port + 1000).
func (s *Loop) ownLeaderAddr() string {
	return fmt.Sprintf("%s:%d", s.Cfg.Node.IP, s.Cfg.Node.Port+1000)
}

// BecomeLeaderNow doet één directe election-poging — voor de boot: is de
// lock vrij (verse cluster of standalone in-memory), dan is deze node
// meteen leader in plaats van na de takeover-drempel (~4 ticks); is hij
// bezet, dan vindt de eerstvolgende Tick de leader en registreert daar.
func (s *Loop) BecomeLeaderNow() bool {
	if s.stopLeader != nil {
		return true
	}
	if !s.Disc.TryBecomeLeader() {
		return false
	}
	stop, l := s.DoBecomeLeader()
	s.stopLeader = stop
	s.l = l
	s.failCount = 0
	s.publishLeader(s.ownLeaderAddr())
	return true
}

// Run tikt elke interval tot done sluit (eerste tick meteen — registreren
// hoort niet 10s te wachten) en geeft bij het einde de leadership netjes
// terug. Bedoeld als goroutine; vervangt de identieke tickers die cmd/agent
// en agentboot elk zelf hadden.
func (s *Loop) Run(done <-chan struct{}, interval time.Duration) {
	// Do not acquire a lease or start a local leader after the owner has already
	// cancelled this loop.
	select {
	case <-done:
		s.stepDown(true)
		return
	default:
	}
	s.Tick()
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-done:
			s.stepDown(true)
			return
		case <-t.C:
			s.Tick()
		}
	}
}

// stepDown stops the in-process leader before releasing its external lease.
// Releasing first leaves a window in which a successor can acquire the lease
// while this process is still serving as leader. Clearing all cached state also
// makes repeated cancellation/step-down calls harmless.
func (s *Loop) stepDown(release bool) {
	stop := s.stopLeader
	s.stopLeader = nil
	s.l = nil
	s.registered = false
	s.lastLeaderAddr = ""
	s.publishLeader("")
	if stop == nil {
		return
	}
	stop()
	if release {
		s.Disc.ReleaseLeadership()
	}
}

func (s *Loop) tryTakeOver(reason string) {
	log.Printf("%s, trying to become leader...", reason)
	s.lastLeaderAddr = ""
	s.publishLeader("")
	if s.Disc.TryBecomeLeader() {
		stop, l := s.DoBecomeLeader()
		s.stopLeader = stop
		s.l = l
		s.failCount = 0
		s.publishLeader(s.ownLeaderAddr())
	}
}

func (s *Loop) leaderFailed(format string, args ...any) {
	s.failCount++
	log.Printf(format, args...)
	if s.failCount >= 4 {
		s.tryTakeOver("Leader unreachable")
	}
	if s.failCount >= 7 {
		log.Println("Likely network isolated, stopping all tasks to avoid duplicates")
		s.Ag.StopAllTasks()
		s.failCount = 4
	}
}

func (s *Loop) Tick() {
	// Use cached leader, only ask the lock backend when unknown.
	leaderAddr := s.lastLeaderAddr
	if leaderAddr == "" {
		leaderAddr = s.Disc.GetLeader()
	}

	if s.stopLeader != nil {
		// We are leader — renew the lock lease.
		renewed, displaced := s.Disc.RenewLease()
		switch {
		case renewed:
			s.failCount = 0
		case displaced:
			// The lock store reports another leader — we have genuinely been
			// replaced, not just cut off. Step down NOW, regardless of connected
			// agents, or we would be a second leader writing to the same cluster.
			log.Println("Lost leadership: lock store reports another leader (stepping down)")
			s.stepDown(false)
			return
		default:
			// Store unreachable (connectivity blip): keep leading while we still
			// see agents. A working LAN survives an internet/lock-store outage
			// without abandoning the cluster — and no one else can take the lease
			// while the store is unreachable to them too. No split-brain.
			agents := s.l.GetAgents()
			if len(agents) > 0 {
				log.Printf("Lock store unreachable but %d agents still connected, staying leader", len(agents))
			} else {
				log.Println("Lost leadership (lock store unreachable + no agents)")
				s.stepDown(false)
				return
			}
		}
		// Self-heartbeat: puur liveness (LastSeen); job-sync is gesloopt —
		// gewenste staat heeft één auteur (leader → S3, leader/persist.go).
		leaderAddr = s.ownLeaderAddr()
		if err := s.DoHeartbeat(leaderAddr, s.Ag.ID(), s.Ag.Endpoint(), s.Cfg.APIKey); err != nil {
			// Eén antwoord ís herstelbaar: "not registered". Dan is de eigen
			// leader-API gewoon bereikbaar maar is hij deze agent VERGETEN —
			// een agent-timeout tijdens een netwerkstoring haalt het record
			// weg. De niet-leader-tak hieronder herregistreert dan; deze tak
			// deed dat niet, en GEMETEN 13-08 op een LicheeRV bleef een node
			// daardoor 70+ heartbeats lang "not registered" roepen zonder ooit
			// te herstellen, met een lege /v1/agents en jobs die nergens
			// geplaatst konden worden.
			if errors.Is(err, ErrNotRegistered) {
				log.Printf("Own leader API forgot this agent (agent timeout during an outage?) — re-registering")
				if rerr := s.DoRegister(leaderAddr, s.Ag.ID(), s.Ag.Endpoint(), s.Ag.GetPlacedTaskCounts(), s.Cfg.APIKey); rerr == nil {
					log.Printf("Re-registered with own leader API %s", leaderAddr)
					s.selfBeatFails = 0
					return
				}
			}
			// Al het andere: zichtbaar maken, niet op reageren. Deze fout werd
			// ooit weggegooid met `_ =`, en dat kostte een middag zoeken (11-08
			// op een LicheeRV): de leader meldt "Agent is dead", de agent
			// zwijgt, /v1/agents blijft leeg en de node herstelt nooit. Een
			// self-heartbeat die op TRANSPORT-niveau faalt betekent dat onze
			// eigen API geen verbinding meer accepteert; dat is geen
			// leiderschapsprobleem (de lease is hierboven net vernieuwd), dus
			// blijft de leader-state met opzet ongemoeid. Eigen teller, want
			// failCount wordt hierboven elke tick door de lease op 0 gezet.
			s.selfBeatFails++
			log.Printf("Self-heartbeat to own leader API %s failed (%d): %v",
				leaderAddr, s.selfBeatFails, err)
		} else if s.selfBeatFails > 0 {
			log.Printf("Self-heartbeat to own leader API recovered after %d failure(s)", s.selfBeatFails)
			s.selfBeatFails = 0
		}
	} else if leaderAddr != "" {
		// On startup (or after leader change): register first with placed counts
		if !s.registered {
			log.Printf("Registering with leader %s...", leaderAddr)
			if err := s.DoRegister(leaderAddr, s.Ag.ID(), s.Ag.Endpoint(), s.Ag.GetPlacedTaskCounts(), s.Cfg.APIKey); err != nil {
				s.leaderFailed("Register failed (%d): %v", s.failCount+1, err)
			} else {
				log.Printf("Registered with leader %s", leaderAddr)
				s.registered = true
				s.failCount = 0
				s.lastLeaderAddr = leaderAddr
				s.publishLeader(leaderAddr)
			}
			return
		}

		// Already registered → heartbeat (puur liveness, geen job-sync)
		err := s.DoHeartbeat(leaderAddr, s.Ag.ID(), s.Ag.Endpoint(), s.Cfg.APIKey)
		if err != nil {
			if errors.Is(err, ErrNotRegistered) {
				log.Printf("Not registered with leader, will re-register...")
				s.registered = false
				s.lastLeaderAddr = ""
			} else {
				s.leaderFailed("Heartbeat failed (%d): %v", s.failCount+1, err)
			}
		} else {
			s.failCount = 0
			s.lastLeaderAddr = leaderAddr
			s.publishLeader(leaderAddr)
		}
	} else {
		// No leader known
		s.leaderFailed("No leader found (%d)", s.failCount+1)
	}
}

// One client for the whole loop, not one per call: an agent knocks on its
// leader's door every second, so the pooled connection is the difference
// between one handshake and one per heartbeat.
var httpClient = &httputil.Client{Timeout: 5 * time.Second}

// ErrNotRegistered: de leader kent deze agent niet (herstart) — herregistreren.
var ErrNotRegistered = errors.New("not registered with leader")

// postJSON sends a POST request with JSON body and API key to the leader.
func postJSON(path string, payload any, apiKey string) (*leanhttp.Response, error) {
	body, _ := json.Marshal(payload)
	call := leanhttp.Call{Method: leanhttp.MethodPost, URL: path, Body: body}
	call.SetHeader("Content-Type", "application/json")
	httputil.SignCall(&call, apiKey) // signs method+path+body, so it goes last
	return httpClient.Do(call)
}

// Register meldt een agent (met placed counts) aan bij de leader.
func Register(leaderAddr, agentID, agentEndpoint, version string, placed map[string]int, apiKey string) error {
	resp, err := postJSON(fmt.Sprintf("http://%s/v1/agents", leaderAddr), map[string]any{
		"id": agentID, "endpoint": agentEndpoint, "version": version, "placed": placed,
	}, apiKey)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != leanhttp.StatusOK {
		return fmt.Errorf("leader returned %d", resp.StatusCode)
	}
	return nil
}

// Heartbeat is puur een levensteken; de job-lijsten die hier vroeger
// meereisden zijn gesloopt (16-07) — gewenste staat heeft één auteur (de
// leader, gecommit naar S3; zie internal/leader/persist.go).
func Heartbeat(leaderAddr, agentID, agentEndpoint, version, apiKey string, tempMilliC int) error {
	resp, err := postJSON(fmt.Sprintf("http://%s/v1/heartbeat", leaderAddr), map[string]any{
		"id": agentID, "endpoint": agentEndpoint, "version": version,
		// De CPU-temperatuur van de node, in milligraden. Eén getal (de
		// heetste sensor); 0 = geen sensor. Liftet mee op de heartbeat omdat
		// dat de enige periodieke agent→leader-lijn is — geen extra verkeer.
		"temp_milli_c": tempMilliC,
	}, apiKey)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == leanhttp.StatusNotFound {
		return ErrNotRegistered
	}
	if resp.StatusCode != leanhttp.StatusOK {
		return fmt.Errorf("leader returned %d", resp.StatusCode)
	}
	return nil
}
