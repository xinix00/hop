package agent

// Live state-overdracht: de agent-state als JSON naar buiten en weer terug.
//
// Dit bestaat voor de kern-flip van HopOS (hop-os/docs/kern-flip.md), waar het
// OS zichzelf onder zijn eigen apps vandaan vervangt. De apps blijven daarbij
// draaien — maar de agent is een gewoon proces in die kern en start opnieuw,
// met een lege state. Zonder overdracht kent hij zijn eigen draaiende taken
// dus niet meer: hij zou ze willen plaatsen en stuiten op slots die al bezet
// zijn door precies die taken.
//
// Waarom JSON en niet iets compacters: types.Job en types.Task dragen hun
// JSON-tags al (ze gaan over de HTTP-API en naar de gecommitte clusterstaat),
// dus dit is het formaat dat al bestaat en al onderhouden wordt. Een tweede
// codering zou een tweede plek zijn om een veld te vergeten.
//
// En waarom het ook zonder S3 werkt, wat de reden is dat het hier hoort en
// niet in de persister: een standalone node (init-jobs, geen lock-backend)
// heeft geen gecommitte staat om uit te herstellen. Die zou bij een kernwissel
// zijn hele taakadministratie kwijt zijn terwijl de taken gewoon doordraaien.
// Met deze overdracht hopt zo'n node net zo goed mee.

import (
	"encoding/json"
	"fmt"

	"github.com/xinix00/hop/internal/types"
)

// handoffVersion staat in het blob zodat een agent van een andere versie een
// onbekende vorm kan weigeren in plaats van hem half te lezen.
const handoffVersion = 1

// handoff is de overdraagbare agent-state: precies de velden van agentState
// die een volgende agent niet zelf kan afleiden.
//
// stateTime gaat NIET mee. Dat is de leeftijd van de laatste
// leader-synchronisatie, en die hoort bij de agent die hem ophaalde — een
// nieuwe agent synchroniseert zelf, en een geërfde tijdstempel zou hem laten
// denken dat hij al bij is.
type handoff struct {
	Version int                    `json:"version"`
	Jobs    map[string]*types.Job  `json:"jobs"`
	Tasks   map[string]*types.Task `json:"tasks"`
}

// Snapshot geeft de huidige state als JSON. Eén op op de state-loop, dus per
// definitie een consistent beeld: er is één schrijver en die staat stil zolang
// deze op draait.
//
// Roep hem zo LAAT mogelijk aan vóór de sprong. Alles wat er ná dit moment nog
// bij komt — een taak die net startte — staat er niet in, en die zou de
// volgende agent dan als vreemde aantreffen.
func (a *Agent) Snapshot() ([]byte, error) {
	// De marshal draait BINNEN de op, niet op een kopie erbuiten: de agent
	// muteert zijn taken in place (state, restart-teller, voortgang), dus een
	// kopie van de pointers zou onder de encoder uit kunnen veranderen. Het
	// kost de state-loop microseconden, één keer per flip.
	r := query(a, func(s *agentState) snapshotResult {
		b, err := json.Marshal(handoff{
			Version: handoffVersion,
			Jobs:    s.jobs,
			Tasks:   s.tasks,
		})
		return snapshotResult{b, err}
	})
	return r.b, r.err
}

// snapshotResult bestaat omdat query één waarde teruggeeft en een marshal er
// twee heeft.
type snapshotResult struct {
	b   []byte
	err error
}

// Restore zet een eerder gemaakte Snapshot terug. Alleen zinvol vóór de agent
// zijn eerste leader-synchronisatie doet; daarna overschrijft die de state toch.
//
// Een onbruikbaar blob is een FOUT en geen stilte: de aanroeper moet kunnen
// kiezen om dan door te gaan met een lege state (de taken draaien nog, ze zijn
// alleen niet meer bekend) in plaats van te denken dat de overdracht slaagde.
func (a *Agent) Restore(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	var h handoff
	if err := json.Unmarshal(b, &h); err != nil {
		return fmt.Errorf("agent handoff: %w", err)
	}
	if h.Version != handoffVersion {
		return fmt.Errorf("agent handoff: version %d, this agent speaks %d", h.Version, handoffVersion)
	}
	if err := query(a, func(s *agentState) error {
		for name, j := range h.Jobs {
			if j != nil {
				s.jobs[name] = j
			}
		}
		for id, t := range h.Tasks {
			if t != nil {
				s.tasks[id] = t
			}
		}
		return nil
	}); err != nil {
		return err
	}

	// De kooien van de herstelde taken aan de runner overdragen: hij moet ze
	// straks kunnen stoppen, en zijn eigen boekhouding is een verse map.
	if ad, ok := a.hopRunner.(interface {
		AdoptRunning(map[string]int)
	}); ok {
		slots := map[string]int{}
		for id, t := range h.Tasks {
			// GEEN state-filter: aanwezigheid is de maat, nooit de state —
			// een task die queued of failed staat houdt zijn kooi net zo goed
			// bezet, en vrijgeven gebeurt door het record te verwijderen.
			if t == nil || t.Pid < 1 {
				continue
			}
			slots[id] = t.Pid
		}
		ad.AdoptRunning(slots)
	}
	return nil
}
