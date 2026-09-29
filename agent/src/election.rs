//! De election- en heartbeat-lus van een node, als toestandsmachine.
//!
//! Bezit de lus-toestand: of we leiden, of we geregistreerd zijn, de laatst
//! bevestigde leader en de faaltellers. Probeer leader te worden via de
//! lease, of vind de leader en registreer en heartbeat daar. De HTTP-aanroepen
//! doet de executor: [`Election::tick`] geeft [`Request`]s, en
//! [`Election::on_reply`] verwerkt het antwoord (en kan een vervolg geven).
//!
//! De fail-safe: na 4 mislukte ticks probeert de node over te nemen; na 7
//! stopt hij al zijn taken, maar ALLEEN als de lock-store nog een levende
//! leader meldt (wij zijn de geïsoleerde) of zelf onbereikbaar is. Zegt de
//! store "geen leader", dan blijven de taken draaien: niemand plaatst ze
//! opnieuw, stoppen is puur verlies (gemeten 08-09-2026 op traqqr: een
//! spook-lease liet het cluster zonder leader, en na 70 s doodden beide
//! agents elke taak).

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;

use types::Time;

use crate::node::Agent;

/// Na zoveel mislukte ticks probeert de node de leiding over te nemen.
const TAKEOVER_AFTER: u32 = 4;

/// Na zoveel mislukte ticks geldt de node als geïsoleerd.
const ISOLATED_AFTER: u32 = 7;

/// De lease-kant, zoals de lus hem ziet: elke vraag antwoordt meteen.
///
/// Op een trage store (Bunny: 4 tot 20 s per PUT, 08-09-2026) hield een
/// synchrone renew de heartbeat van de leader zelf voorbij de dood-drempel;
/// daarom antwoordt een implementatie uit de laatst voltooide store-aanroep
/// en loopt de aanroep zelf elders (de lease is de timer).
pub trait Discoverer {
    /// De leader volgens de laatste lees; `None` = niemand of onbekend.
    fn get_leader(&mut self) -> Option<String>;
    /// Probeer de lease te claimen.
    fn try_become_leader(&mut self) -> bool;
    /// Vernieuw de lease: `(renewed, displaced)`.
    fn renew_lease(&mut self) -> (bool, bool);
    /// Geef de lease terug.
    fn release_leadership(&mut self);
    /// Of de laatste lees een antwoord van de store kreeg.
    fn store_reachable(&self) -> bool {
        false
    }
    /// Vergeet het gecachte antwoord: de volgende vraag leest opnieuw.
    fn invalidate(&mut self) {}
    /// Wanneer onze lease afloopt, als de implementatie dat bijhoudt.
    fn lease_expires_at(&self) -> Option<Time> {
        None
    }
}

/// Een aanroep of gebeurtenis voor de executor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Request {
    /// `POST /v1/agents` bij deze leader, met de placed-tellers.
    Register {
        /// Het leader-adres.
        leader: String,
    },
    /// `POST /v1/heartbeat` bij deze leader.
    Heartbeat {
        /// Het leader-adres.
        leader: String,
    },
    /// Heartbeat bij onze eigen leader-API (puur levensteken).
    SelfHeartbeat {
        /// Het adres van onze leader-API.
        leader: String,
    },
    /// Herregistratie bij onze eigen leader-API, die ons vergat.
    SelfRegister {
        /// Het adres van onze leader-API.
        leader: String,
    },
    /// Start de leader-helft op deze node.
    StartLeader,
    /// Stop de leader-helft op deze node (vóór het teruggeven van de lease).
    StopLeader,
}

/// Wat er misging met een aanroep bij de leader.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LinkError {
    /// 404: de leader kent ons niet (herstart); herregistreren.
    NotRegistered,
    /// Transport, timeout of een andere status.
    Failed,
}

/// De lus-toestand.
#[derive(Debug)]
pub struct Election {
    own_leader: String,
    leading: bool,
    registered: bool,
    last_leader: String,
    fail_count: u32,
    self_beat_fails: u32,
    out: Vec<Request>,
}

impl Election {
    /// Een lus voor een node op `ip`, met de agent op `port` (de leader-API zit op `port + 1000`).
    pub fn new(ip: &str, port: u16) -> Self {
        Self {
            own_leader: format!("{ip}:{}", u32::from(port) + 1000),
            leading: false,
            registered: false,
            last_leader: String::new(),
            fail_count: 0,
            self_beat_fails: 0,
            out: Vec::new(),
        }
    }

    /// Of deze node leidt.
    pub fn is_leading(&self) -> bool {
        self.leading
    }

    /// Het aantal opeenvolgende mislukte ticks.
    pub fn fail_count(&self) -> u32 {
        self.fail_count
    }

    /// Het aantal opeenvolgende mislukte self-heartbeats.
    pub fn self_beat_fails(&self) -> u32 {
        self.self_beat_fails
    }

    /// Of we bij de leader geregistreerd zijn.
    pub fn is_registered(&self) -> bool {
        self.registered
    }

    /// Het adres van onze eigen leader-API.
    pub fn own_leader(&self) -> &str {
        &self.own_leader
    }

    fn emit(&mut self, r: Request) {
        if self.out.try_reserve(1).is_ok() {
            self.out.push(r);
        }
    }

    fn publish(&self, agent: &mut Agent, addr: &str) {
        agent.set_leader_addr(addr);
        if addr != self.own_leader {
            agent.set_lease_expires_at(Time::ZERO);
        }
    }

    fn become_leader(&mut self, agent: &mut Agent) {
        self.leading = true;
        self.fail_count = 0;
        self.emit(Request::StartLeader);
        let own = self.own_leader.clone();
        self.publish(agent, &own);
    }

    /// Eén directe poging bij het opstarten: is de lock vrij, dan leidt deze node meteen.
    pub fn become_leader_now<D: Discoverer>(
        &mut self,
        disc: &mut D,
        agent: &mut Agent,
    ) -> Vec<Request> {
        if !self.leading && disc.try_become_leader() {
            self.become_leader(agent);
        }
        core::mem::take(&mut self.out)
    }

    /// Treedt af: eerst de leader-helft stoppen, dan pas de lease teruggeven.
    ///
    /// Andersom ontstaat een venster waarin een opvolger de lease pakt terwijl
    /// dit proces nog als leader dient. Twee keer aftreden is onschuldig.
    pub fn step_down<D: Discoverer>(
        &mut self,
        disc: &mut D,
        agent: &mut Agent,
        release: bool,
    ) -> Vec<Request> {
        self.step_down_inner(disc, agent, release);
        core::mem::take(&mut self.out)
    }

    fn step_down_inner<D: Discoverer>(&mut self, disc: &mut D, agent: &mut Agent, release: bool) {
        let was = self.leading;
        self.leading = false;
        self.registered = false;
        self.last_leader.clear();
        self.publish(agent, "");
        if !was {
            return;
        }
        self.emit(Request::StopLeader);
        if release {
            disc.release_leadership();
        }
    }

    fn try_take_over<D: Discoverer>(&mut self, disc: &mut D, agent: &mut Agent) {
        self.last_leader.clear();
        self.publish(agent, "");
        // De volgende tick moet de store opnieuw vragen: of er nog een levende
        // leader is, beslist tussen "blijven proberen" en "geïsoleerd".
        disc.invalidate();
        if disc.try_become_leader() {
            self.become_leader(agent);
        }
    }

    /// Het enige antwoord dat het verlies van de leader veilig maakt: de store
    /// antwoordde, en niemand houdt een levende lease.
    fn store_says_no_leader<D: Discoverer>(disc: &mut D) -> bool {
        disc.get_leader().is_none() && disc.store_reachable()
    }

    fn leader_failed<D: Discoverer>(&mut self, disc: &mut D, agent: &mut Agent) {
        self.fail_count = self.fail_count.saturating_add(1);
        if self.fail_count >= TAKEOVER_AFTER {
            self.try_take_over(disc, agent);
        }
        if self.fail_count >= ISOLATED_AFTER {
            if !Self::store_says_no_leader(disc) {
                agent.stop_all();
            }
            self.fail_count = TAKEOVER_AFTER;
        }
    }

    fn no_leader<D: Discoverer>(&mut self, disc: &mut D, agent: &mut Agent) {
        self.fail_count = self.fail_count.saturating_add(1);
        if self.fail_count >= TAKEOVER_AFTER {
            self.try_take_over(disc, agent);
            if !self.leading {
                self.fail_count = TAKEOVER_AFTER;
            }
        }
    }

    /// Eén tick. `agents_connected` is wat de lokale leader aan agents ziet (0 als we niet leiden).
    pub fn tick<D: Discoverer>(
        &mut self,
        disc: &mut D,
        agent: &mut Agent,
        agents_connected: usize,
    ) -> Vec<Request> {
        self.tick_inner(disc, agent, agents_connected);
        core::mem::take(&mut self.out)
    }

    fn tick_inner<D: Discoverer>(
        &mut self,
        disc: &mut D,
        agent: &mut Agent,
        agents_connected: usize,
    ) {
        // De gecachte leader; alleen de lock-store vragen als hij onbekend is.
        let leader = if self.last_leader.is_empty() {
            disc.get_leader().unwrap_or_default()
        } else {
            self.last_leader.clone()
        };
        if self.leading {
            let (renewed, displaced) = disc.renew_lease();
            if let Some(t) = disc.lease_expires_at() {
                agent.set_lease_expires_at(t);
            }
            if displaced {
                // Echt vervangen, niet alleen afgesneden: nu aftreden, anders zijn
                // er twee leaders die naar hetzelfde cluster schrijven.
                self.step_down_inner(disc, agent, false);
                return;
            }
            if renewed {
                self.fail_count = 0;
            } else if agents_connected == 0 {
                // Store onbereikbaar en geen agents: leiderschap kwijt.
                self.step_down_inner(disc, agent, false);
                return;
            }
            // Store onbereikbaar maar agents verbonden: blijven leiden. Een werkend
            // LAN overleeft zo een storing van internet of lock-store.
            let own = self.own_leader.clone();
            self.emit(Request::SelfHeartbeat { leader: own });
        } else if !leader.is_empty() {
            if self.registered {
                self.emit(Request::Heartbeat { leader });
            } else {
                self.emit(Request::Register { leader });
            }
        } else if Self::store_says_no_leader(disc) {
            self.no_leader(disc, agent);
        } else {
            self.leader_failed(disc, agent);
        }
    }

    /// Verwerkt het antwoord op een [`Request`]; kan een vervolg geven.
    pub fn on_reply<D: Discoverer>(
        &mut self,
        disc: &mut D,
        agent: &mut Agent,
        req: &Request,
        result: Result<(), LinkError>,
    ) -> Vec<Request> {
        match (req, result) {
            (Request::Register { leader }, Ok(())) => {
                self.registered = true;
                self.fail_count = 0;
                self.last_leader.clone_from(leader);
                self.publish(agent, leader);
            }
            (Request::Register { .. }, Err(_)) => self.leader_failed(disc, agent),
            (Request::Heartbeat { leader }, Ok(())) => {
                self.fail_count = 0;
                self.last_leader.clone_from(leader);
                self.publish(agent, leader);
            }
            // De leader vergat ons (hij herstartte); hij leidt nog, dus opnieuw
            // registreren op hetzelfde adres, zonder de lock-store te vragen.
            (Request::Heartbeat { .. }, Err(LinkError::NotRegistered)) => self.registered = false,
            (Request::Heartbeat { .. }, Err(LinkError::Failed)) => self.leader_failed(disc, agent),
            (Request::SelfHeartbeat { .. }, Ok(())) => self.self_beat_fails = 0,
            // Onze eigen leader-API is bereikbaar maar vergat ons (een agent-timeout
            // tijdens een storing). GEMETEN 13-08 op een LicheeRV: zonder deze tak
            // riep een node 70+ heartbeats lang "not registered" zonder te herstellen.
            (Request::SelfHeartbeat { leader }, Err(LinkError::NotRegistered)) => {
                let leader = leader.clone();
                self.emit(Request::SelfRegister { leader });
            }
            // Transportfout op de eigen API: geen leiderschapsprobleem (de lease is
            // net vernieuwd), dus alleen tellen, zichtbaar maken, niet reageren.
            (Request::SelfHeartbeat { .. }, Err(LinkError::Failed)) => {
                self.self_beat_fails = self.self_beat_fails.saturating_add(1);
            }
            (Request::SelfRegister { .. }, Ok(())) => self.self_beat_fails = 0,
            (Request::SelfRegister { .. }, Err(_)) => {
                self.self_beat_fails = self.self_beat_fails.saturating_add(1);
            }
            (Request::StartLeader | Request::StopLeader, _) => {}
        }
        core::mem::take(&mut self.out)
    }
}
