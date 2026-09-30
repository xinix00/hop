//! De leader op HopOS en zijn agents op andere nodes: de boeken van de eigenaar en de dispatch-taak.
//!
//! `leader::Transport` is synchroon: de leader vraagt "draai deze job op die
//! agent" en wil meteen een antwoord (de daemon blokkeert zijn eigenaar-thread
//! erop, met termijnen, zoals Go). Op HopOS draaien de netstack, de
//! verbindingen en de eigenaar op één executor; blokkeren op het net kan daar
//! niet, en een geneste executor-ronde verbiedt het handboek (§4).
//!
//! Daarom antwoordt de transport van de eigenaar (`local::Local`)
//! voor een agent op een andere node uit deze [`Relay`], en voert de
//! dispatch-taak ([`dispatch_task`]) de echte aanroep daarna uit:
//!
//! - `run`: het laatst geleerde antwoord van die agent op die job, als dat
//!   een weigering was die nog vers is ([`REFUSAL_TTL`]); anders "aangenomen"
//!   en de `POST /run` in de rij. Weigert de agent alsnog (vol, affinity,
//!   onbereikbaar), dan boekt de eigenaar de plaatsing af
//!   (`Leader::mark_unplaced`, dezelfde weg als een hand-back van een agent)
//!   en onthoudt hij de weigering, zodat de reconcile die daarop volgt een
//!   andere agent kiest in plaats van dezelfde opnieuw;
//! - `stop_job`, `stop_task`, `delete_job`: in de rij, meteen "gedaan"; een
//!   stop die niet aankomt, ziet de leader bij de volgende registratie van
//!   die agent (`placed`), zoals na een netsplitsing;
//! - `tasks`: de laatste lijst die de dispatch-taak van die agent haalde
//!   ([`Relay::refresh`] vraagt ze elke leader-tik opnieuw).
//!
//! De eigenaar wacht zo nooit op een andere node, en wat de leader besluit
//! is binnen één rondreis bijgesteld naar wat de agent echt deed.

use alloc::collections::{BTreeMap, BTreeSet};
use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use core::time::Duration;

use leader::RunReply;
use sync::Full;
use types::json;
use types::time::SECOND;
use types::{Agent, Job, Nanos, Task};

use crate::client::Client;
use crate::fetch::{Connect, Resolve};
use crate::link::signed;
use crate::mail::{DispatchQueue, Inbox, Mail, Nap, Remote, deliver};

/// Hoe lang een weigering van een agent geldt: daarna mag de leader het
/// weer proberen (er kwam ruimte vrij, de agent kwam terug). Drie
/// leader-tikken: de vangnet-reconcile draait elke derde.
pub const REFUSAL_TTL: Nanos = 30 * SECOND;

/// De termijn van `/run` en `/tasks` (Go: 5 s).
pub const QUICK: Duration = Duration::from_secs(5);

/// De termijn van stoppen en verwijderen (Go: 60 s; docker doet 10 s
/// SIGTERM plus 10 s SIGKILL).
pub const SLOW: Duration = Duration::from_secs(60);

/// De grootste takenlijst die de leader van een agent leest (zoals de host).
pub const MAX_TASKS_BODY: usize = 8 << 20;

/// De boeken van de eigenaar over agents op andere nodes.
#[derive(Debug, Default)]
pub struct Relay {
    /// De aanroepen die nog naar de dispatch-taak moeten.
    out: Vec<Remote>,
    /// Weigeringen per (agent, job), met het moment waarop ze verlopen.
    refused: BTreeMap<(String, String), (RunReply, Nanos)>,
    /// De laatste takenlijst per agent.
    tasks: BTreeMap<String, Vec<Task>>,
    /// Agents waarvan een takenlijst onderweg is: één tegelijk per agent.
    asking: BTreeSet<String>,
    /// Aanroepen die niet in de rij pasten, voor de logregel.
    dropped: u64,
}

impl Relay {
    /// Lege boeken.
    pub fn new() -> Self {
        Self::default()
    }

    fn push(&mut self, r: Remote) {
        if self.out.try_reserve(1).is_ok() {
            self.out.push(r);
        } else {
            self.dropped = self.dropped.saturating_add(1);
        }
    }

    /// `POST /run` bij een andere agent, zoals de leader hem vraagt.
    pub fn run(&mut self, now: Nanos, agent: &Agent, job: &Job, replace: bool) -> RunReply {
        let key = (agent.id.clone(), job.name.clone());
        if let Some(&(reply, until)) = self.refused.get(&key) {
            if now < until {
                return reply;
            }
            self.refused.remove(&key);
        }
        let Ok(body) = job.to_json() else {
            return RunReply::Rejected(500);
        };
        self.push(Remote::Run {
            agent: agent.id.clone(),
            endpoint: agent.endpoint.clone(),
            job: job.name.clone(),
            body,
            replace,
        });
        RunReply::Accepted
    }

    /// `POST /stop/{job}`: in de rij.
    pub fn stop_job(&mut self, agent: &Agent, job: &str) -> bool {
        self.push(Remote::StopJob {
            endpoint: agent.endpoint.clone(),
            job: String::from(job),
        });
        true
    }

    /// `POST /stop-task/{id}`: in de rij.
    pub fn stop_task(&mut self, agent: &Agent, task: &str) {
        self.push(Remote::StopTask {
            endpoint: agent.endpoint.clone(),
            task: String::from(task),
        });
    }

    /// `DELETE /delete/{job}`: in de rij; de weigeringen van die job vervallen.
    pub fn delete_job(&mut self, agent: &Agent, job: &str) {
        self.refused.retain(|(_, j), _| j != job);
        self.push(Remote::DeleteJob {
            endpoint: agent.endpoint.clone(),
            job: String::from(job),
        });
    }

    /// De laatst gehaalde takenlijst van `agent`.
    pub fn tasks(&self, agent: &Agent) -> Option<Vec<Task>> {
        let known = self.tasks.get(&agent.id)?;
        let mut out = Vec::new();
        out.try_reserve_exact(known.len()).ok()?;
        out.extend(known.iter().cloned());
        Some(out)
    }

    /// Vraagt de takenlijst van elke agent in `agents` opnieuw (één uitstaand per agent).
    pub fn refresh<'a>(&mut self, agents: impl Iterator<Item = &'a Agent>) {
        let mut seen = BTreeSet::new();
        for a in agents {
            seen.insert(a.id.clone());
            if self.asking.contains(&a.id) {
                continue;
            }
            self.asking.insert(a.id.clone());
            self.push(Remote::Tasks {
                agent: a.id.clone(),
                endpoint: a.endpoint.clone(),
            });
        }
        // Een agent die de leader niet meer kent, heeft geen lijst meer.
        self.tasks.retain(|id, _| seen.contains(id));
        self.refused.retain(|(id, _), _| seen.contains(id));
    }

    /// Verwerkt een takenlijst van de dispatch-taak.
    pub fn on_tasks(&mut self, agent: &str, tasks: Option<Vec<Task>>) {
        self.asking.remove(agent);
        match tasks {
            Some(t) => {
                self.tasks.insert(String::from(agent), t);
            }
            None => {
                self.tasks.remove(agent);
            }
        }
    }

    /// Onthoudt een weigering van `agent` voor `job`, tot `now + REFUSAL_TTL`.
    pub fn refuse(&mut self, now: Nanos, agent: &str, job: &str, reply: RunReply) {
        let until = now.saturating_add(REFUSAL_TTL);
        self.refused
            .insert((String::from(agent), String::from(job)), (reply, until));
    }

    /// Geeft de aanroepen voor de dispatch-taak af.
    pub fn take(&mut self) -> Vec<Remote> {
        core::mem::take(&mut self.out)
    }

    /// Hoeveel aanroepen niet pasten (geheugen of een volle rij), sinds de vorige vraag.
    pub fn take_dropped(&mut self) -> u64 {
        core::mem::take(&mut self.dropped)
    }

    /// Zet de aanroepen in de rij van de dispatch-taak; wat niet past, telt als gevallen.
    ///
    /// Een `run` die niet past, is een weigering ("onbereikbaar"): die
    /// komt terug als [`Mail::Ran`]-achtige uitkomst voor de eigenaar, via
    /// de lijst die deze functie teruggeeft.
    pub fn flush(&mut self, queue: &DispatchQueue) -> Vec<(String, String)> {
        let mut unsent = Vec::new();
        for r in self.take() {
            if let Err(Full(back)) = queue.try_send(r) {
                self.dropped = self.dropped.saturating_add(1);
                match back {
                    Remote::Run { agent, job, .. } => unsent.push((agent, job)),
                    Remote::Tasks { agent, .. } => {
                        self.asking.remove(&agent);
                    }
                    _ => {}
                }
            }
        }
        unsent
    }

    /// Vergeet alles: deze node leidt niet meer.
    pub fn clear(&mut self) {
        *self = Self::default();
    }
}

/// De URL van `path` op de agent op `endpoint`.
fn url(endpoint: &str, path: &str) -> String {
    format!("{}{path}", endpoint.trim_end_matches('/'))
}

/// De takenlijst uit een antwoord van `GET /tasks`.
fn parse_tasks(body: &[u8]) -> Option<Vec<Task>> {
    let v = json::parse(body).ok()?;
    v.as_array()?
        .iter()
        .map(|t| Task::from_value(t).ok())
        .collect()
}

/// Voert één aanroep uit; het antwoord voor de eigenaar, als er een is.
async fn call<C: Connect, R: Resolve>(
    client: &mut Client<C, R>,
    key: &[u8],
    r: Remote,
) -> Option<Mail> {
    match r {
        Remote::Run {
            agent,
            endpoint,
            job,
            body,
            replace,
        } => {
            let path = if replace { "/run?replace=1" } else { "/run" };
            let got = signed(
                client,
                key,
                "POST",
                &url(&endpoint, path),
                Some(body.as_bytes()),
                QUICK,
                64 << 10,
            )
            .await;
            let reply = match got {
                Ok((status, _)) => RunReply::from_status(status),
                Err(_) => RunReply::Unreachable,
            };
            Some(Mail::Ran { agent, job, reply })
        }
        Remote::StopJob { endpoint, job } => {
            let path = format!("/stop/{job}");
            let _ = signed(
                client,
                key,
                "POST",
                &url(&endpoint, &path),
                Some(b""),
                SLOW,
                64 << 10,
            )
            .await;
            None
        }
        Remote::StopTask { endpoint, task } => {
            let path = format!("/stop-task/{task}");
            let _ = signed(
                client,
                key,
                "POST",
                &url(&endpoint, &path),
                Some(b""),
                SLOW,
                64 << 10,
            )
            .await;
            None
        }
        Remote::DeleteJob { endpoint, job } => {
            let path = format!("/delete/{job}");
            let _ = signed(
                client,
                key,
                "DELETE",
                &url(&endpoint, &path),
                None,
                SLOW,
                64 << 10,
            )
            .await;
            None
        }
        Remote::Tasks { agent, endpoint } => {
            let got = signed(
                client,
                key,
                "GET",
                &url(&endpoint, "/tasks"),
                None,
                QUICK,
                MAX_TASKS_BODY,
            )
            .await;
            let tasks = match got {
                Ok((200, body)) => parse_tasks(&body),
                _ => None,
            };
            Some(Mail::Tasks { agent, tasks })
        }
    }
}

/// De dispatch-taak: bezit `client`, voert de aanroepen van de leader bij andere agents uit.
pub async fn dispatch_task<C: Connect, R: Resolve, N: Nap>(
    mut client: Client<C, R>,
    key: Vec<u8>,
    calls: &'static DispatchQueue,
    inbox: &'static Inbox,
    nap: N,
) {
    loop {
        let r = calls.recv().await;
        if let Some(mail) = call(&mut client, &key, r).await {
            deliver(inbox, mail, &nap).await;
        }
    }
}
