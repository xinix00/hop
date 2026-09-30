//! Eén pollronde: wat hopprom van Hop vraagt, en welke vraag mag missen.
//!
//! Bezit de invoer van de [`crate::Collector`] ([`Poll`]) en de volgorde
//! van de aanroepen over een [`Source`]. De bron doet het net (hoplib: op
//! de host over hostnet, op HopOS over applib); de antwoordtypen zijn die
//! van hoplib. Hier staat alleen de regel, zodat die één keer op de host
//! getoetst wordt:
//!
//! - **Verplicht**: `/v1/agents`, `/v1/jobs` en `/v1/tasks`. Mist er één,
//!   dan is de ronde mislukt en blijven de metrics van de vorige ronde
//!   staan (Go deed hetzelfde voor agents en jobs). Taken zijn nu ook
//!   verplicht: in Go waren het N aanroepen (`/v1/jobs/{naam}/status`) die
//!   elk mochten missen, nu is het er één, en een ronde zonder taken zou elke
//!   taakmetriek op nul zetten en bij de volgende ronde elke taak als nieuwe
//!   start tellen.
//! - **Best-effort**: de capaciteit per agent, `/v1/status` en `/leader`.
//!   Een misser houdt de vorige waarde vast; de bron krijgt hem terug als
//!   [`Miss`], om te loggen.

use alloc::string::String;
use alloc::vec::Vec;
use core::fmt;
use core::future::Future;

pub use hoplib::{AgentTasks, Capacity, Leader, Status};
use types::{Agent, Job};

use crate::{Result, push};

/// Alles van één ronde: de invoer van [`crate::Collector::observe`].
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Poll {
    /// De geregistreerde agents.
    pub agents: Vec<Agent>,
    /// Alle jobs.
    pub jobs: Vec<Job>,
    /// De taken per agent.
    pub tasks: Vec<AgentTasks>,
    /// De capaciteit per agent-id; `None` is een misser (de vorige blijft).
    pub capacities: Vec<(String, Option<Capacity>)>,
    /// `/v1/status`; `None` is een misser.
    pub status: Option<Status>,
    /// `/leader`; `None` is een misser.
    pub leader: Option<Leader>,
}

impl Poll {
    /// Het aantal taken over alle bereikte agents.
    pub fn task_count(&self) -> usize {
        self.tasks
            .iter()
            .filter_map(|a| a.tasks.as_ref())
            .map(Vec::len)
            .sum()
    }
}

/// Een stap van de ronde, voor de logregel van een misser.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Step {
    /// `GET /v1/agents`.
    Agents,
    /// `GET /v1/jobs`.
    Jobs,
    /// `GET /v1/tasks`.
    Tasks,
    /// `GET /v1/agents/{id}/capacity`.
    Capacity,
    /// `GET /v1/status`.
    Status,
    /// `GET /leader`.
    Leader,
}

impl Step {
    /// De route van deze stap.
    pub fn path(self) -> &'static str {
        match self {
            Self::Agents => "/v1/agents",
            Self::Jobs => "/v1/jobs",
            Self::Tasks => "/v1/tasks",
            Self::Capacity => "/v1/agents/{id}/capacity",
            Self::Status => "/v1/status",
            Self::Leader => "/leader",
        }
    }
}

impl fmt::Display for Step {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.path())
    }
}

/// Een aanroep die faalde, met de fout van de bron.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Miss<E> {
    /// Welke stap.
    pub step: Step,
    /// De fout van de bron.
    pub err: E,
}

impl<E: fmt::Display> fmt::Display for Miss<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.step, self.err)
    }
}

/// Waar een ronde zijn antwoorden vandaan haalt.
///
/// Elke methode is één aanroep naar Hop; de bron ondertekent, verbindt en
/// leest. Een foutstatus is een fout van de bron.
pub trait Source {
    /// De fout van een aanroep, voor de logregel.
    type Error: fmt::Display;

    /// `GET /v1/agents`.
    fn agents(&mut self) -> impl Future<Output = Result<Vec<Agent>, Self::Error>>;
    /// `GET /v1/jobs`.
    fn jobs(&mut self) -> impl Future<Output = Result<Vec<Job>, Self::Error>>;
    /// `GET /v1/tasks`.
    fn tasks(&mut self) -> impl Future<Output = Result<Vec<AgentTasks>, Self::Error>>;
    /// `GET /v1/agents/{id}/capacity` voor `agent`.
    fn capacity(&mut self, agent: &Agent) -> impl Future<Output = Result<Capacity, Self::Error>>;
    /// `GET /v1/status`.
    fn status(&mut self) -> impl Future<Output = Result<Status, Self::Error>>;
    /// `GET /leader`.
    fn leader(&mut self) -> impl Future<Output = Result<Leader, Self::Error>>;
    /// Geheugen voor de ronde zelf was op.
    fn out_of_memory(&self) -> Self::Error;
}

/// Wat een ronde oplevert: de [`Poll`] en de missers van de best-effort-stappen.
pub type Polled<E> = (Poll, Vec<Miss<E>>);

/// Doet één ronde over `src`.
///
/// Faalt een verplichte stap, dan is dat de fout; de best-effort-stappen
/// staan als `None` in de [`Poll`] en als [`Miss`] in de lijst.
pub async fn poll<S: Source>(src: &mut S) -> Result<Polled<S::Error>, Miss<S::Error>> {
    let agents = src.agents().await.map_err(|err| Miss {
        step: Step::Agents,
        err,
    })?;
    let jobs = src.jobs().await.map_err(|err| Miss {
        step: Step::Jobs,
        err,
    })?;
    let tasks = src.tasks().await.map_err(|err| Miss {
        step: Step::Tasks,
        err,
    })?;
    let oom = |src: &S| Miss {
        step: Step::Capacity,
        err: src.out_of_memory(),
    };
    let mut misses = Vec::new();
    let mut capacities = Vec::new();
    capacities
        .try_reserve_exact(agents.len())
        .map_err(|_| oom(src))?;
    for a in &agents {
        let cap = match src.capacity(a).await {
            Ok(c) => Some(c),
            Err(err) => {
                push(
                    &mut misses,
                    Miss {
                        step: Step::Capacity,
                        err,
                    },
                )
                .map_err(|_| oom(src))?;
                None
            }
        };
        let id = types::try_string(&a.id).map_err(|_| oom(src))?;
        capacities.push((id, cap));
    }
    let status = best_effort(src.status().await, Step::Status, &mut misses);
    let leader = best_effort(src.leader().await, Step::Leader, &mut misses);
    let poll = Poll {
        agents,
        jobs,
        tasks,
        capacities,
        status,
        leader,
    };
    Ok((poll, misses))
}

/// Het antwoord, of `None` met de fout in `misses` (en zonder geheugen
/// daarvoor alleen `None`: de ronde zelf gaat door).
fn best_effort<T, E>(r: Result<T, E>, step: Step, misses: &mut Vec<Miss<E>>) -> Option<T> {
    match r {
        Ok(v) => Some(v),
        Err(err) => {
            let _ = push(misses, Miss { step, err });
            None
        }
    }
}
