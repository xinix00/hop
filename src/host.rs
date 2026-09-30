//! De agent-API op de host (feature `std`): blokkerend over `hostnet::Http`.
//!
//! Bezit een [`Client`] en de HTTP-client van hostnet. Elke aanroep dialt
//! vers en blokkeert de thread tot het antwoord er is (hostnet's model: één
//! executor per thread, geen reactor). Een plugin die tegelijk wil luisteren
//! en bedienen, geeft de stroom een eigen thread en stuurt wat hij hoort als
//! bericht naar de eigenaar van zijn staat (handboek §1).

use alloc::vec::Vec;
use core::ops::ControlFlow;
use core::time::Duration;

use hostnet::{Call as HttpCall, Http, Open};

use crate::{
    AgentInfo, AgentTasks, Applied, Backoff, Call, Capacity, Client, Error, Event, Events, Job,
    JobStatus, Leader, MAX_BODY, Reason, Request, Response, Result, Status, Task,
};

/// De termijn per fase van een gewone aanroep (verbinden, kop, elke lees).
/// Een apply wacht op de plaatsing; een halve minuut is ruim voor een
/// plugin, die zelden apply doet.
pub const TIMEOUT: Duration = Duration::from_secs(30);

/// De stiltetermijn van de stroom: de agent schrijft elke 15 s een
/// keepalive, dus wie een minuut zwijgt, is weg (dezelfde maat als de CLI).
pub const STREAM_IDLE: Duration = Duration::from_secs(60);

/// Hoeveel bytes de stroom per lees ophaalt.
const READ_CHUNK: usize = 4 << 10;

/// Een agent, bereikt vanaf de host.
#[derive(Clone, Debug)]
pub struct Agent {
    client: Client,
    http: Http,
    timeout: Duration,
}

impl Agent {
    /// Een agent op het adres en met de sleutel van `client`.
    pub fn new(client: Client) -> Self {
        Self {
            client,
            http: Http::new(),
            timeout: TIMEOUT,
        }
    }

    /// Dezelfde agent met een andere termijn per fase.
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// De client (adres en sleutel).
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Verstuurt één verzoek en buffert het antwoord (hoogstens [`MAX_BODY`]).
    ///
    /// Elke status is een antwoord; alleen het transport faalt.
    pub fn send(&self, req: &Request) -> Result<Response> {
        let headers: Vec<(&str, &str)> = req
            .headers
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let call = HttpCall {
            method: req.method,
            url: &req.url,
            headers: &headers,
            body: (!req.body.is_empty()).then_some(req.body.as_slice()),
            timeout: self.timeout,
        };
        let r = self
            .http
            .request(&call, MAX_BODY)
            .map_err(|e| Error::Net(Reason::of(&e)))?;
        Ok(Response {
            status: r.status,
            body: r.body,
        })
    }

    /// Verstuurt een [`Call`] en leest zijn antwoord.
    pub fn run<T>(&self, call: Result<Call<T>>) -> Result<T> {
        let call = call?;
        call.parse(&self.send(call.request())?)
    }

    /// Het clusteroverzicht (`/v1/status`).
    pub fn status(&self) -> Result<Status> {
        self.run(self.client.status())
    }

    /// De geregistreerde agents (`/v1/agents`).
    pub fn agents(&self) -> Result<Vec<AgentInfo>> {
        self.run(self.client.agents())
    }

    /// Alle jobs (`/v1/jobs`).
    pub fn jobs(&self) -> Result<Vec<Job>> {
        self.run(self.client.jobs())
    }

    /// Eén job op naam, of `None`.
    pub fn job(&self, name: &str) -> Result<Option<Job>> {
        self.run(self.client.job(name))
    }

    /// De taken per agent: `/v1/tasks` bij de leader, en bij een leader
    /// zonder die route elke agent zelf (een agent die faalt, staat er met
    /// `tasks: None` in, zoals een `unreachable` van de leader).
    pub fn tasks(&self) -> Result<Vec<AgentTasks>> {
        if let Some(all) = self.run(self.client.tasks())? {
            return Ok(all);
        }
        let agents = self.agents()?;
        let mut out = Vec::new();
        out.try_reserve_exact(agents.len())
            .map_err(|_| Error::OutOfMemory)?;
        for a in agents {
            let tasks: Option<Vec<Task>> = self.run(self.client.agent_tasks(&a)).ok();
            out.push(AgentTasks { agent: a.id, tasks });
        }
        Ok(out)
    }

    /// De agents en taken van één job: `/v1/jobs/{naam}/status`, en bij
    /// een leader zonder die route [`Agent::tasks`] gefilterd op de job.
    pub fn job_status(&self, name: &str) -> Result<JobStatus> {
        if let Some(st) = self.run(self.client.job_status(name))? {
            return Ok(st);
        }
        let all = self.tasks()?;
        let agents = self.agents()?;
        Ok(JobStatus::from_tasks(name, agents, all))
    }

    /// De capaciteit van `agent`: via de leader, en bij een leader zonder
    /// die doorgifte bij de agent zelf.
    pub fn capacity(&self, agent: &AgentInfo) -> Result<Capacity> {
        match self.run(self.client.capacity(agent))? {
            Some(c) => Ok(c),
            None => self.run(self.client.agent_capacity(agent)),
        }
    }

    /// Wie er leidt (`/leader` van de lokale agent).
    pub fn leader(&self) -> Result<Leader> {
        self.run(self.client.leader())
    }

    /// Maakt of werkt een job bij.
    pub fn apply(&self, job: &Job) -> Result<Applied> {
        self.run(self.client.apply(job))
    }

    /// Verwijdert een job.
    pub fn delete(&self, name: &str) -> Result {
        self.run(self.client.delete(name))
    }

    /// De stroom van `/v1/events`, die bij de eerste [`EventStream::next`] verbindt.
    pub fn stream(&self) -> EventStream<'_> {
        EventStream {
            agent: self,
            open: None,
            events: Events::new(),
            backoff: Backoff::default(),
            tried: false,
        }
    }

    /// Volgt `/v1/events` tot `on_event` stopt, en verbindt opnieuw na elke breuk.
    ///
    /// `on_lost` hoort elke breuk met de wachttijd tot de volgende poging:
    /// de plek voor één logregel. Na elke (her)verbinding komt eerst een
    /// `ping` ([`Event::is_resync`]): dan leest de plugin alles opnieuw,
    /// want wat er tussendoor gebeurde, zag hij niet.
    pub fn events(
        &self,
        mut on_event: impl FnMut(&Event) -> ControlFlow<()>,
        mut on_lost: impl FnMut(&Error, Duration),
    ) {
        let mut s = self.stream();
        loop {
            match s.next() {
                Ok(e) => {
                    if on_event(&e).is_break() {
                        return;
                    }
                }
                Err(e) => on_lost(&e, s.backoff.peek()),
            }
        }
    }
}

/// Een levende `/v1/events`-stroom met herverbinden.
///
/// Bezit de verbinding (`Drop` sluit hem), de lezer en de backoff.
pub struct EventStream<'a> {
    agent: &'a Agent,
    open: Option<Open>,
    events: Events,
    backoff: Backoff,
    /// Of er al een poging was: alleen dan wacht de volgende eerst.
    tried: bool,
}

impl core::fmt::Debug for EventStream<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("EventStream")
            .field("connected", &self.open.is_some())
            .field("backoff", &self.backoff)
            .finish_non_exhaustive()
    }
}

impl EventStream<'_> {
    /// Dezelfde stroom met een ander wachtbeleid.
    #[must_use]
    pub fn with_backoff(mut self, backoff: Backoff) -> Self {
        self.backoff = backoff;
        self
    }

    /// De volgende gebeurtenis; blokkeert tot er een is.
    ///
    /// Een fout is een breuk (verbinden, status, stilte, einde); de stroom
    /// is dan dicht, en de volgende aanroep wacht de backoff en verbindt
    /// opnieuw. Opgeven of doorgaan is aan de aanroeper.
    #[expect(
        clippy::should_implement_trait,
        reason = "de stroom eindigt nooit: een breuk is een fout, geen None"
    )]
    pub fn next(&mut self) -> Result<Event> {
        let mut buf = [0u8; READ_CHUNK];
        loop {
            match self.events.next() {
                Ok(Some(e)) => {
                    if e.kind == "ping" {
                        // De stroom staat: de volgende breuk begint weer kort.
                        self.backoff.reset();
                    }
                    return Ok(e);
                }
                Ok(None) => {}
                Err(e) => return Err(self.drop_conn(e)),
            }
            let Some(open) = self.open.as_mut() else {
                self.connect()?;
                continue;
            };
            let n = match open.read(&mut buf) {
                Ok(0) => return Err(self.drop_conn(Error::Closed)),
                Ok(n) => n,
                Err(e) => return Err(self.drop_conn(Error::Net(Reason::of(&e)))),
            };
            if let Err(e) = self.events.push(buf.get(..n).unwrap_or_default()) {
                return Err(self.drop_conn(e));
            }
        }
    }

    /// Sluit de verbinding en geeft `e` door.
    fn drop_conn(&mut self, e: Error) -> Error {
        self.open = None;
        self.events.reset();
        e
    }

    /// Opent de stroom, na de backoff als er al een poging was.
    fn connect(&mut self) -> Result {
        if self.tried {
            std::thread::sleep(self.backoff.next_delay());
        }
        self.tried = true;
        self.events.reset();
        let req = self.agent.client.events()?;
        let headers: Vec<(&str, &str)> = req
            .headers
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let call = HttpCall {
            method: req.method,
            url: &req.url,
            headers: &headers,
            body: None,
            timeout: STREAM_IDLE,
        };
        let mut open = self
            .agent
            .http
            .open(&call)
            .map_err(|e| Error::Net(Reason::of(&e)))?;
        if open.status() != 200 {
            let body = open.read_to_end(4 << 10).unwrap_or_default();
            return Err(crate::model::status_error(open.status(), &body));
        }
        self.open = Some(open);
        Ok(())
    }
}
