//! De agent-API voor een bewoner van HopOS (feature `hopos`): async op de executor van de app-core.
//!
//! Bezit een [`Client`], een dialer en een wekker. De dialer
//! ([`SlotDial`]) zoekt de host op met de resolver van applib, verbindt
//! over de netstack van het slot en geeft een `applib::tcp::TcpConn`; de
//! client van leanhttp doet de rest. Elke aanroep dialt vers: een plugin
//! vraagt een paar keer per minuut, en een pool is een tabel die iemand moet
//! bezitten.
//!
//! Waarom een [`Idle`]-omhulsel: leanhttp zet na de antwoordkop de
//! leestermijn uit, en een body (of een stroom) van een agent die wegviel,
//! zou een taak dan voor altijd vasthouden. Het omhulsel maakt van "geen
//! termijn" een stiltetermijn.
//!
//! Dialer en wekker zijn parameters zodat deze module op de host te toetsen
//! is tegen een nep-verbinding; in een slot is het altijd
//! `Agent<SlotDial, &'static Exec>` ([`Agent::new`]).

use alloc::vec::Vec;
use core::future::Future;
use core::ops::ControlFlow;
use core::task::{Context, Poll};
use core::time::Duration;

use applib::appnet::{self, NetError, TcpStream};
use applib::rt::Exec;
use applib::tcp::TcpConn;
use leanhttp::{AsyncRead, AsyncWrite, Close, Header, IoError, Target};

use crate::{
    AgentInfo, AgentTasks, Applied, Backoff, Call, Capacity, Client, Error, Event, Events, Job,
    JobStatus, Leader, MAX_BODY, Reason, Request, Response, Result, Status, Task,
};

/// De termijn van verbinden en van de antwoordkop.
pub const TIMEOUT: Duration = Duration::from_secs(10);

/// De stiltetermijn van elke lees na de kop: de agent schrijft op een
/// stroom elke 15 s een keepalive, dus een minuut stilte is een dode agent.
pub const IDLE: Duration = Duration::from_secs(60);

/// Hoeveel bytes de stroom per lees ophaalt.
const READ_CHUNK: usize = 2 << 10;

/// Een dialer voor de agent: leanhttp's `Dial`, kopieerbaar per aanroep,
/// met de reden van de laatste mislukte dial.
pub trait Dialer: leanhttp::Dial + Clone {
    /// Waarom de laatste dial faalde, als hij dat weet.
    fn why(&self) -> Option<Reason> {
        None
    }
}

/// Een wekker: slaap `d` op het timerwiel.
pub trait Nap {
    /// Slaapt `d`.
    fn nap(&self, d: Duration) -> impl Future<Output = ()>;
}

impl Nap for &'static Exec {
    fn nap(&self, d: Duration) -> impl Future<Output = ()> {
        self.after(d)
    }
}

/// Verbindingen over de netstack van dit slot.
#[derive(Clone, Copy)]
pub struct SlotDial {
    exec: &'static Exec,
    why: Option<NetError>,
}

impl core::fmt::Debug for SlotDial {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("SlotDial").field("why", &self.why).finish()
    }
}

impl SlotDial {
    /// Een dialer waarvan de termijnen op `exec` lopen.
    pub fn new(exec: &'static Exec) -> Self {
        Self { exec, why: None }
    }
}

impl leanhttp::Dial for SlotDial {
    type Conn = Idle<TcpConn>;

    async fn dial(&mut self, target: Target<'_>) -> leanhttp::Result<Self::Conn> {
        // Geen TLS in de bewoner: de agent staat op het slot-LAN of op de
        // node zelf. `is_encrypted` is dus `false` en leanhttp weigert
        // `https://` al vóór hij hier komt.
        let ip = appnet::resolve(target.host).await.map_err(|e| {
            self.why = Some(e);
            leanhttp::Error::Connect
        })?;
        let s = TcpStream::connect_timeout(ip, target.port, TIMEOUT)
            .await
            .map_err(|e| {
                self.why = Some(e);
                leanhttp::Error::Connect
            })?;
        Ok(Idle::new(TcpConn::new(s, self.exec), IDLE))
    }
}

impl Dialer for SlotDial {
    fn why(&self) -> Option<Reason> {
        self.why.map(|e| Reason::of(&e))
    }
}

/// Een verbinding waarop "geen termijn" een stiltetermijn is.
///
/// Een termijn van applib's `TcpConn` is een vaste deadline vanaf het zetten;
/// een gelukte read zet hem niet opnieuw. Een stilte is iets anders: na
/// elke read die bytes bracht begint hij opnieuw. Zonder dat ging een
/// `/v1/events`-stroom precies [`IDLE`] na zijn kop dicht, ook met de
/// keepalive van de agent elke 15 s (19-10, spin-service). Een termijn die
/// de aanroeper zelf zet (de kop) blijft een vaste deadline.
#[derive(Debug)]
pub struct Idle<C> {
    conn: C,
    idle: Duration,
    /// Of de leestermijn nu de stilte is (de aanroeper zei "geen termijn").
    silence: bool,
}

impl<C> Idle<C> {
    /// Omhult `conn`; een lees of schrijf zonder termijn krijgt `idle`.
    pub fn new(conn: C, idle: Duration) -> Self {
        Self {
            conn,
            idle,
            silence: false,
        }
    }

    /// De omhulde verbinding.
    pub fn get_mut(&mut self) -> &mut C {
        &mut self.conn
    }
}

impl<C: AsyncRead> AsyncRead for Idle<C> {
    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize, IoError>> {
        let read = self.conn.poll_read(cx, buf);
        if self.silence
            && let Poll::Ready(Ok(n)) = read
            && n > 0
        {
            // De stilte begint opnieuw; lukt dat niet, dan geldt de oude.
            let _ = self.conn.set_read_timeout(Some(self.idle));
        }
        read
    }

    fn set_read_timeout(&mut self, t: Option<Duration>) -> Result<(), IoError> {
        self.silence = t.is_none();
        self.conn.set_read_timeout(Some(t.unwrap_or(self.idle)))
    }
}

impl<C: AsyncWrite> AsyncWrite for Idle<C> {
    fn poll_write(&mut self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, IoError>> {
        self.conn.poll_write(cx, buf)
    }

    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), IoError>> {
        self.conn.poll_flush(cx)
    }

    fn set_write_timeout(&mut self, t: Option<Duration>) -> Result<(), IoError> {
        self.conn.set_write_timeout(Some(t.unwrap_or(self.idle)))
    }
}

impl<C: Close> Close for Idle<C> {
    fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), IoError>> {
        self.conn.poll_close(cx)
    }

    fn has_grown(&self) -> bool {
        self.conn.has_grown()
    }
}

/// Een agent, bereikt vanuit een slot.
#[derive(Clone, Debug)]
pub struct Agent<D = SlotDial, N = &'static Exec> {
    client: Client,
    dial: D,
    nap: N,
}

impl Agent {
    /// Een agent op het adres en met de sleutel van `client`, over de netstack van dit slot.
    pub fn new(client: Client, exec: &'static Exec) -> Self {
        Self {
            client,
            dial: SlotDial::new(exec),
            nap: exec,
        }
    }
}

/// Een [`Request`] als leanhttp-call; de koppen gaan in `header`.
fn call<'a>(req: &'a Request, header: Header) -> leanhttp::Call<'a> {
    leanhttp::Call {
        method: req.method,
        url: &req.url,
        header,
        body: (!req.body.is_empty()).then_some(req.body.as_slice()),
        header_timeout: Some(TIMEOUT),
        ..leanhttp::Call::default()
    }
}

/// De koppen van een [`Request`] als leanhttp-`Header`.
fn header(req: &Request) -> Result<Header> {
    let mut h = Header::new();
    for (k, v) in &req.headers {
        h.set(k, v).map_err(|e| Error::Net(Reason::of(&e)))?;
    }
    Ok(h)
}

/// Een fout van leanhttp, met de reden van de dialer als die er een weet.
fn net<D: Dialer>(dial: &D, e: leanhttp::Error) -> Error {
    Error::Net(dial.why().unwrap_or_else(|| Reason::of(&e)))
}

impl<D: Dialer, N: Nap> Agent<D, N> {
    /// Een agent met een eigen dialer en wekker (tests, of een ander net).
    pub fn with(client: Client, dial: D, nap: N) -> Self {
        Self { client, dial, nap }
    }

    /// De client (adres en sleutel).
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Verstuurt één verzoek en buffert het antwoord (hoogstens [`MAX_BODY`]).
    ///
    /// Elke status is een antwoord; alleen het transport faalt.
    pub async fn send(&self, req: &Request) -> Result<Response> {
        let mut dial = self.dial.clone();
        let c = call(req, header(req)?);
        let mut resp = leanhttp::fetch(&mut dial, c)
            .await
            .map_err(|e| net(&dial, e))?;
        let body = resp
            .read_to_end(MAX_BODY)
            .await
            .map_err(|e| Error::Net(Reason::of(&e)))?;
        Ok(Response {
            status: resp.status,
            body,
        })
    }

    /// Verstuurt een [`Call`] en leest zijn antwoord.
    pub async fn run<T>(&self, call: Result<Call<T>>) -> Result<T> {
        let call = call?;
        let resp = self.send(call.request()).await?;
        call.parse(&resp)
    }

    /// Het clusteroverzicht (`/v1/status`).
    pub async fn status(&self) -> Result<Status> {
        self.run(self.client.status()).await
    }

    /// De geregistreerde agents (`/v1/agents`).
    pub async fn agents(&self) -> Result<Vec<AgentInfo>> {
        self.run(self.client.agents()).await
    }

    /// Alle jobs (`/v1/jobs`).
    pub async fn jobs(&self) -> Result<Vec<Job>> {
        self.run(self.client.jobs()).await
    }

    /// Eén job op naam, of `None`.
    pub async fn job(&self, name: &str) -> Result<Option<Job>> {
        self.run(self.client.job(name)).await
    }

    /// De taken per agent: `/v1/tasks` bij de leader, en bij een leader
    /// zonder die route elke agent zelf (een agent die faalt, staat er met
    /// `tasks: None` in).
    pub async fn tasks(&self) -> Result<Vec<AgentTasks>> {
        if let Some(all) = self.run(self.client.tasks()).await? {
            return Ok(all);
        }
        let agents = self.agents().await?;
        let mut out = Vec::new();
        out.try_reserve_exact(agents.len())
            .map_err(|_| Error::OutOfMemory)?;
        for a in agents {
            let tasks: Option<Vec<Task>> = self.run(self.client.agent_tasks(&a)).await.ok();
            out.push(AgentTasks { agent: a.id, tasks });
        }
        Ok(out)
    }

    /// De agents en taken van één job: `/v1/jobs/{naam}/status`, en bij
    /// een leader zonder die route [`Agent::tasks`] gefilterd op de job.
    pub async fn job_status(&self, name: &str) -> Result<JobStatus> {
        if let Some(st) = self.run(self.client.job_status(name)).await? {
            return Ok(st);
        }
        let all = self.tasks().await?;
        let agents = self.agents().await?;
        Ok(JobStatus::from_tasks(name, agents, all))
    }

    /// De capaciteit van `agent`: via de leader, en bij een leader zonder
    /// die doorgifte bij de agent zelf.
    pub async fn capacity(&self, agent: &AgentInfo) -> Result<Capacity> {
        match self.run(self.client.capacity(agent)).await? {
            Some(c) => Ok(c),
            None => self.run(self.client.agent_capacity(agent)).await,
        }
    }

    /// Wie er leidt (`/leader` van de lokale agent).
    pub async fn leader(&self) -> Result<Leader> {
        self.run(self.client.leader()).await
    }

    /// Maakt of werkt een job bij.
    pub async fn apply(&self, job: &Job) -> Result<Applied> {
        self.run(self.client.apply(job)).await
    }

    /// Verwijdert een job.
    pub async fn delete(&self, name: &str) -> Result {
        self.run(self.client.delete(name)).await
    }

    /// De stroom van `/v1/events`, die bij de eerste [`EventStream::next`] verbindt.
    pub fn stream(&self) -> EventStream<'_, D, N> {
        EventStream {
            agent: self,
            resp: None,
            events: Events::new(),
            backoff: Backoff::default(),
            tried: false,
        }
    }

    /// Volgt `/v1/events` tot `on_event` stopt, en verbindt opnieuw na elke breuk.
    ///
    /// `on_event` mag zelf de agent aanroepen (`agent.jobs().await`): de
    /// stroom leent de agent alleen. `on_lost` hoort elke breuk met de
    /// wachttijd tot de volgende poging. Na elke (her)verbinding komt eerst
    /// een `ping` ([`Event::is_resync`]).
    pub async fn events(
        &self,
        mut on_event: impl AsyncFnMut(&Event) -> ControlFlow<()>,
        mut on_lost: impl FnMut(&Error, Duration),
    ) {
        let mut s = self.stream();
        loop {
            match s.next().await {
                Ok(e) => {
                    if on_event(&e).await.is_break() {
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
pub struct EventStream<'a, D: Dialer, N> {
    agent: &'a Agent<D, N>,
    resp: Option<leanhttp::Response<D::Conn>>,
    events: Events,
    backoff: Backoff,
    /// Of er al een poging was: alleen dan wacht de volgende eerst.
    tried: bool,
}

impl<D: Dialer, N> core::fmt::Debug for EventStream<'_, D, N> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("EventStream")
            .field("connected", &self.resp.is_some())
            .field("backoff", &self.backoff)
            .finish_non_exhaustive()
    }
}

impl<D: Dialer, N: Nap> EventStream<'_, D, N> {
    /// Dezelfde stroom met een ander wachtbeleid.
    #[must_use]
    pub fn with_backoff(mut self, backoff: Backoff) -> Self {
        self.backoff = backoff;
        self
    }

    /// De volgende gebeurtenis.
    ///
    /// Een fout is een breuk (verbinden, status, stilte, einde); de stroom
    /// is dan dicht, en de volgende aanroep wacht de backoff en verbindt
    /// opnieuw. Opgeven of doorgaan is aan de aanroeper.
    pub async fn next(&mut self) -> Result<Event> {
        let mut buf = [0u8; READ_CHUNK];
        loop {
            match self.events.next() {
                Ok(Some(e)) => {
                    if e.kind == "ping" {
                        self.backoff.reset();
                    }
                    return Ok(e);
                }
                Ok(None) => {}
                Err(e) => return Err(self.drop_conn(e)),
            }
            let Some(resp) = self.resp.as_mut() else {
                self.connect().await?;
                continue;
            };
            let n = match resp.read(&mut buf).await {
                Ok(0) => return Err(self.drop_conn(Error::Closed)),
                Ok(n) => n,
                Err(e) => return Err(self.drop_conn(Error::Net(Reason::of(&e)))),
            };
            if let Err(e) = self.events.push(buf.get(..n).unwrap_or_default()) {
                return Err(self.drop_conn(e));
            }
        }
    }

    /// Sluit de verbinding (`Drop`) en geeft `e` door.
    fn drop_conn(&mut self, e: Error) -> Error {
        self.resp = None;
        self.events.reset();
        e
    }

    /// Opent de stroom, na de backoff als er al een poging was.
    async fn connect(&mut self) -> Result {
        if self.tried {
            self.agent.nap.nap(self.backoff.next_delay()).await;
        }
        self.tried = true;
        self.events.reset();
        let req = self.agent.client.events()?;
        let mut dial = self.agent.dial.clone();
        let mut resp = leanhttp::fetch(&mut dial, call(&req, header(&req)?))
            .await
            .map_err(|e| net(&dial, e))?;
        if resp.status != 200 {
            let body = resp.read_to_end(4 << 10).await.unwrap_or_default();
            return Err(crate::model::status_error(resp.status, &body));
        }
        self.resp = Some(resp);
        Ok(())
    }
}
