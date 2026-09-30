//! De verbindingen met de edge en wat erover binnenkomt.
//!
//! Bezit per verbindingsindex één taak die de verbinding aanhoudt (dial,
//! HTTP/2 als server, registreren, bedienen, en na een val opnieuw met een
//! gespreide terugval), de levende ingress-tabel, en het bedienen van elke
//! stream die de edge opent:
//!
//! - `cf-cloudflared-proxy-connection-upgrade: control-stream`: de
//!   Cap'n Proto-registratie ([`crate::register`]), daarna blijft de stream
//!   open en leest de tunnel door (een ongelezen stream vult vensters, en
//!   dan valt de verbinding stil);
//! - `update-configuration`: een config-push, die de hele tabel in één keer
//!   omwisselt of hem laat staan;
//! - `websocket` of een kale TCP-stroom (`cf-cloudflared-proxy-src`): een
//!   eerlijke 502, want die paden draagt de tunnel (nog) niet;
//! - de rest: een bezoeker, naar de dienst uit de tabel ([`crate::origin`]).
//!
//! Eén eigenaar per stuk staat (handboek §1): de verbindingstaak bezit zijn
//! verbinding; de tabel en de toestand per index zijn leesbare tabellen van
//! deze core (`LocalCell`), die niemand over een `.await` leent. De handlers
//! van leanh2 draaien in de taak van hun verbinding.
//!
//! De edge is de HTTP/2-client, wij zijn de server: wij bellen uit, en daarna
//! stuurt de edge óns de preface, pingt hij (zonder PING-antwoord opent hij
//! nooit een stream; dat antwoord geeft leanh2), en opent hij stream 1 met
//! de control-stream.

#![forbid(unsafe_code)]

use alloc::boxed::Box;
use alloc::string::String;
use alloc::vec::Vec;
use core::cell::{Cell, RefCell};
use core::fmt;
use core::fmt::Write as _;
use core::future::Future;
use core::pin::Pin;
use core::sync::atomic::{AtomicU32, AtomicU64, Ordering::Relaxed};
use core::time::Duration;

use applib::appnet::{self, NetError};
use applib::rt::Exec;
use applib::{App, clock, log};
use leanh2::{GoAway, Handler, Request, Response, ServeError};
use leanhttp::IoError;
use leantls::Roots;
use sync::{Either, Local, LocalCell, Signal, select};

use crate::capnp;
use crate::config::MAX_CONNECTIONS;
use crate::edge::{self, DialError};
use crate::edgeproto::{self, Bundle, Source};
use crate::entropy::{HARVEST_ROUNDS, Pool};
use crate::ingress::{self, Table, Update};
use crate::json;
use crate::origin;
use crate::register::{self, Answer, ClientInfo, Refusal, Token, Uuid};

/// De kop waarmee de edge zegt wat een stream is (cloudflared's
/// `connection/http2.go`).
const UPGRADE: &str = "cf-cloudflared-proxy-connection-upgrade";
/// De control-stream.
const UPGRADE_CONTROL: &str = "control-stream";
/// Een config-push.
const UPGRADE_CONFIG: &str = "update-configuration";
/// Een websocket.
const UPGRADE_WEBSOCKET: &str = "websocket";
/// Een kale TCP-stroom (WARP, `cloudflared access`).
const TCP_SRC: &str = "cf-cloudflared-proxy-src";

/// De versie die de tunnel meldt. `dev` is wat de Go-voorganger stuurde
/// (zijn build zette geen andere) en waarmee de edge registreerde (19-08);
/// een verzonnen versienummer zou de edge als een oude cloudflared kunnen
/// lezen.
const CLIENT_VERSION: &str = "dev";

/// De architectuur die de tunnel meldt; het dashboard toont hem.
#[cfg(target_arch = "aarch64")]
const CLIENT_ARCH: &str = "hopos_arm64";
/// De architectuur die de tunnel meldt; het dashboard toont hem.
#[cfg(target_arch = "riscv64")]
const CLIENT_ARCH: &str = "hopos_riscv64";
/// De architectuur die de tunnel meldt; het dashboard toont hem.
#[cfg(not(any(target_arch = "aarch64", target_arch = "riscv64")))]
const CLIENT_ARCH: &str = "hopos_host";

/// Hoe lang een verbinding mag bestaan zonder registratie: een edge die geen
/// control-stream opent of niet antwoordt, houdt de index anders vast.
const REGISTER_WINDOW: Duration = Duration::from_secs(30);

/// De eerste terugval na een val; hij verdubbelt tot [`BACKOFF_MAX`].
const BACKOFF_MIN: Duration = Duration::from_secs(1);
/// De langste terugval.
const BACKOFF_MAX: Duration = Duration::from_secs(30);

/// De grootste config-push die de tunnel leest (als in Go).
const CONFIG_MAX: usize = 1 << 20;

/// De buffer voor een registratie-antwoord; groter wordt overgeslagen.
const ANSWER_CAP: usize = 4096;

/// De buffer voor een segmenttabel van [`capnp::MAX_SEGMENTS`] segmenten.
const TABLE_CAP: usize = 4 + 4 * capnp::MAX_SEGMENTS as usize;

/// Zoveel boodschappen mag de edge sturen voor het antwoord op de
/// registratie (de bootstrap-return komt eerst).
const ANSWER_TRIES: usize = 8;

/// De eerste zoveel verzoeken krijgen elk een logregel, daarna één per
/// [`REQ_LOG_EVERY`].
const REQ_LOG_FIRST: u64 = 16;
/// Zie [`REQ_LOG_FIRST`].
const REQ_LOG_EVERY: u64 = 100;

/// Het langste pad in een logregel.
const LOG_PATH: usize = 96;

/// De levende ingress-tabel. Eén push schrijft hem in één statement; een
/// verzoek leest hem kort en neemt een kopie van zijn dienst mee.
pub(crate) static RULES: LocalCell<Table> = LocalCell::cell(Table::empty());

/// Verzoeken sinds de start, naar een dienst of niet.
static REQUESTS: AtomicU64 = AtomicU64::new(0);
/// Mislukte verzoeken sinds de start.
static FAILURES: AtomicU64 = AtomicU64::new(0);
/// Verbindingen die nu geregistreerd zijn.
static UP: AtomicU32 = AtomicU32::new(0);
/// Verbindingstaken die nog proberen.
static LIVE: AtomicU32 = AtomicU32::new(0);

/// Gaat af als de laatste verbindingstaak opgaf (de edge zei "niet
/// opnieuw"); `main` stopt de app dan.
pub(crate) static GAVE_UP: Signal = Signal::new();

/// Hoe de control-stream van een index eindigde, als dat niet goed was.
#[derive(Debug)]
enum ControlEnd {
    /// De edge weigerde de registratie.
    Refused(Refusal),
    /// De edge gaf een RPC-uitzondering.
    Exception(String),
    /// De stream of het bericht brak.
    Broken(ControlError),
}

/// De toestand van één verbindingsindex, gedeeld tussen de taak en de
/// handlers van zijn verbinding (dezelfde taak).
struct Slot {
    /// De verbinding van dit moment is geregistreerd.
    registered: Cell<bool>,
    /// Mislukte registraties op rij; de edge wil het weten.
    attempts: Cell<u8>,
    /// Hoe de control-stream eindigde.
    end: RefCell<Option<ControlEnd>>,
}

impl Slot {
    const fn new() -> Self {
        Self {
            registered: Cell::new(false),
            attempts: Cell::new(0),
            end: RefCell::new(None),
        }
    }
}

/// Eén slot per index.
static SLOTS: Local<[Slot; MAX_CONNECTIONS as usize]> =
    Local::new([const { Slot::new() }; MAX_CONNECTIONS as usize]);

/// De stopbel per index: de control-stream belt als registreren mislukt,
/// en de taak breekt dan de verbinding af.
static STOPS: [Signal; MAX_CONNECTIONS as usize] =
    [const { Signal::new() }; MAX_CONNECTIONS as usize];

/// Het slot van `index`.
fn slot(index: u8) -> &'static Slot {
    let slots = SLOTS.get();
    // INVARIANT: `index` komt uit `0..connections` en dat is hooguit
    // MAX_CONNECTIONS (config.rs); de terugval is de eerste.
    slots.get(usize::from(index)).unwrap_or(&slots[0])
}

/// De stopbel van `index`.
fn stop(index: u8) -> &'static Signal {
    STOPS.get(usize::from(index)).unwrap_or(&STOPS[0])
}

/// Wat alle verbindingen delen: gezet in `main` vóór de eerste spawn en
/// daarna alleen gelezen.
pub(crate) struct Shared {
    /// Het slot.
    pub(crate) app: &'static App,
    /// De executor van de app-core.
    pub(crate) exec: &'static Exec,
    /// Het token.
    pub(crate) token: Token,
    /// De edge-namen of -adressen.
    pub(crate) edges: Vec<String>,
    /// De CA's van de edge.
    pub(crate) roots: Roots<'static>,
}

/// Waarom een verbinding eindigde.
enum Why {
    /// De edge-naam werd geen adres.
    Resolve(NetError),
    /// De kern synct de wandklok nog niet; zonder tijd geen ketentoets.
    NoClock,
    /// TCP of TLS.
    Dial(DialError),
    /// HTTP/2 stopte.
    Serve(ServeError<IoError>),
    /// Geen registratie binnen [`REGISTER_WINDOW`].
    NotRegistered,
    /// De control-stream eindigde (zie het slot).
    Control(ControlEnd),
}

impl fmt::Display for Why {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Resolve(e) => write!(f, "resolving the edge: {e}"),
            Self::NoClock => f.write_str(
                "the wall clock is not synced yet, so the edge certificate cannot be checked",
            ),
            Self::Dial(e) => write!(f, "{e}"),
            Self::Serve(e) => write!(f, "{e}"),
            Self::NotRegistered => {
                write!(f, "not registered within {} s", REGISTER_WINDOW.as_secs())
            }
            Self::Control(ControlEnd::Refused(r)) => {
                write!(f, "edge refused this connection: {}", r.cause)
            }
            Self::Control(ControlEnd::Exception(e)) => write!(f, "edge raised an exception: {e}"),
            Self::Control(ControlEnd::Broken(e)) => write!(f, "control stream: {e}"),
        }
    }
}

/// Houdt verbindingsindex `index` bezet tot de edge zegt dat het geen zin
/// meer heeft. Eén taak per index; de indexen samen zijn de vaste pool.
pub(crate) async fn keep_connected(sh: &'static Shared, index: u8) {
    LIVE.fetch_add(1, Relaxed);
    let mut pool = pool_for(sh, index);
    let mut backoff = BACKOFF_MIN;
    let edges = sh.edges.len().max(1);
    for attempt in 0usize.. {
        // Elke index begint bij een andere naam en schuift door bij elke
        // poging: vier verbindingen op één edge-machine is geen HA.
        let name = sh
            .edges
            .get((usize::from(index) + attempt) % edges)
            .map_or("", String::as_str);
        let (was_up, why) = connect_once(sh, index, name, &mut pool).await;
        if was_up {
            backoff = BACKOFF_MIN;
        }
        if let Why::Control(ControlEnd::Refused(r)) = &why {
            if !r.should_retry {
                // Een configuratiefout (een ingetrokken token) lost een
                // herhaling niet op.
                log!(
                    "cloudflared-lean: connection {index} given up: {why} (the edge says do not retry) HOPOS_CFTUNNEL_GIVEUP index={index}"
                );
                if LIVE.fetch_sub(1, Relaxed) == 1 {
                    GAVE_UP.set();
                }
                return;
            }
            if r.retry_after > backoff {
                backoff = r.retry_after.min(BACKOFF_MAX * 10);
            }
        }
        let wait = leanrand::jitter(&mut pool, backoff);
        log!(
            "cloudflared-lean: connection {index} lost: {why}; retrying in {} ms HOPOS_CFTUNNEL_RETRY index={index} up={}",
            wait.as_millis(),
            UP.load(Relaxed)
        );
        sh.exec.after(wait).await;
        backoff = (backoff * 2).min(BACKOFF_MAX);
    }
}

/// De willekeur van één verbindingstaak: het slot, de index, de wandklok en
/// de jitter van de teller.
fn pool_for(sh: &Shared, index: u8) -> Pool {
    let mut seed = [0u8; 25];
    seed[..8].copy_from_slice(&sh.app.slot().to_le_bytes());
    seed[8..16].copy_from_slice(&sh.app.wall_ns().unwrap_or(0).to_le_bytes());
    seed[16..24].copy_from_slice(&sh.exec.now().to_le_bytes());
    seed[24] = index;
    let mut pool = Pool::new(&seed);
    pool.harvest(clock::now_ns, HARVEST_ROUNDS);
    pool
}

/// Eén verbinding van begin tot eind; geeft of hij geregistreerd was en
/// waarom hij eindigde.
async fn connect_once(sh: &'static Shared, index: u8, name: &str, pool: &mut Pool) -> (bool, Why) {
    let ip = match appnet::resolve(name).await {
        Ok(ip) => ip,
        Err(e) => return (false, Why::Resolve(e)),
    };
    let Some(wall) = sh.app.wall_ns() else {
        return (false, Why::NoClock);
    };
    pool.stir(&sh.exec.now().to_le_bytes());
    let io = match edge::dial(sh.exec, ip, sh.roots, wall / 1_000_000_000, pool).await {
        Ok(io) => io,
        Err(e) => return (false, Why::Dial(e)),
    };
    // De tijd van de handshake is een gebeurtenis van buiten.
    pool.stir(&sh.exec.now().to_le_bytes());

    let s = slot(index);
    s.registered.set(false);
    s.end.replace(None);
    stop(index).take();
    let edge_addr = Addr(ip);
    let mut h2 = leanh2::Conn::new(
        io,
        Edge {
            sh,
            index,
            addr: edge_addr,
        },
    );
    let watchdog = async {
        sh.exec.after(REGISTER_WINDOW).await;
        if s.registered.get() {
            core::future::pending::<()>().await;
        }
    };
    let serve = h2.serve(core::future::pending::<GoAway>());
    let why = match select(stop(index).wait(), select(watchdog, serve)).await {
        Either::Left(()) => s.end.take().map_or(Why::NotRegistered, Why::Control),
        Either::Right(Either::Left(())) => Why::NotRegistered,
        Either::Right(Either::Right(r)) => match (r, s.end.take()) {
            (_, Some(end)) => Why::Control(end),
            (Err(e), None) => Why::Serve(e),
            (Ok(()), None) => Why::Serve(ServeError::Transport(IoError::Closed)),
        },
    };
    // De verbinding gaat hier dicht: `h2` bezit het transport.
    drop(h2);
    let was_up = s.registered.replace(false);
    if was_up {
        UP.fetch_sub(1, Relaxed);
    }
    (was_up, why)
}

/// Een IPv4-adres met de edge-poort, voor een logregel.
#[derive(Clone, Copy)]
struct Addr([u8; 4]);

impl fmt::Display for Addr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let [a, b, c, d] = self.0;
        write!(f, "{a}.{b}.{c}.{d}:{}", edge::PORT)
    }
}

/// De handler van één verbinding: elke stream van de edge.
struct Edge {
    /// Wat alle verbindingen delen.
    sh: &'static Shared,
    /// De index van deze verbinding.
    index: u8,
    /// Het adres van de edge, voor de logregels.
    addr: Addr,
}

/// De future van één stream. leanh2 bewaart hem in een vaste tabel en eist
/// dus `Unpin`: een `Box` per stream, zoals de goroutine per stream in Go.
type StreamFuture<'s> = Pin<Box<dyn Future<Output = leanh2::Result> + 's>>;

impl Handler for Edge {
    type Future<'s>
        = StreamFuture<'s>
    where
        Self: 's;

    fn call<'s>(&mut self, req: Request<'s>, res: Response<'s>) -> StreamFuture<'s> {
        Box::pin(serve_stream(self.sh, self.index, self.addr, req, res))
    }
}

/// Of de upgrade-kop van `req` gelijk is aan `want`.
fn upgrade_is(req: &Request<'_>, want: &str) -> bool {
    req.get(UPGRADE)
        .is_some_and(|v| v.eq_ignore_ascii_case(want))
}

/// Bedient één stream.
async fn serve_stream(
    sh: &'static Shared,
    index: u8,
    addr: Addr,
    mut req: Request<'_>,
    mut res: Response<'_>,
) -> leanh2::Result {
    if upgrade_is(&req, UPGRADE_CONTROL) {
        control(sh, index, addr, &mut req, &mut res).await;
        return Ok(());
    }
    if upgrade_is(&req, UPGRADE_CONFIG) {
        return config_update(&mut req, &mut res).await;
    }
    if req.get(TCP_SRC).is_some() {
        // Bewust niet ondersteund in plaats van half: een 502 met een reden
        // is duidelijker dan een verbinding die stil niets doet.
        return reply(&mut res, 502, "this tunnel serves HTTP only").await;
    }
    if upgrade_is(&req, UPGRADE_WEBSOCKET) {
        // Het pad bestaat (een bidirectionele stream), maar zonder toets
        // tegen een echte websocket-oorsprong belooft de tunnel het niet.
        return reply(&mut res, 502, "this tunnel does not carry websockets yet").await;
    }
    proxy(sh, &mut req, &mut res).await
}

/// Waarom de control-stream brak.
#[derive(Debug)]
enum ControlError {
    /// De stream eindigde.
    Eof,
    /// leanh2 gaf een fout.
    Stream(leanh2::Error),
    /// Een bericht was kapot.
    Capnp(capnp::Error),
    /// Geen antwoord op de registratie na [`ANSWER_TRIES`] boodschappen.
    NoAnswer,
}

impl fmt::Display for ControlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Eof => f.write_str("the edge closed it"),
            Self::Stream(e) => write!(f, "{e}"),
            Self::Capnp(e) => write!(f, "{e}"),
            Self::NoAnswer => write!(
                f,
                "no answer to the registration in {ANSWER_TRIES} messages"
            ),
        }
    }
}

impl From<leanh2::Error> for ControlError {
    fn from(e: leanh2::Error) -> Self {
        Self::Stream(e)
    }
}

impl From<capnp::Error> for ControlError {
    fn from(e: capnp::Error) -> Self {
        Self::Capnp(e)
    }
}

/// De control-stream: 200 terug, de registratie, en daarna doorlezen zolang
/// de verbinding leeft. Een mislukte registratie belt de stopbel.
async fn control(
    sh: &'static Shared,
    index: u8,
    addr: Addr,
    req: &mut Request<'_>,
    res: &mut Response<'_>,
) {
    let s = slot(index);
    let end = match register(sh, index, addr, req, res).await {
        Ok(()) => drain(req).await.err().map(ControlEnd::Broken),
        Err(end) => Some(end),
    };
    if !s.registered.get() {
        s.attempts.set(s.attempts.get().saturating_add(1));
    }
    if let Some(end) = end {
        s.end.replace(Some(end));
    }
    stop(index).set();
}

/// De registratie zelf, tot en met de marker.
async fn register(
    sh: &'static Shared,
    index: u8,
    addr: Addr,
    req: &mut Request<'_>,
    res: &mut Response<'_>,
) -> Result<(), ControlEnd> {
    let broken = |e: ControlError| ControlEnd::Broken(e);
    res.write_header(200, &[]).map_err(|e| broken(e.into()))?;
    let s = slot(index);
    let info = ClientInfo {
        client_id: &sh.token.tunnel_id,
        features: &[],
        version: CLIENT_VERSION,
        arch: CLIENT_ARCH,
    };
    let mut buf = [0u8; register::MESSAGE_CAP];
    let msg = register::bootstrap(&mut buf).map_err(|e| broken(e.into()))?;
    res.write(msg).await.map_err(|e| broken(e.into()))?;
    let msg = register::register_call(&mut buf, &sh.token, index, &info, s.attempts.get())
        .map_err(|e| broken(e.into()))?;
    res.write(msg).await.map_err(|e| broken(e.into()))?;

    let mut table = [0u8; TABLE_CAP];
    let mut answer = [0u8; ANSWER_CAP];
    for _ in 0..ANSWER_TRIES {
        let Some(seg) = next_message(req, &mut table, &mut answer)
            .await
            .map_err(broken)?
        else {
            continue;
        };
        match register::read_answer(seg).map_err(|e| broken(e.into()))? {
            Answer::Other => {}
            Answer::Registered(d) => {
                s.registered.set(true);
                s.attempts.set(0);
                let up = UP.fetch_add(1, Relaxed) + 1;
                log!(
                    "cloudflared-lean: connection {index} registered at {} (remotely managed: {}) HOPOS_CFTUNNEL_UP edge={addr} conn={} colo={} index={index} up={up}",
                    d.location.as_str(),
                    d.remotely_managed,
                    Uuid(&d.uuid),
                    d.location.as_str(),
                );
                return Ok(());
            }
            Answer::Refused(r) => return Err(ControlEnd::Refused(r)),
            Answer::Exception(e) => return Err(ControlEnd::Exception(e)),
        }
    }
    Err(broken(ControlError::NoAnswer))
}

/// Leest de control-stream door tot hij eindigt: latere RPC (een nette
/// afmelding) heeft geen antwoord van ons nodig, maar een ongelezen stream
/// houdt krediet vast.
async fn drain(req: &mut Request<'_>) -> Result<(), ControlError> {
    let mut table = [0u8; TABLE_CAP];
    loop {
        next_message(req, &mut table, &mut []).await?;
    }
}

/// Vult `buf` helemaal uit de body.
async fn read_exact(req: &mut Request<'_>, buf: &mut [u8]) -> Result<(), ControlError> {
    let mut got = 0;
    while got < buf.len() {
        let n = req.body.read(buf.get_mut(got..).unwrap_or(&mut [])).await?;
        if n == 0 {
            return Err(ControlError::Eof);
        }
        got += n;
    }
    Ok(())
}

/// Leest één Cap'n Proto-boodschap. Past hij als één segment in `out`, dan
/// geeft hij dat segment; anders slaat hij hem over en geeft `None`.
async fn next_message<'b>(
    req: &mut Request<'_>,
    table: &mut [u8; TABLE_CAP],
    out: &'b mut [u8],
) -> Result<Option<&'b [u8]>, ControlError> {
    let mut first = [0u8; 4];
    read_exact(req, &mut first).await?;
    let n = capnp::segment_count(first)?;
    let rest = table
        .get_mut(..capnp::table_rest(n))
        .ok_or(capnp::Error::OutOfBounds)?;
    read_exact(req, rest).await?;
    let len = capnp::body_len(rest, n)?;
    if n == 1 && len <= out.len() {
        let seg = out.get_mut(..len).ok_or(capnp::Error::OutOfBounds)?;
        read_exact(req, seg).await?;
        return Ok(Some(seg));
    }
    // Overslaan, door de tabel als kladblok.
    let mut left = len;
    while left > 0 {
        let k = left.min(table.len());
        read_exact(req, table.get_mut(..k).unwrap_or(&mut [])).await?;
        left -= k;
    }
    Ok(None)
}

/// Neemt een config-push aan en antwoordt met de versie die nu staat.
async fn config_update(req: &mut Request<'_>, res: &mut Response<'_>) -> leanh2::Result {
    let body = read_all(req, CONFIG_MAX).await;
    let current = RULES.borrow().version();
    let outcome = body.and_then(|b| {
        let (version, config) = ingress::parse_push(&b)?;
        Ok((version, ingress::update(current, version, config)?))
    });
    let (applied, err) = match outcome {
        Ok((_, Update::Stale)) => (current, None),
        Ok((version, Update::New(table))) => {
            let n = table.len();
            // De hele tabel in één keer; de oude gaat na de lening weg.
            let old = RULES.replace(table);
            drop(old);
            log!(
                "cloudflared-lean: configuration {version} applied, {n} rules HOPOS_CFTUNNEL_CONFIG version={version} rules={n}"
            );
            RULES.borrow().describe(|l| log!("cloudflared-lean:   {l}"));
            (version, None)
        }
        Err(e) => {
            log!(
                "cloudflared-lean: configuration refused: {e} HOPOS_CFTUNNEL_CONFIG_FAIL version={current}"
            );
            (current, Some(e))
        }
    };
    let mut out = String::new();
    let _ = write!(out, "{{\"lastAppliedVersion\":{applied}");
    if let Some(e) = err {
        let mut msg = String::new();
        let _ = write!(msg, "{e}");
        let _ = out.write_str(",\"err\":");
        let _ = json::write_string(&mut out, &msg);
    }
    out.push('}');
    res.write_header(200, &[("content-type", "application/json")])?;
    res.write(out.as_bytes()).await?;
    Ok(())
}

/// De body van een verzoek, tot `limit` bytes.
async fn read_all(req: &mut Request<'_>, limit: usize) -> Result<Vec<u8>, ingress::Error> {
    let mut body = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let n = req
            .body
            .read(&mut chunk)
            .await
            .map_err(|_| ingress::Error::Body)?;
        if n == 0 {
            return Ok(body);
        }
        if body.len() + n > limit {
            return Err(ingress::Error::TooLarge { limit });
        }
        body.try_reserve(n)
            .map_err(|_| ingress::Error::OutOfMemory)?;
        body.extend_from_slice(chunk.get(..n).unwrap_or(&[]));
    }
}

/// Een antwoord dat de tunnel zelf maakt, niet de dienst: een 404 omdat
/// geen regel past, een 502 omdat de dienst niet opnam. De meta-kop zegt
/// dat eerlijk (`cloudflared`), want de edge onderscheidt het.
async fn reply(res: &mut Response<'_>, code: u16, msg: &str) -> leanh2::Result {
    let mut b = Bundle::new();
    b.add("content-type", "text/plain; charset=utf-8");
    let bundle = b.finish().unwrap_or_default();
    res.write_header(
        code,
        &[
            (edgeproto::HEADER_USER, &bundle),
            (edgeproto::HEADER_META, Source::Tunnel.meta()),
        ],
    )?;
    if !msg.is_empty() {
        // Een HEAD of een 204 heeft geen body; dat is geen fout van de stream.
        if res.write(msg.as_bytes()).await.is_ok() {
            let _ = res.write(b"\n").await;
        }
    }
    Ok(())
}

/// Een bezoeker: de dienst uit de tabel, of een antwoord van de tunnel.
async fn proxy(
    sh: &'static Shared,
    req: &mut Request<'_>,
    res: &mut Response<'_>,
) -> leanh2::Result {
    let n = REQUESTS.fetch_add(1, Relaxed).wrapping_add(1);
    let host = origin::request_host(&req.authority, req.get("host"));
    // Een kopie van de dienst: geen lening op de tabel over de `.await`s.
    let service = RULES.borrow().route(host, &req.path);
    let Some(service) = service else {
        log_request(n, req, "-", 404);
        return reply(res, 404, "no ingress rule matches this hostname and path").await;
    };
    // De vaste "diensten" van Cloudflare: een status zonder oorsprong.
    if let Some(code) = service.as_str().strip_prefix("http_status:") {
        let code = code
            .parse::<u16>()
            .ok()
            .filter(|c| (200..=599).contains(c))
            .unwrap_or(404);
        log_request(n, req, service.as_str(), code);
        return reply(res, code, "").await;
    }
    match origin::proxy(sh.exec, service.as_str(), req, res).await {
        Ok(status) => {
            log_request(n, req, service.as_str(), status);
            Ok(())
        }
        Err(e) => {
            let f = FAILURES.fetch_add(1, Relaxed).wrapping_add(1);
            if f <= REQ_LOG_FIRST || f.is_multiple_of(REQ_LOG_EVERY) {
                log!(
                    "cloudflared-lean: stream {} to {}: {e} HOPOS_CFTUNNEL_REQ_FAIL n={f}",
                    req.stream_id,
                    service.as_str()
                );
            }
            match e {
                // De koppen zijn al weg: de stream resetten is het enige
                // eerlijke einde.
                origin::Error::Edge(e) => Err(e),
                origin::Error::BodyTooLarge => {
                    reply(
                        res,
                        413,
                        "this tunnel buffers request bodies; this one is too large",
                    )
                    .await
                }
                _ => reply(res, 502, "the local service did not answer").await,
            }
        }
    }
}

/// Eén regel per verzoek voor de eerste [`REQ_LOG_FIRST`], daarna één per
/// [`REQ_LOG_EVERY`] met de teller: logs over de outbox zijn goedkoop, maar
/// een pagina die elke seconde herladen wordt, hoort de console niet te
/// vullen. Het pad zonder query (daar staan soms sleutels in), afgekapt.
fn log_request(n: u64, req: &Request<'_>, service: &str, status: u16) {
    if n > REQ_LOG_FIRST && !n.is_multiple_of(REQ_LOG_EVERY) {
        return;
    }
    let path = req.path.split('?').next().unwrap_or("");
    let mut end = path.len().min(LOG_PATH);
    while !path.is_char_boundary(end) {
        end -= 1;
    }
    log!(
        "cloudflared-lean: stream {} {} {}{} -> {service} {status} HOPOS_CFTUNNEL_REQ n={n}",
        req.stream_id,
        req.method,
        origin::request_host(&req.authority, req.get("host")),
        path.get(..end).unwrap_or(""),
    );
}
