//! De taken van de bewoner: start, acceptors, werkers, metrics, watcher.

use alloc::boxed::Box;
use alloc::string::String;
use alloc::vec::Vec;
use core::cell::Cell;
use core::ops::ControlFlow;
use core::sync::atomic::{AtomicU64, Ordering::Relaxed};
use core::time::Duration;

use applib::appnet::{self, TcpListener, TcpStream};
use applib::rt::Exec;
use applib::tcp::TcpConn;
use applib::{App, EXEC, clock, log};
use hoplb::proxy::{self, Env};
use hoplb::watch::{Pending, RETRY, Sync};
use hoplb::{Error, Metrics, Pick, Record, RouteTable, Watcher};
use hoplib::hopos::Agent;
use hoplib::{Client, Event};
use leanhttp::IoError;
use sync::mpsc::Mailbox;
use sync::spsc::{Channel, Receiver, Sender};
use sync::{Either, Local, LocalCell, select};

use crate::config;

/// Verkeerswerkers: zoveel clientverbindingen tegelijk. Elke werker houdt
/// twee leesbuffers en een hap vast (samen 80 KiB); acht is één huishouden
/// met browsers, en de rest wacht kort op de eerste die vrijkomt.
pub(crate) const WORKERS: usize = 8;

/// Admin-werkers: Prometheus en een health-check.
pub(crate) const ADMIN_WORKERS: usize = 2;

/// Metingen in het venster per reeks. De host houdt er 10.000; hier is de
/// heap een deel van de partitie, en 1.000 geeft dezelfde vier kwantielen.
const MAX_SAMPLES: usize = 1_000;

/// Reeksen (domein, backend) op zijn hoogst.
const MAX_SERIES: usize = 128;

/// De stilte op een admin-verbinding (welcome: `READ_CAP`).
const ADMIN_READ_CAP: Duration = Duration::from_secs(5);

/// Hoe lang het bellen van een backend mag duren.
const DIAL_TIMEOUT: Duration = Duration::from_secs(10);

/// Hoe vaak een acceptor kijkt of er een werker vrij is als ze allemaal
/// bezig zijn (welcome: `BUSY_POLL`).
const BUSY_POLL: Duration = Duration::from_millis(5);

/// Hoe lang een admin-werker op de metrics-taak wacht.
const SCRAPE_TIMEOUT: Duration = Duration::from_secs(5);

/// Hoe lang de watcher slaapt als er niets wacht.
const IDLE_WAIT: Duration = Duration::from_secs(3600);

/// Om de zoveel verzoeken één logregel.
const LOG_EVERY: u64 = 100;

/// Wat de metrics-taak krijgt.
enum MetricsMsg {
    /// Eén afgehandeld verzoek.
    Record(Record),
    /// Admin-werker `i` wil de Prometheus-tekst.
    Scrape(usize),
}

/// De brievenbus van de metrics-taak: elke werker schrijft, alleen zij leest.
static METRICS: Mailbox<MetricsMsg, 256> = Mailbox::new();

/// De antwoorden van de metrics-taak, één rij per admin-werker.
static SCRAPES: Local<[Channel<String, 1>; ADMIN_WORKERS]> =
    Local::new([const { Channel::new() }; ADMIN_WORKERS]);

/// De geldende routetabel. De watcher vervangt hem in één keer; een werker
/// leent hem voor één `pick` en nooit over een `.await` (handboek §1.1).
static ROUTES: LocalCell<RouteTable> = LocalCell::cell(RouteTable::new());

/// De gebeurtenissen van de stroom naar de watcher.
static EVENTS: Local<Channel<Event, 16>> = Local::new(Channel::new());

/// De rijen naar de werkers: één verbinding tegelijk.
static TRAFFIC_Q: Local<[Channel<TcpStream, 1>; WORKERS]> =
    Local::new([const { Channel::new() }; WORKERS]);
static TRAFFIC_BUSY: Local<[Cell<bool>; WORKERS]> =
    Local::new([const { Cell::new(false) }; WORKERS]);
static ADMIN_Q: Local<[Channel<TcpStream, 1>; ADMIN_WORKERS]> =
    Local::new([const { Channel::new() }; ADMIN_WORKERS]);
static ADMIN_BUSY: Local<[Cell<bool>; ADMIN_WORKERS]> =
    Local::new([const { Cell::new(false) }; ADMIN_WORKERS]);

/// Verzoeken sinds de start.
static REQUESTS: AtomicU64 = AtomicU64::new(0);

/// Metingen die wegvielen omdat de brievenbus vol was.
static DROPPED: AtomicU64 = AtomicU64::new(0);

/// De start van de bewoner.
#[expect(
    clippy::expect_used,
    reason = "de start van de bin: zonder netstack, listener of taken is er niets te balanceren, en een luide paniek met reden is het goede einde"
)]
pub(crate) async fn hoplb(app: &'static App) {
    let exec: &'static Exec = EXEC.get();
    let http = port_env(app, "ER_PORT_HTTP", config::DEFAULT_HTTP);
    let admin = port_env(app, "ER_PORT_ADMIN", config::DEFAULT_ADMIN);
    let agent = config::agent_url(
        app.env("HOPLB_AGENT"),
        applib::net::slot_ip(config::HOP_SLOT),
    );
    let tag = app.env("HOPLB_TAG").unwrap_or("");
    let key = app.env("HOPLB_API_KEY").filter(|k| !k.is_empty());
    let verbose = app.env("HOPLB_VERBOSE") == Some("1");

    let net = appnet::up(app).expect("hoplb: network stack");
    let client = Client::new(&agent, key).expect("hoplb: HOPLB_AGENT is not a URL");
    let watcher = Watcher::new(tag).expect("hoplb: tag filter");
    let traffic = TcpListener::bind(http).expect("hoplb: listen on ER_PORT_HTTP");
    let admin_l = TcpListener::bind(admin).expect("hoplb: listen on ER_PORT_ADMIN");

    // De metrics-taak bezit de zenders van de antwoordrijen, elke
    // admin-werker zijn ontvanger.
    let mut replies: Vec<Sender<'static, String, 1>> = Vec::new();
    replies
        .try_reserve_exact(ADMIN_WORKERS)
        .expect("hoplb: reply table");
    let mut admin_tx: Vec<Sender<'static, TcpStream, 1>> = Vec::new();
    admin_tx
        .try_reserve_exact(ADMIN_WORKERS)
        .expect("hoplb: admin table");
    for (i, (q, r)) in ADMIN_Q.get().iter().zip(SCRAPES.get().iter()).enumerate() {
        let (tx, rx) = q.split().expect("hoplb: admin queue split once");
        let (rtx, rrx) = r.split().expect("hoplb: reply queue split once");
        admin_tx.push(tx);
        replies.push(rtx);
        exec.spawn(admin_worker(i, rx, rrx, exec))
            .expect("hoplb: spawn admin worker");
    }
    exec.spawn(metrics(replies)).expect("hoplb: spawn metrics");

    let mut traffic_tx: Vec<Sender<'static, TcpStream, 1>> = Vec::new();
    traffic_tx
        .try_reserve_exact(WORKERS)
        .expect("hoplb: worker table");
    for (i, q) in TRAFFIC_Q.get().iter().enumerate() {
        let (tx, rx) = q.split().expect("hoplb: queue split once");
        traffic_tx.push(tx);
        exec.spawn(traffic_worker(i, rx, exec, verbose))
            .expect("hoplb: spawn worker");
    }

    let (ev_tx, ev_rx) = EVENTS.get().split().expect("hoplb: event queue split once");
    let agent_ref: &'static Agent = Box::leak(Box::new(Agent::new(client.clone(), exec)));
    exec.spawn(stream(Agent::new(client, exec), ev_tx))
        .expect("hoplb: spawn stream");
    exec.spawn(watch(agent_ref, watcher, ev_rx, exec))
        .expect("hoplb: spawn watcher");
    exec.spawn(accept(admin_l, admin_tx, ADMIN_BUSY.get(), exec))
        .expect("hoplb: spawn admin acceptor");

    let [a, b, c, d] = net.ip();
    log!(
        "hoplb {}: proxy on {a}.{b}.{c}.{d}:{http}, admin on :{admin}, agent {agent}, tag {tag:?}, {WORKERS} workers HOPOS_HOPLB_UP port={http} admin={admin}",
        hoplb::VERSION
    );
    accept(traffic, traffic_tx, TRAFFIC_BUSY.get(), exec).await;
}

/// Een poort uit de env, luid als hij onzin is.
fn port_env(app: &App, key: &str, default: u16) -> u16 {
    match config::port(app.env(key), default) {
        Ok(p) => p,
        Err(p) => {
            log!(
                "hoplb: {key}={:?} is not a port, using {p} HOPOS_HOPLB_PORT",
                app.env(key)
            );
            p
        }
    }
}

/// Een acceptor: elke verbinding naar de eerste vrije werker.
async fn accept<const N: usize>(
    listener: TcpListener,
    mut senders: Vec<Sender<'static, TcpStream, 1>>,
    busy: &'static [Cell<bool>; N],
    exec: &'static Exec,
) {
    loop {
        let mut stream = match listener.accept().await {
            Ok(s) => s,
            Err(e) => {
                log!("hoplb: accept: {e} HOPOS_HOPLB_ACCEPT");
                exec.after(Duration::from_millis(100)).await;
                continue;
            }
        };
        loop {
            match hand_off(stream, &mut senders, busy) {
                None => break,
                Some(back) => {
                    stream = back;
                    exec.after(BUSY_POLL).await;
                }
            }
        }
    }
}

/// Geeft `stream` aan een vrije werker; alle werkers bezig is `Some` terug.
fn hand_off(
    stream: TcpStream,
    senders: &mut [Sender<'static, TcpStream, 1>],
    busy: &[Cell<bool>],
) -> Option<TcpStream> {
    let free = senders
        .iter_mut()
        .zip(busy.iter())
        .find(|(tx, b)| !b.get() && tx.free() > 0);
    match free {
        Some((tx, b)) => match tx.try_send(stream) {
            Ok(()) => {
                b.set(true);
                None
            }
            Err(sync::Full(back)) => Some(back),
        },
        None => Some(stream),
    }
}

/// De schil van de proxy in dit slot.
struct SlotEnv {
    exec: &'static Exec,
    /// `HOPLB_VERBOSE=1`: één logregel per verzoek (Go logde er altijd één;
    /// hier alleen op verzoek, want de console van de kern is gedeeld).
    verbose: bool,
    /// Welke verkeerswerker een verbinding heeft; allemaal is vol
    /// (`Env::crowded`).
    busy: &'static [Cell<bool>; WORKERS],
}

impl Env for SlotEnv {
    type Conn = TcpConn;

    fn pick(&mut self, host: &str) -> Pick {
        // Eén lening, binnen dit statement.
        ROUTES.borrow().pick(host)
    }

    fn crowded(&self) -> bool {
        self.busy.iter().all(Cell::get)
    }

    async fn dial(&mut self, addr: &str) -> hoplb::Result<TcpConn> {
        let (host, port) = split_addr(addr).ok_or(Error::BadAddress)?;
        let ip = match appnet::parse_ip4(host) {
            Some(ip) => ip,
            None => appnet::resolve(host)
                .await
                .map_err(|_| Error::Dial(IoError::Other))?,
        };
        let s = TcpStream::connect_timeout(ip, port, DIAL_TIMEOUT)
            .await
            .map_err(|e| Error::Dial(applib::tcp::io_error(e)))?;
        Ok(TcpConn::new(s, self.exec))
    }

    fn now_ns(&self) -> u64 {
        clock::now_ns()
    }

    fn record(&mut self, r: Record) {
        let n = REQUESTS.fetch_add(1, Relaxed).wrapping_add(1);
        if self.verbose {
            log!(
                "hoplb: {} -> {} {} {}us HOPOS_HOPLB_REQUEST",
                r.domain,
                if r.backend.is_empty() {
                    "-"
                } else {
                    &r.backend
                },
                r.code,
                r.nanos / 1_000
            );
        } else if n.is_multiple_of(LOG_EVERY) {
            log!(
                "hoplb: requests={n} last={} -> {} {} HOPOS_HOPLB_REQUESTS",
                r.domain,
                r.backend,
                r.code
            );
        }
        if METRICS.try_send(MetricsMsg::Record(r)).is_err() && DROPPED.fetch_add(1, Relaxed) == 0 {
            log!("hoplb: metrics mailbox full, dropping records HOPOS_HOPLB_METRICS_DROP");
        }
    }

    fn backend_error(&mut self, host: &str, addr: &str, err: Error) {
        log!("hoplb: proxy error for {host} -> {addr}: {err} HOPOS_HOPLB_BACKEND");
    }
}

/// `host:poort` uit elkaar.
fn split_addr(addr: &str) -> Option<(&str, u16)> {
    let (h, p) = addr.rsplit_once(':')?;
    Some((
        h.trim_start_matches('[').trim_end_matches(']'),
        p.parse().ok()?,
    ))
}

/// Eén verkeerswerker: wacht op een verbinding, draait de proxy tot hij
/// sluit, en meldt zich weer vrij.
async fn traffic_worker(
    i: usize,
    mut rx: Receiver<'static, TcpStream, 1>,
    exec: &'static Exec,
    verbose: bool,
) {
    let mut env = SlotEnv {
        exec,
        verbose,
        busy: TRAFFIC_BUSY.get(),
    };
    loop {
        let stream = rx.recv().await;
        let mut peer = [0u8; 15];
        let ip = match stream.remote() {
            Ok(ep) => fmt_ip(ep.ip, &mut peer),
            Err(_) => "",
        };
        let ip: String = String::from(ip);
        let conn = TcpConn::new(stream, exec);
        // Een verbinding die eindigt met een termijn of een reset is een
        // browser die wegging; geen logregel waard.
        let _ = proxy::serve(conn, &ip, &mut env).await;
        if let Some(b) = TRAFFIC_BUSY.get().get(i) {
            b.set(false);
        }
    }
}

/// Een IPv4-adres als tekst, in `buf`.
fn fmt_ip(ip: [u8; 4], buf: &mut [u8; 15]) -> &str {
    let mut n = 0;
    for (k, byte) in ip.iter().enumerate() {
        if k > 0
            && let Some(dot) = buf.get_mut(n)
        {
            *dot = b'.';
            n += 1;
        }
        let digits = [byte / 100, byte / 10 % 10, byte % 10];
        let skip = if *byte >= 100 {
            0
        } else if *byte >= 10 {
            1
        } else {
            2
        };
        for d in digits.iter().skip(skip) {
            if let Some(c) = buf.get_mut(n) {
                *c = b'0' + d;
                n += 1;
            }
        }
    }
    core::str::from_utf8(buf.get(..n).unwrap_or_default()).unwrap_or("")
}

/// Eén admin-werker.
async fn admin_worker(
    i: usize,
    mut rx: Receiver<'static, TcpStream, 1>,
    mut replies: Receiver<'static, String, 1>,
    exec: &'static Exec,
) {
    loop {
        let stream = rx.recv().await;
        let conn = TcpConn::new(stream, exec).with_read_cap(ADMIN_READ_CAP);
        let _ = hoplb::admin::serve(conn, async || scrape(i, &mut replies, exec).await).await;
        if let Some(b) = ADMIN_BUSY.get().get(i) {
            b.set(false);
        }
    }
}

/// Vraagt de metrics-taak om de tekst en wacht op het antwoord.
async fn scrape(
    i: usize,
    replies: &mut Receiver<'static, String, 1>,
    exec: &'static Exec,
) -> Option<String> {
    // Een oud antwoord (een vraag die eerder te laat was) eerst weg.
    while replies.try_recv().is_some() {}
    METRICS.try_send(MetricsMsg::Scrape(i)).ok()?;
    match select(replies.recv(), exec.after(SCRAPE_TIMEOUT)).await {
        Either::Left(text) => Some(text),
        Either::Right(()) => None,
    }
}

/// De metrics-taak: bezit de [`Metrics`], leest de brievenbus.
async fn metrics(mut replies: Vec<Sender<'static, String, 1>>) {
    let mut m = Metrics::with_limits(MAX_SAMPLES, MAX_SERIES);
    let mut folded = 0;
    loop {
        match METRICS.recv().await {
            MetricsMsg::Record(r) => {
                if let Err(e) = m.record(&r) {
                    log!("hoplb: metrics: {e} HOPOS_HOPLB_METRICS");
                }
                if m.folded() > folded {
                    if folded == 0 {
                        log!(
                            "hoplb: metrics over {MAX_SERIES} series, folding HOPOS_HOPLB_METRICS_FOLD"
                        );
                    }
                    folded = m.folded();
                }
            }
            MetricsMsg::Scrape(i) => {
                let mut out = String::new();
                if m.render(&mut out).is_ok()
                    && let Some(tx) = replies.get_mut(i)
                {
                    // Vol betekent: de vorige vraag is nog niet opgehaald.
                    let _ = tx.try_send(out);
                }
            }
        }
    }
}

/// De stroom: elke gebeurtenis van `/v1/events` naar de watcher.
async fn stream(agent: Agent, mut to: Sender<'static, Event, 16>) {
    agent
        .events(
            async |e: &Event| {
                if e.kind == "ping" {
                    log!("hoplb: SSE connected, seeding routes");
                }
                to.send(e.clone()).await;
                ControlFlow::Continue(())
            },
            |err, retry_in| {
                log!(
                    "hoplb: SSE disconnected: {err}, reconnecting in {retry_in:?} HOPOS_HOPLB_SSE"
                );
            },
        )
        .await;
}

/// De watcher: wacht op een gebeurtenis of het einde van de wacht, vraagt
/// de agent, en vervangt de routetabel.
async fn watch(
    agent: &'static Agent,
    mut w: Watcher,
    mut from: Receiver<'static, Event, 16>,
    exec: &'static Exec,
) {
    let mut pending = Pending::new();
    loop {
        let due = pending.due().unwrap_or_else(|| {
            clock::now_ns().saturating_add(u64::try_from(IDLE_WAIT.as_nanos()).unwrap_or(u64::MAX))
        });
        if let Either::Left(e) = select(from.recv(), exec.until(due)).await {
            if e.is_resync() {
                pending.clear();
                if !full(agent, &mut w).await {
                    pending.retry(clock::now_ns(), RETRY);
                }
            } else if let Some((job, is_full)) = w.classify(&e.data)
                && let Err(err) = pending.push(&job, is_full, clock::now_ns())
            {
                log!("hoplb: pending: {err}");
            }
        }
        match pending.take(clock::now_ns()) {
            None => {}
            Some(Sync::Full) => {
                if !full(agent, &mut w).await {
                    pending.retry(clock::now_ns(), RETRY);
                }
            }
            Some(Sync::Jobs(jobs)) => {
                if !jobs_sync(agent, &mut w, &jobs).await && !full(agent, &mut w).await {
                    pending.retry(clock::now_ns(), RETRY);
                    continue;
                }
                publish(&w);
            }
        }
    }
}

/// Alles opnieuw (Go: `sync`); `false` als het faalde.
async fn full(agent: &Agent, w: &mut Watcher) -> bool {
    let agents = match agent.agents().await {
        Ok(a) => a,
        Err(e) => {
            log!("hoplb: failed to fetch agents: {e} HOPOS_HOPLB_SYNC");
            return false;
        }
    };
    let jobs = match agent.jobs().await {
        Ok(j) => j,
        Err(e) => {
            log!("hoplb: failed to fetch jobs: {e} HOPOS_HOPLB_SYNC");
            return false;
        }
    };
    let tasks = match agent.tasks().await {
        Ok(t) => t,
        Err(e) => {
            log!("hoplb: failed to fetch tasks: {e} HOPOS_HOPLB_SYNC");
            return false;
        }
    };
    if let Err(e) = w.apply_full(&agents, &jobs, tasks) {
        log!("hoplb: sync: {e} HOPOS_HOPLB_SYNC");
        return false;
    }
    publish(w);
    true
}

/// De taken van elke job in `jobs` (Go: `syncJob`).
async fn jobs_sync(agent: &Agent, w: &mut Watcher, jobs: &[String]) -> bool {
    for job in jobs {
        let st = match agent.job_status(job).await {
            Ok(st) => st,
            Err(e) => {
                log!("hoplb: failed to fetch job status for {job}: {e} HOPOS_HOPLB_SYNC");
                return false;
            }
        };
        if let Err(e) = w.apply_job(job, st) {
            log!("hoplb: sync {job}: {e} HOPOS_HOPLB_SYNC");
            return false;
        }
    }
    true
}

/// Bouwt de tabel en zet hem in [`ROUTES`].
fn publish(w: &Watcher) {
    match w.build_routes() {
        Ok(t) => {
            let (n, backends) = (t.len(), t.backends());
            // De enige schrijver, en de lening leeft alleen hier.
            *ROUTES.borrow_mut() = t;
            log!(
                "hoplb: routes updated: {n} patterns, {backends} backends HOPOS_HOPLB_ROUTES n={n} backends={backends}"
            );
        }
        Err(e) => log!("hoplb: build routes: {e} HOPOS_HOPLB_SYNC"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ips_and_addresses() {
        let mut b = [0u8; 15];
        assert_eq!(fmt_ip([10, 100, 0, 2], &mut b), "10.100.0.2");
        assert_eq!(fmt_ip([255, 255, 255, 255], &mut b), "255.255.255.255");
        assert_eq!(fmt_ip([0, 9, 99, 100], &mut b), "0.9.99.100");
        assert_eq!(split_addr("10.0.2.15:80"), Some(("10.0.2.15", 80)));
        assert_eq!(split_addr("node.local:8080"), Some(("node.local", 8080)));
        assert_eq!(split_addr("nope"), None);
    }
}
