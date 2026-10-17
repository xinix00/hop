//! De twee servers: verkeer (de proxy) en admin, elk een vaste pool
//! threads op een kloon van de listener (de vorm van agentd's `http.rs`).
//!
//! Een verkeerswerker bezit zijn verbinding en zijn eigen kopie van de
//! routetabel; elk verzoek gaat door [`hoplb::proxy::serve`] en eindigt met
//! een meting naar de eigenaar.

use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, SyncSender};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use hoplb::proxy::{self, Env};
use hoplb::{Error, Pick, Record, RouteTable};
use hostnet::{StdConn, block_on};
use leanhttp::{AsyncRead, AsyncWrite, Close, IoError};

use crate::log;
use crate::owner::{self, GENERATION, Msg};

/// Verkeerswerkers: zoveel clientverbindingen tegelijk. Een browser houdt
/// er twee tot zes open; met de keep-alive-kap van 5 s ([`proxy::Limits`])
/// draagt dit enkele tientallen gelijktijdige gebruikers. Een SSE-stroom
/// houdt zijn werker vast zolang hij loopt.
pub(crate) const WORKERS: usize = 64;

/// Admin-werkers: Prometheus en een health-check.
pub(crate) const ADMIN_WORKERS: usize = 2;

/// Hoe lang het bellen van een backend mag duren (Go's transport: 30 s;
/// een backend op het LAN die na 10 s niet opneemt, is weg).
const DIAL_TIMEOUT: Duration = Duration::from_secs(10);

/// De stiltetermijn van een std-socket als niemand een termijn zet.
const SOCKET_IDLE: Duration = Duration::from_secs(60);

/// De kap op elke leestermijn van de admin-poort (agentd: `READ_CAP`).
const ADMIN_READ_CAP: Duration = Duration::from_secs(5);

/// Hoe lang een werker op de eigenaar wacht.
const OWNER_TIMEOUT: Duration = Duration::from_secs(5);

/// Start [`WORKERS`] verkeerswerkers op `listener`.
pub(crate) fn spawn_traffic(
    listener: &TcpListener,
    owner: &SyncSender<Msg>,
) -> std::io::Result<()> {
    let busy = Arc::new(AtomicUsize::new(0));
    for i in 0..WORKERS {
        let l = listener.try_clone()?;
        let owner = owner.clone();
        let busy = busy.clone();
        std::thread::Builder::new()
            .name(format!("traffic-{i}"))
            .spawn(move || traffic(&l, owner, busy))?;
    }
    Ok(())
}

/// Start [`ADMIN_WORKERS`] admin-werkers op `listener`.
pub(crate) fn spawn_admin(listener: &TcpListener, owner: &SyncSender<Msg>) -> std::io::Result<()> {
    for i in 0..ADMIN_WORKERS {
        let l = listener.try_clone()?;
        let owner = owner.clone();
        std::thread::Builder::new()
            .name(format!("admin-{i}"))
            .spawn(move || admin(&l, &owner))?;
    }
    Ok(())
}

/// De schil van de proxy voor één werker.
struct HostEnv {
    owner: SyncSender<Msg>,
    routes: RouteTable,
    generation: u64,
    epoch: Instant,
    /// Hoeveel verkeersthreads een verbinding hebben; [`WORKERS`] is vol
    /// (`Env::crowded`).
    busy: Arc<AtomicUsize>,
}

impl HostEnv {
    /// Haalt een nieuwere tabel op als de eigenaar er een publiceerde.
    fn refresh(&mut self) {
        if GENERATION.load(Relaxed) == self.generation {
            return;
        }
        let (tx, rx) = mpsc::sync_channel(1);
        if self.owner.send(Msg::Snapshot(tx)).is_err() {
            return;
        }
        if let Ok((generation, routes)) = rx.recv_timeout(OWNER_TIMEOUT) {
            self.routes = routes;
            self.generation = generation;
        }
    }
}

impl Env for HostEnv {
    type Conn = StdConn<TcpStream>;

    fn pick(&mut self, host: &str) -> Pick {
        self.refresh();
        self.routes.pick(host)
    }

    async fn dial(&mut self, addr: &str) -> hoplb::Result<Self::Conn> {
        let addrs = addr.to_socket_addrs().map_err(|_| Error::BadAddress)?;
        let mut last = Error::BadAddress;
        for a in addrs {
            match TcpStream::connect_timeout(&a, DIAL_TIMEOUT) {
                Ok(s) => {
                    let _ = s.set_nodelay(true);
                    return Ok(StdConn::new(s, Some(SOCKET_IDLE)));
                }
                Err(e) => last = Error::Dial(io_error(&e)),
            }
        }
        Err(last)
    }

    fn now_ns(&self) -> u64 {
        u64::try_from(self.epoch.elapsed().as_nanos()).unwrap_or(u64::MAX)
    }

    fn record(&mut self, r: Record) {
        // Go logde elk verzoek; hier na afloop, met de status en de duur.
        log!(
            "{} -> {} {} {:.1}ms",
            r.domain,
            if r.backend.is_empty() {
                "-"
            } else {
                &r.backend
            },
            r.code,
            r.nanos as f64 / 1e6
        );
        owner::record(&self.owner, r);
    }

    fn backend_error(&mut self, host: &str, addr: &str, err: Error) {
        log!("Proxy error for {host} -> {addr}: {err}");
    }

    fn crowded(&self) -> bool {
        self.busy.load(Ordering::Acquire) >= WORKERS
    }
}

fn io_error(e: &std::io::Error) -> IoError {
    match e.kind() {
        std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock => IoError::TimedOut,
        std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::ConnectionRefused => {
            IoError::Reset
        }
        _ => IoError::Other,
    }
}

fn traffic(l: &TcpListener, owner: SyncSender<Msg>, busy: Arc<AtomicUsize>) {
    let mut env = HostEnv {
        owner,
        routes: RouteTable::new(),
        generation: u64::MAX,
        epoch: Instant::now(),
        busy,
    };
    loop {
        let (stream, peer) = match l.accept() {
            Ok(s) => s,
            Err(e) => {
                log!("accept: {e}");
                std::thread::sleep(Duration::from_millis(100));
                continue;
            }
        };
        let _ = stream.set_nodelay(true);
        let ip = peer.ip().to_string();
        let conn = StdConn::new(stream, Some(SOCKET_IDLE));
        env.busy.fetch_add(1, Ordering::AcqRel);
        // Een verbinding die eindigt met een termijn of een reset is een
        // client die wegging; geen logregel waard.
        let _ = block_on(proxy::serve(conn, &ip, &mut env));
        env.busy.fetch_sub(1, Ordering::AcqRel);
    }
}

fn admin(l: &TcpListener, owner: &SyncSender<Msg>) {
    loop {
        let stream = match l.accept() {
            Ok((s, _)) => s,
            Err(e) => {
                log!("admin accept: {e}");
                std::thread::sleep(Duration::from_millis(100));
                continue;
            }
        };
        let conn = Capped(StdConn::new(stream, Some(ADMIN_READ_CAP)));
        let _ = block_on(hoplb::admin::serve(conn, async || scrape(owner)));
    }
}

/// De Prometheus-tekst van de eigenaar.
fn scrape(owner: &SyncSender<Msg>) -> Option<String> {
    let (tx, rx) = mpsc::sync_channel(1);
    owner.send(Msg::Scrape(tx)).ok()?;
    rx.recv_timeout(OWNER_TIMEOUT).ok()
}

/// Een std-socket met een kap op elke leestermijn (agentd: `Capped`): de
/// keep-alive van leanhttp (60 s) houdt anders een admin-werker vast.
struct Capped(StdConn<TcpStream>);

impl AsyncRead for Capped {
    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize, IoError>> {
        self.0.poll_read(cx, buf)
    }

    fn set_read_timeout(&mut self, t: Option<Duration>) -> Result<(), IoError> {
        self.0
            .set_read_timeout(Some(t.map_or(ADMIN_READ_CAP, |t| t.min(ADMIN_READ_CAP))))
    }
}

impl AsyncWrite for Capped {
    fn poll_write(&mut self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, IoError>> {
        self.0.poll_write(cx, buf)
    }

    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), IoError>> {
        self.0.poll_flush(cx)
    }

    fn set_write_timeout(&mut self, t: Option<Duration>) -> Result<(), IoError> {
        self.0.set_write_timeout(t)
    }
}

impl Close for Capped {
    fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), IoError>> {
        self.0.poll_close(cx)
    }
}
