//! De host-daemon (feature `std`): de UDP-server op std-sockets en een
//! watcher per peer, als threads die berichten sturen.
//!
//! De vorm (handboek §1, en hostnet's "threads, geen reactor"): elke staat
//! heeft één thread als eigenaar.
//!
//! - De **server-thread** bezit de [`Cache`]. Hij beantwoordt vragen en legt
//!   tussen twee vragen de berichten van de watchers erin ([`Update`]); een
//!   leestermijn van [`DRAIN_EVERY`] houdt de rij kort als er geen vragen
//!   komen. Geen mutex: een vraag leest de cache als `&`, een bericht
//!   verandert hem als `&mut`, en dat is dezelfde thread.
//! - Per peer een **watcher-thread** die de [`Watcher`] bezit: hij ontdekt de
//!   clusternaam, ververst met hoplib's [`Agent`] en stuurt het nieuwe beeld
//!   naar de server-thread.
//! - Per peer een **lezer-thread** die de SSE-stroom bezit (hoplib's
//!   `EventStream`, blokkerend, herverbindt zelf) en elke gebeurtenis als
//!   bericht naar zijn watcher stuurt. Zo kan de watcher wachten op een
//!   gebeurtenis óf op zijn samenvoegtermijn (`recv_timeout`), zoals Go's
//!   `select` over de regels en de debounce-timer.
//!
//! Stoppen: SIGINT en SIGTERM beëindigen het proces (std heeft geen
//! signaal-API, zie PORT.md); er is niets te bewaren, de cache is een
//! afgeleide van de clusters.

use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender, TryRecvError};
use std::thread;
use std::time::{Duration, Instant};

use hoplib::host::Agent;
use hoplib::{Backoff, Client, Event};

use crate::config::{self, Config, Flags, Peer};
use crate::watcher::{Action, RECONNECT, Refresh, Update, Watcher};
use crate::{Cache, Server};

/// Hoe lang de server-thread hooguit op een vraag wacht voordat hij de
/// berichten van de watchers verwerkt.
pub const DRAIN_EVERY: Duration = Duration::from_millis(250);

/// De grootste vraag die de server leest; alles erboven is geen vraag.
const QUERY_MAX: usize = 4096;

/// Waarom de daemon niet start of stopt.
#[derive(Debug)]
pub enum HostError {
    /// De kern weigerde (vlaggen, config, geheugen).
    Core(crate::Error),
    /// Een systeemaanroep faalde; `what` zegt welke.
    Io {
        /// Wat er gedaan werd.
        what: &'static str,
        /// De fout van het systeem.
        err: io::Error,
    },
    /// De agent-API weigerde een peer-adres.
    Hop(hoplib::Error),
}

impl core::fmt::Display for HostError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            HostError::Core(e) => write!(f, "{e}"),
            HostError::Io { what, err } => write!(f, "{what}: {err}"),
            HostError::Hop(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for HostError {}

impl From<crate::Error> for HostError {
    fn from(e: crate::Error) -> Self {
        HostError::Core(e)
    }
}

/// Het resultaat van de daemon.
pub type Result<T = (), E = HostError> = core::result::Result<T, E>;

/// Eén logregel op stderr, met `hopdns: ` ervoor.
macro_rules! log {
    ($($arg:tt)*) => {
        eprintln!("hopdns: {}", format_args!($($arg)*))
    };
}

/// Leest het YAML-bestand op `path` (Go's `LoadConfig`).
pub fn load_config(path: &str) -> Result<Config> {
    let text = std::fs::read_to_string(path).map_err(|err| HostError::Io {
        what: "read config",
        err,
    })?;
    Ok(Config::parse(&text)?)
}

/// Een draaiende daemon: de socket en de server, klaar om te bedienen.
#[derive(Debug)]
pub struct Daemon {
    sock: UdpSocket,
    server: Server,
    updates: Receiver<Update>,
}

impl Daemon {
    /// Start de daemon voor `flags`: de config, de socket, en een watcher
    /// (met lezer) per peer. Bedienen doet [`Daemon::serve`].
    pub fn start(flags: &Flags) -> Result<Self> {
        let cnames = match &flags.config {
            Some(path) => load_config(path)?.cname_table()?,
            None => crate::CNAMEs::default(),
        };
        let mut server = Server::new(&flags.domain)?;
        let n_cnames = cnames.len();
        server.set_cnames(cnames);
        let (ip, port) = config::parse_listen(&flags.listen)?;
        let sock = UdpSocket::bind((ip, port)).map_err(|err| HostError::Io {
            what: "bind udp",
            err,
        })?;
        sock.set_read_timeout(Some(DRAIN_EVERY))
            .map_err(|err| HostError::Io {
                what: "udp read timeout",
                err,
            })?;
        log!(
            "starting on {}, domain={}, peers={}, cnames={} HOPDNS_START",
            flags.listen,
            server.domain(),
            flags.peers.len(),
            n_cnames
        );
        let (tx, updates) = mpsc::channel();
        for raw in &flags.peers {
            let peer = Peer::parse(raw, &flags.api_key)?;
            let client =
                Client::new(&peer.endpoint, peer.api_key.as_deref()).map_err(HostError::Hop)?;
            let tx = tx.clone();
            thread::Builder::new()
                .name(format!("watch {}", peer.endpoint))
                .spawn(move || watch(&peer, client, &tx))
                .map_err(|err| HostError::Io {
                    what: "spawn watcher",
                    err,
                })?;
        }
        Ok(Self {
            sock,
            server,
            updates,
        })
    }

    /// Het adres waarop de daemon luistert.
    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.sock.local_addr().map_err(|err| HostError::Io {
            what: "udp local address",
            err,
        })
    }

    /// Bedient vragen tot de socket faalt; deze thread bezit de cache.
    pub fn serve(self) -> Result {
        let mut cache = Cache::new();
        let mut query = [0u8; QUERY_MAX];
        let mut answer = [0u8; crate::wire::EDNS_MAX];
        if let Ok(addr) = self.sock.local_addr() {
            log!(
                "DNS server listening on udp {addr} (domain: {}) HOPDNS_UP",
                self.server.domain()
            );
        }
        loop {
            drain(&self.updates, &mut cache);
            let (n, from) = match self.sock.recv_from(&mut query) {
                Ok(got) => got,
                Err(e)
                    if matches!(
                        e.kind(),
                        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                    ) =>
                {
                    continue;
                }
                // Een ICMP-weigering op een eerder antwoord (Linux meldt die
                // op de volgende recv): geen reden om te stoppen.
                Err(e) if e.kind() == io::ErrorKind::ConnectionRefused => continue,
                Err(err) => {
                    return Err(HostError::Io {
                        what: "udp recv",
                        err,
                    });
                }
            };
            let Some(msg) = query.get(..n) else { continue };
            if let Some(len) = self.server.handle(&cache, msg, &mut answer)
                && let Some(out) = answer.get(..len)
            {
                // Een verloren antwoord is UDP: de resolver vraagt opnieuw.
                let _ = self.sock.send_to(out, from);
            }
        }
    }
}

/// Legt elk wachtend bericht in de cache.
fn drain(updates: &Receiver<Update>, cache: &mut Cache) {
    loop {
        match updates.try_recv() {
            Ok(u) => {
                let cluster = String::from(u.cluster());
                if let Err(e) = u.apply(cache) {
                    log!("cache update for {cluster} dropped: {e} HOPDNS_CACHE_FAIL");
                }
            }
            Err(TryRecvError::Empty | TryRecvError::Disconnected) => return,
        }
    }
}

/// Wat de lezer-thread aan zijn watcher meldt.
enum Heard {
    /// Een gebeurtenis.
    Event(Event),
    /// De stroom brak; de lezer verbindt over [`RECONNECT`] opnieuw.
    Lost(hoplib::Error),
}

/// De lezer: bezit de stroom en stuurt wat hij hoort. Stopt als de watcher
/// weg is.
fn read_events(client: Client, tx: &Sender<Heard>) {
    let agent = Agent::new(client);
    let mut stream = agent
        .stream()
        .with_backoff(Backoff::new(RECONNECT, RECONNECT));
    loop {
        let heard = match stream.next() {
            Ok(e) => Heard::Event(e),
            Err(e) => Heard::Lost(e),
        };
        if tx.send(heard).is_err() {
            return;
        }
    }
}

/// De watcher-thread van één peer.
fn watch(peer: &Peer, client: Client, updates: &Sender<Update>) {
    let t0 = Instant::now();
    let now = || u64::try_from(t0.elapsed().as_nanos()).unwrap_or(u64::MAX);
    let ep = peer.endpoint.as_str();
    let agent = Agent::new(client.clone());
    let mut w = match Watcher::new(ep) {
        Ok(w) => w,
        Err(e) => {
            log!("[{ep}] watcher not started: {e} HOPDNS_PEER_FAIL");
            return;
        }
    };
    let (tx, heard) = mpsc::channel();
    let mut reader = Some((client, tx));
    loop {
        match w.next(now()) {
            Action::Discover => match agent.status() {
                Ok(st) => match w.discovered(now(), &st.cluster_name) {
                    Ok(()) => {
                        log!("[{ep}] cluster: {} HOPDNS_PEER_CLUSTER", st.cluster_name);
                        if let Some((client, tx)) = reader.take() {
                            let spawned = thread::Builder::new()
                                .name(format!("events {ep}"))
                                .spawn(move || read_events(client, &tx));
                            if let Err(e) = spawned {
                                log!("[{ep}] event reader not started: {e} HOPDNS_PEER_FAIL");
                                return;
                            }
                        }
                    }
                    Err(e) => {
                        log!("[{ep}] failed to discover cluster name: {e} HOPDNS_PEER_DISCOVER")
                    }
                },
                Err(e) => {
                    log!("[{ep}] failed to discover cluster name: {e} HOPDNS_PEER_DISCOVER");
                    w.discover_failed(now());
                }
            },
            Action::Refresh(r) => refresh(&agent, &mut w, &r, updates, now()),
            Action::Wait(deadline) => {
                let got = match deadline {
                    None => heard.recv().map_err(|_| RecvTimeoutError::Disconnected),
                    Some(t) => heard.recv_timeout(Duration::from_nanos(t.saturating_sub(now()))),
                };
                let cluster = w.cluster().unwrap_or("?").to_owned();
                match got {
                    Ok(Heard::Event(e)) => {
                        if e.kind == "ping" {
                            log!("[{ep}] ({cluster}) SSE connected, seeding cache HOPDNS_SSE_UP");
                        }
                        if let Err(err) = w.event(now(), &e) {
                            log!("[{ep}] ({cluster}) event dropped: {err} HOPDNS_EVENT_FAIL");
                        }
                    }
                    Ok(Heard::Lost(e)) => {
                        log!(
                            "[{ep}] ({cluster}) SSE disconnected: {e}, reconnecting in {}s HOPDNS_SSE_LOST",
                            RECONNECT.as_secs()
                        );
                        w.lost();
                    }
                    Err(RecvTimeoutError::Timeout) => {}
                    Err(RecvTimeoutError::Disconnected) => {
                        log!(
                            "[{ep}] ({cluster}) event reader gone, watcher stops HOPDNS_PEER_FAIL"
                        );
                        return;
                    }
                }
            }
        }
    }
}

/// Voert een verversing uit en stuurt het resultaat naar de server-thread.
fn refresh(agent: &Agent, w: &mut Watcher, r: &Refresh, updates: &Sender<Update>, now: u64) {
    let ep = w.endpoint().to_owned();
    let cluster = w.cluster().unwrap_or("?").to_owned();
    match r {
        Refresh::Full => {
            let got = agent.agents().and_then(|a| agent.tasks().map(|t| (a, t)));
            match got {
                Ok((agents, tasks)) => match w.refreshed_full(&agents, &tasks, None) {
                    Ok(u) => {
                        if let Update::Replace { jobs, .. } = &u {
                            log!(
                                "[{ep}] ({cluster}) cache updated: {} jobs HOPDNS_CACHE",
                                jobs.len()
                            );
                        }
                        let _ = updates.send(u);
                    }
                    Err(e) => log!("[{ep}] ({cluster}) rebuild failed: {e} HOPDNS_REFRESH_FAIL"),
                },
                Err(e) => {
                    log!("[{ep}] ({cluster}) failed to fetch jobs: {e} HOPDNS_REFRESH_FAIL");
                    w.refresh_failed(now, r);
                }
            }
        }
        Refresh::Jobs(jobs) => {
            for job in jobs {
                let built = agent
                    .job_status(job)
                    .map_err(|e| e.to_string())
                    .and_then(|st| w.refreshed_job(job, &st, None).map_err(|e| e.to_string()));
                match built {
                    Ok(u) => {
                        if let Update::Set { ips, .. } = &u {
                            log!(
                                "[{ep}] ({cluster}) cache updated job {job}: {} IPs HOPDNS_CACHE_JOB",
                                ips.len()
                            );
                        }
                        let _ = updates.send(u);
                    }
                    Err(e) => {
                        log!(
                            "[{ep}] ({cluster}) failed to fetch job {job}: {e} HOPDNS_REFRESH_FAIL"
                        );
                        w.refresh_failed(now, r);
                        return;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    // TestLoadConfig: het bestand van schijf, met een kale en een geciteerde
    // sleutel.
    #[test]
    fn load_config() {
        let dir = std::env::temp_dir().join(format!("hopdns-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("hopdns.yaml");
        std::fs::write(
            &path,
            "cnames:\n  mail.hop.local: mailserver.example.com\n  \"*.apps.hop.local\": ingress.prod-eu.hop.local\n",
        )
        .unwrap();
        let cfg = super::load_config(path.to_str().unwrap()).unwrap();
        assert_eq!(cfg.cname("mail.hop.local"), Some("mailserver.example.com"));
        assert_eq!(
            cfg.cname("*.apps.hop.local"),
            Some("ingress.prod-eu.hop.local")
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    // TestLoadConfigMissing: een ontbrekend bestand is een fout.
    #[test]
    fn load_config_missing() {
        assert!(super::load_config("/nonexistent/path/hopdns.yaml").is_err());
    }
}
