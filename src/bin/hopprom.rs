//! hopprom on a host: polls a Hop agent and serves `/metrics` and `/health`.
//!
//! Drie threads, elk met één eigenaar en berichten ertussen (handboek §1;
//! geen mutex, geen gedeelde tabel):
//!
//! - **poll**: bezit de [`Collector`] en de [`HostSource`]. Elke `-interval`
//!   één ronde; een geslaagde ronde gaat als [`Msg::Snapshot`] (een
//!   [`Metrics`] als waarde) naar de server.
//! - **accept**: bezit de listener en geeft elke verbinding als
//!   [`Msg::Conn`] door.
//! - **server** (de hoofdthread): bezit de laatste snapshot en bedient de
//!   verbindingen één voor één. Een scrape rendert de snapshot van dat
//!   moment, met de klok van dat moment (de lease loopt af tussen twee
//!   rondes).
//!
//! Waarom één serverthread: een scrape is een paar kilobyte tekst en
//! Prometheus vraagt hem eens per tien seconden; een pool zou de snapshot
//! moeten delen. Een stille verbinding houdt hem hooguit [`READ_CAP`] vast.

#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )
)]

use std::net::{TcpListener, TcpStream};
use std::sync::mpsc::{self, Receiver, Sender};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use hopprom::host::{self, HostSource};
use hopprom::http::{self, Route};
use hopprom::{Collector, Metrics};
use hostnet::{StdConn, block_on};
use leanhttp::{AsyncRead, AsyncWrite, Close, Exchange, IoError, Mux};

/// De langste stilte op een verbinding voordat de server hem sluit.
const READ_CAP: Duration = Duration::from_secs(5);

/// Wat de server-eigenaar ontvangt.
enum Msg {
    /// De metrics van een geslaagde ronde.
    Snapshot(Box<Metrics>),
    /// Een nieuwe verbinding.
    Conn(TcpStream),
}

/// De vlaggen, met de standaarden van de Go-generatie.
#[derive(Debug, PartialEq, Eq)]
struct Flags {
    listen: String,
    agent: String,
    interval: Duration,
    api_key: String,
}

impl Default for Flags {
    fn default() -> Self {
        Self {
            listen: String::from(":9090"),
            agent: String::from("http://127.0.0.1:8080"),
            interval: Duration::from_secs(5),
            api_key: String::new(),
        }
    }
}

const USAGE: &str =
    "usage: hopprom [-listen :9090] [-agent http://127.0.0.1:8080] [-interval 5s] [-api-key KEY]";

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let flags = match parse(&args) {
        Ok(f) => f,
        Err(why) => {
            eprintln!("hopprom: {why}\n{USAGE}");
            std::process::exit(2);
        }
    };
    let addr = listen_addr(&flags.listen);
    let listener = match TcpListener::bind(&addr) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("hopprom: listen on {addr}: {e}");
            std::process::exit(1);
        }
    };
    let mux = match http::mux() {
        Ok(m) => m,
        Err(e) => {
            eprintln!("hopprom: routes: {e}");
            std::process::exit(1);
        }
    };
    eprintln!(
        "hopprom {}: agent {}, interval {}s, metrics on http://{addr}/metrics",
        env!("CARGO_PKG_VERSION"),
        flags.agent,
        flags.interval.as_secs_f64()
    );
    let src = match HostSource::new(&flags.agent, &flags.api_key) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("hopprom: -agent {:?}: {e}", flags.agent);
            std::process::exit(2);
        }
    };
    let (tx, rx) = mpsc::channel();
    let poll_tx = tx.clone();
    let interval = flags.interval;
    let spawned = std::thread::Builder::new()
        .name(String::from("poll"))
        .spawn(move || poll_loop(src, interval, &poll_tx))
        .and_then(|_| {
            std::thread::Builder::new()
                .name(String::from("accept"))
                .spawn(move || accept_loop(&listener, &tx))
        });
    if let Err(e) = spawned {
        eprintln!("hopprom: spawn: {e}");
        std::process::exit(1);
    }
    serve_loop(&rx, &mux);
}

/// Waarom de vlaggen niet lazen.
#[derive(Debug, PartialEq, Eq)]
enum FlagError {
    /// `-h` of `-help`.
    Help,
    /// Een argument zonder `-`.
    Unexpected(String),
    /// Een vlag zonder waarde erachter.
    NeedsValue(String),
    /// Een vlag die hopprom niet kent.
    Unknown(String),
    /// Een `-interval` die geen duur boven nul is.
    Interval(String),
}

impl std::fmt::Display for FlagError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Help => f.write_str("help"),
            Self::Unexpected(a) => write!(f, "unexpected argument {a:?}"),
            Self::NeedsValue(n) => write!(f, "flag -{n} needs a value"),
            Self::Unknown(n) => write!(f, "unknown flag -{n}"),
            Self::Interval(v) => write!(
                f,
                "-interval {v:?} is not a duration above zero (5s, 1m30s)"
            ),
        }
    }
}

/// Leest de vlaggen: `-naam waarde`, `-naam=waarde`, en ook met `--`.
fn parse(args: &[String]) -> Result<Flags, FlagError> {
    let mut f = Flags::default();
    let mut it = args.iter();
    while let Some(arg) = it.next() {
        let name = arg.trim_start_matches('-');
        if name.len() == arg.len() {
            return Err(FlagError::Unexpected(arg.clone()));
        }
        if matches!(name, "h" | "help") {
            return Err(FlagError::Help);
        }
        let (name, value) = match name.split_once('=') {
            Some((n, v)) => (n, v.to_string()),
            None => match it.next() {
                Some(v) => (name, v.clone()),
                None => return Err(FlagError::NeedsValue(name.to_string())),
            },
        };
        match name {
            "listen" => f.listen = value,
            "agent" => f.agent = value,
            "api-key" => f.api_key = value,
            "interval" => match types::time::parse_duration(&value) {
                Ok(ns) if ns > 0 => f.interval = Duration::from_nanos(ns),
                _ => return Err(FlagError::Interval(value)),
            },
            _ => return Err(FlagError::Unknown(name.to_string())),
        }
    }
    Ok(f)
}

/// `:9090` luistert op alle adressen, zoals Go's `net.Listen`.
fn listen_addr(listen: &str) -> String {
    match listen.strip_prefix(':') {
        Some(port) => format!("0.0.0.0:{port}"),
        None => listen.to_string(),
    }
}

/// De poll-thread: eigenaar van de collector.
fn poll_loop(mut src: HostSource, interval: Duration, tx: &Sender<Msg>) {
    let mut collector = Collector::new();
    // Falen is luid, en één keer (handboek §6): een regel als de fout
    // verandert, en één als het weer lukt.
    let mut failing: Option<String> = None;
    let mut missing = String::new();
    loop {
        let started = Instant::now();
        let outcome = match block_on(hopprom::poll(&mut src)) {
            Ok((p, misses)) => {
                let now = misses
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join("; ");
                if now != missing && !now.is_empty() {
                    eprintln!("hopprom: best-effort calls missed, keeping the last values: {now}");
                }
                missing = now;
                collector
                    .observe(&p, host::now())
                    .map(|m| (m, p.agents.len(), p.task_count()))
                    .map_err(|e| format!("collect: {e}"))
            }
            Err(miss) => Err(format!("poll {}: {miss}", src.base())),
        };
        match outcome {
            Ok((m, agents, tasks)) => {
                if failing.take().is_some() || collector.rounds() == 1 {
                    eprintln!(
                        "hopprom: polling {}: agents={agents} tasks={tasks}",
                        src.base()
                    );
                }
                if tx.send(Msg::Snapshot(Box::new(m))).is_err() {
                    return; // De server is weg; het proces ook.
                }
            }
            Err(why) => {
                if failing.as_deref() != Some(why.as_str()) {
                    eprintln!("hopprom: {why}");
                }
                failing = Some(why);
            }
        }
        std::thread::sleep(interval.saturating_sub(started.elapsed()));
    }
}

/// De accept-thread: eigenaar van de listener.
fn accept_loop(listener: &TcpListener, tx: &Sender<Msg>) {
    loop {
        match listener.accept() {
            Ok((s, _)) => {
                let _ = s.set_nodelay(true);
                if tx.send(Msg::Conn(s)).is_err() {
                    return;
                }
            }
            Err(e) => {
                eprintln!("hopprom: accept: {e}");
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }
}

/// De server: eigenaar van de laatste snapshot.
fn serve_loop(rx: &Receiver<Msg>, mux: &Mux<Route>) {
    let mut latest: Option<Box<Metrics>> = None;
    while let Ok(msg) = rx.recv() {
        match msg {
            Msg::Snapshot(m) => latest = Some(m),
            Msg::Conn(s) => {
                let conn = Capped(StdConn::new(s, Some(READ_CAP)));
                let snapshot = latest.as_deref();
                // Een verbinding die eindigt met een termijn of een reset is
                // een client die wegging; geen logregel waard.
                let _ = block_on(leanhttp::serve(
                    conn,
                    async |ex: &mut Exchange<'_, Capped>| {
                        http::handle(ex, mux, || http::body(snapshot, host::now())).await
                    },
                ));
            }
        }
    }
}

/// Een std-socket met een plafond op elke leestermijn (zoals agentd): de
/// keep-alive van leanhttp (60 s) zou de ene serverthread anders een minuut
/// vasthouden.
struct Capped(StdConn<TcpStream>);

impl AsyncRead for Capped {
    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize, IoError>> {
        self.0.poll_read(cx, buf)
    }

    fn set_read_timeout(&mut self, t: Option<Duration>) -> Result<(), IoError> {
        let t = Some(t.map_or(READ_CAP, |t| t.min(READ_CAP)));
        self.0.set_read_timeout(t)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn args(a: &[&str]) -> Vec<String> {
        a.iter().map(|s| (*s).to_string()).collect()
    }

    #[test]
    fn the_flags_of_the_go_generation() {
        assert_eq!(parse(&[]).unwrap(), Flags::default());
        let f = parse(&args(&[
            "-listen",
            ":9191",
            "--agent=http://10.0.0.2:8080",
            "-interval",
            "1m30s",
            "-api-key",
            "k",
        ]))
        .unwrap();
        assert_eq!(f.listen, ":9191");
        assert_eq!(f.agent, "http://10.0.0.2:8080");
        assert_eq!(f.interval, Duration::from_secs(90));
        assert_eq!(f.api_key, "k");
        assert_eq!(parse(&args(&["-h"])), Err(FlagError::Help));
        assert!(matches!(
            parse(&args(&["-interval", "0s"])),
            Err(FlagError::Interval(_))
        ));
        assert!(matches!(
            parse(&args(&["-agent"])),
            Err(FlagError::NeedsValue(_))
        ));
        assert!(matches!(
            parse(&args(&["-port", "1"])),
            Err(FlagError::Unknown(_))
        ));
        assert!(matches!(
            parse(&args(&["serve"])),
            Err(FlagError::Unexpected(_))
        ));
    }

    #[test]
    fn a_bare_port_listens_everywhere() {
        assert_eq!(listen_addr(":9090"), "0.0.0.0:9090");
        assert_eq!(listen_addr("127.0.0.1:9090"), "127.0.0.1:9090");
    }
}
