//! `hoplockserver-hopos`: de compare-and-swap-server als bewoner van HopOS.
//!
//! Hetzelfde protocol als de host-bin, byte voor byte (de toestandsmachine
//! en de leanhttp-naad van de bibliotheek), in een slot van HopOS: Hop
//! plaatst hem als job met een gepubliceerde poort, en de kern zet die poort
//! van de uplink door naar het slot (DNAT). Een cluster zonder host kan zo
//! zijn eigen lease-server draaien.
//!
//! Uit de env van het slot (de jobspec):
//!
//! - `ER_PORT_HTTP`: de poort, uit `"ports": {"http": 8090}` (standaard 8090);
//! - `HOPLOCK_API_KEY`: de verplichte `X-API-Key` (leeg: zonder, luid);
//! - `HOPLOCK_DATA`: de map van de store (standaard `/data`), bedoeld als het
//!   volume van de job (`"volumes": {"/volumes/hoplock": "/data"}`). Hop
//!   v3.0.0-alpha.10 weigert een job met volumes op HopOS nog (`START_SLOT`
//!   draagt geen mounts); zonder volume ligt `/data` in de eigen root van
//!   het slot, en die maakt de kern bij elke start leeg. Gemeten 30-09 op
//!   QEMU (tools/qemu-test.sh): alles groen, behalve een sleutel over een
//!   herstart van de job heen.
//!
//! De store staat op dat volume via de bestandscalls van de system-API
//! (`hoplockserver::volume`): stat, lezen, schrijven, verwijderen, lijsten.
//! De kern heeft geen fsync en geen rename; de commit van hopfs (elke 10 s en
//! bij de stop van het slot) is de fsync, en elk record draagt een
//! controlegetal zodat een half record nooit als lease terugkomt.
//!
//! De vorm (handboek §1 en §2), zoals welcome:
//!
//! - **de acceptor** geeft elke verbinding als waarde aan een vrije werker
//!   uit een vaste pool van [`WORKERS`];
//! - **een werker** bezit zijn verbinding, draait leanhttp, en stuurt elke
//!   store-operatie als bericht ([`Ask`]) naar de brievenbus van de store;
//!   de uitkomst komt terug over zijn eigen rij;
//! - **de store-taak** is de enige eigenaar van de store en van de
//!   system-client: één operatie tegelijk, dus elke compare-and-swap is
//!   lineariseerbaar zonder slot.
//!
//! Markers op de console van de kern: `HOPOS_HOPLOCK_UP keys=<n>` als de
//! listener staat (n: de sleutels die het volume al had),
//! `HOPOS_HOPLOCK_NO_AUTH` zonder sleutel, `HOPOS_HOPLOCK_VOLUME` als het
//! volume niet te lezen was, `HOPOS_HOPLOCK_CORRUPT` bij een verminkt
//! record, `HOPOS_HOPLOCK_FAIL` bij een start die niet lukt, en om de
//! [`LOG_EVERY`] verzoeken `HOPOS_HOPLOCK_REQUESTS`.

#![cfg_attr(target_os = "none", no_std, no_main)]

extern crate alloc;

use alloc::string::ToString;
use alloc::vec::Vec;
use core::cell::Cell;
use core::sync::atomic::{AtomicU64, Ordering::Relaxed};
use core::time::Duration;

use applib::appnet::{self, SystemClient, TcpListener, TcpStream};
use applib::rt::Exec;
use applib::sys;
use applib::tcp::TcpConn;
use applib::{App, EXEC, log};
use hoplockserver::http::{self, Unanswered};
use hoplockserver::server::{Done, Op, run};
use hoplockserver::volume::{Volume, VolumeError, VolumeStore};
use hoplockserver::{DEFAULT_PORT, Server};
use leanhttp::Exchange;
use sync::Local;
use sync::mpsc::Mailbox;
use sync::spsc::{Channel, Receiver, Sender};

applib::main!(hoplock);

/// Op de host bestaat dit image niet: daar is dit een lege binary, zodat
/// clippy de bewoner kan lezen.
#[cfg(not(target_os = "none"))]
fn main() {}

/// De map van de store zonder `HOPLOCK_DATA`.
const DEFAULT_DATA: &str = "/data";

/// De werkers: zoveel verbindingen tegelijk. Hop's client opent per
/// verzoek een verse verbinding en een verzoek duurt een paar
/// system-calls; vier is ruim voor een cluster van tientallen nodes, en de
/// rest wacht kort op de eerste die vrijkomt.
const WORKERS: usize = 4;

/// De langste stilte op een keep-alive-verbinding: Go's `ReadHeaderTimeout`.
const READ_CAP: Duration = Duration::from_secs(5);

/// Hoe vaak de acceptor kijkt of er een werker vrij is, als ze alle vier
/// bezig zijn (een koud pad, zoals in welcome).
const BUSY_POLL: Duration = Duration::from_millis(5);

/// Om de zoveel verzoeken één logregel. Een lease wordt elke paar seconden
/// vernieuwd; per verzoek loggen zou de console vullen.
const LOG_EVERY: u64 = 1000;

/// Verzoeken sinds de start (een teller, dus een atomic).
static REQUESTS: AtomicU64 = AtomicU64::new(0);

/// De rij naar elke werker: één verbinding tegelijk.
static CONNS: Local<[Channel<TcpStream, 1>; WORKERS]> =
    Local::new([const { Channel::new() }; WORKERS]);

/// Welke werker een verbinding heeft: de acceptor zet de vlag, de werker
/// wist hem; nooit over een `.await` geleend.
static BUSY: Local<[Cell<bool>; WORKERS]> = Local::new([const { Cell::new(false) }; WORKERS]);

/// De brievenbus van de store-taak. Elke werker heeft hoogstens één vraag
/// uitstaan, dus [`WORKERS`] plaatsen is nooit vol.
static ASKS: Mailbox<Ask, WORKERS> = Mailbox::new();

/// Per werker de rij terug van de store-taak.
static ANSWERS: Local<[Channel<Done, 1>; WORKERS]> =
    Local::new([const { Channel::new() }; WORKERS]);

/// Een vraag aan de store-taak: de operatie en welke werker wacht.
struct Ask {
    worker: usize,
    op: Op,
}

/// Wat elke werker deelt: gezet vóór de eerste spawn, daarna alleen gelezen.
struct Shared {
    exec: &'static Exec,
    server: Server,
}

/// De system-client als volume: de bestandscalls van de kern.
struct SysVolume(SystemClient);

/// Een fout van de system-client als tekst.
fn vol_err(e: &sys::Error) -> VolumeError {
    VolumeError(e.to_string())
}

impl Volume for SysVolume {
    const CHUNK: usize = sys::MAX_CHUNK;

    async fn size(&mut self, path: &str) -> Result<Option<u64>, VolumeError> {
        match self.0.stat(path).await {
            Ok(n) => Ok(Some(n)),
            Err(sys::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(vol_err(&e)),
        }
    }

    async fn read_at(
        &mut self,
        path: &str,
        off: u64,
        dst: &mut [u8],
    ) -> Result<usize, VolumeError> {
        self.0
            .read_into(path, off, dst)
            .await
            .map_err(|e| vol_err(&e))
    }

    async fn replace(&mut self, path: &str, data: &[u8]) -> Result<(), VolumeError> {
        self.0.write_file(path, data).await.map_err(|e| vol_err(&e))
    }

    async fn remove(&mut self, path: &str) -> Result<bool, VolumeError> {
        match self.0.remove(path).await {
            Ok(()) => Ok(true),
            Err(sys::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(vol_err(&e)),
        }
    }

    async fn list(&mut self, path: &str, dst: &mut [u8]) -> Result<Option<usize>, VolumeError> {
        match self.0.list(path, dst).await {
            Ok(n) => Ok(Some(n)),
            Err(sys::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(vol_err(&e)),
        }
    }
}

async fn hoplock(app: &'static App) {
    let exec: &'static Exec = EXEC.get();
    let port = port_of(app.env("ER_PORT_HTTP"));
    let data = app
        .env("HOPLOCK_DATA")
        .filter(|d| !d.is_empty())
        .unwrap_or(DEFAULT_DATA);
    let server = Server::new(app.env("HOPLOCK_API_KEY").unwrap_or(""));
    let net = match appnet::up(app) {
        Ok(n) => n,
        Err(e) => return fail(app, "network stack", &e).await,
    };
    let listener = match TcpListener::bind(port) {
        Ok(l) => l,
        Err(e) => return fail(app, "listen on ER_PORT_HTTP", &e).await,
    };
    let mut store = VolumeStore::new(SysVolume(net.system_client()), data);
    let keys = match store.count_keys().await {
        Ok(n) => n,
        Err(e) => {
            log!(
                "hoplockserver: cannot list {data}: {e}, starting as if empty HOPOS_HOPLOCK_VOLUME"
            );
            0
        }
    };
    if !server.has_auth() {
        log!("hoplockserver: no HOPLOCK_API_KEY, every request is allowed HOPOS_HOPLOCK_NO_AUTH");
    }
    let auth = if server.has_auth() { "on" } else { "off" };
    let key_len = server.key_len();
    let shared: &'static Shared =
        alloc::boxed::Box::leak(alloc::boxed::Box::new(Shared { exec, server }));
    let Some(q) = Queues::split() else {
        return fail(app, "worker queues", &"split twice").await;
    };
    let Queues {
        mut to_workers,
        to_answers,
        receivers,
    } = q;
    if let Err(e) = exec.spawn(store_task(store, to_answers)) {
        return fail(app, "spawn the store task", &e).await;
    }
    for (i, (conns, answers)) in receivers.into_iter().enumerate() {
        if let Err(e) = exec.spawn(worker(i, conns, answers, shared)) {
            return fail(app, "spawn a worker", &e).await;
        }
    }
    let [a, b, c, d] = net.ip();
    log!(
        "hoplockserver: serving on {a}.{b}.{c}.{d}:{port} (data={data}, auth={auth} ({key_len} chars), {WORKERS} workers) HOPOS_HOPLOCK_UP keys={keys}"
    );
    accept(listener, &mut to_workers, exec).await;
}

/// Een start die niet lukt: één regel, en de app stopt met code 1.
async fn fail(app: &'static App, what: &str, e: &dyn core::fmt::Display) {
    log!("hoplockserver: {what}: {e} HOPOS_HOPLOCK_FAIL");
    app.shutdown(1).await;
}

/// De rijen van de werkers, één keer gesplitst: de acceptor krijgt de
/// zenders naar de werkers, de store-taak de zenders terug, en elke werker
/// zijn twee ontvangers.
struct Queues {
    to_workers: Vec<Sender<'static, TcpStream, 1>>,
    to_answers: Vec<Sender<'static, Done, 1>>,
    receivers: Vec<(Receiver<'static, TcpStream, 1>, Receiver<'static, Done, 1>)>,
}

impl Queues {
    /// Splitst alle rijen; `None` als dat al gebeurd was of het geheugen op is.
    fn split() -> Option<Queues> {
        let mut q = Queues {
            to_workers: Vec::new(),
            to_answers: Vec::new(),
            receivers: Vec::new(),
        };
        q.to_workers.try_reserve_exact(WORKERS).ok()?;
        q.to_answers.try_reserve_exact(WORKERS).ok()?;
        q.receivers.try_reserve_exact(WORKERS).ok()?;
        for (c, a) in CONNS.get().iter().zip(ANSWERS.get()) {
            let (ctx, crx) = c.split()?;
            let (atx, arx) = a.split()?;
            q.to_workers.push(ctx);
            q.to_answers.push(atx);
            q.receivers.push((crx, arx));
        }
        Some(q)
    }
}

/// De poort uit `ER_PORT_HTTP`, of [`DEFAULT_PORT`] zonder of bij onzin
/// (luid: een jobspec die iets anders bedoelde, moet dat kunnen zien).
fn port_of(env: Option<&str>) -> u16 {
    match env.map(str::parse::<u16>) {
        None => DEFAULT_PORT,
        Some(Ok(p)) if p != 0 => p,
        Some(_) => {
            log!(
                "hoplockserver: ER_PORT_HTTP={env:?} is not a port, using {DEFAULT_PORT} HOPOS_HOPLOCK_PORT"
            );
            DEFAULT_PORT
        }
    }
}

/// De store-taak: de enige eigenaar van de store en de system-client.
async fn store_task(mut store: VolumeStore<SysVolume>, mut answers: Vec<Sender<'static, Done, 1>>) {
    let mut corrupt_logged = 0u64;
    loop {
        let ask = ASKS.recv().await;
        let done = run(&mut store, ask.op).await;
        if let Some(key) = store.take_corrupt() {
            // Luid de eerste paar keer, daarna tellen we (handboek §6).
            corrupt_logged += 1;
            if corrupt_logged <= 3 {
                log!(
                    "hoplockserver: record {key} on {} is torn or corrupt, treated as absent ({} so far) HOPOS_HOPLOCK_CORRUPT",
                    store.root(),
                    store.corrupt()
                );
            }
        }
        // Elke werker heeft één vraag uit en een rij van één: nooit vol.
        if let Some(tx) = answers.get_mut(ask.worker) {
            let _ = tx.try_send(done);
        }
    }
}

/// De acceptor: elke verbinding naar de eerste vrije werker.
async fn accept(
    listener: TcpListener,
    senders: &mut [Sender<'static, TcpStream, 1>],
    exec: &'static Exec,
) {
    loop {
        let mut stream = match listener.accept().await {
            Ok(s) => s,
            Err(e) => {
                log!("hoplockserver: accept: {e} HOPOS_HOPLOCK_ACCEPT");
                exec.after(Duration::from_millis(100)).await;
                continue;
            }
        };
        loop {
            match hand_off(stream, senders) {
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
fn hand_off(stream: TcpStream, senders: &mut [Sender<'static, TcpStream, 1>]) -> Option<TcpStream> {
    let busy = BUSY.get();
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

/// Eén werker: wacht op een verbinding, bedient hem met leanhttp tot hij
/// sluit, en meldt zich weer vrij.
async fn worker(
    i: usize,
    mut conns: Receiver<'static, TcpStream, 1>,
    mut answers: Receiver<'static, Done, 1>,
    shared: &'static Shared,
) {
    loop {
        let stream = conns.recv().await;
        let conn = TcpConn::new(stream, shared.exec).with_read_cap(READ_CAP);
        // Een verbinding die eindigt met een termijn of een reset is een
        // client die wegging; dat is geen logregel waard.
        let _ = leanhttp::serve(conn, async |ex: &mut Exchange<'_, TcpConn>| {
            count_request();
            http::exchange(ex, &shared.server, |op| ask(i, op, &mut answers)).await
        })
        .await;
        if let Some(b) = BUSY.get().get(i) {
            b.set(false);
        }
    }
}

/// Stuurt `op` naar de store-taak en wacht op de uitkomst.
async fn ask(
    worker: usize,
    op: Op,
    answers: &mut Receiver<'static, Done, 1>,
) -> Result<Done, Unanswered> {
    ASKS.try_send(Ask { worker, op })
        .map_err(|_| Unanswered::Busy)?;
    Ok(answers.recv().await)
}

/// Telt een verzoek, en logt er één regel over om de [`LOG_EVERY`].
fn count_request() {
    let n = REQUESTS.fetch_add(1, Relaxed).wrapping_add(1);
    if n.is_multiple_of(LOG_EVERY) {
        let st = applib::heap::HEAP.stats();
        log!(
            "hoplockserver: requests={n} heap_used={} heap_peak={} HOPOS_HOPLOCK_REQUESTS",
            st.used,
            st.peak
        );
    }
}
