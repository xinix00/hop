//! Hop als bewoner van HopOS: de binary die de kern in het slot met de bevoegdheid start.
//!
//! Bij start: de netstack (`appnet::up`), de config uit de env van het slot
//! (`HOPOS_*`, zie `agentd_hopos::env`), de system-client naar de kern, en
//! de [`Node`]: agent, leader (standalone, of via de verkiezing in een
//! cluster: `HOPOS_LOCK_*`, zie `agentd_hopos::lock`) en de
//! HopOS-runner. Daarna drie soorten taken op de executor van de app-core:
//!
//! - de eigenaar: bezit de [`Node`], handelt de verzoeken uit de [`Hub`]
//!   af en tikt elke seconde. Wat de node bij de kern vraagt (een slot, het
//!   image, een status, de staat op hopfs) en de download van een artifact
//!   zijn `.await`s: de eigenaar geeft dan de core terug, en de netstack en
//!   de verbindingstaken draaien door. Er wordt nergens een executor-ronde
//!   binnen een taak gedraaid (handboek §4);
//! - per poort één acceptor en een vaste pool van [`WORKERS`] werkers
//!   (agent op P, leader op P + 1000): de acceptor geeft elke verbinding als
//!   waarde aan een vrije werker ([`Handoff`]); de werker draait leanhttp
//!   en stuurt elk verzoek als bericht naar de eigenaar. Een open stroom
//!   (`/v1/events`, een log-tail) houdt zijn werker vast en vraagt de
//!   eigenaar elke halve seconde wat er bij kwam.
//!
//! - de klok: SNTP bij de start en elk uur (`pool.ntp.org`), de tijd naar
//!   de kern met `SET_CLOCK`. Pas daarna vertrouwt de downloader de
//!   wandklok voor `https` (een keten heeft een datum nodig).
//!
//! Artifacts komen over `http://` en `https://` (`agentd_hopos::fetch`):
//! TLS met de Mozilla-wortels, hostnamen via de resolver. Bij een schone
//! boot (niets overgenomen uit de bewaarde staat) zaait de node de
//! init-jobs uit `HOPOS_INIT_JOBS` of `/hop/init-jobs.json`.
//!
//! Markers op het log: `HOP_UP`, `HOP_LEADER`, `HOP_JOB_PLACED slot=N`,
//! `HOP_CLOCK_SYNCED`, `HOP_INIT_SEEDED`, en de weigeringen en degradaties
//! `HOPOS_API_NO_AUTH`, `HOPOS_API_INSECURE`, `HOP_NET_FAIL`,
//! `HOP_SNTP_FAIL`, `HOP_CLOCK_FAIL`, `HOP_TLS_ENTROPY_WEAK`, `HOP_TLS_ENTROPY_HW`,
//! `HOP_INIT_FAIL`. In een cluster ook `HOP_CLUSTER`, `HOP_CLUSTER_JOIN`,
//! `HOP_LEADER_LOADING`, `HOP_STATE_LOADED`, `HOP_LEADER_STOPPED`, en de
//! weigeringen `HOP_LOCK_BAD`, `HOP_CLUSTER_NO_CLOCK`, `HOP_LEASE_STORE`,
//! `HOP_LINK_FAIL`, `HOP_RELAY_REFUSED`, `HOP_RELAY_FULL`,
//! `HOP_STATE_SAVE_FAIL`.
//!
//! Canoniek gelinkt (applib/link.ld via build.rs), zoals appspike.
//!
//! Op QEMU: `tools/qemu-test-hop.sh` in de HopOS-repo boot de kern met deze
//! ELF in slot 1 (env, token, wandklok, poorten 8080 en 9080 doorgezet),
//! stuurt van buiten een jobspec en eist appspike in slot 2 (29-09 groen).
//! Wat daar nog niet is: health probes. De cluster over het LAN toetst
//! `tools/qemu-test-cluster.sh` in deze repo. Zonder DNS-server in de env
//! en zonder internet faalt de SNTP-stap met één regel (`HOP_SNTP_FAIL`) en
//! draait Hop door; `http://` naar een adres werkt dan gewoon.

#![cfg_attr(target_os = "none", no_std, no_main)]

extern crate alloc;

use alloc::boxed::Box;
use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use core::future::{Future, poll_fn};
use core::pin::pin;
use core::sync::atomic::{AtomicBool, Ordering::Relaxed};
use core::task::Poll;
use core::time::Duration;

use agentd_hopos::client::{Client, Idle};
use agentd_hopos::entropy::{HARVEST_ROUNDS, Pool};
use agentd_hopos::env::INIT_JOBS_FILE;
use agentd_hopos::forward::{self, Routed, STREAM_IDLE};
use agentd_hopos::lock::{self, ClusterConfig};
use agentd_hopos::mail::{DispatchQueue, Inbox, LeaseQueue, LinkQueue, Mail, Nap, StateQueue};
use agentd_hopos::sntp::{self, NtpLink, PACKET};
use agentd_hopos::{
    Answer, BootConfig, Clock, ClusterParts, Connect, Handoff, HttpImages, Hub, Images, Node, Port,
    Question, Resolve,
};
use applib::appnet::{self, Endpoint, Net, NetError, TcpListener, TcpStream};
use applib::rand::{Origin, Rng};
use applib::rt::Exec;
use applib::{App, EXEC, log};
use discovery::Discovery;
use hop_http::{Ask, Chunk, Streams, TcpConn};
use hopos_runner::KernSys;
use runner::SystemApi;
use sync::mpsc::Mailbox;

applib::main!(resident);

/// Op de host bestaat deze bewoner niet: daar is dit een lege binary, zodat
/// de host-poort (clippy `--all-targets`) hem typecheckt zonder de
/// allocator en de paniekhaak van het slot.
#[cfg(not(target_os = "none"))]
fn main() {}

/// Het ritme van de eigenaar-taak: de tik van agent en leader.
const TICK: Duration = Duration::from_secs(1);

/// De langste stilte op een verbinding: een pool van [`WORKERS`] per poort,
/// dus een keep-alive-client mag een werker niet lang ophouden. Zijn alle
/// werkers bezet, dan wacht een nieuwe verbinding niet eens zo lang: de
/// werker sluit zijn verbinding dan na het antwoord
/// (`Handoff::none_free`, zie `forward::serve`).
const READ_CAP: Duration = Duration::from_secs(2);

/// Werkers per poort. Een open stroom houdt er een vast; met hoogstens drie
/// stromen per node (`MAX_STREAMS` in de node) houdt elke poort er minstens
/// één vrij voor de CLI en de GUI.
const WORKERS: usize = 4;

/// Hoe vaak de acceptor kijkt of er een werker vrij is als ze alle bezig
/// zijn. Een koud pad (een vierde gelijktijdige verbinding), dus pollen is
/// eenvoudiger dan een bel.
const BUSY_POLL: Duration = Duration::from_millis(5);

/// Hoe lang een verbinding naar een artifact-server mag duren.
const DIAL_TIMEOUT: Duration = Duration::from_secs(10);

/// Hoe lang één SNTP-vraag op antwoord wacht (Go: 3 s).
const NTP_TIMEOUT: Duration = Duration::from_secs(3);

/// Het ritme van SNTP na een gelukte synchronisatie (Go: elk uur).
const NTP_EVERY: Duration = Duration::from_secs(3600);

/// Na een mislukte synchronisatie eerder opnieuw: een node die boot voor
/// zijn uplink er is, hoeft geen uur op een klok te wachten.
const NTP_RETRY: Duration = Duration::from_secs(300);

/// Zoveel mislukkingen op rij krijgen een eigen regel; daarna één per
/// twaalf (eens per uur bij [`NTP_RETRY`]).
const NTP_LOUD: u32 = 3;

/// Het grootste init-jobs-bestand dat de bewoner leest.
const INIT_FILE_MAX: usize = 64 << 10;

/// Of SNTP in deze boot gelukt is: pas dan is de wandklok van de kern
/// vertrouwd genoeg voor een certificaatdatum. Eén schrijver (de
/// kloktaak), gelezen door de downloader; een vlag, geen protocol.
static CLOCK_SYNCED: AtomicBool = AtomicBool::new(false);

/// De rijen van de cluster (zie `agentd_hopos::mail`): statisch, want de
/// taken leven zo lang als de bewoner. Standalone blijven ze leeg.
static LEASE_Q: LeaseQueue = Mailbox::new();
/// De rij naar de link-taak.
static LINK_Q: LinkQueue = Mailbox::new();
/// De rij naar de staat-taak.
static STATE_Q: StateQueue = Mailbox::new();
/// De rij naar de dispatch-taak.
static DISPATCH_Q: DispatchQueue = Mailbox::new();
/// De brievenbus van de eigenaar.
static INBOX: Inbox = Mailbox::new();

/// Hoe lang een aanroep van de cluster (lease, staat, link, dispatch) na
/// zijn kop mag zwijgen: een lease of snapshot is klein.
const CLUSTER_IDLE: Duration = Duration::from_secs(20);

/// Nu in milliseconden op de wandklok van de kern: de lease vergelijkt
/// tijden van verschillende nodes. Zonder wandklok de monotone klok (en de
/// bewoner zegt `HOP_NO_CLOCK`).
fn wall_ms() -> u64 {
    applib::app()
        .and_then(App::wall_ns)
        .unwrap_or_else(applib::clock::now_ns)
        / 1_000_000
}

/// Nu in Unix-seconden: de klok van de S3-handtekening.
fn wall_secs() -> u64 {
    wall_ms() / 1000
}

/// Of SNTP in deze boot lukte: pas dan doet een geclusterde node mee.
fn clock_ok() -> bool {
    CLOCK_SYNCED.load(Relaxed)
}

/// Nu in Unix-seconden, alleen na SNTP: de datum van een certificaatketen.
fn trusted_secs() -> Option<u64> {
    CLOCK_SYNCED.load(Relaxed).then(wall_secs)
}

/// TCP-verbindingen van de cluster: een aanroep die na zijn kop zwijgt,
/// wordt na `idle` gesloten (`agentd_hopos::client::Idle`).
#[derive(Copy, Clone)]
struct ClusterConnect {
    net: &'static Net,
    exec: &'static Exec,
    idle: Duration,
}

impl Connect for ClusterConnect {
    type Conn = Idle<TcpConn>;

    async fn connect(&mut self, ip: [u8; 4], port: u16) -> Result<Self::Conn, String> {
        let s = self
            .net
            .tcp_connect_timeout(ip, port, DIAL_TIMEOUT)
            .await
            .map_err(|e| format!("{e}"))?;
        Ok(Idle::new(TcpConn::new(s, self.exec), self.idle))
    }
}

/// Slapen op het timerwiel van de app-core, voor de taken van de cluster.
#[derive(Copy, Clone)]
struct ExecNap(&'static Exec);

impl Nap for ExecNap {
    async fn nap(&self, d: Duration) {
        self.0.after(d).await;
    }
}

/// TCP-verbindingen naar artifact-servers over de netstack van het slot.
#[derive(Copy, Clone)]
struct SlotConnect {
    net: &'static Net,
    exec: &'static Exec,
}

impl Connect for SlotConnect {
    type Conn = TcpConn;

    async fn connect(&mut self, ip: [u8; 4], port: u16) -> Result<TcpConn, String> {
        let s = self
            .net
            .tcp_connect_timeout(ip, port, DIAL_TIMEOUT)
            .await
            .map_err(|e| format!("{e}"))?;
        Ok(TcpConn::new(s, self.exec))
    }
}

/// De resolver van de bewoner: `applib::appnet::resolve`, één A-vraag over
/// UDP naar `DNS` uit de env.
#[derive(Copy, Clone, Default)]
struct SlotResolver;

impl Resolve for SlotResolver {
    async fn resolve(&mut self, host: &str) -> Result<[u8; 4], String> {
        // Een naam die al een IP is gaat er meteen doorheen, de rest via de
        // DNS uit de env.
        applib::appnet::resolve(host)
            .await
            .map_err(|e| alloc::format!("{e}"))
    }
}

/// De klok van de downloader: de wandklok van de kern, maar alleen
/// vertrouwd na SNTP.
struct SlotClock {
    app: &'static App,
    exec: &'static Exec,
}

impl Clock for SlotClock {
    fn trusted_unix_secs(&self) -> Option<u64> {
        if !CLOCK_SYNCED.load(Relaxed) {
            return None;
        }
        self.app.wall_ns().map(|ns| ns / 1_000_000_000)
    }

    fn mono_ns(&self) -> u64 {
        self.exec.now()
    }
}

/// SNTP over een UDP-socket van het slot, naar `port` van de server.
struct UdpLink {
    net: &'static Net,
    port: u16,
}

impl NtpLink for UdpLink {
    async fn exchange(
        &mut self,
        server: [u8; 4],
        req: &[u8; PACKET],
        resp: &mut [u8],
    ) -> Result<usize, String> {
        let mut sock = self.net.udp_bind(0).map_err(|e| format!("bind: {e}"))?;
        sock.set_timeout(Some(NTP_TIMEOUT));
        let to = Endpoint {
            ip: server,
            port: self.port,
        };
        sock.send_to(to, req)
            .await
            .map_err(|e| format!("send: {e}"))?;
        loop {
            match sock.recv_from(resp).await {
                Ok((n, from)) if from == to => return Ok(n),
                // Een datagram van een ander adres is niet het antwoord.
                Ok(_) => {}
                Err(NetError::Timeout) => {
                    return Err(format!("no answer within {} s", NTP_TIMEOUT.as_secs()));
                }
                Err(e) => return Err(format!("recv: {e}")),
            }
        }
    }
}

/// De kloktaak: SNTP bij de start en elk uur, de tijd naar de kern met
/// `SET_CLOCK`. Luid als het niet lukt; de node draait door, en `https`
/// wacht op een vertrouwde klok.
///
/// Een eigen system-verbinding per synchronisatie, die daarna weer dichtgaat:
/// de kern laat een slot twee verbindingen toe, en de eigenaar-taak houdt
/// de eerste; een uur lang een tweede openhouden voor één call is zonde.
async fn clock_task(net: &'static Net, exec: &'static Exec, server: String, port: u16) {
    let mut fails: u32 = 0;
    let mut seq: u64 = 0;
    loop {
        let mut link = UdpLink { net, port };
        let got = sntp::sync(
            &mut SlotResolver,
            &mut link,
            &server,
            || exec.now(),
            || {
                seq = seq.wrapping_add(1);
                exec.now() ^ seq.rotate_left(48)
            },
        )
        .await;
        let wait = match got {
            Ok(sample) => {
                let mut sys = KernSys::new(net.system_client(), 1);
                let unix_ns = sample.unix_at(exec.now());
                match sys.set_clock(unix_ns).await {
                    Ok(()) => {
                        CLOCK_SYNCED.store(true, Relaxed);
                        fails = 0;
                        log!(
                            "hop: clock set from {server} (stratum {}, delay {} us) to {} s HOP_CLOCK_SYNCED",
                            sample.stratum,
                            sample.delay_ns / 1000,
                            unix_ns / 1_000_000_000
                        );
                        NTP_EVERY
                    }
                    Err(e) => {
                        log!("hop: SET_CLOCK refused by the kernel: {e} HOP_CLOCK_FAIL");
                        NTP_RETRY
                    }
                }
            }
            Err(e) => {
                fails = fails.saturating_add(1);
                if fails <= NTP_LOUD || fails.is_multiple_of(12) {
                    log!(
                        "hop: SNTP {server}:{port} failed ({fails}x): {e}; the wall clock is not synced, https downloads wait for it HOP_SNTP_FAIL"
                    );
                }
                NTP_RETRY
            }
        };
        exec.after(wait).await;
    }
}

/// De object-store van de apps (`agentd_hopos::objstore`): haalt de
/// store-calls van de apps op bij de kern (`NEXT_STORE`, een lange wacht),
/// doet de S3-kant en meldt af. Een eigen system-verbinding, blijvend: de
/// kern geeft het slot van Hop er één meer dan een app (`MAX_HOP_CONNS`).
/// Zonder `HOPOS_S3_*` weigert hij elke call luid, zodat een app hoort
/// waarom zijn pull faalt.
async fn store_task(
    app: &'static App,
    net: &'static Net,
    exec: &'static Exec,
    s3: Option<agentd_hopos::env::S3Config>,
    cluster: String,
) {
    use agentd_hopos::objstore::{Service, bucket};
    let bucket = s3.map(|c| {
        let cfg = bucket::Config {
            endpoint: c.endpoint,
            bucket: c.bucket,
            region: c.region,
            key: c.key,
            secret: c.secret,
            path_style: c.path_style,
        };
        // Een eigen pool: de handshakes van de store delen geen staat met
        // die van de downloader (de waarschuwing over de bron gaf entropy()).
        let mut pool = Pool::new(&app.slot().to_le_bytes());
        pool.stir(&exec.now().to_le_bytes());
        pool.harvest(applib::clock::now_ns, HARVEST_ROUNDS);
        kernel_seed(app, &mut pool);
        let b = bucket::S3Bucket::new(
            &cfg,
            SlotConnect { net, exec },
            SlotResolver,
            pool,
            wall_secs,
            trusted_secs,
        );
        if b.is_plain_http() {
            log!(
                "hop: object store {} is plain http: requests are signed but readable on the way; use https outside a test HOP_STORE_PLAIN_HTTP",
                cfg.endpoint
            );
        }
        log!(
            "hop: object store for apps: {} bucket {} under apps/{cluster}/ HOP_STORE_UP",
            cfg.endpoint,
            cfg.bucket
        );
        b
    });
    if bucket.is_none() {
        log!(
            "hop: no object store configured (hopos.s3.*); app store calls are refused HOP_STORE_NONE"
        );
    }
    let mut svc = Service::new(bucket, &cluster);
    let mut sys = KernSys::new(net.system_client(), 1);
    let mut refused: u32 = 0;
    loop {
        match svc.serve_one(&mut sys).await {
            // Een pull van iets dat er niet is, is een vraag, geen fout.
            Ok(Some(o))
                if !matches!(
                    o.status,
                    runner::StoreStatus::Ok | runner::StoreStatus::NotFound
                ) =>
            {
                log!(
                    "hop: store {} {} for slot {}: {} HOP_STORE_FAIL",
                    o.task.op.name(),
                    o.task.key,
                    o.task.slot.0,
                    o.why
                )
            }
            Ok(Some(o)) if !o.delivered => log!(
                "hop: store {} {} for slot {}: the task was gone, result dropped HOP_STORE_GONE",
                o.task.op.name(),
                o.task.key,
                o.task.slot.0
            ),
            Ok(_) => refused = 0,
            Err(e) => {
                // Een kern zonder rij of zonder verbinding: luid, de eerste
                // paar keer, dan eens per minuut.
                refused = refused.saturating_add(1);
                if refused <= 3 || refused.is_multiple_of(60) {
                    log!("hop: NEXT_STORE failed ({refused}x): {e} HOP_STORE_KERNEL");
                }
                exec.after(Duration::from_secs(1)).await;
            }
        }
    }
}

/// De willekeur voor TLS: het slot, de klok, de jitter van de teller, en
/// het zaad dat de kern op de control-page legt (`applib::rand`). Eén
/// regel over de bron: `HOP_TLS_ENTROPY_HW` als dat zaad uit een
/// hardware-RNG komt, anders `HOP_TLS_ENTROPY_WEAK` met de reden.
fn entropy(app: &App, exec: &'static Exec) -> Pool {
    let mut seed = Vec::new();
    seed.extend_from_slice(&app.slot().to_le_bytes());
    seed.extend_from_slice(&app.wall_ns().unwrap_or(0).to_le_bytes());
    seed.extend_from_slice(&exec.now().to_le_bytes());
    let mut pool = Pool::new(&seed);
    pool.harvest(applib::clock::now_ns, HARVEST_ROUNDS);
    match kernel_seed(app, &mut pool) {
        o if o.is_hardware() => log!(
            "hop: TLS randomness from the kernel seed ({o}, hardware) mixed with timer jitter ({HARVEST_ROUNDS} samples) HOP_TLS_ENTROPY_HW source={o}"
        ),
        Origin::None => log!(
            "hop: TLS randomness from timer jitter only ({HARVEST_ROUNDS} samples): the kernel put no seed on the control page (an older kernel) HOP_TLS_ENTROPY_WEAK"
        ),
        o => log!(
            "hop: TLS randomness from the kernel seed and timer jitter ({HARVEST_ROUNDS} samples), but the kernel seeds itself from {o}: this node has no hardware RNG HOP_TLS_ENTROPY_WEAK"
        ),
    }
    pool
}

/// Mengt 32 bytes uit de DRBG van applib (het zaad van de kern plus
/// jitter) in `pool` en geeft de bron van dat zaad.
fn kernel_seed(app: &App, pool: &mut Pool) -> Origin {
    let mut rng = Rng::open(app);
    let mut b = rng.array::<32>();
    pool.stir(&b);
    b.fill(0);
    rng.origin()
}

/// Een eigen pool voor een client van de cluster (`tag` maakt hem anders
/// dan die van de downloader); dezelfde bron, zonder de regel opnieuw.
fn quiet_pool(app: &App, exec: &'static Exec, tag: u8) -> Pool {
    let mut seed = Vec::new();
    seed.push(tag);
    seed.extend_from_slice(&app.slot().to_le_bytes());
    seed.extend_from_slice(&exec.now().to_le_bytes());
    let mut pool = Pool::new(&seed);
    pool.harvest(applib::clock::now_ns, HARVEST_ROUNDS);
    kernel_seed(app, &mut pool);
    pool
}

/// Een HTTP-client van de cluster over de netstack van het slot.
fn cluster_client(
    app: &App,
    net: &'static Net,
    exec: &'static Exec,
    tag: u8,
    idle: Duration,
) -> Client<ClusterConnect, SlotResolver> {
    Client::new(
        ClusterConnect { net, exec, idle },
        SlotResolver,
        quiet_pool(app, exec, tag),
        trusted_secs,
    )
}

/// Start de cluster: de boot-claim, en de taken van lease, staat, link en
/// dispatch. Geeft de onderdelen voor [`Node::new_clustered`].
async fn start_cluster(
    app: &'static App,
    net: &'static Net,
    exec: &'static Exec,
    cfg: &BootConfig,
    cc: ClusterConfig,
    init_jobs: Option<String>,
) -> Result<ClusterParts, String> {
    let owner = format!("{}:{}", cfg.node_ip, cfg.leader_port());
    let disc = Discovery::new(owner, cc.ttl_ms);
    // De boot-claim doet de node zelf zodra de klok gezet is (een lease is
    // een tijd op de wandklok), via de lease-taak: `Node::cluster_boot`.
    let lease = lock::open_lease(
        &cc,
        cluster_client(app, net, exec, 1, CLUSTER_IDLE),
        wall_secs,
    );
    let state = lock::open_state(
        &cc,
        cluster_client(app, net, exec, 2, CLUSTER_IDLE),
        wall_secs,
    );
    log!(
        "hop: cluster state in {} HOP_STATE_STORE",
        lock::StateBackend::describe(&state)
    );
    let nap = ExecNap(exec);
    let key = cfg.api_key.clone();
    exec.spawn(agentd_hopos::lease::lease_task(
        disc, lease, &LEASE_Q, &INBOX, wall_ms, nap,
    ))
    .map_err(|e| format!("lease task: {e}"))?;
    exec.spawn(agentd_hopos::lease::state_task(
        state, &STATE_Q, &INBOX, nap,
    ))
    .map_err(|e| format!("state task: {e}"))?;
    exec.spawn(agentd_hopos::link::link_task(
        cluster_client(app, net, exec, 3, CLUSTER_IDLE),
        key.clone(),
        &LINK_Q,
        &INBOX,
        nap,
    ))
    .map_err(|e| format!("link task: {e}"))?;
    exec.spawn(agentd_hopos::relay::dispatch_task(
        cluster_client(app, net, exec, 4, CLUSTER_IDLE),
        key,
        &DISPATCH_Q,
        &INBOX,
        nap,
    ))
    .map_err(|e| format!("dispatch task: {e}"))?;
    Ok(ClusterParts {
        cfg: cc,
        clock_ok,
        lease: &LEASE_Q,
        link: &LINK_Q,
        state: &STATE_Q,
        dispatch: &DISPATCH_Q,
        wall_ms,
        init_jobs,
        node_dead: config::Config::default().timeouts.node_dead_threshold,
    })
}

/// Wacht op een verzoek in de bus, een bericht van de cluster, of de tik;
/// het bericht als dat het eerst kwam.
async fn wake(hub: &Hub, inbox: &Inbox, tick: impl Future<Output = ()>) -> Option<Mail> {
    let mut a = pin!(hub.wait());
    let mut b = pin!(inbox.recv());
    let mut c = pin!(tick);
    poll_fn(|cx| {
        if let Poll::Ready(m) = b.as_mut().poll(cx) {
            return Poll::Ready(Some(m));
        }
        if a.as_mut().poll(cx).is_ready() || c.as_mut().poll(cx).is_ready() {
            return Poll::Ready(None);
        }
        Poll::Pending
    })
    .await
}

/// De init-jobs van deze boot: uit de env, of anders uit
/// [`INIT_JOBS_FILE`] in het volume. `None` als er geen zijn.
async fn init_specs(cfg: &BootConfig, net: &'static Net) -> Option<String> {
    if let Some(j) = &cfg.init_jobs {
        return Some(j.clone());
    }
    let mut c = net.system_client();
    let size = match c.stat(INIT_JOBS_FILE).await {
        Ok(s) => usize::try_from(s).unwrap_or(usize::MAX),
        Err(applib::sys::Error::NotFound { .. }) => return None,
        Err(e) => {
            log!("hop: {INIT_JOBS_FILE}: {e}; no init jobs HOP_INIT_FAIL");
            return None;
        }
    };
    if size > INIT_FILE_MAX {
        log!(
            "hop: {INIT_JOBS_FILE} is {size} bytes, limit {INIT_FILE_MAX}; no init jobs HOP_INIT_FAIL"
        );
        return None;
    }
    let mut buf = alloc::vec![0u8; size];
    let mut at = 0;
    while at < size {
        match c
            .read_into(
                INIT_JOBS_FILE,
                at as u64,
                buf.get_mut(at..).unwrap_or_default(),
            )
            .await
        {
            Ok(0) => break,
            Ok(n) => at += n,
            Err(e) => {
                log!("hop: {INIT_JOBS_FILE}: {e}; no init jobs HOP_INIT_FAIL");
                return None;
            }
        }
    }
    buf.truncate(at);
    match String::from_utf8(buf) {
        Ok(s) => Some(s),
        Err(_) => {
            log!("hop: {INIT_JOBS_FILE} is not UTF-8; no init jobs HOP_INIT_FAIL");
            None
        }
    }
}

/// Nu in Unix-nanoseconden: de wandklok van de kern, of de monotone klok als die er (nog) niet is.
fn now(app: &App, exec: &Exec) -> u64 {
    app.wall_ns().unwrap_or_else(|| exec.now())
}

/// Zet de regels van de node op het log.
fn flush<S, I>(node: &mut Node<S, I>)
where
    S: runner::SystemApi,
    I: Images,
{
    for line in node.take_lines() {
        log!("{line}");
    }
}

/// De acceptor van één poort: elke verbinding als waarde naar een vrije werker.
///
/// De listener is al gebonden vóór de spawn: zodra `HOP_UP` op het log
/// staat, neemt de stack een SYN aan, ook als deze taak nog geen ronde had.
async fn accept(
    listener: TcpListener,
    exec: &'static Exec,
    pool: &'static Handoff<TcpStream>,
    number: u16,
) {
    loop {
        let mut stream = match listener.accept().await {
            Ok(s) => s,
            Err(e) => {
                log!("hop: accept on :{number}: {e}");
                exec.after(Duration::from_millis(100)).await;
                continue;
            }
        };
        // Alle werkers bezig: kort wachten tot er een vrijkomt.
        while let Err(back) = pool.give(stream) {
            stream = back;
            exec.after(BUSY_POLL).await;
        }
    }
}

/// De naden van een open stroom voor één werker: de bus naar de eigenaar
/// en het timerwiel van de app-core.
struct HubStreams {
    hub: &'static Hub,
    exec: &'static Exec,
    slot: usize,
}

impl Streams for HubStreams {
    async fn poll(&mut self, ask: Ask) -> Chunk {
        self.hub.poll(self.slot, ask).await
    }

    async fn nap(&mut self, d: Duration) {
        self.exec.after(d).await;
    }

    fn now(&self) -> u64 {
        self.exec.now()
    }

    fn done(&mut self) {
        self.hub.stream_done(self.slot);
    }
}

/// Eén werker: wacht op een verbinding, leanhttp, elk verzoek als bericht
/// naar de eigenaar met vak `slot` in de bus. Een doorgifte naar een andere
/// node (de leader, of een agent als deze node leidt) voert de werker zelf
/// uit met zijn eigen client (`agentd_hopos::forward`).
async fn work(
    pool: &'static Handoff<TcpStream>,
    i: usize,
    exec: &'static Exec,
    hub: &'static Hub,
    slot: usize,
    port: Port,
    mut client: Client<ClusterConnect, SlotResolver>,
) {
    let mut streams = HubStreams { hub, exec, slot };
    loop {
        let stream = pool.take(i).await;
        let conn = TcpConn::new(stream, exec).with_read_cap(READ_CAP);
        // Een verbinding die eindigt met een termijn of een reset is gewoon
        // een client die wegging; dat is geen logregel waard.
        let _ = forward::serve(
            conn,
            async |req| hub.ask_routed(slot, port, req).await,
            &mut streams,
            &mut client,
            port,
            || pool.none_free(),
        )
        .await;
        pool.free(i);
    }
}

/// Bindt een poort, of zegt luid waarom niet.
fn bind(net: &'static Net, number: u16) -> Option<TcpListener> {
    match net.tcp_listen(number) {
        Ok(l) => Some(l),
        Err(e) => {
            log!("hop: cannot listen on :{number}: {e} HOP_LISTEN_FAIL");
            None
        }
    }
}

async fn resident(app: &'static App) {
    let exec: &'static Exec = EXEC.get();
    log!("hop: resident starting in slot {} HOP_BOOT", app.slot());
    let net = match appnet::up(app) {
        Ok(n) => n,
        Err(e) => {
            log!("hop: no network, no API: {e} HOP_NET_FAIL");
            return;
        }
    };
    let [a, b, c, d] = net.ip();
    let slot_ip = format!("{a}.{b}.{c}.{d}");
    let cfg = match BootConfig::from_env(|k| app.env(k).map(String::from), app.slot(), &slot_ip) {
        Ok(c) => c,
        Err(e) => {
            // Fail closed, zoals de Go-kern: de bewoner blijft leven (de
            // heartbeat loopt) maar opent geen API.
            log!("hop: REFUSING to start agent/leader: {e} HOPOS_API_NO_AUTH");
            core::future::pending::<()>().await;
            return;
        }
    };
    if cfg.insecure {
        log!("hop: WARNING API authentication is OFF (HOPOS_INSECURE=1) HOPOS_API_INSECURE");
    }
    if cfg.insecure_ignored {
        log!("hop: HOPOS_INSECURE=1 ignored: HOPOS_APIKEY is set, the API authenticates");
    }
    // De lock van de cluster (`HOPOS_LOCK_URL`, of `HOPOS_LOCK_TYPE=s3`); zonder
    // blijft de node de standalone leader. Een lock die niet klopt, is een
    // weigering zoals een ontbrekende sleutel: een node die stil standalone
    // draait naast zijn cluster, is een tweede leader.
    let cluster_cfg = match ClusterConfig::from_env(
        |k| app.env(k).map(String::from),
        &cfg.cluster,
        cfg.s3.as_ref(),
    ) {
        Ok(c) => c,
        Err(e) => {
            log!("hop: REFUSING to start agent/leader: {e} HOP_LOCK_BAD");
            core::future::pending::<()>().await;
            return;
        }
    };
    if cfg.memory_defaulted {
        log!(
            "hop: HOPOS_MEMORY not set; planning against {} bytes HOP_MEMORY_DEFAULT",
            cfg.memory
        );
    }
    if app.wall_ns().is_none() {
        log!("hop: no wall clock from the kernel; task times count from boot HOP_NO_CLOCK");
    }

    // De tijdserver: `HOPOS_NTP` (`host` of `host:poort`), anders pool.ntp.org.
    let (ntp_host, ntp_port) = sntp::server_from(app.env(sntp::ENV_NTP));
    if let Err(e) = exec.spawn(clock_task(net, exec, ntp_host, ntp_port)) {
        log!("hop: cannot spawn the clock task: {e}; no SNTP, https refused HOP_SNTP_FAIL");
    }
    if let Err(e) = exec.spawn(store_task(
        app,
        net,
        exec,
        cfg.s3.clone(),
        cfg.cluster.clone(),
    )) {
        log!(
            "hop: cannot spawn the store task: {e}; app store calls wait and fail HOP_STORE_KERNEL"
        );
    }

    let sys = KernSys::new(net.system_client(), cfg.cores);
    let images = HttpImages::new(
        SlotConnect { net, exec },
        SlotResolver,
        SlotClock { app, exec },
        entropy(app, exec),
    );
    if images.root_count() == 0 {
        log!("hop: the built-in root certificates did not parse; https refused HOP_TLS_NO_ROOTS");
    }
    // Het adres zoals de andere nodes deze node zien (achter een NAT anders
    // dan het slot-adres); de listeners blijven op de poorten van `cfg`.
    let seen = match lock::advertised(&cfg, |k| app.env(k).map(String::from)) {
        Ok(c) => c,
        Err(e) => {
            log!("hop: REFUSING to start agent/leader: {e} HOP_LOCK_BAD");
            core::future::pending::<()>().await;
            return;
        }
    };
    let clustered = cluster_cfg.is_some();
    let mut node = match cluster_cfg {
        None => Node::new(&seen, sys, images, now(app, exec)),
        Some(cc) => {
            // Geclusterd: de init-jobs zaait wie leider wordt op een schone
            // clusterstaat, niet de boot van deze node.
            let specs = init_specs(&cfg, net).await;
            match start_cluster(app, net, exec, &seen, cc, specs).await {
                Ok(parts) => Node::new_clustered(&seen, sys, images, now(app, exec), parts),
                Err(e) => {
                    log!("hop: cannot start the cluster: {e} HOP_SPAWN_FAIL");
                    return;
                }
            }
        }
    };
    // Hop begint leeg: zijn staat komt uit de object-store (geclusterd) of
    // uit de init-jobs, nooit uit een bestand op hopfs. Wat de kern nog aan
    // bewoners heeft, is van niemand.
    node.sweep_strays().await;
    flush(&mut node);
    if clustered {
        // De boot-claim: raak, dan leidt deze node nu (de staat laadt eerst).
        node.cluster_boot(now(app, exec));
        flush(&mut node);
    }
    if !clustered && let Some(specs) = init_specs(&cfg, net).await {
        if let Err(e) = node.seed_init_jobs(&specs, now(app, exec)).await {
            log!("hop: init jobs not seeded: {e} HOP_INIT_FAIL");
        }
        flush(&mut node);
    }

    // Eén bus voor het leven van de bewoner, met een vak per werker; de
    // taken krijgen `&'static`.
    let hub: &'static Hub = Box::leak(Box::new(Hub::new(2 * WORKERS)));
    // De listeners binden hier, vóór de spawn: een spawn krijgt zijn slot pas
    // in de volgende ronde van de executor, en zo hoeft niemand daarop te
    // wachten. Een poort die niet bindt, is luid en kost alleen die poort.
    let ports = [
        (0, Port::Agent, cfg.port),
        (WORKERS, Port::Leader, cfg.leader_port()),
    ];
    for (first_slot, port, number) in ports {
        let Some(l) = bind(net, number) else {
            continue;
        };
        let pool: &'static Handoff<TcpStream> = Box::leak(Box::new(Handoff::new(WORKERS)));
        let mut spawned = exec.spawn(accept(l, exec, pool, number));
        for i in 0..WORKERS {
            if spawned.is_err() {
                break;
            }
            let tag = u8::try_from(16 + first_slot + i).unwrap_or(u8::MAX);
            let client = cluster_client(app, net, exec, tag, STREAM_IDLE);
            spawned = exec.spawn(work(pool, i, exec, hub, first_slot + i, port, client));
        }
        if let Err(e) = spawned {
            log!("hop: cannot spawn the listeners: {e} HOP_SPAWN_FAIL");
            return;
        }
    }
    log!(
        "hop: agent up node={} cluster={} agent=:{} leader=:{} cores={} system={} HOP_UP",
        cfg.node_id,
        cfg.cluster,
        cfg.port,
        cfg.leader_port(),
        cfg.cores,
        u8::from(cfg.system_core)
    );

    let tick_ns = u64::try_from(TICK.as_nanos()).unwrap_or(u64::MAX);
    let mut next_tick = now(app, exec);
    loop {
        // Eerst de bus leeg (level-triggered): wat binnenkwam terwijl de
        // eigenaar op de kern wachtte, ligt er nog en wordt nu afgehandeld.
        while let Some((slot, q)) = hub.next() {
            match q {
                Question::Http(port, req) => {
                    let answer = match node.handle_routed(port, &req, now(app, exec)).await {
                        Routed::Reply(r) => Answer::Reply(r),
                        Routed::Forward(f) => Answer::Forward(f),
                    };
                    hub.answer(slot, answer);
                }
                Question::Poll(ask) => {
                    let chunk = node.poll(&ask, now(app, exec));
                    hub.answer(slot, Answer::Chunk(chunk));
                }
                Question::StreamDone => node.stream_done(),
            }
            flush(&mut node);
        }
        // De antwoorden van de taken van de cluster (lease, link, staat,
        // dispatch); standalone blijft de inbox leeg.
        while let Some(m) = INBOX.try_recv() {
            node.on_mail(m, now(app, exec));
            flush(&mut node);
        }
        let t = now(app, exec);
        if t >= next_tick {
            node.tick(t).await;
            next_tick = t.saturating_add(tick_ns);
            flush(&mut node);
        }
        let wait = Duration::from_nanos(next_tick.saturating_sub(now(app, exec)));
        if let Some(m) = wake(hub, &INBOX, exec.after(wait)).await {
            node.on_mail(m, now(app, exec));
            flush(&mut node);
        }
    }
}
