//! Hop als bewoner van HopOS: de binary die de kern in het slot met de bevoegdheid start.
//!
//! Bij start: de netstack (`appnet::up`), de config uit de env van het slot
//! (`HOPOS_*`, zie `agentd_hopos::env`), de system-client naar de kern, en
//! de [`Node`]: agent, leader (standalone, zoals de Go-kern in fase 1) en de
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
//! `HOP_SNTP_FAIL`, `HOP_CLOCK_FAIL`, `HOP_TLS_ENTROPY_WEAK`,
//! `HOP_INIT_FAIL`, `HOP_S3_SKIPPED`.
//!
//! Canoniek gelinkt (applib/link.ld via build.rs), zoals appspike.
//!
//! Op QEMU: `tools/qemu-test-hop.sh` in de HopOS-repo boot de kern met deze
//! ELF in slot 1 (env, token, wandklok, poorten 8080 en 9080 doorgezet),
//! stuurt van buiten een jobspec en eist appspike in slot 2 (29-09 groen).
//! Wat daar nog niet is: health probes en S3. Zonder DNS-server in de env
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

use agentd_hopos::entropy::{HARVEST_ROUNDS, Pool};
use agentd_hopos::env::INIT_JOBS_FILE;
use agentd_hopos::sntp::{self, NtpLink, PACKET};
use agentd_hopos::{
    Answer, BootConfig, Clock, Connect, Handoff, HttpImages, Hub, Images, Node, Port, Question,
    Resolve,
};
use applib::appnet::{self, Endpoint, Net, NetError, TcpListener, TcpStream};
use applib::rt::Exec;
use applib::{App, EXEC, log};
use hop_http::{Ask, Chunk, Streams, TcpConn};
use hopos_runner::KernSys;
use runner::SystemApi;

applib::main!(resident);

/// Op de host bestaat deze bewoner niet: daar is dit een lege binary, zodat
/// de host-poort (clippy `--all-targets`) hem typecheckt zonder de
/// allocator en de paniekhaak van het slot.
#[cfg(not(target_os = "none"))]
fn main() {}

/// Het ritme van de eigenaar-taak: de tik van agent en leader.
const TICK: Duration = Duration::from_secs(1);

/// De langste stilte op een verbinding: een pool van [`WORKERS`] per poort,
/// dus een keep-alive-client mag een werker niet lang ophouden.
const READ_CAP: Duration = Duration::from_secs(2);

/// Werkers per poort. Een open stroom houdt er een vast; met hoogstens twee
/// stromen per node (`MAX_STREAMS` in de node) houdt elke poort er minstens
/// één vrij voor de CLI en de GUI.
const WORKERS: usize = 3;

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

/// De resolver van de bewoner.
///
/// TODO: HopOS v3.0.0-alpha.5 heeft nog geen resolver in applib; die zit
/// in de volgende tag (`applib::appnet::resolve`, één A-vraag over UDP naar
/// `DNS` uit de env). Tot de bump is een hostnaam een luide fout met de
/// naam erin, en werken alleen adressen. De bump is deze ene impl:
/// `appnet::resolve(host).await.map_err(|e| format!("{e}"))`.
#[derive(Copy, Clone, Default)]
struct SlotResolver;

impl Resolve for SlotResolver {
    async fn resolve(&mut self, host: &str) -> Result<[u8; 4], String> {
        // De resolver van applib (sinds HopOS alpha.6): een naam die al een
        // IP is gaat er meteen doorheen, de rest via de DNS uit de env.
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

/// SNTP over een UDP-socket van het slot.
struct UdpLink {
    net: &'static Net,
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
            port: sntp::PORT,
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
async fn clock_task(net: &'static Net, exec: &'static Exec) {
    let mut fails: u32 = 0;
    let mut seq: u64 = 0;
    loop {
        let mut link = UdpLink { net };
        let got = sntp::sync(
            &mut SlotResolver,
            &mut link,
            sntp::SERVER,
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
                            "hop: clock set from {} (stratum {}, delay {} us) to {} s HOP_CLOCK_SYNCED",
                            sntp::SERVER,
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
                        "hop: SNTP {} failed ({fails}x): {e}; the wall clock is not synced, https downloads wait for it HOP_SNTP_FAIL",
                        sntp::SERVER
                    );
                }
                NTP_RETRY
            }
        };
        exec.after(wait).await;
    }
}

/// De willekeur voor TLS: het slot, de klok, en de jitter van de teller.
fn entropy(app: &App, exec: &'static Exec) -> Pool {
    let mut seed = Vec::new();
    seed.extend_from_slice(&app.slot().to_le_bytes());
    seed.extend_from_slice(&app.wall_ns().unwrap_or(0).to_le_bytes());
    seed.extend_from_slice(&exec.now().to_le_bytes());
    let mut pool = Pool::new(&seed);
    pool.harvest(applib::clock::now_ns, HARVEST_ROUNDS);
    log!(
        "hop: TLS randomness from timer jitter only ({HARVEST_ROUNDS} samples): the slot has no hardware RNG yet HOP_TLS_ENTROPY_WEAK"
    );
    pool
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
    S: runner::SystemApi + agent::Store,
    I: Images,
{
    for line in node.take_lines() {
        log!("{line}");
    }
}

/// Wacht op `a` of `b`, wat het eerst klaar is.
async fn either(a: impl Future<Output = ()>, b: impl Future<Output = ()>) {
    let mut a = pin!(a);
    let mut b = pin!(b);
    poll_fn(|cx| {
        if a.as_mut().poll(cx).is_ready() || b.as_mut().poll(cx).is_ready() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
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
/// naar de eigenaar met vak `slot` in de bus.
async fn work(
    pool: &'static Handoff<TcpStream>,
    i: usize,
    exec: &'static Exec,
    hub: &'static Hub,
    slot: usize,
    port: Port,
) {
    let mut streams = HubStreams { hub, exec, slot };
    loop {
        let stream = pool.take(i).await;
        let conn = TcpConn::new(stream, exec).with_read_cap(READ_CAP);
        // Een verbinding die eindigt met een termijn of een reset is gewoon
        // een client die wegging; dat is geen logregel waard.
        let _ = hop_http::serve(
            conn,
            async |req| hub.ask(slot, port, req).await,
            &mut streams,
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
    if let Some(s3) = &cfg.s3 {
        // Endpoint en bucket zijn geen geheimen; sleutel en geheim wel.
        log!(
            "hop: S3 configured ({} bucket {}) but cluster state on S3 is not wired yet; running standalone HOP_S3_SKIPPED",
            s3.endpoint,
            s3.bucket
        );
    }
    if cfg.memory_defaulted {
        log!(
            "hop: HOPOS_MEMORY not set; planning against {} bytes HOP_MEMORY_DEFAULT",
            cfg.memory
        );
    }
    if app.wall_ns().is_none() {
        log!("hop: no wall clock from the kernel; task times count from boot HOP_NO_CLOCK");
    }

    if let Err(e) = exec.spawn(clock_task(net, exec)) {
        log!("hop: cannot spawn the clock task: {e}; no SNTP, https refused HOP_SNTP_FAIL");
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
    let mut node = Node::new(&cfg, sys, images, now(app, exec));
    // Schoon is: niets overgenomen en niets fout gelezen. Een staat die niet
    // te lezen is, is geen lege staat; dan geen zaad (Go: nooit zaaien op
    // een opslagfout).
    let clean = match node.restore(now(app, exec)).await {
        Ok(0) => true,
        Ok(n) => {
            log!("hop: adopted {n} running cage(s) from the saved state HOP_ADOPTED");
            false
        }
        Err(e) => {
            log!("hop: saved agent state not restored: {e}");
            false
        }
    };
    flush(&mut node);
    if clean && let Some(specs) = init_specs(&cfg, net).await {
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
            spawned = exec.spawn(work(pool, i, exec, hub, first_slot + i, port));
        }
        if let Err(e) = spawned {
            log!("hop: cannot spawn the listeners: {e} HOP_SPAWN_FAIL");
            return;
        }
    }
    log!(
        "hop: agent up node={} cluster={} agent=:{} leader=:{} cores={} HOP_UP",
        cfg.node_id,
        cfg.cluster,
        cfg.port,
        cfg.leader_port(),
        cfg.cores
    );

    let tick_ns = u64::try_from(TICK.as_nanos()).unwrap_or(u64::MAX);
    let mut next_tick = now(app, exec);
    loop {
        // Eerst de bus leeg (level-triggered): wat binnenkwam terwijl de
        // eigenaar op de kern wachtte, ligt er nog en wordt nu afgehandeld.
        while let Some((slot, q)) = hub.next() {
            match q {
                Question::Http(port, req) => {
                    let reply = node.handle(port, &req, now(app, exec)).await;
                    hub.answer(slot, Answer::Reply(reply));
                }
                Question::Poll(ask) => {
                    let chunk = node.poll(&ask, now(app, exec));
                    hub.answer(slot, Answer::Chunk(chunk));
                }
                Question::StreamDone => node.stream_done(),
            }
            flush(&mut node);
        }
        let t = now(app, exec);
        if t >= next_tick {
            node.tick(t).await;
            next_tick = t.saturating_add(tick_ns);
            flush(&mut node);
        }
        let wait = Duration::from_nanos(next_tick.saturating_sub(now(app, exec)));
        either(hub.wait(), exec.after(wait)).await;
    }
}
