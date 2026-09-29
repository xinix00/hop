//! Hop als bewoner van HopOS: de binary die de kern in het slot met de bevoegdheid start.
//!
//! Bij start: de netstack (`appnet::up`), de config uit de env van het slot
//! (`HOPOS_*`, zie `agentd_hopos::env`), de system-client naar de kern, en
//! de [`Node`]: agent, leader (standalone, zoals de Go-kern in fase 1) en de
//! HopOS-runner. Daarna drie soorten taken op de executor van de app-core:
//!
//! - de eigenaar: bezit de [`Node`], handelt de verzoeken uit de [`Hub`]
//!   af en tikt elke seconde;
//! - per poort één verbindingstaak (agent op P, leader op P + 1000): accept,
//!   leanhttp, en elk verzoek als bericht naar de eigenaar.
//!
//! Markers op het log: `HOP_UP`, `HOP_LEADER`, `HOP_JOB_PLACED slot=N`, en
//! de weigeringen `HOPOS_API_NO_AUTH`, `HOP_NET_FAIL`.
//!
//! Canoniek gelinkt (applib/link.ld via build.rs), zoals appspike.
//!
//! Op QEMU: `tools/qemu-test-hop.sh` in de HopOS-repo boot de kern met deze
//! ELF in slot 1 (env, token, wandklok, poorten 8080 en 9080 doorgezet),
//! stuurt van buiten een jobspec en eist appspike in slot 2 (29-09 groen).
//! Wat daar nog niet is: hopfs (SaveState faalt met één regel,
//! `HOP_STATE_SKIPPED`), health probes en S3.

#![cfg_attr(target_os = "none", no_std, no_main)]

extern crate alloc;

use alloc::boxed::Box;
use alloc::format;
use alloc::string::String;
use core::future::{Future, poll_fn};
use core::pin::pin;
use core::task::{Context, Poll, Waker};
use core::time::Duration;

use agentd_hopos::{BootConfig, Hub, Images, Node, Port, Sink};
use applib::appnet::{self, Net, TcpListener};
use applib::rt::Exec;
use applib::{App, EXEC, log};
use hop_http::TcpConn;
use hopos_runner::{Block, KernSys};

applib::main!(resident);

/// Op de host bestaat deze bewoner niet: daar is dit een lege binary, zodat
/// de host-poort (clippy `--all-targets`) hem typecheckt zonder de
/// allocator en de paniekhaak van het slot.
#[cfg(not(target_os = "none"))]
fn main() {}

/// Het ritme van de eigenaar-taak: de tik van agent en leader.
const TICK: Duration = Duration::from_secs(1);

/// De langste stilte op een verbinding: elke poort bedient één verbinding
/// tegelijk, dus een keep-alive-client mag de volgende niet lang ophouden.
const READ_CAP: Duration = Duration::from_secs(2);

/// Wacht op een kern-call door de executor van deze core ronden te laten draaien.
///
/// De system-client wacht op zijn TCP-verbinding, en die schuift alleen op
/// als de pomp-taak van de netstack draait; dus pollt deze wachter zijn
/// future en laat daartussen de andere taken één ronde (`Exec::step`). De
/// taak die wacht, zit tijdens zijn eigen poll niet in de takentabel en
/// wordt dus niet opnieuw gepolld. Een wek voor die taak die in de ronde
/// valt, gaat verloren; de eigenaar-lus kijkt daarom altijd eerst zelf in
/// de bus (level-triggered) voordat hij wacht. Er wordt gespind, niet
/// geslapen: een call op het slot-LAN duurt tientallen microseconden, en de
/// termijn van de client (10 s) begrenst het ergste geval.
struct Nested(&'static Exec);

impl Block for Nested {
    fn block_on<F: Future>(&mut self, f: F) -> F::Output {
        let mut f = pin!(f);
        let mut cx = Context::from_waker(Waker::noop());
        loop {
            if let Poll::Ready(v) = f.as_mut().poll(&mut cx) {
                return v;
            }
            self.0.step();
        }
    }
}

/// Geeft de core één ronde terug, zodat de executor wat er gespawnd is een eigen slot geeft.
///
/// Nodig vóór de eerste [`Nested`]-wacht na elke spawn. De executor van
/// applib v3.0.0-alpha.3 zet een verse spawn in het eerste lege slot, en
/// tijdens zijn eigen poll is het slot van de eigenaar-taak leeg: de ronde
/// binnen `Nested` zette de RX-pomp van appnet daar neer, en zodra de
/// eigenaar Pending gaf, overschreef de buitenste ronde hem. Gemeten 29-09
/// op QEMU virt: de pomp stond na 14 timer-wekken stil, een SYN op :8080
/// kreeg nooit antwoord. De executor in HopOS weigert dat slot sindsdien
/// (`Slot::polling`); tot Hop een tag met die fix volgt, is dit de wacht.
async fn settle() {
    let mut yielded = false;
    poll_fn(|cx| {
        if yielded {
            return Poll::Ready(());
        }
        yielded = true;
        cx.waker().wake_by_ref();
        Poll::Pending
    })
    .await;
}

/// Hoe lang een verbinding naar een artifact-server mag duren.
const DIAL_TIMEOUT: Duration = Duration::from_secs(10);

/// De leesbuffer van een download: één hap die de runner in brokken naar de kern stroomt.
const DOWNLOAD_BUF: usize = 64 << 10;

/// Verbindingen naar artifact-servers over de netstack van het slot.
///
/// Alleen een IP-adres als host: de bewoner heeft nog geen resolver (de
/// DNS-server staat wel in de env van het slot, `DNS`). Een hostnaam faalt
/// luid met [`leanhttp::Error::Connect`], en de URL staat in de log van de
/// taak.
struct SlotDial {
    net: &'static Net,
    exec: &'static Exec,
}

impl leanhttp::Dial for SlotDial {
    type Conn = TcpConn;

    async fn dial(&mut self, target: leanhttp::Target<'_>) -> leanhttp::Result<TcpConn> {
        let ip = appnet::parse_ip4(target.host).ok_or(leanhttp::Error::Connect)?;
        let s = self
            .net
            .tcp_connect_timeout(ip, target.port, DIAL_TIMEOUT)
            .await
            .map_err(|_| leanhttp::Error::Connect)?;
        Ok(TcpConn::new(s, self.exec))
    }
}

/// De downloader van artifacts: `http://` met leanhttp, en de bytes via de runner de kooi in.
///
/// De download loopt binnen de eigenaar-taak (via [`Nested`]): de API van
/// de node wacht zolang, wat voor één image op het LAN seconden zijn. Een
/// eigen downloadtaak met de bytes als berichten is de volgende stap.
/// Artifact-headers en S3 gaan nog niet mee.
struct HttpImages {
    dial: SlotDial,
    exec: &'static Exec,
}

impl Images for HttpImages {
    fn fetch(&mut self, url: &str, sink: &mut dyn Sink) -> Result<(), String> {
        let Self { dial, exec } = self;
        Nested(exec).block_on(async {
            // `get` eist 200 en een Content-Length: een image zonder lengte
            // kan de kern niet plaatsen.
            let mut resp = leanhttp::get(dial, url)
                .await
                .map_err(|e| format!("download {url}: {e}"))?;
            let len = resp
                .length
                .ok_or_else(|| format!("download {url}: no Content-Length"))?;
            sink.begin(len)?;
            let mut buf = alloc::vec::Vec::new();
            buf.try_reserve_exact(DOWNLOAD_BUF)
                .map_err(|_| String::from("download buffer: out of memory"))?;
            buf.resize(DOWNLOAD_BUF, 0);
            loop {
                let n = resp
                    .read(&mut buf)
                    .await
                    .map_err(|e| format!("download {url}: {e}"))?;
                if n == 0 {
                    return Ok(());
                }
                sink.chunk(buf.get(..n).unwrap_or_default())?;
            }
        })
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

/// Eén poort: accepteren, leanhttp, elk verzoek als bericht naar de eigenaar.
async fn listen(
    net: &'static Net,
    exec: &'static Exec,
    hub: &'static Hub,
    slot: usize,
    port: Port,
    number: u16,
) {
    let listener: TcpListener = match net.tcp_listen(number) {
        Ok(l) => l,
        Err(e) => {
            log!("hop: cannot listen on :{number}: {e} HOP_LISTEN_FAIL");
            return;
        }
    };
    loop {
        let stream = match listener.accept().await {
            Ok(s) => s,
            Err(e) => {
                log!("hop: accept on :{number}: {e}");
                exec.after(Duration::from_millis(100)).await;
                continue;
            }
        };
        let conn = TcpConn::new(stream, exec).with_read_cap(READ_CAP);
        // Een verbinding die eindigt met een termijn of een reset is gewoon
        // een client die wegging; dat is geen logregel waard.
        let _ = hop_http::serve(conn, async |req| hub.ask(slot, port, req).await).await;
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
    // De RX-pomp is net gespawnd: eerst een slot voor hem (zie `settle`).
    settle().await;
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
    if cfg.memory_defaulted {
        log!(
            "hop: HOPOS_MEMORY not set; planning against {} bytes HOP_MEMORY_DEFAULT",
            cfg.memory
        );
    }
    if app.wall_ns().is_none() {
        log!("hop: no wall clock from the kernel; task times count from boot HOP_NO_CLOCK");
    }

    let sys = KernSys::new(net.system_client(), Nested(exec), cfg.cores);
    let images = HttpImages {
        dial: SlotDial { net, exec },
        exec,
    };
    let mut node = Node::new(&cfg, sys, images, now(app, exec));
    match node.restore() {
        Ok(0) => {}
        Ok(n) => log!("hop: adopted {n} running cage(s) from the saved state HOP_ADOPTED"),
        Err(e) => log!("hop: saved agent state not restored: {e}"),
    }
    flush(&mut node);

    // Eén bus voor het leven van de bewoner; de taken krijgen `&'static`.
    let hub: &'static Hub = Box::leak(Box::new(Hub::new(2)));
    let spawned = exec
        .spawn(listen(net, exec, hub, 0, Port::Agent, cfg.port))
        .and_then(|()| exec.spawn(listen(net, exec, hub, 1, Port::Leader, cfg.leader_port())));
    if let Err(e) = spawned {
        log!("hop: cannot spawn the listeners: {e} HOP_SPAWN_FAIL");
        return;
    }
    settle().await;
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
        // Eerst de bus leeg: een wek die tijdens een kern-call verloren ging
        // (zie `Nested`), wordt hier alsnog gezien.
        while let Some((slot, port, req)) = hub.next() {
            let reply = node.handle(port, &req, now(app, exec));
            hub.answer(slot, reply);
            flush(&mut node);
        }
        let t = now(app, exec);
        if t >= next_tick {
            node.tick(t);
            next_tick = t.saturating_add(tick_ns);
            flush(&mut node);
        }
        let wait = Duration::from_nanos(next_tick.saturating_sub(now(app, exec)));
        either(hub.wait(), exec.after(wait)).await;
    }
}
