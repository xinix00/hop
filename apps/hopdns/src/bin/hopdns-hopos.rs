//! hopdns-hopos: hopdns als bewoner van HopOS.
//!
//! Dezelfde kern als de host-daemon (de codec, de cache, de server, de
//! watcher), op de executor van applib en de netstack van het slot. De
//! config komt uit de env van de jobspec in plaats van vlaggen:
//!
//! | Env | Standaard | Betekenis |
//! | --- | --- | --- |
//! | `ER_PORT_DNS` | `5353` | de UDP-poort (Hop zet hem uit `"ports":{"dns":5353}`) |
//! | `HOPDNS_PEER` | `http://HOP:9080` | peers met komma's; `HOP` is het slot-adres van Hop op deze node (Go: `HOPDNS_PEERS`, ook gelezen) |
//! | `HOPDNS_DOMAIN` | `hop.local` | het domein |
//! | `HOPDNS_API_KEY` | leeg | de HMAC-sleutel, tenzij een peer `key@` draagt (Go: `HOP_API_KEY`, ook gelezen) |
//! | `HOPDNS_SELFTEST` | geen | een jobnaam: vraag die over UDP aan jezelf en log de uitkomst |
//! | `ER_ATTR_NODE_ID` | geen | de eigen node: zijn taken krijgen hun slot-adres |
//!
//! De kern publiceert de poorten van de jobspec op de uplink, tcp én udp
//! (DNAT), dus een resolver op het LAN vraagt het adres van de node.
//!
//! De vorm (handboek §1 en §2): de **server-taak** bezit de cache en
//! wacht op een vraag óf een bericht ([`select`]); per peer bezit een
//! **watcher-taak** zijn toestandsmachine en een **lezer-taak** zijn
//! SSE-stroom. Berichten gaan door vaste brievenbussen ([`Mailbox`]); een
//! volle bus laat de zender kort wachten (tegendruk over TCP), er valt
//! niets weg.
//!
//! Markers: `HOPOS_HOPDNS_UP port=<n>` als de socket staat,
//! `HOPOS_HOPDNS_CACHE jobs=<n> clusters=<n>` als de cache verandert,
//! `HOPOS_HOPDNS_SELFTEST ok ip=<a.b.c.d>` (of `fail`) van de zelftoets.

#![cfg_attr(target_os = "none", no_std, no_main)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )
)]

extern crate alloc;

use alloc::boxed::Box;
use alloc::string::String;
use alloc::vec::Vec;
use core::net::Ipv4Addr;
use core::time::Duration;

use applib::appnet::{self, Endpoint, Net, UdpSocket};
use applib::rt::Exec;
use applib::{App, EXEC, log};
use hopdns::config::{self, Peer};
use hopdns::watcher::{Action, Local, RECONNECT, Refresh, Update, Watcher};
use hopdns::wire::{self, rtype};
use hopdns::{Cache, DEFAULT_DOMAIN, Server};
use hoplib::hopos::Agent;
use hoplib::{Backoff, Client, Event};
use sync::mpsc::Mailbox;
use sync::{Either, Full, select};

applib::main!(resident);

/// Op de host bestaat deze bewoner niet: daar is dit een lege binary, zodat
/// clippy `--all-targets` hem typecheckt.
#[cfg(not(target_os = "none"))]
fn main() {}

/// De poort zonder `ER_PORT_DNS` (Go: 5353).
const DEFAULT_PORT: u16 = 5353;

/// De peer zonder `HOPDNS_PEER`: de leader-API van de eigen Hop.
const DEFAULT_PEER: &str = "http://HOP:9080";

/// De naam die in een peer-URL voor het slot-adres van Hop staat.
const HOP_ALIAS: &str = "HOP";

/// Het slot van Hop: de eerste bewoner (PORT.md beslissing 1).
const HOP_SLOT: u64 = 1;

/// Zoveel peers tegelijk: elk kost twee taken en een timer, en de
/// executor van de app heeft er 64 en 32.
const MAX_PEERS: usize = 4;

/// De grootste vraag die de server leest.
const QUERY_MAX: usize = 1500;

/// Hoe lang een zender wacht als de brievenbus vol is.
const BACKOFF_FULL: Duration = Duration::from_millis(10);

/// De zelftoets: zoveel pogingen, zoveel ertussen, zolang per antwoord.
const SELFTEST_TRIES: u32 = 120;
const SELFTEST_EVERY: Duration = Duration::from_millis(500);
const SELFTEST_WAIT: Duration = Duration::from_secs(2);

/// Wat een lezer aan zijn watcher meldt.
enum Heard {
    /// Een gebeurtenis van de stroom.
    Event(Event),
    /// De stroom brak; hoplib verbindt over [`RECONNECT`] opnieuw.
    Lost(hoplib::Error),
}

/// Per peer de bus van lezer naar watcher.
static HEARD: [Mailbox<Heard, 32>; MAX_PEERS] = [const { Mailbox::new() }; MAX_PEERS];

/// De bus van de watchers naar de server-taak.
static UPDATES: Mailbox<Update, 16> = Mailbox::new();

/// Wat elke watcher van de node weet: gezet vóór de eerste spawn.
struct Node {
    exec: &'static Exec,
    node: Option<&'static str>,
}

impl Node {
    fn local(&self) -> Option<Local<'static>> {
        self.node.map(|node| Local {
            node,
            slot_ip: applib::net::slot_ip,
        })
    }
}

async fn resident(app: &'static App) {
    let exec: &'static Exec = EXEC.get();
    let net = match appnet::up(app) {
        Ok(n) => n,
        Err(e) => {
            log!("hopdns: network stack: {e} HOPOS_HOPDNS_FAIL");
            app.exit(1);
        }
    };
    let port = port_of(app.env("ER_PORT_DNS"));
    let domain = app.env("HOPDNS_DOMAIN").unwrap_or(DEFAULT_DOMAIN);
    let key = app
        .env("HOPDNS_API_KEY")
        .or(app.env("HOP_API_KEY"))
        .unwrap_or("");
    let peers = app
        .env("HOPDNS_PEER")
        .or(app.env("HOPDNS_PEERS"))
        .unwrap_or(DEFAULT_PEER);
    let server = match Server::new(domain) {
        Ok(s) => s,
        Err(e) => {
            log!("hopdns: domain {domain:?}: {e} HOPOS_HOPDNS_FAIL");
            app.exit(1);
        }
    };
    let sock = match net.udp_bind(port) {
        Ok(s) => s,
        Err(e) => {
            log!("hopdns: udp port {port}: {e} HOPOS_HOPDNS_FAIL");
            app.exit(1);
        }
    };
    let node: &'static Node = Box::leak(Box::new(Node {
        exec,
        node: app.env("ER_ATTR_NODE_ID"),
    }));
    let started = start_peers(node, peers, key);
    if let Some(job) = app.env("HOPDNS_SELFTEST") {
        let name = alloc::format!("{job}.{}", server.domain());
        if let Err(e) = exec.spawn(selftest(exec, net, name, port)) {
            log!("hopdns: selftest not started: {e} HOPOS_HOPDNS_SELFTEST fail");
        }
    }
    let [a, b, c, d] = net.ip();
    log!(
        "hopdns: serving dns on {a}.{b}.{c}.{d}:{port} (udp), domain={}, peers={started}, node={} HOPOS_HOPDNS_UP port={port}",
        server.domain(),
        node.node.unwrap_or("?")
    );
    serve(exec, sock, &server).await;
}

/// De poort uit `ER_PORT_DNS`, of [`DEFAULT_PORT`] zonder of bij onzin.
fn port_of(env: Option<&str>) -> u16 {
    match env.map(str::parse::<u16>) {
        None => DEFAULT_PORT,
        Some(Ok(p)) if p != 0 => p,
        Some(_) => {
            log!(
                "hopdns: ER_PORT_DNS={env:?} is not a port, using {DEFAULT_PORT} HOPOS_HOPDNS_PORT"
            );
            DEFAULT_PORT
        }
    }
}

/// Start per peer een lezer en een watcher; geeft het aantal.
fn start_peers(node: &'static Node, list: &str, key: &str) -> usize {
    let hop = applib::net::slot_ip(HOP_SLOT);
    let mut n = 0;
    for (raw, inbox) in config::peer_list(list).zip(HEARD.iter()) {
        let peer =
            config::with_host_alias(raw, HOP_ALIAS, hop).and_then(|url| Peer::parse(&url, key));
        let peer = match peer {
            Ok(p) => p,
            Err(e) => {
                log!("hopdns: peer {raw:?}: {e} HOPOS_HOPDNS_PEER_FAIL");
                continue;
            }
        };
        let client = match Client::new(&peer.endpoint, peer.api_key.as_deref()) {
            Ok(c) => c,
            Err(e) => {
                log!("hopdns: peer {}: {e} HOPOS_HOPDNS_PEER_FAIL", peer.endpoint);
                continue;
            }
        };
        let agent: &'static Agent = Box::leak(Box::new(Agent::new(client, node.exec)));
        let spawned = node
            .exec
            .spawn(read_events(node.exec, agent, inbox))
            .and_then(|()| node.exec.spawn(watch(node, peer.endpoint, agent, inbox)));
        match spawned {
            Ok(()) => n += 1,
            Err(e) => log!("hopdns: peer {raw}: not started: {e} HOPOS_HOPDNS_PEER_FAIL"),
        }
    }
    if config::peer_list(list).count() > MAX_PEERS {
        log!("hopdns: more than {MAX_PEERS} peers, the rest is ignored HOPOS_HOPDNS_PEER_FAIL");
    }
    n
}

/// Zet `v` in `bus`; wacht kort bij een volle bus.
async fn post<T, const N: usize>(exec: &'static Exec, bus: &Mailbox<T, N>, mut v: T) {
    loop {
        match bus.try_send(v) {
            Ok(()) => return,
            Err(Full(back)) => {
                v = back;
                exec.after(BACKOFF_FULL).await;
            }
        }
    }
}

/// De server-taak: bezit de cache, beantwoordt vragen, legt berichten erin.
async fn serve(exec: &'static Exec, sock: UdpSocket, server: &Server) {
    let mut cache = Cache::new();
    let mut query = [0u8; QUERY_MAX];
    let mut answer = [0u8; wire::EDNS_MAX];
    let mut shown = (usize::MAX, usize::MAX);
    loop {
        match select(sock.recv_from(&mut query), UPDATES.recv()).await {
            Either::Left(Ok((n, from))) => {
                let Some(msg) = query.get(..n) else { continue };
                if let Some(len) = server.handle(&cache, msg, &mut answer)
                    && let Some(out) = answer.get(..len)
                {
                    // Een verloren antwoord is UDP: de resolver vraagt opnieuw.
                    let _ = sock.send_to(from, out).await;
                }
            }
            Either::Left(Err(e)) => {
                log!("hopdns: udp recv: {e} HOPOS_HOPDNS_RECV");
                exec.after(Duration::from_millis(100)).await;
            }
            Either::Right(first) => {
                apply(&mut cache, first);
                while let Some(u) = UPDATES.try_recv() {
                    apply(&mut cache, u);
                }
                let now = (cache.jobs(), cache.clusters());
                if now != shown {
                    shown = now;
                    log!(
                        "hopdns: cache jobs={} clusters={} HOPOS_HOPDNS_CACHE jobs={}",
                        now.0,
                        now.1,
                        now.0
                    );
                }
            }
        }
    }
}

fn apply(cache: &mut Cache, u: Update) {
    let cluster = String::from(u.cluster());
    if let Err(e) = u.apply(cache) {
        log!("hopdns: cache update for {cluster} dropped: {e} HOPOS_HOPDNS_CACHE_FAIL");
    }
}

/// De lezer van één peer: bezit de stroom (hoplib herverbindt zelf, met
/// Go's vaste wachttijd) en meldt elke gebeurtenis en elke breuk.
async fn read_events(
    exec: &'static Exec,
    agent: &'static Agent,
    inbox: &'static Mailbox<Heard, 32>,
) {
    let mut stream = agent
        .stream()
        .with_backoff(Backoff::new(RECONNECT, RECONNECT));
    loop {
        let heard = match stream.next().await {
            Ok(e) => Heard::Event(e),
            Err(e) => Heard::Lost(e),
        };
        post(exec, inbox, heard).await;
    }
}

/// De watcher van één peer.
async fn watch(
    node: &'static Node,
    ep: String,
    agent: &'static Agent,
    inbox: &'static Mailbox<Heard, 32>,
) {
    let exec = node.exec;
    let mut w = match Watcher::new(&ep) {
        Ok(w) => w,
        Err(e) => {
            log!("hopdns: [{ep}] watcher not started: {e} HOPOS_HOPDNS_PEER_FAIL");
            return;
        }
    };
    loop {
        match w.next(exec.now()) {
            Action::Discover => match agent.status().await {
                Ok(st) => match w.discovered(exec.now(), &st.cluster_name) {
                    Ok(()) => log!(
                        "hopdns: [{ep}] cluster: {} HOPOS_HOPDNS_PEER",
                        st.cluster_name
                    ),
                    Err(e) => log!(
                        "hopdns: [{ep}] failed to discover cluster name: {e} HOPOS_HOPDNS_DISCOVER"
                    ),
                },
                Err(e) => {
                    log!(
                        "hopdns: [{ep}] failed to discover cluster name: {e} HOPOS_HOPDNS_DISCOVER"
                    );
                    w.discover_failed(exec.now());
                }
            },
            Action::Refresh(r) => refresh(node, &ep, agent, &mut w, &r).await,
            Action::Wait(deadline) => {
                let heard = match deadline {
                    None => Some(inbox.recv().await),
                    Some(t) => match select(inbox.recv(), exec.until(t)).await {
                        Either::Left(h) => Some(h),
                        Either::Right(()) => None,
                    },
                };
                // Een `ping` van vóór de ontdekking blijft in de watcher
                // staan: zodra de naam er is, zaait hij.
                match heard {
                    Some(Heard::Event(e)) => {
                        if e.kind == "ping" {
                            log!("hopdns: [{ep}] SSE connected, seeding cache HOPOS_HOPDNS_SSE_UP");
                        }
                        if let Err(err) = w.event(exec.now(), &e) {
                            log!("hopdns: [{ep}] event dropped: {err} HOPOS_HOPDNS_EVENT_FAIL");
                        }
                    }
                    Some(Heard::Lost(e)) => {
                        log!(
                            "hopdns: [{ep}] SSE disconnected: {e}, reconnecting in {}s HOPOS_HOPDNS_SSE_LOST",
                            RECONNECT.as_secs()
                        );
                        w.lost();
                    }
                    None => {}
                }
            }
        }
    }
}

/// Voert een verversing uit en stuurt het resultaat naar de server-taak.
async fn refresh(
    node: &'static Node,
    ep: &str,
    agent: &'static Agent,
    w: &mut Watcher,
    r: &Refresh,
) {
    let exec = node.exec;
    let local = node.local();
    match r {
        Refresh::Full => {
            let got = match agent.agents().await {
                Ok(a) => agent.tasks().await.map(|t| (a, t)),
                Err(e) => Err(e),
            };
            match got {
                Ok((agents, tasks)) => match w.refreshed_full(&agents, &tasks, local.as_ref()) {
                    Ok(u) => post(exec, &UPDATES, u).await,
                    Err(e) => log!("hopdns: [{ep}] rebuild failed: {e} HOPOS_HOPDNS_REFRESH_FAIL"),
                },
                Err(e) => {
                    log!("hopdns: [{ep}] failed to fetch jobs: {e} HOPOS_HOPDNS_REFRESH_FAIL");
                    w.refresh_failed(exec.now(), r);
                }
            }
        }
        Refresh::Jobs(jobs) => {
            for job in jobs {
                let built = match agent.job_status(job).await {
                    Ok(st) => w.refreshed_job(job, &st, local.as_ref()).ok(),
                    Err(e) => {
                        log!(
                            "hopdns: [{ep}] failed to fetch job {job}: {e} HOPOS_HOPDNS_REFRESH_FAIL"
                        );
                        None
                    }
                };
                match built {
                    Some(u) => post(exec, &UPDATES, u).await,
                    None => {
                        w.refresh_failed(exec.now(), r);
                        return;
                    }
                }
            }
        }
    }
}

/// De zelftoets: vraagt `name` over UDP aan de eigen server (de netstack
/// bezorgt een datagram aan het eigen adres via zijn loopback) tot er een
/// A-record komt, en logt het adres.
async fn selftest(exec: &'static Exec, net: &'static Net, name: String, port: u16) {
    let to = Endpoint { ip: net.ip(), port };
    let mut q = [0u8; 512];
    let mut buf = [0u8; wire::EDNS_MAX];
    for attempt in 1..=SELFTEST_TRIES {
        exec.after(SELFTEST_EVERY).await;
        let id = u16::try_from(attempt % 0xFFFF).unwrap_or(1);
        let Ok(n) = wire::encode_query(id, &name, rtype::A, None, &mut q) else {
            log!("hopdns: selftest {name}: not a DNS name HOPOS_HOPDNS_SELFTEST fail");
            return;
        };
        let Ok(mut sock) = net.udp_bind(0) else {
            continue;
        };
        sock.set_timeout(Some(SELFTEST_WAIT));
        if sock
            .send_to(to, q.get(..n).unwrap_or_default())
            .await
            .is_err()
        {
            continue;
        }
        let Ok((len, _)) = sock.recv_from(&mut buf).await else {
            continue;
        };
        let Ok(m) = wire::decode(buf.get(..len).unwrap_or_default()) else {
            continue;
        };
        let ips: Vec<Ipv4Addr> = m.a_records().collect();
        if let Some(first) = ips.first() {
            log!(
                "hopdns: selftest {name} -> {first} ({} A, rcode {}, attempt {attempt}) HOPOS_HOPDNS_SELFTEST ok ip={first}",
                ips.len(),
                m.rcode()
            );
            return;
        }
    }
    log!(
        "hopdns: selftest {name}: no A record after {SELFTEST_TRIES} tries HOPOS_HOPDNS_SELFTEST fail"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_port_comes_from_the_env_or_is_5353() {
        assert_eq!(port_of(Some("53")), 53);
        assert_eq!(port_of(None), 5353);
        assert_eq!(port_of(Some("0")), 5353);
        assert_eq!(port_of(Some("dns")), 5353);
    }
}
