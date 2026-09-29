//! `agentd`: de Hop-daemon voor Linux en macOS, zoals `OLD/cmd/agent` in Go.
//!
//! Bij de start (hier, vóór de eerste thread): de vlaggen en de config
//! (JSON, `config::Config`), het node-id, het IP, de maat van de host, de
//! lease-opslag en de opslag van de clusterstaat. Dan de threads, elk met
//! één eigenaar en alleen kanalen ertussen (handboek §1):
//!
//! - de eigenaar ([`node::Node`]): agent, leader-helft, verkiezing, runner
//!   en de API's; tikt elke seconde en verwerkt de berichten in volgorde;
//! - per poort een vaste pool verbindingsthreads ([`http`]), agent op P,
//!   leader op P + 1000;
//! - de lease ([`elector`]), de aanroepen bij de leader en de probes
//!   ([`link`]), de clusterstaat ([`persist`]) en de voorbereiding van taken
//!   ([`prep`]).
//!
//! Markers op stderr: `HOP_UP`, `HOP_LEADER`, `HOP_JOB_PLACED`,
//! `HOP_JOB_FAILED`, `HOP_STATE_LOADED`, `HOP_INIT_SEEDED`, en de
//! weigeringen `HOP_STATE_FAIL`, `HOP_STATE_SAVE_FAIL`, `HOP_LEASE_*`,
//! `HOP_API_NO_AUTH`.
//!
//! Wat hier (nog) niet is: SIGTERM netjes afhandelen. std heeft geen
//! signaal-API, en een handler zou `unsafe` FFI of een crate van buiten
//! vragen; een gedode daemon laat zijn lease verlopen (TTL) in plaats van
//! hem los te laten, en zijn processen ruimt de volgende start op
//! (`HostRunner::init`).

#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )
)]
#![forbid(unsafe_code)]

mod boot;
mod elector;
mod http;
mod link;
mod msg;
mod net;
mod node;
mod persist;
mod prep;
#[cfg(test)]
mod tests;

use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use agent::{Agent, Election, Settings};
use discovery::Discovery;
use runner::LogPolicy;
use runner::host::{HostConfig, HostRunner};
use types::Nanos;
use types::time::MILLISECOND;

use crate::elector::Elector;
use crate::msg::Port;
use crate::node::{Node, Parts};
use crate::prep::Prep;

/// Het ritme van de eigenaar: de tik van agent, runner en lease.
const TICK: Duration = Duration::from_secs(1);

/// Nu in Unix-nanoseconden, de klok van agent en leader.
fn now() -> Nanos {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if let Err(e) = run(&args) {
        eprintln!("hop: {e}");
        std::process::exit(1);
    }
}

/// De runner-config uit de node-config.
fn host_config(cfg: &config::Config) -> HostConfig {
    HostConfig {
        rootfs_base: PathBuf::from(&cfg.paths.rootfs_base),
        isolate: cfg.runner.isolate,
        docker_socket: PathBuf::from(&cfg.runner.docker_socket),
        logs: LogPolicy {
            tail_lines: usize::try_from(cfg.runner.log_tail_lines).unwrap_or(usize::MAX),
            keep_ms: cfg.runner.log_keep_seconds.saturating_mul(1000),
        }
        .or_default(),
    }
}

/// Bindt een poort of zegt waarom niet.
fn bind(port: u16) -> Result<TcpListener, String> {
    TcpListener::bind(("0.0.0.0", port)).map_err(|e| format!("cannot listen on :{port}: {e}"))
}

#[expect(
    clippy::too_many_lines,
    reason = "de boot is één rij stappen in volgorde; opgeknipt leest hij slechter"
)]
fn run(args: &[String]) -> Result<(), String> {
    let flags = boot::parse_flags(args)?;
    let (mut cfg, from_file) = boot::load_config(&flags.config)?;
    if !flags.config.is_empty() && !from_file {
        eprintln!("hop: config {} not found; using defaults", flags.config);
    }
    let standalone = boot::apply_flags(&mut cfg, &flags)?;
    // Een typefout in de init-jobs stopt de daemon bij de start, niet pas bij
    // de eerste overname op een schone boot.
    leader::decode_init_jobs(&cfg.cluster.init_jobs)
        .map_err(|e| format!("cluster.init_jobs: {e}"))?;
    let (node_id, note) = boot::node_id(&cfg)?;
    if let Some(n) = note {
        eprintln!("{n}");
    }
    if cfg.node.ip.is_empty() {
        let ip = if cfg.node.network.is_empty() {
            boot::outbound_ip()
                .map(|ip| ip.to_string())
                .unwrap_or_else(|| String::from("127.0.0.1"))
        } else {
            let ip = boot::pick_ip_in_network(&cfg.node.network)
                .map_err(|e| format!("node.network {:?}: {e}", cfg.node.network))?;
            eprintln!("hop: picked {ip} from node.network {}", cfg.node.network);
            ip.to_string()
        };
        cfg.node.ip = ip;
    }
    if cfg.api_key.is_empty() {
        eprintln!("hop: WARNING the API has no key: every request is accepted HOP_API_NO_AUTH");
    }
    let leader_port = cfg.node.port.checked_add(1000).ok_or_else(|| {
        format!(
            "node.port {} leaves no room for the leader port",
            cfg.node.port
        )
    })?;
    eprintln!("hop: starting agentd {}", node::VERSION);
    eprintln!("hop: node {node_id} on {}:{}", cfg.node.ip, cfg.node.port);
    eprintln!("hop: cluster {}", cfg.cluster.name);
    if standalone {
        eprintln!("hop: standalone mode (in-memory lock backend, single node)");
    } else {
        eprintln!("hop: lock backend {}", store::lock_label(&cfg.cluster.lock));
    }

    // De runner: oude taakmappen en containers van een vorige start weg (Go: Init).
    let host = host_config(&cfg);
    let mut runner = HostRunner::new(host);
    if let Err(e) = runner.init() {
        return Err(format!("runner init: {e}"));
    }
    let docker = runner.docker_available();

    // De agent en zijn node.
    let mut settings = Settings::from_config(&cfg).map_err(|e| format!("settings: {e}"))?;
    settings.id.clone_from(&node_id);
    settings.endpoint = format!("http://{}:{}", cfg.node.ip, cfg.node.port);
    settings.cpu_cores = boot::cpu_cores();
    settings.memory_bytes = boot::memory_bytes();
    let seed = hostnet::entropy().map_err(|e| format!("no entropy: {e}"))?;
    settings.seed = seed
        .iter()
        .take(8)
        .fold(0u64, |acc, b| (acc << 8) | u64::from(*b));
    // De gemeten attributen, en daarover die uit de config (Go: de operator
    // heeft het laatste woord over zijn eigen labels).
    let mut attrs = std::collections::BTreeMap::new();
    attrs.insert(String::from("node.id"), node_id.clone());
    attrs.insert(String::from("node.arch"), String::from(boot::go_arch()));
    attrs.insert(String::from("node.os"), String::from(boot::go_os()));
    attrs.insert(String::from("node.docker"), docker.to_string());
    for (k, v) in cfg.node.attributes.iter() {
        attrs.insert(String::from(k), v.clone());
    }
    settings.attributes = attrs;
    let agent = Agent::new(settings);

    // De lease: één synchrone claim vóór de threads (lock vrij = meteen leider).
    let ttl_ms = cfg.timeouts.leader_lease / MILLISECOND;
    let owner_addr = format!("{}:{leader_port}", cfg.node.ip);
    let mut disc = Discovery::new(owner_addr, ttl_ms);
    if !standalone && !store::lock_configured(&cfg.cluster.lock) {
        return Err(String::from("cluster.lock is incomplete"));
    }
    let mut lease =
        store::open_lease(&cfg, standalone).map_err(|e| format!("lock backend: {e}"))?;
    let holding = elector::claim_now(&mut disc, &mut lease);
    if !holding && let Some(why) = lease.last_error() {
        eprintln!("hop: lease store: {why}");
    }

    // De threads. Eerst de kanalen; de listeners binden vóór de eerste
    // eigenaar-ronde, zodat HOP_UP betekent dat de poorten open zijn.
    let (tx, rx) = mpsc::channel();
    let lease_ops =
        elector::spawn(disc, lease, tx.clone()).map_err(|e| format!("lease thread: {e}"))?;
    let key = cfg.api_key.clone().into_bytes();
    let link =
        link::spawn_link(key.clone(), tx.clone()).map_err(|e| format!("link thread: {e}"))?;
    let probes = link::spawn_probes(tx.clone()).map_err(|e| format!("probe thread: {e}"))?;
    let st = store::open_state_store(&cfg, standalone);
    eprintln!("hop: cluster state in {}", st.describe());
    let persist = persist::spawn(st, tx.clone()).map_err(|e| format!("persist thread: {e}"))?;
    let prep = Prep::spawn(&|| host_config(&cfg), &tx).map_err(|e| format!("prep threads: {e}"))?;
    let agent_l = bind(cfg.node.port)?;
    let leader_l = bind(leader_port)?;
    http::spawn_pool(&agent_l, Port::Agent, &tx, &key).map_err(|e| format!("http threads: {e}"))?;
    http::spawn_pool(&leader_l, Port::Leader, &tx, &key)
        .map_err(|e| format!("http threads: {e}"))?;

    let parts = Parts {
        agent,
        runner,
        elector: Elector::new(lease_ops, ttl_ms, holding),
        election: Election::new(&cfg.node.ip, cfg.node.port),
        key,
        cluster: cfg.cluster.name.clone(),
        clustered: !standalone,
        init_jobs: cfg.cluster.init_jobs.clone(),
        node_dead: cfg.timeouts.node_dead_threshold,
        load_wait: Duration::from_millis(discovery::backend_timeout_for(ttl_ms)).saturating_mul(2),
        link,
        probes,
        persist,
        prep,
    };
    let mut node = Node::new(parts, now());
    node.boot(now());
    eprintln!(
        "hop: agent up node={node_id} cluster={} agent=:{} leader=:{leader_port} docker={docker} HOP_UP",
        cfg.cluster.name, cfg.node.port
    );
    // De eigenaar-lus: berichten in volgorde, en elke seconde de tik.
    let tick = u64::try_from(TICK.as_nanos()).unwrap_or(u64::MAX);
    let mut next = now();
    loop {
        let wait = Duration::from_nanos(next.saturating_sub(now()));
        match rx.recv_timeout(wait) {
            Ok(msg) => node.on_msg(msg, now()),
            Err(RecvTimeoutError::Timeout) => {}
            // Alle zenders weg kan niet (de eigenaar houdt er zelf een); toch netjes.
            Err(RecvTimeoutError::Disconnected) => break,
        }
        let t = now();
        if t >= next {
            node.tick(t);
            next = t.saturating_add(tick);
        }
    }
    node.shutdown(now());
    drop(tx);
    Ok(())
}
