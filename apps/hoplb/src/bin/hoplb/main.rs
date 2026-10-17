//! hoplb op een gewoon OS (Linux, macOS): de host-daemon.
//!
//! Dezelfde vlaggen als de Go-versie (`-listen`, `-admin-listen`, `-agent`,
//! `-tag`, `-api-key`). De vorm is die van agentd (handboek §1, "Waarom
//! threads en geen eigen reactor" in hostnet): threads met elk één
//! eigenaar, berichten over kanalen, geen mutex.
//!
//! | Thread | Bezit | Praat met |
//! | --- | --- | --- |
//! | eigenaar ([`owner`]) | de metrics en de geldende routetabel | krijgt metingen, tabellen en vragen |
//! | stroom ([`watch`]) | de SSE-verbinding naar de agent | geeft gebeurtenissen aan de watcher |
//! | watcher ([`watch`]) | de cache van het cluster, de wacht | vraagt de agent, geeft tabellen aan de eigenaar |
//! | verkeer ×[`server::WORKERS`] | een kloon van de listener, een eigen kopie van de tabel | meldt elk verzoek aan de eigenaar |
//! | admin ×[`server::ADMIN_WORKERS`] | een kloon van de admin-listener | vraagt de eigenaar om de metrics |
//!
//! De tabel gaat als gepubliceerde snapshot naar de werkers: de eigenaar
//! hoogt een generatie op, een werker die bij zijn volgende verzoek een
//! nieuwere generatie ziet, vraagt een eigen kopie. Een werker die stil in
//! `accept` staat, kost zo niets en loopt niets op.
//!
//! Wat niet meegaat: de nette stop op SIGTERM (Go's `signal.Notify`). std
//! heeft geen signaal-API; het OS stopt het proces, en een verbinding die
//! halverwege was, ziet een reset (dezelfde open beslissing als agentd,
//! PORT.md §7).

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

mod flags;
mod owner;
mod server;
mod watch;

use std::net::TcpListener;
use std::process::ExitCode;
use std::sync::mpsc;

use flags::Flags;

/// Eén logregel op stderr, met de naam ervoor (Go's `log.Printf`; de
/// tijd zet journald of launchd erbij).
#[macro_export]
macro_rules! log {
    ($($arg:tt)*) => {
        eprintln!("hoplb: {}", format_args!($($arg)*))
    };
}

fn main() -> ExitCode {
    let flags = match Flags::parse(std::env::args().skip(1)) {
        Ok(f) => f,
        Err(flags::Error::Help) => {
            eprintln!("{}", flags::USAGE);
            return ExitCode::SUCCESS;
        }
        Err(e) => {
            eprintln!("hoplb: {e}\n{}", flags::USAGE);
            return ExitCode::from(2);
        }
    };
    match run(&flags) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            log!("{e}");
            ExitCode::FAILURE
        }
    }
}

fn run(f: &Flags) -> Result<(), String> {
    log!("starting hoplb {}", hoplb::VERSION);
    log!("  HTTP traffic: {}", f.listen);
    log!("  Admin:        {} (/health, /metrics)", f.admin_listen);
    log!("  Agent:        {}", f.agent);
    log!("  Tag filter:   {:?}", f.tag);

    let client = hoplib::Client::new(
        &f.agent,
        (!f.api_key.is_empty()).then_some(f.api_key.as_str()),
    )
    .map_err(|e| format!("agent {}: {e}", f.agent))?;
    let watcher = hoplb::Watcher::new(&f.tag).map_err(|e| format!("tag filter: {e}"))?;

    let traffic = bind(&f.listen)?;
    let admin = bind(&f.admin_listen)?;

    let (to_owner, inbox) = mpsc::sync_channel(owner::INBOX);
    server::spawn_traffic(&traffic, &to_owner).map_err(|e| format!("traffic workers: {e}"))?;
    server::spawn_admin(&admin, &to_owner).map_err(|e| format!("admin workers: {e}"))?;
    watch::spawn(client, watcher, to_owner).map_err(|e| format!("watcher: {e}"))?;
    log!("HTTP server listening on {}", f.listen);
    log!("Admin server listening on {}", f.admin_listen);

    // De hoofdthread wordt de eigenaar: hij leeft zo lang als het proces.
    owner::run(inbox);
    Err("owner: every sender is gone".into())
}

/// Bindt een Go-adres: `:80` is elke interface (eerst dual-stack `[::]`,
/// anders `0.0.0.0`), `127.0.0.1:80` precies dat.
fn bind(addr: &str) -> Result<TcpListener, String> {
    let tries: Vec<String> = match addr.strip_prefix(':') {
        Some(port) => vec![format!("[::]:{port}"), format!("0.0.0.0:{port}")],
        None => vec![addr.to_string()],
    };
    let mut last = String::new();
    for a in &tries {
        match TcpListener::bind(a) {
            Ok(l) => return Ok(l),
            Err(e) => last = format!("listen on {addr}: {e}"),
        }
    }
    Err(last)
}
