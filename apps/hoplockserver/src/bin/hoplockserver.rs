//! `hoplockserver`: de compare-and-swap-server voor hoplock, op een host (Go: `cmd/hoplockserver`).
//!
//! Vlaggen zoals in Go: `-listen :8090`, `-data ./data`, `-api-key`
//! (leeg: `HOPLOCK_API_KEY`, en is die ook leeg, dan zonder authenticatie).
//! De store-thread en de verbindingsthreads staan in `hoplockserver::host`;
//! deze `main` leest alleen de vlaggen, opent de store, bindt de poort en
//! parkeert.
//!
//! Markers op stderr: `HOPLOCK_UP keys=<n>` als de poort open is,
//! `HOPLOCK_NO_AUTH` als er geen sleutel is, `HOPLOCK_FAIL` bij een start
//! die niet lukt.

#![forbid(unsafe_code)]

use std::process::ExitCode;

use hoplockserver::Server;
use hoplockserver::host::{self, FileStore, FlagError};

/// De versie van deze build (die van de werkruimte).
const VERSION: &str = env!("CARGO_PKG_VERSION");

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let env_key = std::env::var("HOPLOCK_API_KEY").ok();
    let flags = match host::parse_flags(&args, env_key.as_deref()) {
        Ok(f) => f,
        Err(FlagError::Help) => {
            eprintln!("{}", host::USAGE);
            return ExitCode::SUCCESS;
        }
        Err(e) => {
            eprintln!("{e}\n{}", host::USAGE);
            return ExitCode::from(2);
        }
    };
    match run(&flags) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("hoplockserver: {e} HOPLOCK_FAIL");
            ExitCode::FAILURE
        }
    }
}

fn run(flags: &host::Flags) -> Result<(), String> {
    let store =
        FileStore::open(&flags.data).map_err(|e| format!("open store {}: {e}", flags.data))?;
    let keys = store
        .count_keys()
        .map_err(|e| format!("read store {}: {e}", flags.data))?;
    let listener =
        host::bind(&flags.listen).map_err(|e| format!("listen on {}: {e}", flags.listen))?;
    let server = Server::new(&flags.api_key);
    host::start(&listener, store, &server).map_err(|e| format!("start threads: {e}"))?;
    if !server.has_auth() {
        eprintln!(
            "hoplockserver: no -api-key and no HOPLOCK_API_KEY, every request is allowed HOPLOCK_NO_AUTH"
        );
    }
    let addr = listener
        .local_addr()
        .map_or_else(|_| flags.listen.clone(), |a| a.to_string());
    eprintln!(
        "hoplockserver {VERSION} listening on {} ({addr}, data={}, auth={}, {} workers) HOPLOCK_UP keys={keys}",
        flags.listen,
        flags.data,
        host::auth_label(&server),
        host::WORKERS,
    );
    // De threads doen het werk; deze houdt het proces open.
    loop {
        std::thread::park();
    }
}
