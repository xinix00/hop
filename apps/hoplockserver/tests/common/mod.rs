//! Een echte hoplockserver op 127.0.0.1 voor de integratietests.

#![allow(clippy::unwrap_used)]

use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

use hoplockserver::Server;
use hoplockserver::host::{self, FileStore};

/// Een draaiende server: de basis-URL en de datamap.
pub(crate) struct Running {
    /// De basis-URL, zonder slash op het eind.
    pub(crate) url: String,
    /// De datamap van de store.
    pub(crate) data: PathBuf,
}

/// Start de host-server met `api_key` op een vrije poort, met een verse datamap.
pub(crate) fn serve(api_key: &str) -> Running {
    static N: AtomicU64 = AtomicU64::new(0);
    let data = std::env::temp_dir().join(format!(
        "hoplockserver-it-{}-{}",
        std::process::id(),
        N.fetch_add(1, Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&data);
    let store = FileStore::open(&data).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    host::start(&listener, store, &Server::new(api_key)).unwrap();
    Running { url, data }
}
