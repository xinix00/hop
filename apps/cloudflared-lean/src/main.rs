//! cloudflared-lean: een node achter NAT publiek bereikbaar via een
//! Cloudflare-tunnel, zonder inkomende poort, op applib en lean.
//!
//! De Rust-vorm van `OLD/apps/cloudflared-lean` (Go, TamaGo). Het programma
//! belt uit naar de edge en praat het tunnelprotocol zelf:
//!
//! - `edge`: TCP en TLS 1.3 naar de edge (leanhttps over leantls, tegen
//!   Cloudflare's eigen CA's, SNI `h2.cftunnel.com`, geen ALPN);
//! - leanh2: HTTP/2 als server, want de edge is de client;
//! - `register` en `capnp`: de Cap'n Proto-registratie op de control-stream;
//! - `ingress` en `regex`: de routeertabel die Cloudflare naar ons duwt;
//! - `origin`: de poot naar de lokale dienst, met leanhttp;
//! - `tunnel`: de verbindingen, en wat er per stream binnenkomt.
//!
//! De instellingen komen uit de env van de jobspec (`config`), met
//! cloudflared's eigen namen; het token is een geheim en staat nooit in de
//! repo. Markers op de console: `HOPOS_CFTUNNEL_UP` per geregistreerde
//! verbinding (met het edge-adres en de verbindings-id), `HOPOS_CFTUNNEL_REQ`
//! voor de eerste zestien verzoeken en daarna elk honderdste,
//! `HOPOS_CFTUNNEL_FAIL <reden>` als de tunnel stopt, `HOPOS_CFTUNNEL_STOP`
//! als de kern de app vraagt te stoppen (elke verbinding gaat dan dicht). De
//! README heeft de
//! jobspec.

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

mod b64;
mod capnp;
mod config;
mod edge;
mod edgeproto;
mod ingress;
mod json;
mod origin;
mod regex;
mod register;
mod tunnel;

use alloc::boxed::Box;
use core::fmt;

use applib::appnet;
use applib::rt::Exec;
use applib::{App, EXEC, log};
use sync::{Either, select};

use crate::config::Config;
use crate::register::Uuid;
use crate::tunnel::{GAVE_UP, RULES, Shared};

applib::main!(cloudflared);

/// Op de host bestaat dit image niet: daar is dit een lege binary, zodat de
/// host-poort de protocollagen kan toetsen en clippy de rest kan lezen.
#[cfg(not(target_os = "none"))]
fn main() {}

/// De versie van dit image (die van de werkruimte).
const VERSION: &str = env!("CARGO_PKG_VERSION");

/// De start: de env, het net, de CA's, de tabel, en één taak per
/// verbindingsindex. Keert alleen terug als de tunnel stopt.
async fn cloudflared(app: &'static App) {
    let exec: &'static Exec = EXEC.get();
    let cfg = match Config::from_env(|k| app.env(k)) {
        Ok(c) => c,
        Err(e) => return fail(app, e).await,
    };
    let net = match appnet::up(app) {
        Ok(n) => n,
        Err(e) => return fail(app, Reason("network stack", &e)).await,
    };
    let roots = match edge::roots() {
        Ok(r) => r,
        Err(e) => return fail(app, e).await,
    };
    let [a, b, c, d] = net.ip();
    log!(
        "cloudflared-lean {VERSION}: slot at {a}.{b}.{c}.{d}, tunnel {}, {} connections to {}",
        Uuid(&cfg.token.tunnel_id),
        cfg.connections,
        cfg.edges.join(","),
    );
    let n = cfg.table.len();
    RULES.replace(cfg.table);
    log!(
        "cloudflared-lean: {n} rules from {} until the first push HOPOS_CFTUNNEL_CONFIG version=0 rules={n}",
        cfg.table_from
    );
    RULES.borrow().describe(|l| log!("cloudflared-lean:   {l}"));

    // Eén keer bij de start, en voor de levensduur van de app: de taken lenen
    // het, niemand schrijft het nog.
    let shared: &'static Shared = Box::leak(Box::new(Shared {
        app,
        exec,
        token: cfg.token,
        edges: cfg.edges,
        roots,
    }));
    for i in 0..cfg.connections {
        if let Err(e) = exec.spawn(tunnel::keep_connected(shared, i)) {
            return fail(app, Reason("spawning a connection task", &e)).await;
        }
    }
    // De taken lopen tot de kern vraagt te stoppen (dan gaat elke verbinding
    // dicht en doet de main-schil het net-afscheid); alleen als de edge
    // élke verbinding "niet opnieuw" zei (een ingetrokken token), stopt de
    // app zelf, want dan is stoppen eerlijker dan blijven hangen.
    match select(app.stopped(), GAVE_UP.wait()).await {
        Either::Left(()) => {
            log!("cloudflared-lean: stopping, closing the tunnel HOPOS_CFTUNNEL_STOP");
            tunnel::close_all();
        }
        Either::Right(()) => {
            fail(
                app,
                "the edge refused every connection and said not to retry",
            )
            .await;
        }
    }
}

/// Een reden met de fout die erbij hoort.
struct Reason<'a, E>(&'static str, &'a E);

impl<E: fmt::Display> fmt::Display for Reason<'_, E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.0, self.1)
    }
}

/// Stopt de app met één regel, de reden als laatste: dat is de regel die
/// Hop op de node-console echoot.
async fn fail(app: &'static App, why: impl fmt::Display) {
    log!("cloudflared-lean: stopping HOPOS_CFTUNNEL_FAIL {why}");
    app.shutdown(1).await;
}
