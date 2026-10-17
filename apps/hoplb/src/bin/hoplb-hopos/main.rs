//! hoplb als bewoner van HopOS: dezelfde kern als de host-daemon, op de
//! executor van applib in een eigen slot.
//!
//! De configuratie komt uit de jobspec, niet uit vlaggen:
//!
//! | Env | Betekenis | Standaard |
//! | --- | --- | --- |
//! | `ER_PORT_HTTP` | de verkeerspoort (Hop zet hem uit `"ports":{"http":80}`) | 80 |
//! | `ER_PORT_ADMIN` | `/health` en `/metrics` (`"ports":{"admin":9091}`) | 9091 |
//! | `HOPLB_AGENT` | de agent; de host `HOP` is het slot van Hop | `http://HOP:9080` |
//! | `HOPLB_TAG` | de tag-filter (`lb:haas`) | geen |
//! | `HOPLB_API_KEY` | de sleutel voor `X-Hop-Auth` | geen |
//! | `HOPLB_VERBOSE` | `1`: één logregel per verzoek (`HOPOS_HOPLB_REQUEST`) | uit |
//!
//! `HOP` is een naam die alleen hier bestaat: Hop is de eerste bewoner en
//! staat altijd in slot 1 (PORT.md §6, beslissing 1), dus zijn adres op het
//! slot-LAN ligt vast (`applib::net::slot_ip(1)`, 10.100.0.2). Er is geen
//! DNS voor nodig en geen node-IP.
//!
//! De vorm (handboek §1 en §2):
//!
//! | Taak | Bezit |
//! | --- | --- |
//! | acceptor per poort | de listener; geeft elke verbinding als waarde aan een vrije werker |
//! | verkeer ×[`slot::WORKERS`] | zijn verbinding; leest de routetabel kort (geen lening over een `.await`) |
//! | admin ×[`slot::ADMIN_WORKERS`] | zijn verbinding; vraagt de metrics-taak om de tekst |
//! | metrics | de [`hoplb::Metrics`]: metingen en vragen komen over één brievenbus |
//! | stroom | de SSE-verbinding (hoplib), geeft gebeurtenissen aan de watcher |
//! | watcher | de cache van het cluster en de wacht; schrijft de routetabel |
//!
//! De routetabel is de leesbare tabel van handboek §1.1: één schrijver (de
//! watcher, die hem in één keer vervangt), veel korte lezers.
//!
//! Markers: `HOPOS_HOPLB_UP` als beide listeners staan, en
//! `HOPOS_HOPLB_ROUTES n=<patronen> backends=<n>` na elke nieuwe tabel.

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

mod config;
mod slot;

applib::main!(slot::hoplb);

/// Op de host bestaat dit image niet: daar is dit een lege binary, zodat de
/// host-poort de configuratie kan toetsen en clippy de rest kan lezen.
#[cfg(not(target_os = "none"))]
fn main() {}
