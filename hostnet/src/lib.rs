//! De host-kant van het net (std): std-sockets als leanhttp-verbinding, een block_on, de HTTP(S)-client, de S3-transporten.
//!
//! Op HopOS draait Hop op de executor van applib en praat leanhttp met de
//! netstack van het slot (`hop-http`). Op Linux en macOS is er een kernel
//! met blokkerende sockets, en deze crate is de naad daarheen. Hij bezit:
//!
//! - [`StdConn`]: een `std::net::TcpStream` (of een Unix-socket) als
//!   leanhttp-[`leanhttp::Conn`]. Elke poll leest of schrijft blokkerend en
//!   is dus meteen `Ready`; de fasetermijnen van leanhttp worden
//!   socket-termijnen.
//! - [`block_on`]: de kleinste executor die daarbij past. Een future die
//!   alleen [`StdConn`]s pollt, wordt in één ronde klaar; een onverwachte
//!   `Pending` kost een korte slaap en een nieuwe poll.
//! - [`Http`]: de client voor `http://` en `https://` (`leanhttps::WebDial`
//!   over de std-dial: ketenverificatie tegen `leantls::MOZILLA_ROOTS`, de
//!   systeemklok als datum), met [`Http::request`] voor API-verkeer (en
//!   [`Http::request_until`] met één totale termijn per aanroep),
//!   [`Http::stream`] voor downloads en [`Http::open`] voor stromen die de
//!   aanroeper hap voor hap leest (SSE, een log-tail door de proxy).
//! - [`s3_http`]: `leans3http::Http` over dezelfde dial, voor de lease en de
//!   staat; [`S3Transport`]: een gestroomd `leans3::Transport` voor de
//!   downloads van de runner (leans3http buffert elke body).
//!
//! # Waarom threads en geen eigen reactor
//!
//! Het handboek (§1) verbiedt de mutex, niet de thread: een thread is een
//! taak met een eigen eigenaar, en wat tussen threads gaat is een bericht
//! over een kanaal. Een reactor (epoll/kqueue) zou honderden regels `unsafe`
//! FFI kosten voor een daemon die hooguit een handvol verbindingen tegelijk
//! heeft. Deze crate kiest daarom blokkerende sockets met één executor per
//! thread ([`block_on`]); de daemon (`agentd`) houdt de staat in één
//! eigenaar-thread en laat een vaste pool verbindingsthreads berichten
//! sturen.

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

mod client;
mod conn;
mod exec;
mod s3;

pub use client::{Call, Error, HostConn, Http, Open, Reply, Result, entropy, unix_secs};
pub use conn::{Socket, StdConn};
pub use exec::block_on;
pub use s3::{S3Response, S3Transport, s3_http};

#[cfg(test)]
mod tests;
