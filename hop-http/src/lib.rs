//! De HTTP-adapter van Hop: leanhttp over een applib-TcpStream, api::Request erin, api::Response eruit.
//!
//! De handlers van `api` zijn puur: een [`api::Request`] erin, een
//! [`api::Response`] plus een [`api::Effect`] eruit. Deze crate is de dunne
//! laag eromheen die de crate-doc van `api` belooft:
//!
//! - [`TcpConn`]: `leanhttp`'s [`AsyncRead`], [`AsyncWrite`] en [`Close`]
//!   over een `applib::appnet::TcpStream`, met de fasetermijnen van de
//!   server op het timerwiel van de executor van de app-core.
//! - [`serve`]: één verbinding met [`leanhttp::serve`]; per verzoek een
//!   [`api::Request`] (methode, pad, query, headers, en de body binnen de
//!   KAM-grens van leanhttp, [`leanhttp::MAX_BODY_BYTES`]), de handler van
//!   de aanroeper, en het [`Reply`] terug op de draad.
//! - [`refuse`]: de luide weigering van een [`api::Effect`] dat deze node
//!   (nog) niet kan uitvoeren, in plaats van een stil leeg antwoord.
//!
//! Wat hier niet staat: de staat van de node en de keuze welke API een
//! verzoek krijgt (dat is de eigenaar-taak in `agentd-hopos`), en de
//! HMAC-toets (die doen `NodeApi` en `LeaderApi` zelf met `auth`; de adapter
//! geeft alleen de header ongewijzigd door).

#![cfg_attr(not(test), no_std)]
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

extern crate alloc;

mod conn;

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;

use api::{Effect, Method, Request, Response};
use leanhttp::{AsyncRead, AsyncWrite, Close, Conn, Exchange};

pub use conn::TcpConn;

/// Wat de eigenaar-taak op een verzoek terugstuurt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Reply {
    /// Een gewoon antwoord.
    Plain(Response),
    /// Een SSE-stroom als momentopname: de kop van `head`, elke regel één
    /// `data:`-gebeurtenis, en dan het einde van de stroom.
    ///
    /// Een levende tail (de verbinding open houden en nieuwe regels
    /// doorspoelen) vraagt een wekker van de logring naar deze taak; die
    /// naad is er nog niet, dus de client leest wat er nu is en verbindt
    /// opnieuw voor meer.
    Events {
        /// Status en headers.
        head: Response,
        /// De regels.
        lines: Vec<String>,
    },
}

/// Een fout van de adapter: die van leanhttp.
pub type Error = leanhttp::Error;

/// Leest het verzoek van een [`Exchange`] als [`api::Request`], met de body.
///
/// De body is al begrensd: leanhttp weigert alles boven
/// [`leanhttp::MAX_BODY_BYTES`] (KAM: 1 MiB) met een 413 voordat de handler
/// draait, dus een grote POST kost geen geheugen vóór de HMAC-toets.
pub async fn read_request<C: Conn>(ex: &mut Exchange<'_, C>) -> Result<Request, Error> {
    let mut headers = Vec::new();
    headers
        .try_reserve_exact(ex.req.header.len())
        .map_err(|_| Error::Alloc {
            bytes: ex.req.header.len(),
        })?;
    for (k, v) in ex.req.header.iter() {
        headers.push((String::from(k), String::from(v)));
    }
    let body = ex.read_body_to_end().await?;
    Ok(Request {
        method: Method::parse(&ex.req.method),
        path: ex.req.path.clone(),
        query: ex.req.raw_query.clone(),
        headers,
        body,
    })
}

/// Zet status en headers van `r` op het antwoord van `ex`.
fn write_head<C: Conn>(ex: &mut Exchange<'_, C>, r: &Response) -> Result<(), Error> {
    for (k, v) in &r.headers {
        // leanhttp rekent de lengte zelf; een meegegeven lengte zou de
        // framing van de handler worden in plaats van die van de server.
        if k.eq_ignore_ascii_case("Content-Length") {
            continue;
        }
        ex.header_mut().set(k, v)?;
    }
    ex.write_header(r.status)
}

/// Schrijft een [`Reply`] op de draad.
pub async fn write_reply<C: Conn>(ex: &mut Exchange<'_, C>, reply: &Reply) -> Result<(), Error> {
    match reply {
        Reply::Plain(r) => {
            write_head(ex, r)?;
            if !r.body.is_empty() {
                ex.write(&r.body).await?;
            }
            Ok(())
        }
        Reply::Events { head, lines } => {
            write_head(ex, head)?;
            ex.header_mut().set("Content-Type", "text/event-stream")?;
            ex.header_mut().set("Cache-Control", "no-cache")?;
            for line in lines {
                ex.write(b"data: ").await?;
                ex.write(line.as_bytes()).await?;
                ex.write(b"\n\n").await?;
            }
            ex.flush().await
        }
    }
}

/// Bedient één verbinding: elk verzoek gaat als [`api::Request`] naar `handler`, het [`Reply`] terug.
///
/// Keert terug als de verbinding sluit; een fout is er één van leanhttp
/// (een termijn, een reset) en de verbinding is dan dicht.
pub async fn serve<C, H>(conn: C, mut handler: H) -> Result<(), Error>
where
    C: AsyncRead + AsyncWrite + Close,
    H: AsyncFnMut(Request) -> Reply,
{
    let out = leanhttp::serve(conn, async |ex: &mut Exchange<'_, C>| {
        let req = read_request(ex).await?;
        let reply = handler(req).await;
        write_reply(ex, &reply).await
    })
    .await?;
    // Geen enkele route neemt de verbinding over; komt er toch een terug,
    // dan valt hij hier (Drop sluit hem).
    drop(out);
    Ok(())
}

/// De luide weigering van een [`Effect`] dat deze node nog niet uitvoert; `None` voor [`Effect::None`].
///
/// - `Proxy` naar een andere leader: 502 met het adres. Op HopOS draait de
///   leader in fase 1 op dezelfde node (standalone), en dat verzoek handelt
///   de eigenaar-taak zelf af voordat hij hier komt.
/// - `Flip`: 501, want de kern-flip over de system-API (op 0x46) is nog niet
///   aangesloten.
/// - `Logs`: 404; de eigenaar-taak beantwoordt hem uit de logring als hij
///   de taak kent.
pub fn refuse(effect: &Effect) -> Option<Response> {
    match effect {
        Effect::None => None,
        Effect::Proxy { leader, .. } => Some(Response::error(
            502,
            &format!("hop on HopOS: proxy to leader {leader} is not wired yet"),
        )),
        Effect::Flip { .. } => Some(Response::error(
            501,
            "hop on HopOS: kernel flip over the system API is not wired yet",
        )),
        Effect::Logs { task_id, .. } => {
            Some(Response::error(404, &format!("no logs for task {task_id}")))
        }
    }
}

#[cfg(test)]
mod tests;
