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
//! - [`Reply::Stream`]: een open SSE-stroom (`/v1/events`, een log-tail met
//!   `?follow=1`). De verbindingstaak houdt hem open en vraagt de eigenaar
//!   elke [`STREAM_POLL`] wat er bij kwam ([`Ask`], [`Chunk`]); de eigenaar
//!   heeft zo geen tabel van wachtende verbindingen.
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

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use core::future::Future;
use core::time::Duration;

use api::{Effect, LogStream, Method, Request, Response};
use leanhttp::{AsyncRead, AsyncWrite, Close, Conn, Exchange};

pub use applib::tcp::TcpConn;

/// Hoe vaak een open stroom de eigenaar vraagt wat er bij kwam. De
/// logregels komen per tik van de eigenaar (1 s) in de ring; een halve
/// seconde houdt een tail vlot zonder de eigenaar te bestoken.
pub const STREAM_POLL: Duration = Duration::from_millis(500);

/// Na zoveel stilte schrijft een stroom een [`api::KEEPALIVE`]: de schrijf
/// is hoe hij merkt dat zijn lezer weg is, en dan geeft hij zijn taak terug.
pub const KEEPALIVE_EVERY: Duration = Duration::from_secs(15);

/// Wat een open stroom de eigenaar vraagt: wat er sinds zijn volgnummer bij kwam.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Ask {
    /// De regels van een taak na `seq` (0: wat de ring nog heeft).
    Logs {
        /// De taak.
        task_id: String,
        /// De stroom.
        stream: LogStream,
        /// Het volgnummer van de laatste regel die de lezer heeft.
        seq: u64,
    },
    /// De meldingen na `seq` ([`api::EventLog::since`]).
    Events {
        /// Het volgnummer waarmee de lezer vraagt.
        seq: u64,
    },
}

impl Ask {
    /// Dezelfde vraag met een nieuw volgnummer.
    fn at(&self, seq: u64) -> Self {
        match self {
            Self::Logs {
                task_id, stream, ..
            } => Self::Logs {
                task_id: task_id.clone(),
                stream: *stream,
                seq,
            },
            Self::Events { .. } => Self::Events { seq },
        }
    }

    fn seq(&self) -> u64 {
        match self {
            Self::Logs { seq, .. } | Self::Events { seq } => *seq,
        }
    }
}

/// Het antwoord op een [`Ask`]: de SSE-bytes, het nieuwe volgnummer, en of
/// de stroom af is (de ring dicht, of de eigenaar weet de taak niet meer).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Chunk {
    /// De SSE-gebeurtenissen, klaar voor de draad.
    pub text: String,
    /// Het volgnummer voor de volgende vraag.
    pub seq: u64,
    /// Na deze bytes is de stroom af.
    pub done: bool,
}

/// Wat de eigenaar-taak op een verzoek terugstuurt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Reply {
    /// Een gewoon antwoord.
    Plain(Response),
    /// Een SSE-stroom als momentopname: de kop van `head`, elke regel één
    /// `data:`-gebeurtenis, en dan het einde van de stroom. Een levende tail
    /// is [`Reply::Stream`].
    Events {
        /// Status en headers.
        head: Response,
        /// De regels.
        lines: Vec<String>,
    },
    /// Een open SSE-stroom: de kop, `first` (de ping van `/v1/events`, of
    /// niets), en dan de antwoorden op `ask` tot de eigenaar zegt dat het af
    /// is of de lezer weggaat.
    Stream {
        /// Status en headers.
        head: Response,
        /// Wat meteen na de kop komt.
        first: String,
        /// De eerste vraag aan de eigenaar.
        ask: Ask,
    },
}

impl Reply {
    /// Status en headers van dit antwoord: de kop van een stroom, of het
    /// antwoord zelf. De eigenaar zet er de CORS-koppen van de agent-poort
    /// op ([`api::cors`]), ook als het antwoord van de leader kwam.
    pub fn head_mut(&mut self) -> &mut Response {
        match self {
            Self::Plain(r) | Self::Events { head: r, .. } | Self::Stream { head: r, .. } => r,
        }
    }
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
            stream_head(ex, head)?;
            let mut out = String::new();
            for line in lines {
                api::data_frame(line, &mut out);
            }
            if !out.is_empty() {
                ex.write(out.as_bytes()).await?;
            }
            ex.flush().await
        }
        Reply::Stream { head, first, .. } => {
            stream_head(ex, head)?;
            if !first.is_empty() {
                ex.write(first.as_bytes()).await?;
            }
            ex.flush().await
        }
    }
}

/// Zet de kop van een SSE-stroom.
fn stream_head<C: Conn>(ex: &mut Exchange<'_, C>, head: &Response) -> Result<(), Error> {
    ex.header_mut().set("Content-Type", "text/event-stream")?;
    ex.header_mut().set("Cache-Control", "no-cache")?;
    write_head(ex, head)
}

/// Wat [`serve`] naast de handler nodig heeft om een [`Reply::Stream`] open
/// te houden: de eigenaar vragen, slapen, de klok, en het einde melden.
pub trait Streams {
    /// Vraagt de eigenaar wat er sinds het volgnummer van `ask` bij kwam.
    fn poll(&mut self, ask: Ask) -> impl Future<Output = Chunk>;
    /// Slaapt `d` op het timerwiel van de executor.
    fn nap(&mut self, d: Duration) -> impl Future<Output = ()>;
    /// Nu, in nanoseconden, op de klok van [`Streams::nap`].
    fn now(&self) -> u64;
    /// De stroom is af (of kwam nooit op gang): de eigenaar telt hem af.
    fn done(&mut self);
}

/// Bedient één verbinding: elk verzoek gaat als [`api::Request`] naar `handler`, het [`Reply`] terug.
///
/// Een [`Reply::Stream`] blijft open via `streams`; na zijn
/// einde, op elk pad, meldt `streams.done()` hem af.
///
/// Keert terug als de verbinding sluit; een fout is er één van leanhttp
/// (een termijn, een reset) en de verbinding is dan dicht.
pub async fn serve<C, H, S>(conn: C, mut handler: H, streams: &mut S) -> Result<(), Error>
where
    C: AsyncRead + AsyncWrite + Close,
    H: AsyncFnMut(Request) -> Reply,
    S: Streams,
{
    let out = leanhttp::serve(conn, async |ex: &mut Exchange<'_, C>| {
        let req = read_request(ex).await?;
        let reply = handler(req).await;
        let Reply::Stream { ref ask, .. } = reply else {
            return write_reply(ex, &reply).await;
        };
        let r = match write_reply(ex, &reply).await {
            Ok(()) => pump(ex, ask, streams).await,
            Err(e) => Err(e),
        };
        streams.done();
        r
    })
    .await?;
    // Geen enkele route neemt de verbinding over; komt er toch een terug,
    // dan valt hij hier (Drop sluit hem).
    drop(out);
    Ok(())
}

/// Houdt een [`Reply::Stream`] open: vraag de eigenaar wat er bij kwam,
/// schrijf het, slaap [`STREAM_POLL`], tot de eigenaar zegt dat het af is of
/// een schrijf faalt (de lezer is weg). Om de [`KEEPALIVE_EVERY`] zonder
/// bytes een [`api::KEEPALIVE`].
///
/// Publiek voor een eigen [`serve`]-lus die naast de antwoorden van de
/// eigenaar ook doorgiftes kent (de cluster van `agentd-hopos`): de stroom
/// van de eigenaar blijft zo één implementatie.
pub async fn pump<C: Conn, S: Streams>(
    ex: &mut Exchange<'_, C>,
    first: &Ask,
    streams: &mut S,
) -> Result<(), Error> {
    let keepalive = u64::try_from(KEEPALIVE_EVERY.as_nanos()).unwrap_or(u64::MAX);
    let mut seq = first.seq();
    let mut quiet = streams.now();
    loop {
        let c = streams.poll(first.at(seq)).await;
        if !c.text.is_empty() {
            ex.write(c.text.as_bytes()).await?;
            ex.flush().await?;
            quiet = streams.now();
        }
        seq = c.seq;
        if c.done {
            return Ok(());
        }
        if streams.now().saturating_sub(quiet) >= keepalive {
            ex.write(api::KEEPALIVE.as_bytes()).await?;
            ex.flush().await?;
            quiet = streams.now();
        }
        streams.nap(STREAM_POLL).await;
    }
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
