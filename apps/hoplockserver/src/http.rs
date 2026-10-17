//! De naad naar leanhttp: één verzoek door de toestandsmachine, over elke verbinding.
//!
//! Bezit niets. De verbinding is van de werker die [`leanhttp::serve`]
//! draait, de store van zijn eigenaar; `run` is hoe de werker een
//! [`Op`] bij die eigenaar krijgt (een kanaal naar de store-thread, de
//! brievenbus van de store-taak) en de uitkomst terug. Komt er geen
//! uitkomst, dan zegt `run` waarom ([`Unanswered`]) en krijgt de client een
//! 503 of 504.
//!
//! Wat leanhttp anders doet dan Go's `net/http`, en wat een client daarvan
//! ziet (KAM, `lean/KAM.md`): een niet-canoniek pad (`//a`, `/a/../b`) is
//! 400 in plaats van Go's 301 naar het schone pad; een body boven de 1 MiB
//! is 413 in plaats van Go's 400; een chunked verzoekbody is 501 en een
//! `Expect` 417. Hop's client (en de Go-client) stuurt geen van die vier.

use core::future::Future;

use leanhttp::{Conn, Exchange};

use crate::server::{Done, Head, Op, Response, Server, Step, respond};

/// Bedient één verzoek op `ex`: kop, zo nodig de body, de store via `run`, het antwoord.
pub async fn exchange<C, F, Fut>(
    ex: &mut Exchange<'_, C>,
    server: &Server,
    run: F,
) -> leanhttp::Result
where
    C: Conn,
    F: FnOnce(Op) -> Fut,
    Fut: Future<Output = Result<Done, Unanswered>>,
{
    let step = {
        let req = &ex.req;
        let head = Head {
            method: &req.method,
            path: &req.path,
            api_key: req.header.get("X-API-Key").unwrap_or(""),
            if_match: req.header.get("If-Match").unwrap_or(""),
            if_none_match: req.header.get("If-None-Match").unwrap_or(""),
        };
        server.begin(&head)
    };
    let step = match step {
        Step::ReadBody(pending) => {
            let body = ex.read_body_to_end().await?;
            server.with_body(pending, body)
        }
        other => other,
    };
    let resp = match step {
        Step::Respond(r) => r,
        Step::Store(op) => match run(op).await {
            Ok(done) => respond(done),
            Err(why) => why.response(),
        },
        // Een tweede body-vraag bestaat niet; zou hij komen, dan is dat een
        // fout van de server, geen antwoord aan de client.
        Step::ReadBody(_) => Response::error(500, "internal server error"),
    };
    write(ex, &resp).await
}

/// Waarom een werker geen uitkomst van de store kreeg.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Unanswered {
    /// De eigenaar van de store is weg (zijn rij is dicht).
    Gone,
    /// De rij naar de eigenaar is vol.
    Busy,
    /// De eigenaar antwoordde niet op tijd.
    Late,
}

impl Unanswered {
    /// Het antwoord aan de client in plaats van de uitkomst.
    #[must_use]
    pub fn response(self) -> Response {
        match self {
            Self::Gone => Response::error(503, "store is gone"),
            Self::Busy => Response::error(503, "store is busy"),
            Self::Late => Response::error(504, "store did not answer in time"),
        }
    }
}

/// Schrijft `resp` op de draad; de lengte rekent leanhttp zelf.
pub async fn write<C: Conn>(ex: &mut Exchange<'_, C>, resp: &Response) -> leanhttp::Result {
    for (k, v) in resp.headers() {
        ex.header_mut().set(k, v)?;
    }
    ex.write_header(resp.status)?;
    for part in resp.body_parts() {
        if !part.is_empty() {
            ex.write(part).await?;
        }
    }
    Ok(())
}
