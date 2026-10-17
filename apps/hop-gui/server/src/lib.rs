//! De host van het Hop-dashboard: de statische bestanden ingebakken, en één handler die ze serveert.
//!
//! Bezit de tabel van bestanden ([`FILES`]: pad, inhoud, content-type,
//! ETag), gemaakt door `build.rs` met `include_bytes!` uit de map erboven,
//! en de handler ([`serve`]) die op één [`leanhttp::Exchange`] antwoordt:
//! `GET /` is `index.html`, elk ander pad het bestand met die naam, met
//! `ETag` en `Cache-Control: no-cache` (de browser vraagt elke keer, en
//! krijgt een 304 zolang het bestand niet veranderde), `HEAD` zonder body,
//! en de rest 404 of 405.
//!
//! Bezit geen sockets en geen threads of taken: dat doen de twee binaries,
//! `hop-gui` op een host (std-sockets via hop's `hostnet`) en
//! `hop-gui-hopos` in een slot van HopOS (applib). Beide roepen per
//! verzoek [`serve`] aan, dus op beide plekken zijn het dezelfde bytes.
//!
//! Wat hier niet staat: de API van Hop. Het dashboard praat vanuit de
//! browser direct met een agent (`X-Hop-Auth`, CORS); deze host levert
//! alleen de bladzijde.

#![no_std]
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

#[cfg(test)]
extern crate std;

use leanhttp::{Conn, Exchange};

/// Eén ingebakken bestand.
#[derive(Debug, PartialEq, Eq)]
pub struct File {
    /// Het pad in de URL, met een `/` ervoor (`/app.js`, `/vendor/...`).
    pub path: &'static str,
    /// De inhoud, zoals hij in de repo staat.
    pub body: &'static [u8],
    /// Het content-type op de extensie.
    pub content_type: &'static str,
    /// De ETag met aanhalingstekens: FNV-1a 64 over de inhoud, bij de build.
    pub etag: &'static str,
}

/// Alle bestanden van het dashboard: de vier aan de root, dan `vendor/` op naam.
pub static FILES: &[File] = include!(concat!(env!("OUT_DIR"), "/files.rs"));

/// Hoe lang een browser een bestand mag houden zonder te vragen: niet. Met
/// de ETag is vragen een 304 zonder body, en een nieuw dashboard staat er
/// meteen, zonder dat iemand zijn cache moet legen.
pub const CACHE_CONTROL: &str = "no-cache";

/// Het bestand op `path`; `/` is `index.html`.
pub fn find(path: &str) -> Option<&'static File> {
    let path = if path == "/" { "/index.html" } else { path };
    FILES.iter().find(|f| f.path == path)
}

/// Het aantal bytes van alle bestanden samen (de marker bij de start).
pub fn total_bytes() -> usize {
    FILES.iter().map(|f| f.body.len()).sum()
}

/// Of `if_none_match` (de kop van de browser) `etag` noemt.
///
/// De kop is een kommalijst of `*`; een zwakke vergelijking volstaat voor
/// een GET (RFC 9110 §13.1.2), dus `W/` telt niet mee.
pub fn etag_matches(if_none_match: &str, etag: &str) -> bool {
    if_none_match.split(',').map(str::trim).any(|t| {
        let t = t.strip_prefix("W/").unwrap_or(t);
        t == "*" || t == etag
    })
}

/// Wat de handler op een verzoek antwoordt.
#[derive(Debug, PartialEq, Eq)]
pub enum Answer {
    /// 200 met het bestand.
    File(&'static File),
    /// 304: de browser heeft deze versie al.
    NotModified(&'static File),
    /// 404.
    NotFound,
    /// 405: alleen GET en HEAD.
    MethodNotAllowed,
}

/// Het antwoord op `method path`, met de `If-None-Match` van de browser.
pub fn answer(method: &str, path: &str, if_none_match: Option<&str>) -> Answer {
    if method != "GET" && method != "HEAD" {
        return Answer::MethodNotAllowed;
    }
    match find(path) {
        None => Answer::NotFound,
        Some(f) if if_none_match.is_some_and(|h| etag_matches(h, f.etag)) => Answer::NotModified(f),
        Some(f) => Answer::File(f),
    }
}

/// Beantwoordt één verzoek op `ex`.
///
/// Met een `Content-Length` gaat een bestand direct de draad op, in plaats
/// van eerst gebufferd; het font in `vendor/` is vier megabyte.
pub async fn serve<C: Conn>(ex: &mut Exchange<'_, C>) -> leanhttp::Result {
    let inm = ex.req.header.get("If-None-Match");
    match answer(&ex.req.method, &ex.req.path, inm) {
        Answer::File(f) => {
            head(ex, f)?;
            let mut len = [0u8; 20];
            ex.header_mut()
                .set("Content-Length", decimal(f.body.len(), &mut len))?;
            ex.write(f.body).await?;
            Ok(())
        }
        Answer::NotModified(f) => {
            head(ex, f)?;
            ex.write_header(304)
        }
        Answer::NotFound => ex.error(404, "not found").await,
        Answer::MethodNotAllowed => {
            ex.header_mut().set("Allow", "GET, HEAD")?;
            ex.error(405, "method not allowed").await
        }
    }
}

/// De koppen die een 200 en een 304 delen.
fn head<C: Conn>(ex: &mut Exchange<'_, C>, f: &File) -> leanhttp::Result {
    let h = ex.header_mut();
    h.set("Content-Type", f.content_type)?;
    h.set("ETag", f.etag)?;
    h.set("Cache-Control", CACHE_CONTROL)?;
    h.set("X-Content-Type-Options", "nosniff")?;
    Ok(())
}

/// `n` in decimalen, in `buf`.
fn decimal(mut n: usize, buf: &mut [u8; 20]) -> &str {
    let mut i = buf.len();
    loop {
        i -= 1;
        if let Some(b) = buf.get_mut(i) {
            // Een cijfer: n % 10 past altijd in een u8.
            *b = b'0' + u8::try_from(n % 10).unwrap_or(0);
        }
        n /= 10;
        if n == 0 || i == 0 {
            break;
        }
    }
    core::str::from_utf8(buf.get(i..).unwrap_or_default()).unwrap_or("0")
}

#[cfg(test)]
mod tests;
