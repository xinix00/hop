//! De admin-poort: `/health` en `/metrics` (Go: de `adminMux` in
//! `cmd/hoplb/main.go`).
//!
//! Een eigen poort, zodat een firewall hem kan dichthouden terwijl het
//! verkeer open staat (README, "Why separate ports?"). Bezit niets: de tekst
//! van `/metrics` komt van wie de [`crate::Metrics`] bezit, via de
//! `scrape`-functie van de schil.

use alloc::string::String;

use leanhttp::{Conn, Exchange};

use crate::error::Result;
use crate::metrics::CONTENT_TYPE;

/// De body van `/health` (Go: `fmt.Fprintln(w, "ok")`).
pub const HEALTH_BODY: &[u8] = b"ok\n";

/// Wat een pad op de admin-poort is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Page {
    /// `/health`: 200, `ok`.
    Health,
    /// `/metrics`: de Prometheus-tekst.
    Metrics,
    /// De rest: 404, zoals Go's `ServeMux` (`404 page not found`).
    NotFound,
}

/// De pagina bij `path`. Go registreerde beide zonder methode, dus elke
/// methode telt.
pub fn page(path: &str) -> Page {
    match path {
        "/health" => Page::Health,
        "/metrics" => Page::Metrics,
        _ => Page::NotFound,
    }
}

/// Bedient één verbinding op de admin-poort tot hij sluit.
///
/// `scrape` geeft de tekst van `/metrics`, of `None` als de eigenaar niet
/// antwoordt (dan 503).
pub async fn serve<C, S>(conn: C, mut scrape: S) -> Result
where
    C: Conn,
    S: AsyncFnMut() -> Option<String>,
{
    leanhttp::serve(conn, async |ex: &mut Exchange<'_, C>| {
        match page(&ex.req.path) {
            Page::Health => {
                ex.header_mut()
                    .set("Content-Type", "text/plain; charset=utf-8")?;
                ex.write(HEALTH_BODY).await?;
                Ok(())
            }
            Page::Metrics => match scrape().await {
                Some(text) => {
                    ex.header_mut().set("Content-Type", CONTENT_TYPE)?;
                    ex.write(text.as_bytes()).await?;
                    Ok(())
                }
                None => ex.error(503, "metrics unavailable").await,
            },
            Page::NotFound => {
                ex.header_mut().set("X-Content-Type-Options", "nosniff")?;
                ex.error(404, "404 page not found").await
            }
        }
    })
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{Pipe, block_on};

    fn get(path: &str) -> String {
        let req = alloc::format!("GET {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n");
        let pipe = Pipe::new(&[req.as_bytes()]);
        let out = block_on(serve(pipe.clone(), async || {
            Some(String::from("# metrics\n"))
        }));
        assert!(out.is_ok(), "{out:?}");
        pipe.text()
    }

    #[test]
    fn health_metrics_and_the_rest() {
        let h = get("/health");
        assert!(h.starts_with("HTTP/1.1 200 "), "{h}");
        assert!(h.ends_with("\r\n\r\nok\n"), "{h}");
        let m = get("/metrics");
        assert!(
            m.contains("Content-Type: text/plain; version=0.0.4\r\n"),
            "{m}"
        );
        assert!(m.ends_with("# metrics\n"), "{m}");
        let n = get("/nope");
        assert!(n.starts_with("HTTP/1.1 404 "), "{n}");
        assert!(n.ends_with("404 page not found\n"), "{n}");
    }
}
