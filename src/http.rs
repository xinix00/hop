//! De twee routes van de exporter, over leanhttp: `/metrics` en `/health`.
//!
//! Bezit de routetabel en het antwoord; niet de verbinding (die is van de
//! host-bin of de bewoner) en niet de metrics (die krijgt het antwoord als
//! kant-en-klare tekst). Zo leent niemand een snapshot over een `.await`:
//! de aanroeper rendert eerst, synchroon, en geeft dan de bytes.

use alloc::string::String;

use leanhttp::{Conn, Exchange, Found, Mux};

use crate::CONTENT_TYPE;

/// De routes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Route {
    /// `GET /metrics`: de Prometheus-tekst.
    Metrics,
    /// `GET /health`: 200, `ok`.
    Health,
}

/// De routetabel.
pub fn mux() -> leanhttp::Result<Mux<Route>> {
    let mut mux = Mux::default();
    mux.handle("GET /metrics", Route::Metrics)?;
    mux.handle("GET /health", Route::Health)?;
    Ok(mux)
}

/// Wat `/metrics` op dit moment te zeggen heeft.
#[derive(Debug)]
pub enum Body {
    /// De tekst van de laatste geslaagde ronde.
    Text(String),
    /// Er is nog geen geslaagde ronde: 503, en Prometheus ziet `up 0`.
    NotYet,
    /// Renderen faalde (de heap was op): 503.
    OutOfMemory,
}

/// Beantwoordt één verzoek. `metrics` wordt alleen gevraagd voor `/metrics`,
/// en synchroon: de lening op de metrics eindigt vóór de eerste schrijf.
pub async fn handle<C: Conn>(
    ex: &mut Exchange<'_, C>,
    mux: &Mux<Route>,
    metrics: impl FnOnce() -> Body,
) -> leanhttp::Result {
    match mux.find(&mut ex.req)? {
        Found::Route(Route::Health) => {
            ex.header_mut()
                .set("Content-Type", "text/plain; charset=utf-8")?;
            ex.write(b"ok\n").await?;
            Ok(())
        }
        Found::Route(Route::Metrics) => match metrics() {
            Body::Text(text) => {
                ex.header_mut().set("Content-Type", CONTENT_TYPE)?;
                ex.write(text.as_bytes()).await?;
                Ok(())
            }
            Body::NotYet => {
                ex.error(503, "no successful poll of the hop agent yet")
                    .await
            }
            Body::OutOfMemory => ex.error(503, "out of memory rendering the metrics").await,
        },
        Found::NotFound => ex.error(404, "not found").await,
        Found::MethodNotAllowed(allow) => {
            ex.header_mut().set("Allow", &allow)?;
            ex.error(405, "method not allowed").await
        }
    }
}

/// Rendert `m` voor `/metrics`.
pub fn body(m: Option<&crate::Metrics>, now: Option<types::Time>) -> Body {
    let Some(m) = m else {
        return Body::NotYet;
    };
    let mut out = String::new();
    // Een typische scrape is een paar kilobyte; één keer ruim reserveren
    // scheelt een handvol hergroeiingen.
    if out.try_reserve(8 << 10).is_err() {
        return Body::OutOfMemory;
    }
    match crate::render(m, now, &mut out) {
        Ok(()) => Body::Text(out),
        Err(_) => Body::OutOfMemory,
    }
}
