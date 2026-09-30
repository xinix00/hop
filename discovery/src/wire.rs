//! De lease op de draad: Go's `hoplock.State` als JSON, en de ETag-citaten.
//!
//! Bezit alleen de vorm. Go schrijft
//! `{"generation":N,"expires_at":"<RFC 3339>","owner":"ip:poort"}` met de
//! tijd in de lokale zone van de schrijver (`+02:00`) en nanoseconden; wij
//! schrijven UTC met nanoseconden en lezen beide, zodat een Go- en een
//! Rust-node één lease-object kunnen delen.
//!
//! Hier en niet in `store`, zodat de host-backends (`store`, std) en die van
//! de HopOS-bewoner (`agentd-hopos`, `no_std`) dezelfde bytes schrijven: een
//! host-agent en een HopOS-node delen één lease op één hoplockserver of
//! bucket.

use alloc::string::{String, ToString};
use alloc::vec::Vec;

use types::json::{self, Value};
use types::time::{MILLISECOND, Time};

use crate::LeaseState;

/// Schrijft `state` als `hoplock.State`-JSON.
///
/// `owner` valt weg als hij leeg is, zoals Go's `omitempty`.
pub fn encode(state: &LeaseState) -> Result<Vec<u8>, types::Error> {
    let mut out = alloc::format!("{{\"generation\":{},\"expires_at\":", state.generation);
    let mut at = String::new();
    Time(state.expires_at.saturating_mul(MILLISECOND)).write_rfc3339(&mut at)?;
    json::write_string(&at, &mut out)?;
    if !state.owner.is_empty() {
        out.push_str(",\"owner\":");
        json::write_string(&state.owner, &mut out)?;
    }
    out.push('}');
    Ok(out.into_bytes())
}

/// Leest `hoplock.State`-JSON; ontbrekende velden zijn nul, zoals in Go.
///
/// Onbekende velden worden overgeslagen: een nieuwere Go-node mag er iets bij
/// zetten zonder dat een oudere Rust-node de lease onleesbaar vindt.
pub fn decode(body: &[u8]) -> Result<LeaseState, &'static str> {
    let v = json::parse(body).map_err(|_| "not JSON")?;
    let obj = v.as_object().ok_or("not a JSON object")?;
    let generation = match obj.get("generation") {
        None | Some(Value::Null) => 0,
        Some(g) => match (g.as_u64(), g.as_i64()) {
            (Some(n), _) => n,
            // Go's int64 kan negatief zijn; een negatieve generatie is geen
            // generatie, en 0 laat de volgende overname bij 1 beginnen.
            (None, Some(_)) => 0,
            (None, None) => return Err("generation is not an integer"),
        },
    };
    let expires_at = match obj.get("expires_at") {
        None | Some(Value::Null) => 0,
        Some(t) => {
            let s = t.as_str().ok_or("expires_at is not a string")?;
            Time::parse_rfc3339(s)
                .map_err(|_| "expires_at is not RFC 3339")?
                .0
                / MILLISECOND
        }
    };
    let owner = match obj.get("owner") {
        None | Some(Value::Null) => String::new(),
        Some(o) => o.as_str().ok_or("owner is not a string")?.to_string(),
    };
    Ok(LeaseState {
        generation,
        expires_at,
        owner,
    })
}

/// Haalt één paar omringende dubbele aanhalingstekens van een ETag.
///
/// Hetzner Object Storage en Ceph RGW geven een ETag in de RFC 7232-vorm
/// (`"abc"`) maar vergelijken `If-Match` met de kale waarde. `None` als er
/// niets te strippen is, zodat de aanroeper alleen dan opnieuw probeert.
pub fn strip_quotes(etag: &str) -> Option<&str> {
    etag.strip_prefix('"').and_then(|s| s.strip_suffix('"'))
}
