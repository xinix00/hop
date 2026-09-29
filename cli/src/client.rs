//! Ondertekende verzoeken van de CLI aan leader en agents.
//!
//! Bezit de adressen en de sleutel van één aanroep van `hop`. Elk verzoek
//! draagt `X-Hop-Auth` (HMAC over methode, pad en body) als er een sleutel
//! is; de sleutel zelf gaat nooit over de draad en nooit naar de terminal.

use std::time::Duration;

use hostnet::{Call, Http, Reply};
use types::json::{self, Value};

/// Hoe lang één verzoek mag duren. Een apply wacht op de plaatsing (een
/// rolling update van een paar instanties duurt seconden), dus ruim.
const TIMEOUT: Duration = Duration::from_secs(120);

/// De grootste body die de CLI leest: 8 MiB (een grote jobs-lijst).
const MAX_BODY: usize = 8 << 20;

/// Waar de CLI mee praat.
pub(crate) struct Client {
    pub(crate) leader: String,
    pub(crate) agent: String,
    key: String,
    http: Http,
}

impl Client {
    /// Een client voor `leader` en `agent` (`host:poort` of een URL) met `key` (leeg = geen HMAC).
    pub(crate) fn new(leader: String, agent: String, key: String) -> Self {
        Self {
            leader,
            agent,
            key,
            http: Http::new(),
        }
    }

    /// Een ondertekend verzoek aan een absolute URL.
    pub(crate) fn call(
        &self,
        method: &str,
        url: &str,
        body: Option<&[u8]>,
    ) -> Result<Reply, String> {
        let sig = auth::sign_call(self.key.as_bytes(), method, url, body.unwrap_or_default());
        let sig = sig.map(|s| String::from_utf8_lossy(&s).into_owned());
        let mut headers: Vec<(&str, &str)> = Vec::new();
        if let Some(s) = &sig {
            headers.push((auth::AUTH_HEADER, s));
        }
        if body.is_some() {
            headers.push(("Content-Type", "application/json"));
        }
        let call = Call {
            method,
            url,
            headers: &headers,
            body,
            timeout: TIMEOUT,
        };
        self.http
            .request(&call, MAX_BODY)
            .map_err(|e| format!("{url}: {e}"))
    }

    /// Een verzoek aan de leader-API; een foutstatus wordt de melding uit `{"error": ...}`.
    pub(crate) fn leader(
        &self,
        method: &str,
        path: &str,
        body: Option<&[u8]>,
    ) -> Result<Reply, String> {
        let url = format!("{}{path}", base(&self.leader));
        let r = self.call(method, &url, body)?;
        check(r)
    }

    /// Een verzoek aan een agent (`base` is `http://ip:poort` of `host:poort`).
    pub(crate) fn agent_at(
        &self,
        base_url: &str,
        method: &str,
        path: &str,
        body: Option<&[u8]>,
    ) -> Result<Reply, String> {
        let url = format!("{}{path}", base(base_url));
        let r = self.call(method, &url, body)?;
        check(r)
    }
}

/// `host:poort` wordt `http://host:poort`; een URL blijft, zonder slash erachter.
pub(crate) fn base(addr: &str) -> String {
    let a = addr.trim_end_matches('/');
    if a.contains("://") {
        String::from(a)
    } else {
        format!("http://{a}")
    }
}

/// Een 4xx of 5xx als fout met de reden van de server.
fn check(r: Reply) -> Result<Reply, String> {
    if r.status < 400 {
        return Ok(r);
    }
    let msg = json::parse(&r.body)
        .ok()
        .and_then(|v| {
            v.as_object()
                .and_then(|o| o.get("error"))
                .and_then(Value::as_str)
                .map(String::from)
        })
        .unwrap_or_else(|| format!("request failed: status {}", r.status));
    Err(msg)
}
