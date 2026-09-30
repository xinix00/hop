//! De lease en de clusterstaat op een hoplockserver, async over de netstack van het slot.
//!
//! Hetzelfde CAS-protocol en dezelfde bytes als `store::HoplockLease` op de
//! host: GET geeft de lease met zijn ETag, PUT met `If-None-Match: *` maakt
//! aan, PUT of DELETE met `If-Match: <etag>` vervangt of verwijdert, en 412
//! (of 409) is "gehouden". De authenticatie is één gedeelde `X-API-Key`.
//! De lease-JSON is `discovery::wire`, zodat een host-agent en deze node één
//! lease delen.
//!
//! Bezit per backend de [`Client`] van zijn eigenaar-taak, de basis-URL en de
//! sleutel; de API-sleutel staat alleen in de kop van het verzoek, nooit in
//! een fout of een logregel.

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use core::time::Duration;

use discovery::{LeaseState, wire};

use crate::client::{Client, Reply, Req, excerpt, redact};
use crate::fetch::{Connect, Resolve};
use crate::lock::{LeaseBackend, StateBackend};

/// Hoeveel bytes een lease of snapshot bij het lezen hoogstens is: de grens
/// van de host (`store::MAX_OBJECT`, 4 MiB); een hoplockserver neemt zelf
/// hoogstens 1 MiB per PUT aan.
pub const MAX_OBJECT: usize = 4 << 20;

/// De verbinding met één hoplockserver: client, basis-URL, sleutel en termijn.
struct Server<C, R> {
    client: Client<C, R>,
    base: String,
    api_key: String,
    timeout: Duration,
}

impl<C: Connect, R: Resolve> Server<C, R> {
    fn new(client: Client<C, R>, url: &str, api_key: &str, timeout: Duration) -> Self {
        Self {
            client,
            base: String::from(url.trim_end_matches('/')),
            api_key: String::from(api_key),
            timeout,
        }
    }

    fn url(&self, key: &str) -> String {
        format!("{}/{}", self.base, key.trim_start_matches('/'))
    }

    /// Eén verzoek op `key` met `extra` koppen; elke status is een antwoord.
    async fn call(
        &mut self,
        method: &str,
        key: &str,
        extra: &[(&str, &str)],
        body: Option<&[u8]>,
    ) -> Result<Reply, String> {
        let url = self.url(key);
        let mut headers: Vec<(&str, &str)> = Vec::new();
        headers
            .try_reserve_exact(extra.len() + 1)
            .map_err(|_| String::from("out of memory"))?;
        headers.extend_from_slice(extra);
        if !self.api_key.is_empty() {
            headers.push(("X-API-Key", &self.api_key));
        }
        let req = Req {
            method,
            url: &url,
            headers: &headers,
            body,
            timeout: self.timeout,
        };
        self.client.request(req, MAX_OBJECT).await
    }

    fn status_error(&self, op: &str, key: &str, reply: &Reply) -> String {
        format!(
            "{op} {}: status {}: {}",
            redact(&self.url(key)),
            reply.status,
            excerpt(&reply.body)
        )
    }
}

/// De leader-lease op een hoplockserver.
pub struct HoplockLease<C, R> {
    server: Server<C, R>,
    key: String,
    last_error: Option<String>,
}

impl<C: Connect, R: Resolve> HoplockLease<C, R> {
    /// Een lease op `key` op de server op `url`, met `api_key` als `X-API-Key` (leeg: zonder).
    pub fn new(
        client: Client<C, R>,
        url: &str,
        api_key: &str,
        key: &str,
        timeout: Duration,
    ) -> Self {
        Self {
            server: Server::new(client, url, api_key, timeout),
            key: String::from(key),
            last_error: None,
        }
    }

    /// Bewaart de oorzaak en geeft het antwoord dat `discovery` kent.
    fn unreachable(&mut self, why: String) -> discovery::Error {
        self.last_error = Some(why);
        discovery::Error::Unreachable
    }

    async fn call(
        &mut self,
        method: &str,
        extra: &[(&str, &str)],
        body: Option<&[u8]>,
    ) -> discovery::Result<Reply> {
        let key = self.key.clone();
        match self.server.call(method, &key, extra, body).await {
            Ok(r) => Ok(r),
            Err(e) => Err(self.unreachable(e)),
        }
    }

    fn bad_status(&mut self, op: &str, reply: &Reply) -> discovery::Error {
        let why = self.server.status_error(op, &self.key, reply);
        self.unreachable(why)
    }

    fn missing_etag(&mut self, op: &str) -> discovery::Error {
        let why = format!("{op} {}: response has no ETag", self.key);
        self.unreachable(why)
    }
}

impl<C: Connect, R: Resolve> LeaseBackend for HoplockLease<C, R> {
    async fn read(&mut self) -> discovery::Result<(LeaseState, String)> {
        let reply = self.call("GET", &[], None).await?;
        match reply.status {
            200 => {}
            404 => {
                self.last_error = None;
                return Err(discovery::Error::NoLease);
            }
            _ => return Err(self.bad_status("GET", &reply)),
        }
        let state = match wire::decode(&reply.body) {
            Ok(s) => s,
            Err(why) => {
                let why = format!("lease {}: {why}", self.key);
                return Err(self.unreachable(why));
            }
        };
        let Some(etag) = reply.etag else {
            return Err(self.missing_etag("GET"));
        };
        self.last_error = None;
        Ok((state, etag))
    }

    async fn write(&mut self, prev: &str, state: &LeaseState) -> discovery::Result<String> {
        let Ok(body) = wire::encode(state) else {
            let why = format!("lease {}: out of memory while encoding", self.key);
            return Err(self.unreachable(why));
        };
        let cond = if prev.is_empty() {
            ("If-None-Match", "*")
        } else {
            ("If-Match", prev)
        };
        let headers = [("Content-Type", "application/json"), cond];
        let reply = self.call("PUT", &headers, Some(&body)).await?;
        match reply.status {
            200 | 201 => {}
            // 412 is de mislukte voorwaarde; 409 geeft een server bij een
            // gelijktijdige aanmaak. Voor een lease betekenen ze hetzelfde.
            409 | 412 => return Err(discovery::Error::LeaseHeld),
            _ => return Err(self.bad_status("PUT", &reply)),
        }
        let Some(etag) = reply.etag else {
            return Err(self.missing_etag("PUT"));
        };
        self.last_error = None;
        Ok(etag)
    }

    async fn delete(&mut self, handle: &str) -> discovery::Result {
        if handle.is_empty() {
            return Err(discovery::Error::LeaseHeld);
        }
        let reply = self.call("DELETE", &[("If-Match", handle)], None).await?;
        match reply.status {
            200 | 204 => {
                self.last_error = None;
                Ok(())
            }
            404 => Err(discovery::Error::NoLease),
            412 => Err(discovery::Error::LeaseHeld),
            _ => Err(self.bad_status("DELETE", &reply)),
        }
    }

    fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }
}

/// De clusterstaat als object `state/<cluster>` op de hoplockserver van de lease.
pub struct HoplockState<C, R> {
    server: Server<C, R>,
    key: String,
}

impl<C: Connect, R: Resolve> HoplockState<C, R> {
    /// De staat op `key` op de server op `url`, met dezelfde `api_key` als de lease.
    pub fn new(
        client: Client<C, R>,
        url: &str,
        api_key: &str,
        key: &str,
        timeout: Duration,
    ) -> Self {
        Self {
            server: Server::new(client, url, api_key, timeout),
            key: String::from(key),
        }
    }
}

impl<C: Connect, R: Resolve> StateBackend for HoplockState<C, R> {
    async fn save(&mut self, snapshot: &[u8]) -> Result<(), String> {
        // Onvoorwaardelijk: geen If-Match, de leaseholder is de enige schrijver.
        let headers = [("Content-Type", "application/json")];
        let key = self.key.clone();
        let reply = self
            .server
            .call("PUT", &key, &headers, Some(snapshot))
            .await?;
        match reply.status {
            200 | 201 | 204 => Ok(()),
            _ => Err(self.server.status_error("PUT", &key, &reply)),
        }
    }

    async fn load(&mut self) -> Result<Option<Vec<u8>>, String> {
        let key = self.key.clone();
        let reply = self.server.call("GET", &key, &[], None).await?;
        match reply.status {
            200 => Ok(Some(reply.body)),
            404 => Ok(None),
            _ => Err(self.server.status_error("GET", &key, &reply)),
        }
    }

    fn describe(&self) -> String {
        format!("hoplockserver {}", redact(&self.server.url(&self.key)))
    }
}
