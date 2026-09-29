//! De lease en de clusterstaat op een hoplockserver (Go: `hoplockserver/client`).
//!
//! Hetzelfde CAS-protocol als S3, zonder AWS-account en zonder SigV4: GET
//! geeft de lease met zijn ETag, PUT met `If-None-Match: *` maakt aan, PUT of
//! DELETE met `If-Match: <etag>` vervangt of verwijdert, en 412 (of 409) is
//! "gehouden". De authenticatie is één gedeelde `X-API-Key`.
//!
//! Bezit per backend de host-client, de basis-URL en de sleutel; elk verzoek
//! dialt vers (hostnet heeft geen pool), en de API-sleutel staat alleen in de
//! kop van het verzoek, nooit in een fout of een `Debug`.

use std::fmt;
use std::time::Duration;

use discovery::{Backend, LeaseState};
use hostnet::{Call, Http, Reply};

use crate::{Error, MAX_OBJECT, Result, StateStore, body_excerpt, redact_url, state_key, wire};

/// De verbinding met één hoplockserver: basis-URL, sleutel en termijn.
struct Server {
    http: Http,
    base: String,
    api_key: String,
    timeout: Duration,
}

impl fmt::Debug for Server {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // De API-sleutel blijft weg: een backend in een logregel lekt niets.
        f.debug_struct("Server")
            .field("base", &redact_url(&self.base))
            .field("api_key", &(!self.api_key.is_empty()).then_some("<set>"))
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

impl Server {
    fn new(url: &str, api_key: &str, timeout: Duration) -> Self {
        Self {
            http: Http::new(),
            base: url.trim_end_matches('/').to_string(),
            api_key: api_key.to_string(),
            timeout,
        }
    }

    fn url(&self, key: &str) -> String {
        format!("{}/{}", self.base, key.trim_start_matches('/'))
    }

    /// Eén verzoek op `key` met `extra` koppen; elke status is een antwoord.
    ///
    /// De termijn van de server geldt voor de hele aanroep (verbinden, kop
    /// en body samen, zie `s3::deadline`), niet per fase.
    fn call(
        &self,
        method: &'static str,
        key: &str,
        extra: &[(&str, &str)],
        body: Option<&[u8]>,
    ) -> Result<Reply> {
        let url = self.url(key);
        let mut headers: Vec<(&str, &str)> = extra.to_vec();
        if !self.api_key.is_empty() {
            headers.push(("X-API-Key", &self.api_key));
        }
        let call = Call {
            method,
            url: &url,
            headers: &headers,
            body,
            timeout: self.timeout,
        };
        self.http
            .request_until(&call, MAX_OBJECT, crate::s3::deadline(self.timeout))
            .map_err(|source| Error::Http {
                op: method,
                url: redact_url(&url),
                source,
            })
    }

    fn status_error(&self, op: &'static str, key: &str, reply: &Reply) -> Error {
        Error::Status {
            op,
            url: redact_url(&self.url(key)),
            code: reply.status,
            body: body_excerpt(&reply.body),
        }
    }
}

/// De ETag van een antwoord, als die er is en niet leeg is.
fn etag(reply: &Reply) -> Option<String> {
    reply
        .header("ETag")
        .filter(|e| !e.is_empty())
        .map(str::to_string)
}

/// De leader-lease op een hoplockserver.
#[derive(Debug)]
pub struct HoplockLease {
    server: Server,
    key: String,
    last_error: Option<Error>,
}

impl HoplockLease {
    /// Een lease op `key` op de server op `url`, met `api_key` als `X-API-Key` (leeg: zonder).
    pub fn new(url: &str, api_key: &str, key: &str, timeout: Duration) -> Self {
        Self {
            server: Server::new(url, api_key, timeout),
            key: key.to_string(),
            last_error: None,
        }
    }

    /// De sleutel van het lease-object.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// De laatste fout achter een `Unreachable`; gewist door een aanroep die slaagt.
    pub fn last_error(&self) -> Option<&Error> {
        self.last_error.as_ref()
    }

    fn unreachable(&mut self, e: Error) -> discovery::Error {
        self.last_error = Some(e);
        discovery::Error::Unreachable
    }

    fn missing_etag(&mut self, op: &'static str) -> discovery::Error {
        let e = Error::MissingEtag {
            op,
            key: self.key.clone(),
        };
        self.unreachable(e)
    }

    fn call(
        &mut self,
        method: &'static str,
        extra: &[(&str, &str)],
        body: Option<&[u8]>,
    ) -> discovery::Result<Reply> {
        match self.server.call(method, &self.key, extra, body) {
            Ok(r) => Ok(r),
            Err(e) => Err(self.unreachable(e)),
        }
    }

    fn bad_status(&mut self, op: &'static str, reply: &Reply) -> discovery::Error {
        let e = self.server.status_error(op, &self.key, reply);
        self.unreachable(e)
    }
}

impl Backend for HoplockLease {
    fn read(&mut self) -> discovery::Result<(LeaseState, String)> {
        let reply = self.call("GET", &[], None)?;
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
                let e = Error::BadLease {
                    key: self.key.clone(),
                    why,
                };
                return Err(self.unreachable(e));
            }
        };
        let Some(etag) = etag(&reply) else {
            return Err(self.missing_etag("GET"));
        };
        self.last_error = None;
        Ok((state, etag))
    }

    fn write(&mut self, prev: &str, state: &LeaseState) -> discovery::Result<String> {
        let Ok(body) = wire::encode(state) else {
            let e = Error::BadLease {
                key: self.key.clone(),
                why: "out of memory while encoding",
            };
            return Err(self.unreachable(e));
        };
        let cond = if prev.is_empty() {
            ("If-None-Match", "*")
        } else {
            ("If-Match", prev)
        };
        let headers = [("Content-Type", "application/json"), cond];
        let reply = self.call("PUT", &headers, Some(&body))?;
        match reply.status {
            200 | 201 => {}
            // 412 is de mislukte voorwaarde; 409 geeft een server bij een
            // gelijktijdige aanmaak. Voor een lease betekenen ze hetzelfde.
            409 | 412 => return Err(discovery::Error::LeaseHeld),
            _ => return Err(self.bad_status("PUT", &reply)),
        }
        let Some(etag) = etag(&reply) else {
            return Err(self.missing_etag("PUT"));
        };
        self.last_error = None;
        Ok(etag)
    }

    fn delete(&mut self, handle: &str) -> discovery::Result {
        if handle.is_empty() {
            return Err(discovery::Error::LeaseHeld);
        }
        let reply = self.call("DELETE", &[("If-Match", handle)], None)?;
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
}

/// De clusterstaat als object `state/<cluster>` op de hoplockserver van de lease.
///
/// Zo heeft ook de standaardmodus (gratis, zelf gehost) duurzame gewenste
/// staat: een nieuwe leader verliest na een failover geen jobs die elders
/// draaiden.
#[derive(Debug)]
pub struct HoplockStateStore {
    server: Server,
    key: String,
}

impl HoplockStateStore {
    /// De staat van `cluster` op de server op `url`, met dezelfde `api_key` als de lease.
    pub fn new(url: &str, api_key: &str, cluster: &str, timeout: Duration) -> Self {
        Self {
            server: Server::new(url, api_key, timeout),
            key: state_key(cluster),
        }
    }
}

impl StateStore for HoplockStateStore {
    fn save(&mut self, snapshot: &[u8]) -> Result {
        // Onvoorwaardelijk: geen If-Match, de leaseholder is de enige schrijver.
        let headers = [("Content-Type", "application/json")];
        let reply = self
            .server
            .call("PUT", &self.key, &headers, Some(snapshot))?;
        match reply.status {
            200 | 201 | 204 => Ok(()),
            _ => Err(self.server.status_error("PUT", &self.key, &reply)),
        }
    }

    fn load(&mut self) -> Result<Option<Vec<u8>>> {
        let reply = self.server.call("GET", &self.key, &[], None)?;
        match reply.status {
            200 => Ok(Some(reply.body)),
            404 => Ok(None),
            _ => Err(self.server.status_error("GET", &self.key, &reply)),
        }
    }

    fn describe(&self) -> String {
        format!("hoplockserver {}", redact_url(&self.server.url(&self.key)))
    }
}
