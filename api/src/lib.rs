//! De HTTP-API als pure handlers over eigen request/response-typen; de HTTP-laag (leanhttp) is een dunne adapter erbuiten.
//!
//! Deze crate bezit de routes, de statuscodes en de JSON-vorm van de twee
//! API's van een node: de agent-API ([`NodeApi`], poort P: `/run`, `/tasks`,
//! ...) en de leader-API ([`LeaderApi`], poort P + 1000: `/v1/...`). Hij
//! bezit GEEN sockets, geen streams en geen klok: een handler krijgt een
//! [`Request`] en geeft een [`Response`], en waar het antwoord een stroom of
//! een doorgifte is (een SSE-log, een proxy naar de leader, de kern-flip)
//! geeft hij er een [`Effect`] bij dat de adapter uitvoert.
//!
//! De HMAC-toets komt uit `auth`; een lege sleutel is de ongeauthenticeerde
//! modus (dev, standalone).

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

mod cluster;
mod leader;
mod node;

use alloc::string::String;
use alloc::vec::Vec;
use core::fmt;

use types::de::ObjectBuilder;
use types::json::{self, Value};

pub use cluster::LeaderCluster;
pub use leader::{Cluster, ClusterError, LeaderApi};
pub use node::{Effect, LogStream, NodeApi, PROXY_MAX_BODY};

/// Een HTTP-methode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Method {
    /// GET.
    Get,
    /// POST.
    Post,
    /// DELETE.
    Delete,
    /// PATCH.
    Patch,
    /// OPTIONS (CORS-preflight).
    Options,
    /// Iets anders.
    Other,
}

impl Method {
    /// De naam op de draad.
    pub fn as_str(self) -> &'static str {
        match self {
            Method::Get => "GET",
            Method::Post => "POST",
            Method::Delete => "DELETE",
            Method::Patch => "PATCH",
            Method::Options => "OPTIONS",
            Method::Other => "OTHER",
        }
    }

    /// Leest de naam op de draad.
    pub fn parse(s: &str) -> Self {
        match s {
            "GET" => Method::Get,
            "POST" => Method::Post,
            "DELETE" => Method::Delete,
            "PATCH" => Method::Patch,
            "OPTIONS" => Method::Options,
            _ => Method::Other,
        }
    }
}

/// Een binnenkomend verzoek, zoals de adapter het gelezen heeft.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Request {
    /// De methode.
    pub method: Method,
    /// Het gedecodeerde pad, zonder query.
    pub path: String,
    /// De query zonder `?`.
    pub query: String,
    /// De headers (naam hoofdletterongevoelig).
    pub headers: Vec<(String, String)>,
    /// De body (de adapter begrenst hem al op de transportgrens).
    pub body: Vec<u8>,
}

impl Request {
    /// Een verzoek zonder headers.
    pub fn new(method: Method, target: &str, body: &[u8]) -> Self {
        let (path, query) = match target.split_once('?') {
            Some((p, q)) => (p, q),
            None => (target, ""),
        };
        Self {
            method,
            path: String::from(path),
            query: String::from(query),
            headers: Vec::new(),
            body: body.to_vec(),
        }
    }

    /// Een header op naam.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    /// Een query-parameter op naam.
    pub fn query_param(&self, name: &str) -> Option<&str> {
        self.query.split('&').find_map(|kv| {
            let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
            (k == name).then_some(v)
        })
    }
}

/// Een antwoord.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Response {
    /// De status.
    pub status: u16,
    /// De headers.
    pub headers: Vec<(String, String)>,
    /// De body.
    pub body: Vec<u8>,
}

impl Response {
    /// Een leeg antwoord met `status`.
    pub fn empty(status: u16) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body: Vec::new(),
        }
    }

    /// Een JSON-antwoord.
    pub fn json(status: u16, v: &Value) -> Self {
        let mut r = Self::empty(status);
        match json::to_string(v) {
            Ok(s) => {
                r.body = s.into_bytes();
                r.set_header("Content-Type", "application/json");
            }
            // Zonder geheugen voor de body is een 500 zonder body het eerlijke antwoord.
            Err(_) => r.status = 500,
        }
        r
    }

    /// `{"error": msg}` met `status`, zoals Go's `WriteError`.
    pub fn error(status: u16, msg: &str) -> Self {
        let mut o = ObjectBuilder::new();
        match o.str("error", msg) {
            Ok(_) => Self::json(status, &o.build()),
            Err(_) => Self::empty(status),
        }
    }

    /// Zet een header.
    pub fn set_header(&mut self, name: &str, value: &str) {
        self.headers.retain(|(k, _)| !k.eq_ignore_ascii_case(name));
        if self.headers.try_reserve(1).is_ok() {
            self.headers.push((String::from(name), String::from(value)));
        }
    }

    /// Een header op naam.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    /// De body als JSON (tests en de CLI).
    pub fn json_body(&self) -> Result<Value> {
        json::parse(&self.body).map_err(Error::Json)
    }
}

/// Een API-fout.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    /// Een JSON-fout uit `types`.
    Json(types::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Json(e) => write!(f, "json: {e}"),
        }
    }
}

/// Het resultaat-type van deze crate.
pub type Result<T = (), E = Error> = core::result::Result<T, E>;

/// Toetst de HMAC-handtekening; `Some(antwoord)` als het verzoek geweigerd wordt.
pub(crate) fn check_auth(key: &[u8], req: &Request) -> Option<Response> {
    match auth::verify(
        key,
        req.method.as_str(),
        &req.path,
        &req.body,
        req.header(auth::AUTH_HEADER),
    ) {
        Ok(()) => None,
        Err(e) => {
            let mut msg = String::new();
            let _ = fmt::write(&mut msg, format_args!("{e}"));
            Some(Response::error(e.status(), &msg))
        }
    }
}

/// Bouwt een JSON-object uit (sleutel, waarde)-paren; `None` zonder geheugen.
pub(crate) fn object(pairs: impl IntoIterator<Item = (&'static str, Value)>) -> Option<Value> {
    let mut o = ObjectBuilder::new();
    for (k, v) in pairs {
        o.field(k, v).ok()?;
    }
    Some(o.build())
}

/// Een JSON-antwoord uit paren, of een 500 zonder geheugen.
pub(crate) fn reply(
    status: u16,
    pairs: impl IntoIterator<Item = (&'static str, Value)>,
) -> Response {
    match object(pairs) {
        Some(v) => Response::json(status, &v),
        None => Response::empty(500),
    }
}

/// Een string-waarde, of `null` zonder geheugen.
pub(crate) fn s(v: &str) -> Value {
    Value::string(v).unwrap_or(Value::Null)
}

#[cfg(test)]
mod tests;
