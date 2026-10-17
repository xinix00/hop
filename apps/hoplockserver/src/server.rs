//! De HTTP-afhandeling als toestandsmachine (Go: `internal/server`).
//!
//! Bezit de API-sleutel en de regels; niet de verbinding en niet de store.
//! Een verzoek loopt zo:
//!
//! 1. [`Server::begin`] met de kop ([`Head`]): een antwoord meteen
//!    (`/health`, 401, 400, 405), een store-operatie ([`Op`]), of voor een
//!    PUT de vraag om de body ([`Step::ReadBody`]);
//! 2. [`Server::with_body`] met die body: een antwoord of een operatie;
//! 3. de eigenaar van de store voert de operatie uit ([`run`]) en geeft een
//!    uitkomst ([`Done`]);
//! 4. [`respond`] maakt van de uitkomst het antwoord.
//!
//! De volgorde van de toetsen is die van Go, want die is zichtbaar: eerst de
//! sleutel (401), dan een lege sleutel (400), dan de methode (405), en voor
//! een PUT eerst de body, dan `If-None-Match`, dan de sleutel zelf. Een GET
//! op een ongeldige sleutel geeft 500 en geen 400: Go's `serveGet` kent
//! `ErrBadKey` niet, en een client ziet het verschil.
//!
//! De antwoorden zijn die van Go's `http.Error`: `text/plain;
//! charset=utf-8`, `X-Content-Type-Options: nosniff` en de tekst met een
//! regeleinde.

use alloc::string::{String, ToString};
use alloc::vec::Vec;

use crate::etag::Etag;
use crate::key::Key;
use crate::store::{Condition, Error, Object, Store};

/// De kop van `text/plain`-antwoorden (Go's `http.Error` en de sniffer).
pub const TEXT_PLAIN: &str = "text/plain; charset=utf-8";

/// De kop van een GET-antwoord.
pub const APPLICATION_JSON: &str = "application/json";

/// De methodes die een sleutel kent, in de `Allow` van een 405.
pub const ALLOW: &str = "GET, PUT, DELETE";

/// De kop van een verzoek: wat de toestandsmachine ervan leest.
///
/// Een ontbrekende kop en een lege zijn hetzelfde, zoals Go's `Header.Get`.
#[derive(Clone, Copy, Debug, Default)]
pub struct Head<'a> {
    /// De methode, hoofdlettergevoelig.
    pub method: &'a str,
    /// Het gedecodeerde pad, met de leidende `/`.
    pub path: &'a str,
    /// `X-API-Key`.
    pub api_key: &'a str,
    /// `If-Match`.
    pub if_match: &'a str,
    /// `If-None-Match`.
    pub if_none_match: &'a str,
}

/// Een PUT die op zijn body wacht: de sleutel zoals hij binnenkwam en de voorwaarde.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingPut {
    raw_key: String,
    cond: Condition,
}

/// Een operatie voor de eigenaar van de store; de body reist mee als waarde.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Op {
    /// GET.
    Get(Key),
    /// PUT met een voorwaarde.
    Put {
        /// De sleutel.
        key: Key,
        /// De body, verplaatst.
        body: Vec<u8>,
        /// De voorwaarde.
        cond: Condition,
    },
    /// DELETE met een voorwaarde (alleen `If-Match` telt).
    Delete {
        /// De sleutel.
        key: Key,
        /// De voorwaarde.
        cond: Condition,
    },
}

/// De uitkomst van een [`Op`]; de soort bepaalt de vertaling naar een status.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Done {
    /// Van een GET.
    Get(crate::Result<Object>),
    /// Van een PUT.
    Put(crate::Result<Etag>),
    /// Van een DELETE.
    Delete(crate::Result),
}

/// De volgende stap van een verzoek.
#[derive(Debug, PartialEq, Eq)]
pub enum Step {
    /// Klaar: dit antwoord gaat terug.
    Respond(Response),
    /// Lees de body (hoogstens [`crate::MAX_BODY`]) en ga door met [`Server::with_body`].
    ReadBody(PendingPut),
    /// Laat de eigenaar van de store dit doen en geef de uitkomst aan [`respond`].
    Store(Op),
}

/// De body van een antwoord.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Body {
    /// Geen body.
    Empty,
    /// Een vaste tekst, zonder regeleinde erachter.
    Static(&'static str),
    /// Een regel van `http.Error`: de tekst, daarna `\n`.
    Line(Line),
    /// De bytes van een object.
    Bytes(Vec<u8>),
}

/// De tekst van een foutregel.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Line {
    /// Een vaste tekst.
    Static(&'static str),
    /// Een tekst met getallen of een reden erin (een 500).
    Owned(String),
}

impl Line {
    /// De tekst, zonder het regeleinde.
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Static(s) => s,
            Self::Owned(s) => s,
        }
    }
}

/// Een antwoord: status, de koppen die Go zet, en een body.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Response {
    /// De status.
    pub status: u16,
    /// `ETag`, bij een geslaagde GET of PUT.
    pub etag: Option<Etag>,
    /// `Content-Type`.
    pub content_type: Option<&'static str>,
    /// `X-Content-Type-Options: nosniff`, zoals `http.Error`.
    pub nosniff: bool,
    /// `Allow`, bij een 405.
    pub allow: Option<&'static str>,
    /// De body.
    pub body: Body,
}

impl Response {
    /// Een antwoord zonder koppen en zonder body.
    #[must_use]
    pub const fn status(status: u16) -> Response {
        Response {
            status,
            etag: None,
            content_type: None,
            nosniff: false,
            allow: None,
            body: Body::Empty,
        }
    }

    /// Go's `http.Error(w, msg, status)`.
    #[must_use]
    pub const fn error(status: u16, msg: &'static str) -> Response {
        Response {
            status,
            etag: None,
            content_type: Some(TEXT_PLAIN),
            nosniff: true,
            allow: None,
            body: Body::Line(Line::Static(msg)),
        }
    }

    /// Go's `http.Error(w, err.Error(), 500)`.
    #[must_use]
    pub fn internal(err: &Error) -> Response {
        Response {
            body: Body::Line(Line::Owned(err.to_string())),
            ..Response::error(500, "")
        }
    }

    /// De koppen, in de volgorde waarin ze op de draad gaan.
    pub fn headers(&self) -> impl Iterator<Item = (&'static str, &str)> {
        let etag = self.etag.as_ref().map(|e| ("ETag", e.as_str()));
        let ct = self.content_type.map(|c| ("Content-Type", c));
        let nosniff = self
            .nosniff
            .then_some(("X-Content-Type-Options", "nosniff"));
        let allow = self.allow.map(|a| ("Allow", a));
        [allow, etag, ct, nosniff].into_iter().flatten()
    }

    /// De body in stukken: de tekst en, voor een foutregel, het regeleinde.
    #[must_use]
    pub fn body_parts(&self) -> [&[u8]; 2] {
        match &self.body {
            Body::Empty => [b"", b""],
            Body::Static(s) => [s.as_bytes(), b""],
            Body::Line(l) => [l.as_str().as_bytes(), b"\n"],
            Body::Bytes(b) => [b.as_slice(), b""],
        }
    }
}

/// De server: de API-sleutel en de regels.
#[derive(Clone)]
pub struct Server {
    api_key: String,
}

impl core::fmt::Debug for Server {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        // De sleutel zelf staat in geen Debug; alleen of er een is.
        f.debug_struct("Server")
            .field("auth", &self.has_auth())
            .finish()
    }
}

impl Server {
    /// Een server met `api_key`; leeg zet de authenticatie uit (Go: `server.New`).
    #[must_use]
    pub fn new(api_key: &str) -> Server {
        Server {
            api_key: String::from(api_key),
        }
    }

    /// Vraagt deze server een `X-API-Key`?
    #[must_use]
    pub fn has_auth(&self) -> bool {
        !self.api_key.is_empty()
    }

    /// Hoe lang de sleutel is, voor de opstartregel (Go: `authLabel`).
    #[must_use]
    pub fn key_len(&self) -> usize {
        self.api_key.len()
    }

    /// De eerste stap van een verzoek (Go: `handleHealth` en `handleKey`).
    #[must_use]
    pub fn begin(&self, head: &Head<'_>) -> Step {
        if head.path == "/health" {
            return Step::Respond(Response {
                content_type: Some(TEXT_PLAIN),
                body: Body::Static("ok"),
                ..Response::status(200)
            });
        }
        if !self.is_authorised(head.api_key) {
            return Step::Respond(Response::error(401, "unauthorized"));
        }
        let raw = head.path.strip_prefix('/').unwrap_or(head.path);
        if raw.is_empty() {
            return Step::Respond(Response::error(400, "key required"));
        }
        match head.method {
            "GET" => match Key::parse(raw) {
                Ok(key) => Step::Store(Op::Get(key)),
                // Go's serveGet vertaalt alleen ErrNotFound; de rest is 500.
                Err(e) => Step::Respond(Response::internal(&e)),
            },
            "PUT" => Step::ReadBody(PendingPut {
                raw_key: String::from(raw),
                cond: Condition {
                    if_none_match: String::from(head.if_none_match),
                    if_match: String::from(head.if_match),
                },
            }),
            "DELETE" => match Key::parse(raw) {
                Ok(key) => Step::Store(Op::Delete {
                    key,
                    cond: Condition {
                        if_none_match: String::new(),
                        if_match: String::from(head.if_match),
                    },
                }),
                Err(e) => Step::Respond(key_error(&e)),
            },
            _ => Step::Respond(Response {
                allow: Some(ALLOW),
                ..Response::error(405, "method not allowed")
            }),
        }
    }

    /// De tweede stap van een PUT, met de body (Go: `servePut` tot `store.Put`).
    #[must_use]
    pub fn with_body(&self, pending: PendingPut, body: Vec<u8>) -> Step {
        let PendingPut { raw_key, cond } = pending;
        if !cond.if_none_match.is_empty() && cond.if_none_match != "*" {
            return Step::Respond(Response::error(400, "only If-None-Match: * is supported"));
        }
        match Key::parse(&raw_key) {
            Ok(key) => Step::Store(Op::Put { key, body, cond }),
            Err(e) => Step::Respond(key_error(&e)),
        }
    }

    /// Klopt `got` met de sleutel, in constante tijd (Go: `subtle.ConstantTimeCompare`)?
    fn is_authorised(&self, got: &str) -> bool {
        self.api_key.is_empty() || auth::constant_time_eq(got.as_bytes(), self.api_key.as_bytes())
    }
}

/// Een sleutelfout van PUT of DELETE: 400 voor een ongeldige, 500 voor de rest.
fn key_error(e: &Error) -> Response {
    match e {
        Error::BadKey => Response::error(400, "invalid key"),
        other => Response::internal(other),
    }
}

/// Voert `op` uit op `store`: het werk van de eigenaar van de store.
pub async fn run<S: Store>(store: &mut S, op: Op) -> Done {
    match op {
        Op::Get(key) => Done::Get(store.get(&key).await),
        Op::Put { key, body, cond } => Done::Put(store.put_if(&key, body, &cond).await),
        Op::Delete { key, cond } => Done::Delete(store.delete(&key, &cond).await),
    }
}

/// Het antwoord bij een uitkomst (Go: `serveGet`, `servePut`, `serveDelete`).
#[must_use]
pub fn respond(done: Done) -> Response {
    match done {
        Done::Get(Ok(obj)) => Response {
            etag: Some(obj.etag),
            content_type: Some(APPLICATION_JSON),
            body: Body::Bytes(obj.body),
            ..Response::status(200)
        },
        Done::Get(Err(Error::NotFound)) => Response::error(404, "not found"),
        Done::Get(Err(e)) => Response::internal(&e),
        Done::Put(Ok(etag)) => Response {
            etag: Some(etag),
            ..Response::status(200)
        },
        Done::Put(Err(Error::Precondition)) => Response::error(412, "precondition failed"),
        Done::Put(Err(e)) => key_error(&e),
        Done::Delete(Ok(())) => Response::status(204),
        Done::Delete(Err(Error::NotFound)) => Response::error(404, "not found"),
        Done::Delete(Err(Error::Precondition)) => Response::error(412, "precondition failed"),
        Done::Delete(Err(e)) => key_error(&e),
    }
}
