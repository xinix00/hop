//! Een nep-HTTP-server op 127.0.0.1 voor de tests.
//!
//! Eén thread bezit de listener en de nep-staat; geen mutex. Een test die de
//! staat wil zien, krijgt hem terug bij [`Fake::stop`], samen met het logboek
//! van alle verzoeken. Elke verbinding wordt met `leanhttp::serve` over een
//! [`StdConn`] bediend, op de thread zelf met [`block_on`].

use std::collections::BTreeMap;
use std::net::{TcpListener, TcpStream};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use hostnet::{Call, Http, StdConn, block_on};
use leanhttp::Exchange;

/// De route die de serverthread laat stoppen.
const STOP: &str = "/__stop";

/// Eén gezien verzoek.
#[derive(Clone, Debug)]
pub(crate) struct Seen {
    pub(crate) method: String,
    pub(crate) path: String,
    pub(crate) header: Vec<(String, String)>,
    pub(crate) body: Vec<u8>,
}

impl Seen {
    /// Een kop op naam, leeg als hij er niet is.
    pub(crate) fn header(&self, name: &str) -> &str {
        self.header
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map_or("", |(_, v)| v.as_str())
    }
}

/// Het antwoord van een nep-handler.
#[derive(Clone, Debug, Default)]
pub(crate) struct Answer {
    pub(crate) status: u16,
    pub(crate) etag: Option<String>,
    pub(crate) body: Vec<u8>,
}

impl Answer {
    pub(crate) fn status(status: u16) -> Self {
        Self {
            status,
            ..Self::default()
        }
    }

    pub(crate) fn etag(mut self, etag: &str) -> Self {
        self.etag = Some(etag.to_string());
        self
    }

    pub(crate) fn body(mut self, body: &[u8]) -> Self {
        self.body = body.to_vec();
        self
    }
}

/// Een draaiende nep-server.
pub(crate) struct Fake<S> {
    addr: String,
    thread: JoinHandle<(S, Vec<Seen>)>,
}

impl<S: Send + 'static> Fake<S> {
    /// Start een server met `state` en `handle` op een vrije poort.
    pub(crate) fn spawn(state: S, handle: fn(&mut S, &Seen) -> Answer) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let thread = thread::spawn(move || run(&listener, state, handle));
        Self { addr, thread }
    }

    /// De basis-URL, zonder slash op het eind.
    pub(crate) fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Stopt de server en geeft de staat en alle verzoeken terug.
    pub(crate) fn stop(self) -> (S, Vec<Seen>) {
        let url = format!("{}{STOP}", self.url());
        let _ = Http::new().request(&Call::get(&url, Duration::from_secs(5)), 1024);
        self.thread.join().unwrap()
    }
}

fn run<S>(
    listener: &TcpListener,
    mut state: S,
    handle: fn(&mut S, &Seen) -> Answer,
) -> (S, Vec<Seen>) {
    let mut log = Vec::new();
    let mut stop = false;
    for stream in listener.incoming() {
        let Ok(stream) = stream else { continue };
        let conn = StdConn::new(stream, Some(Duration::from_secs(5)));
        let _ = block_on(leanhttp::serve(
            conn,
            async |ex: &mut Exchange<'_, StdConn<TcpStream>>| {
                let body = ex.read_body_to_end().await?;
                let seen = Seen {
                    method: ex.req.method.clone(),
                    path: ex.req.path.clone(),
                    header: ex
                        .req
                        .header
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect(),
                    body,
                };
                if seen.path == STOP {
                    stop = true;
                    return ex.write_header(200);
                }
                let a = handle(&mut state, &seen);
                log.push(seen);
                if let Some(e) = &a.etag {
                    ex.header_mut().set("ETag", e)?;
                }
                ex.write_header(a.status)?;
                if !a.body.is_empty() {
                    ex.write(&a.body).await?;
                }
                Ok(())
            },
        ));
        if stop {
            break;
        }
    }
    (state, log)
}

/// Een object-store met het CAS-protocol van S3 en hoplockserver.
///
/// Sleutel is het pad; de ETag is `"etag-<n>"` met een teller over de hele
/// store. Met `bare_if_match` vergelijkt hij `If-Match` alleen met de kale
/// ETag (Hetzner, Ceph); met `api_key` weigert hij verzoeken zonder de
/// juiste `X-API-Key` met 401; met `require_sig` weigert hij verzoeken
/// zonder SigV4-handtekening met 403.
#[derive(Debug, Default)]
pub(crate) struct Objects {
    pub(crate) data: BTreeMap<String, (Vec<u8>, String)>,
    pub(crate) seq: u32,
    pub(crate) bare_if_match: bool,
    pub(crate) api_key: Option<&'static str>,
    pub(crate) require_sig: bool,
}

impl Objects {
    fn matches(&self, stored: &str, if_match: &str) -> bool {
        if self.bare_if_match {
            stored.trim_matches('"') == if_match
        } else {
            stored == if_match
        }
    }
}

/// De handler van [`Objects`].
pub(crate) fn objects(s: &mut Objects, r: &Seen) -> Answer {
    if let Some(key) = s.api_key
        && r.header("X-API-Key") != key
    {
        return Answer::status(401).body(b"unauthorized");
    }
    if s.require_sig && !r.header("Authorization").starts_with("AWS4-HMAC-SHA256 ") {
        return Answer::status(403).body(b"missing sigv4 auth");
    }
    let current = s.data.get(&r.path).cloned();
    match r.method.as_str() {
        "GET" | "HEAD" => match current {
            Some((body, etag)) => Answer::status(200).etag(&etag).body(&body),
            None => Answer::status(404).body(b"no such key"),
        },
        "PUT" => {
            let if_none = r.header("If-None-Match");
            let if_match = r.header("If-Match");
            if if_none == "*" && current.is_some() {
                return Answer::status(412).body(b"exists");
            }
            if !if_match.is_empty() {
                let ok = current
                    .as_ref()
                    .is_some_and(|(_, e)| s.matches(e, if_match));
                if !ok {
                    return Answer::status(412).body(b"etag mismatch");
                }
            }
            s.seq += 1;
            let etag = format!("\"etag-{}\"", s.seq);
            s.data
                .insert(r.path.clone(), (r.body.clone(), etag.clone()));
            Answer::status(200).etag(&etag)
        }
        "DELETE" => {
            let Some((_, etag)) = current else {
                return Answer::status(404).body(b"no such key");
            };
            let if_match = r.header("If-Match");
            if !if_match.is_empty() && !s.matches(&etag, if_match) {
                return Answer::status(412).body(b"etag mismatch");
            }
            s.data.remove(&r.path);
            Answer::status(204)
        }
        _ => Answer::status(405),
    }
}

/// Een ghost zoals Bunny Storage na een half toegepaste DELETE: GET zegt
/// 404, HEAD 200 met ETag, een aanmaak krijgt 412, en alleen een PUT met
/// `If-Match` op die ETag komt erdoor.
#[derive(Debug, Default)]
pub(crate) struct Ghost {
    /// De ETag die HEAD geeft.
    pub(crate) etag: String,
    /// Of een PUT op de ETag mag slagen (anders altijd 412).
    pub(crate) takeable: bool,
    /// Hoeveel overnames slaagden.
    pub(crate) takeovers: u32,
}

/// De handler van [`Ghost`].
///
/// `POST /__etag` met de nieuwe ETag als body speelt een levende eigenaar
/// die vernieuwt: de ETag verschuift.
pub(crate) fn ghost(s: &mut Ghost, r: &Seen) -> Answer {
    if r.method == "POST" && r.path == "/__etag" {
        s.etag = String::from_utf8_lossy(&r.body).into_owned();
        return Answer::status(204);
    }
    match r.method.as_str() {
        "GET" => Answer::status(404),
        "HEAD" => Answer::status(200).etag(&s.etag),
        "PUT" if r.header("If-None-Match") == "*" => Answer::status(412),
        "PUT" if s.takeable && r.header("If-Match") == s.etag => {
            s.takeovers += 1;
            Answer::status(200).etag("\"after-takeover\"")
        }
        "PUT" => Answer::status(412),
        _ => Answer::status(405),
    }
}
