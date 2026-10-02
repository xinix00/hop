//! De adapter tegen een in-memory verbinding: verzoekbytes erin, antwoordbytes eruit.
//!
//! leanhttp's eigen pijp is `pub(crate)` in zijn tests; deze is kleiner: de
//! client heeft alles al gestuurd en sluit (EOF na het verzoek), en de
//! server schrijft in een buffer die de test daarna leest.

use alloc::string::String;
use alloc::vec::Vec;
use core::future::Future;
use core::pin::pin;
use core::task::{Context, Poll, Waker};
use std::cell::RefCell;
use std::rc::Rc;

use api::{Effect, LogStream, Method, Request, Response};
use leanhttp::{AsyncRead, AsyncWrite, Close, IoError};

use crate::{Ask, Chunk, Reply, Streams, refuse, serve};

pub(crate) struct Mem {
    input: Vec<u8>,
    at: usize,
    out: Rc<RefCell<Vec<u8>>>,
    /// Een client die na zijn verzoek blijft hangen (een lezer van een
    /// stroom): na de invoer `Pending` in plaats van EOF, en een lees met
    /// termijn (de sondering van `reader_gone`) verloopt meteen.
    hold: bool,
    probing: bool,
}

impl AsyncRead for Mem {
    fn poll_read(&mut self, _: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize, IoError>> {
        let rest = &self.input[self.at..];
        if rest.is_empty() && self.hold {
            return if self.probing {
                Poll::Ready(Err(IoError::TimedOut))
            } else {
                Poll::Pending
            };
        }
        let n = rest.len().min(buf.len());
        buf[..n].copy_from_slice(&rest[..n]);
        self.at += n;
        Poll::Ready(Ok(n))
    }

    fn set_read_timeout(&mut self, t: Option<core::time::Duration>) -> Result<(), IoError> {
        self.probing = t.is_some();
        Ok(())
    }
}

impl AsyncWrite for Mem {
    fn poll_write(&mut self, _: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, IoError>> {
        self.out.borrow_mut().extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }
}

impl Close for Mem {
    fn poll_close(&mut self, _: &mut Context<'_>) -> Poll<Result<(), IoError>> {
        Poll::Ready(Ok(()))
    }
}

pub(crate) fn block_on<F: Future>(f: F) -> F::Output {
    let mut f = pin!(f);
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..100_000 {
        if let Poll::Ready(v) = f.as_mut().poll(&mut cx) {
            return v;
        }
    }
    panic!("future bleef hangen");
}

/// De naden van een stroom in de test: een vaste rij antwoorden van de
/// "eigenaar", een klok die per vraag een seconde verspringt, en een telling
/// van de vragen en de afmeldingen.
#[derive(Default)]
struct Fake {
    chunks: Vec<Chunk>,
    asked: Vec<Ask>,
    done: u32,
    clock: u64,
}

impl Streams for Fake {
    async fn poll(&mut self, ask: Ask) -> Chunk {
        self.asked.push(ask);
        self.clock += 1_000_000_000;
        if self.chunks.is_empty() {
            return Chunk {
                done: true,
                ..Chunk::default()
            };
        }
        self.chunks.remove(0)
    }

    async fn nap(&mut self, _d: core::time::Duration) {}

    fn now(&self) -> u64 {
        self.clock
    }

    fn done(&mut self) {
        self.done += 1;
    }
}

/// Stuurt `raw` naar een server met `handler` en geeft wat hij terugschreef.
fn exchange(raw: &[u8], handler: impl AsyncFnMut(Request) -> Reply) -> String {
    exchange_with(raw, handler, &mut Fake::default())
}

fn exchange_with(
    raw: &[u8],
    handler: impl AsyncFnMut(Request) -> Reply,
    streams: &mut Fake,
) -> String {
    exchange_conn(raw, false, handler, streams)
}

/// Als [`exchange_with`], met een client die na zijn verzoek blijft lezen
/// (een lezer van een stroom) in plaats van te sluiten.
fn exchange_held(
    raw: &[u8],
    handler: impl AsyncFnMut(Request) -> Reply,
    streams: &mut Fake,
) -> String {
    exchange_conn(raw, true, handler, streams)
}

fn exchange_conn(
    raw: &[u8],
    hold: bool,
    handler: impl AsyncFnMut(Request) -> Reply,
    streams: &mut Fake,
) -> String {
    let out = Rc::new(RefCell::new(Vec::new()));
    let conn = Mem {
        input: raw.to_vec(),
        at: 0,
        out: out.clone(),
        hold,
        probing: false,
    };
    block_on(serve(conn, handler, streams)).unwrap();
    String::from_utf8(out.borrow().clone()).unwrap()
}

#[test]
fn a_request_arrives_whole_and_the_response_goes_back() {
    let mut seen = Vec::new();
    let raw = b"POST /run?replace=1 HTTP/1.1\r\nHost: n\r\nX-Hop-Auth: abc\r\nContent-Length: 11\r\n\r\n{\"name\":1}\n";
    let text = exchange(raw, async |req: Request| {
        seen.push(req.clone());
        let mut r = Response::json(201, &types::json::Value::Bool(true));
        r.set_header("X-Test", "yes");
        Reply::Plain(r)
    });
    assert!(text.starts_with("HTTP/1.1 201"), "{text}");
    assert!(text.contains("X-Test: yes"), "{text}");
    assert!(text.contains("Content-Type: application/json"), "{text}");
    assert!(text.ends_with("\r\n\r\ntrue"), "{text}");
    let req = &seen[0];
    assert_eq!(req.method, Method::Post);
    assert_eq!(req.path, "/run");
    assert_eq!(req.query_param("replace"), Some("1"));
    assert_eq!(req.header("x-hop-auth"), Some("abc"));
    assert_eq!(req.body, b"{\"name\":1}\n");
}

#[test]
fn keep_alive_serves_requests_in_order() {
    let raw = b"GET /a HTTP/1.1\r\nHost: n\r\n\r\nGET /b HTTP/1.1\r\nHost: n\r\n\r\n";
    let mut paths = Vec::new();
    let text = exchange(raw, async |req: Request| {
        paths.push(req.path.clone());
        Reply::Plain(Response::error(404, "not found"))
    });
    assert_eq!(paths, ["/a", "/b"]);
    assert_eq!(text.matches("HTTP/1.1 404").count(), 2);
}

#[test]
fn a_body_over_the_kam_limit_never_reaches_the_handler() {
    let raw = format!(
        "POST /v1/jobs HTTP/1.1\r\nHost: n\r\nContent-Length: {}\r\n\r\n",
        leanhttp::MAX_BODY_BYTES + 1
    );
    let mut calls = 0;
    let text = exchange(raw.as_bytes(), async |_req: Request| {
        calls += 1;
        Reply::Plain(Response::empty(200))
    });
    assert_eq!(calls, 0);
    assert!(text.starts_with("HTTP/1.1 413"), "{text}");
}

#[test]
fn events_are_sse_lines() {
    let raw = b"GET /logs/t1 HTTP/1.1\r\nHost: n\r\n\r\n";
    let text = exchange(raw, async |_req: Request| Reply::Events {
        head: Response::empty(200),
        lines: alloc::vec![String::from("one"), String::from("two")],
    });
    assert!(text.contains("Content-Type: text/event-stream"), "{text}");
    assert!(text.contains("data: one\n\n"), "{text}");
    assert!(text.contains("data: two\n\n"), "{text}");
}

#[test]
fn unwired_effects_are_refused_loudly() {
    assert_eq!(refuse(&Effect::None), None);
    let p = refuse(&Effect::Proxy {
        leader: String::from("10.0.0.9:9080"),
        stream: false,
    })
    .unwrap();
    assert_eq!(p.status, 502);
    assert!(String::from_utf8_lossy(&p.body).contains("10.0.0.9:9080"));
    let f = refuse(&Effect::Flip {
        url: String::new(),
        sha256: String::new(),
        cold: false,
    })
    .unwrap();
    assert_eq!(f.status, 501);
    let l = refuse(&Effect::Logs {
        task_id: String::from("t1"),
        stream: LogStream::Stdout,
    })
    .unwrap();
    assert_eq!(l.status, 404);
}

#[test]
fn a_stream_asks_the_owner_until_it_is_done() {
    let raw = b"GET /logs/t1/stdout?follow=1 HTTP/1.1\r\nHost: n\r\n\r\n";
    let mut f = Fake {
        chunks: vec![
            Chunk {
                text: "data: one\n\n".into(),
                seq: 1,
                done: false,
            },
            // Niets nieuws: de volgende vraag houdt hetzelfde nummer.
            Chunk {
                text: String::new(),
                seq: 1,
                done: false,
            },
            Chunk {
                text: String::new(),
                seq: 1,
                done: false,
            },
            Chunk {
                text: "data: two\n\n".into(),
                seq: 2,
                done: true,
            },
        ],
        ..Fake::default()
    };
    let first = Ask::Logs {
        task_id: "t1".into(),
        stream: LogStream::Stdout,
        seq: 0,
    };
    let text = exchange_held(
        raw,
        async |_req: Request| Reply::Stream {
            head: Response::empty(200),
            first: String::new(),
            ask: first.clone(),
        },
        &mut f,
    );
    assert!(text.contains("Content-Type: text/event-stream"), "{text}");
    assert!(text.contains("data: one\n\n"), "{text}");
    assert!(text.contains("data: two\n\n"), "{text}");
    let one = text.find("data: one").unwrap();
    assert!(text[one..].contains("data: two"));
    let seqs: Vec<u64> = f
        .asked
        .iter()
        .map(|a| match a {
            Ask::Logs { seq, .. } | Ask::Events { seq } => *seq,
        })
        .collect();
    assert_eq!(seqs, [0, 1, 1, 1]);
    // Afgemeld, precies één keer.
    assert_eq!(f.done, 1);
}

#[test]
fn a_silent_stream_writes_a_keepalive_and_the_events_stream_starts_with_ping() {
    let raw = b"GET /v1/events HTTP/1.1\r\nHost: n\r\n\r\n";
    // Twintig lege antwoorden: de klok springt een seconde per vraag, dus
    // na vijftien seconden stilte komt er een keepalive.
    let mut f = Fake {
        chunks: (0..20)
            .map(|_| Chunk {
                text: String::new(),
                seq: 7,
                done: false,
            })
            .collect(),
        ..Fake::default()
    };
    let text = exchange_held(
        raw,
        async |_req: Request| Reply::Stream {
            head: Response::empty(200),
            first: String::from(api::PING),
            ask: Ask::Events { seq: 7 },
        },
        &mut f,
    );
    assert!(text.contains("event: ping\ndata: {}\n\n"), "{text}");
    assert_eq!(text.matches(": keepalive").count(), 1, "{text}");
    assert_eq!(f.done, 1);
}

#[test]
fn a_reader_that_leaves_frees_the_stream() {
    // De eigenaar heeft altijd iets nieuws, maar de client sloot al na zijn
    // verzoek (EOF): de stroom ziet dat aan zijn leeskant, eindigt en meldt
    // zich af, in plaats van zijn werker en zijn stroomplek vast te houden
    // tot een schrijf faalt (op de netstack van HopOS: lang niet).
    let raw = b"GET /v1/events HTTP/1.1\r\nHost: n\r\n\r\n";
    let mut f = Fake {
        chunks: (0..100)
            .map(|i| Chunk {
                text: alloc::format!("data: {i}\n\n"),
                seq: i + 1,
                done: false,
            })
            .collect(),
        ..Fake::default()
    };
    let text = exchange_with(
        raw,
        async |_req: Request| Reply::Stream {
            head: Response::empty(200),
            first: String::from(api::PING),
            ask: Ask::Events { seq: 0 },
        },
        &mut f,
    );
    // De kop ging nog weg, met de sluiting erbij; daarna geen pompen meer
    // (ook de ping niet: leanhttp kijkt vóór elk stuk of de lezer er is).
    assert!(text.contains("Connection: close"), "{text}");
    assert!(
        f.asked.len() <= 1,
        "the stream kept pumping to a reader that left: {} asks",
        f.asked.len()
    );
    assert_eq!(f.done, 1);
}

#[test]
fn a_plain_reply_is_not_a_stream() {
    let raw = b"GET /health HTTP/1.1\r\nHost: n\r\n\r\n";
    let mut f = Fake::default();
    exchange_with(
        raw,
        async |_req: Request| Reply::Plain(Response::empty(200)),
        &mut f,
    );
    assert!(f.asked.is_empty());
    assert_eq!(f.done, 0);
}
