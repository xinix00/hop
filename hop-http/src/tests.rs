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

use crate::{Reply, refuse, serve};

pub(crate) struct Mem {
    input: Vec<u8>,
    at: usize,
    out: Rc<RefCell<Vec<u8>>>,
}

impl AsyncRead for Mem {
    fn poll_read(&mut self, _: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize, IoError>> {
        let rest = &self.input[self.at..];
        let n = rest.len().min(buf.len());
        buf[..n].copy_from_slice(&rest[..n]);
        self.at += n;
        Poll::Ready(Ok(n))
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

/// Stuurt `raw` naar een server met `handler` en geeft wat hij terugschreef.
fn exchange(raw: &[u8], handler: impl AsyncFnMut(Request) -> Reply) -> String {
    let out = Rc::new(RefCell::new(Vec::new()));
    let conn = Mem {
        input: raw.to_vec(),
        at: 0,
        out: out.clone(),
    };
    block_on(serve(conn, handler)).unwrap();
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
