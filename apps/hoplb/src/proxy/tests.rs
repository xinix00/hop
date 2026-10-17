//! De proxy tegen verbindingen in het geheugen: de test van
//! `OLD/internal/lb/proxy_test.go` naam voor naam, `BenchmarkProxyHandler`
//! als test met een meetregel, en het gedrag van `httputil.ReverseProxy`
//! dat Go gratis had.

use super::*;
use crate::route::{Backend, Route, RouteTable};
use crate::testutil::{Op, Pipe, bench, block_on};
use alloc::collections::VecDeque;
use alloc::string::ToString;
use alloc::vec;
use core::cell::Cell;

/// Een schil in het geheugen: een routetabel, backends als pijpen die per
/// `dial` om de beurt opgaan, en de metingen.
struct TestEnv {
    routes: RouteTable,
    backends: VecDeque<Pipe>,
    dialed: Vec<String>,
    records: Vec<Record>,
    clock: Cell<u64>,
    errors: Vec<Error>,
    /// Elke werker bezet: het antwoord zegt `Connection: close`.
    crowded: bool,
}

impl TestEnv {
    fn new(routes: &[(&str, &[&str])], backends: Vec<Pipe>) -> Self {
        let routes = RouteTable::from_routes(
            routes
                .iter()
                .map(|(p, addrs)| {
                    Route::new(
                        p.to_string(),
                        addrs.iter().map(|a| Backend::new(a).unwrap()).collect(),
                    )
                })
                .collect(),
        )
        .unwrap();
        Self {
            routes,
            backends: backends.into(),
            dialed: Vec::new(),
            records: Vec::new(),
            clock: Cell::new(0),
            errors: Vec::new(),
            crowded: false,
        }
    }
}

impl Env for TestEnv {
    type Conn = Pipe;

    fn pick(&mut self, host: &str) -> Pick {
        self.routes.pick(host)
    }

    async fn dial(&mut self, addr: &str) -> Result<Pipe> {
        self.dialed.push(addr.to_string());
        self.backends
            .pop_front()
            .ok_or(Error::Dial(leanhttp::IoError::Other))
    }

    fn now_ns(&self) -> u64 {
        // Elke vraag een milliseconde later: zo is de duur van een meting
        // voorspelbaar.
        let t = self.clock.get() + 1_000_000;
        self.clock.set(t);
        t
    }

    fn record(&mut self, r: Record) {
        self.records.push(r);
    }

    fn backend_error(&mut self, _host: &str, _addr: &str, err: Error) {
        self.errors.push(err);
    }

    fn crowded(&self) -> bool {
        self.crowded
    }
}

fn run(client: &Pipe, env: &mut TestEnv, peer: &str) {
    // De client blijft op zijn antwoord wachten; pas na zijn laatste
    // verzoek sluit de proxy hem (de stilte tussen verzoeken verloopt).
    client.linger();
    block_on(serve(client.clone(), peer, env)).unwrap();
    assert!(
        client.is_closed(),
        "the client connection is closed at the end"
    );
}

const OK: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 5\r\n\r\nhello";

#[test]
fn test_status_writer_exposes_flusher() {
    // Go: de wrapper die de status vangt, mag de Flusher niet verbergen,
    // anders buffert een SSE-stroom door hoplb heen. Hier: elke hap van een
    // gestroomd antwoord gaat naar de client en wordt doorgespoeld vóór de
    // proxy de volgende hap van de backend leest.
    let backend = Pipe::new(&[
        b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n",
        b"e\r\ndata: tick 1\n\n\r\n",
        b"e\r\ndata: tick 2\n\n\r\n",
        b"0\r\n\r\n",
    ]);
    let client =
        Pipe::new(&[b"GET /events HTTP/1.1\r\nHost: sse.example.com\r\nConnection: close\r\n\r\n"]);
    let mut env = TestEnv::new(
        &[("sse.example.com", &["10.0.0.1:80"])],
        vec![backend.clone()],
    );
    run(&client, &mut env, "192.0.2.7");
    let text = client.text();
    assert!(text.contains("data: tick 1\n\n"), "{text}");
    // De volgorde op de clientverbinding: tick 1 geschreven, dan een flush,
    // en pas dan tick 2.
    let ops = client.ops();
    let w1 = ops
        .iter()
        .position(|op| matches!(op, Op::Write(b) if b.windows(6).any(|w| w == b"tick 1")))
        .expect("tick 1 written");
    let w2 = ops
        .iter()
        .position(|op| matches!(op, Op::Write(b) if b.windows(6).any(|w| w == b"tick 2")))
        .expect("tick 2 written");
    assert!(
        ops[w1..w2].contains(&Op::Flush),
        "no flush between two SSE events: {ops:?}"
    );
    assert_eq!(env.records[0].code, 200);
}

#[test]
fn a_request_goes_through_with_forwarded_for_and_without_hop_by_hop() {
    let backend = Pipe::new(&[OK]);
    let client = Pipe::new(&[b"GET /x?y=1 HTTP/1.1\r\nHost: api.example.com\r\nX-Forwarded-For: 203.0.113.1\r\n\
Connection: keep-alive, X-Secret\r\nX-Secret: s\r\nKeep-Alive: 5\r\nProxy-Authorization: p\r\nUser-Agent: t\r\n\r\n"]);
    let mut env = TestEnv::new(
        &[("api.example.com", &["10.0.0.1:8080"])],
        vec![backend.clone()],
    );
    run(&client, &mut env, "192.0.2.7");
    let sent = backend.text();
    assert!(sent.starts_with("GET /x?y=1 HTTP/1.1\r\n"), "{sent}");
    assert!(sent.contains("Host: api.example.com\r\n"), "{sent}");
    assert!(sent.contains("User-Agent: t\r\n"), "{sent}");
    assert!(
        sent.contains("X-Forwarded-For: 203.0.113.1, 192.0.2.7\r\n"),
        "{sent}"
    );
    assert!(sent.ends_with("Connection: close\r\n\r\n"), "{sent}");
    for gone in [
        "X-Secret",
        "Keep-Alive",
        "Proxy-Authorization",
        "keep-alive",
    ] {
        assert!(!sent.contains(gone), "{gone} leaked: {sent}");
    }
    let got = client.text();
    assert!(got.starts_with("HTTP/1.1 200 OK\r\n"), "{got}");
    assert!(got.contains("Content-Length: 5\r\n"), "{got}");
    assert!(got.ends_with("\r\n\r\nhello"), "{got}");
    assert_eq!(env.dialed, ["10.0.0.1:8080"]);
    assert_eq!(
        env.records,
        [Record {
            domain: "api.example.com".into(),
            backend: "10.0.0.1:8080".into(),
            code: 200,
            nanos: 1_000_000,
        }]
    );
}

#[test]
fn keep_alive_carries_the_next_request_round_robin() {
    let b1 = Pipe::new(&[OK]);
    let b2 = Pipe::new(&[b"HTTP/1.1 201 Created\r\nContent-Length: 0\r\n\r\n"]);
    let client = Pipe::new(&[
        b"GET /1 HTTP/1.1\r\nHost: a.example.com\r\n\r\n",
        b"POST /2 HTTP/1.1\r\nHost: a.example.com\r\nContent-Length: 3\r\n\r\nabc",
    ]);
    let mut env = TestEnv::new(
        &[("a.example.com", &["10.0.0.1:80", "10.0.0.2:80"])],
        vec![b1.clone(), b2.clone()],
    );
    run(&client, &mut env, "");
    assert_eq!(env.dialed, ["10.0.0.2:80", "10.0.0.1:80"]);
    assert!(
        b2.text()
            .ends_with("Content-Length: 3\r\nConnection: close\r\n\r\nabc"),
        "{}",
        b2.text()
    );
    let got = client.text();
    assert!(
        got.contains("hello") && got.contains("HTTP/1.1 201 Created\r\n"),
        "{got}"
    );
    assert_eq!(
        env.records.iter().map(|r| r.code).collect::<Vec<_>>(),
        [200, 201]
    );
    // Zonder peer geen X-Forwarded-For.
    assert!(!b1.text().contains("X-Forwarded-For"));
}

#[test]
fn a_crowded_proxy_closes_after_the_answer() {
    let b1 = Pipe::new(&[OK]);
    let b2 = Pipe::new(&[OK]);
    let client = Pipe::new(&[
        b"GET /1 HTTP/1.1\r\nHost: a.example.com\r\n\r\n",
        b"GET /2 HTTP/1.1\r\nHost: a.example.com\r\n\r\n",
    ]);
    let mut env = TestEnv::new(
        &[("a.example.com", &["10.0.0.1:80"])],
        vec![b1.clone(), b2.clone()],
    );
    env.crowded = true;
    run(&client, &mut env, "");
    // Eén antwoord, met de sluiting erbij; het tweede verzoek wacht niet op
    // de stilte van deze verbinding maar krijgt een verse werker.
    let got = client.text();
    assert!(got.contains("Connection: close\r\n"), "{got}");
    assert_eq!(got.matches("HTTP/1.1 200").count(), 1, "{got}");
    assert_eq!(env.dialed.len(), 1);
}

#[test]
fn a_client_that_left_ends_the_relay_and_the_backend() {
    // Een backend die een stroom begint en dan zwijgt; de client is na zijn
    // verzoek al weg (EOF). De proxy ziet dat bij de eerste hap en stopt,
    // in plaats van tot `backend_idle` op de backend te wachten.
    let b1 = Pipe::new(&[
        b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n",
        b"b\r\ndata: one\n\n\r\n",
    ]);
    let client = Pipe::new(&[b"GET /events HTTP/1.1\r\nHost: a.example.com\r\n\r\n"]);
    let mut env = TestEnv::new(&[("a.example.com", &["10.0.0.1:80"])], vec![b1.clone()]);
    let _ = block_on(serve(client.clone(), "", &mut env));
    assert!(client.is_closed());
    assert!(b1.is_closed(), "the backend goes down with the client");
    // De kop ging nog weg; de eerste hap niet meer.
    let got = client.text();
    assert!(got.starts_with("HTTP/1.1 200"), "{got}");
    assert!(!got.contains("data: one"), "{got}");
}

#[test]
fn no_route_is_502_and_no_backend_is_503() {
    let client = Pipe::new(&[
        b"GET / HTTP/1.1\r\nHost: nope.example.com\r\n\r\n",
        b"GET / HTTP/1.1\r\nHost: down.example.com\r\n\r\n",
    ]);
    let mut env = TestEnv::new(&[("down.example.com", &[])], vec![]);
    run(&client, &mut env, "");
    let got = client.text();
    assert!(got.starts_with("HTTP/1.1 502 Bad Gateway\r\n"), "{got}");
    assert!(got.contains("\r\n\r\nno route for host\n"), "{got}");
    assert!(
        got.contains("HTTP/1.1 503 Service Unavailable\r\n"),
        "{got}"
    );
    assert!(got.ends_with("no healthy backend\n"), "{got}");
    let codes: Vec<_> = env
        .records
        .iter()
        .map(|r| (r.domain.as_str(), r.backend.as_str(), r.code))
        .collect();
    assert_eq!(
        codes,
        [("nope.example.com", "", 502), ("down.example.com", "", 503)]
    );
}

#[test]
fn a_dead_backend_is_502_backend_error() {
    let client =
        Pipe::new(&[b"GET / HTTP/1.1\r\nHost: a.example.com\r\nConnection: close\r\n\r\n"]);
    let mut env = TestEnv::new(&[("a.example.com", &["10.0.0.9:80"])], vec![]);
    run(&client, &mut env, "");
    assert!(
        client.text().contains("502 Bad Gateway") && client.text().ends_with("backend error\n")
    );
    assert_eq!(env.records[0].backend, "10.0.0.9:80");
    assert_eq!(env.records[0].code, 502);
    assert_eq!(env.errors.len(), 1);
}

#[test]
fn a_backend_that_hangs_up_before_answering_is_502() {
    let backend = Pipe::new(&[b"HTTP/1.1 200"]);
    let client = Pipe::new(&[b"GET / HTTP/1.1\r\nHost: a.example.com\r\n\r\n"]);
    let mut env = TestEnv::new(&[("a.example.com", &["10.0.0.1:80"])], vec![backend]);
    run(&client, &mut env, "");
    assert!(
        client.text().starts_with("HTTP/1.1 502 "),
        "{}",
        client.text()
    );
    assert_eq!(env.records[0].code, 502);
}

#[test]
fn a_chunked_upload_streams_to_the_backend() {
    let backend = Pipe::new(&[OK]);
    let client = Pipe::new(&[
        b"PUT /up HTTP/1.1\r\nHost: a.example.com\r\nTransfer-Encoding: chunked\r\nExpect: 100-continue\r\n\r\n",
        b"3\r\nabc\r\n",
        b"0\r\n\r\n",
    ]);
    let mut env = TestEnv::new(
        &[("a.example.com", &["10.0.0.1:80"])],
        vec![backend.clone()],
    );
    run(&client, &mut env, "");
    let sent = backend.text();
    assert!(sent.contains("Transfer-Encoding: chunked\r\n"), "{sent}");
    assert!(!sent.contains("Expect"), "{sent}");
    assert!(sent.ends_with("\r\n\r\n3\r\nabc\r\n0\r\n\r\n"), "{sent}");
    assert!(
        client
            .text()
            .starts_with("HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\n")
    );
}

#[test]
fn an_eof_framed_answer_closes_the_client() {
    let backend = Pipe::new(&[b"HTTP/1.0 200 OK\r\n\r\nall of it", b" until the end"]);
    let client = Pipe::new(&[
        b"GET / HTTP/1.1\r\nHost: a.example.com\r\n\r\n",
        b"GET /never HTTP/1.1\r\nHost: a.example.com\r\n\r\n",
    ]);
    let mut env = TestEnv::new(&[("a.example.com", &["10.0.0.1:80"])], vec![backend]);
    run(&client, &mut env, "");
    let got = client.text();
    assert!(got.contains("Connection: close\r\n"), "{got}");
    assert!(got.ends_with("all of it until the end"), "{got}");
    assert_eq!(env.records.len(), 1, "the second request is never read");
}

#[test]
fn an_http10_client_gets_chunked_unpacked() {
    let backend =
        Pipe::new(&[b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nhi\r\n0\r\n\r\n"]);
    let client = Pipe::new(&[b"GET / HTTP/1.0\r\nHost: a.example.com\r\n\r\n"]);
    let mut env = TestEnv::new(&[("a.example.com", &["10.0.0.1:80"])], vec![backend]);
    run(&client, &mut env, "");
    let got = client.text();
    assert!(!got.contains("Transfer-Encoding"), "{got}");
    assert!(
        got.contains("Connection: close\r\n") && got.ends_with("\r\n\r\nhi"),
        "{got}"
    );
}

#[test]
fn head_and_304_keep_their_informative_length() {
    let backend = Pipe::new(&[b"HTTP/1.1 200 OK\r\nContent-Length: 1234\r\n\r\n"]);
    let client =
        Pipe::new(&[b"HEAD / HTTP/1.1\r\nHost: a.example.com\r\nConnection: close\r\n\r\n"]);
    let mut env = TestEnv::new(&[("a.example.com", &["10.0.0.1:80"])], vec![backend]);
    run(&client, &mut env, "");
    assert!(
        client.text().contains("Content-Length: 1234\r\n"),
        "{}",
        client.text()
    );
    assert!(client.text().ends_with("\r\n\r\n"));
}

#[test]
fn bad_requests_get_their_status_and_a_closed_connection() {
    for (req, status) in [
        (&b"GET / HTTP/1.1\r\n\r\n"[..], "400"),
        (b"GET / HTTP/3\r\nHost: a\r\n\r\n", "505"),
        (b"CONNECT a:443 HTTP/1.1\r\nHost: a\r\n\r\n", "501"),
        (
            b"POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: gzip\r\n\r\n",
            "501",
        ),
        (b"GET / HTTP/1.1\r\nHost: a\r\nHost: b\r\n\r\n", "400"),
    ] {
        let client = Pipe::new(&[req]);
        let mut env = TestEnv::new(&[("a", &["10.0.0.1:80"])], vec![]);
        run(&client, &mut env, "");
        assert!(
            client
                .text()
                .starts_with(&alloc::format!("HTTP/1.1 {status} ")),
            "{req:?}: {}",
            client.text()
        );
        assert!(env.dialed.is_empty());
    }
}

#[test]
fn an_absolute_target_becomes_a_path_and_its_host_routes() {
    let backend = Pipe::new(&[OK]);
    let client = Pipe::new(&[
        b"GET http://a.example.com/p?q HTTP/1.1\r\nHost: other\r\nConnection: close\r\n\r\n",
    ]);
    let mut env = TestEnv::new(
        &[("a.example.com", &["10.0.0.1:80"])],
        vec![backend.clone()],
    );
    run(&client, &mut env, "");
    assert!(
        backend.text().starts_with("GET /p?q HTTP/1.1\r\n"),
        "{}",
        backend.text()
    );
}

#[test]
fn benchmark_proxy_handler() {
    // Go: het hele ServeHTTP-pad tegen een nep-backend, 36 µs/op met
    // httptest; hier het hele pad van de proxy in het geheugen.
    let routes = [("api.example.com", &["10.0.0.1:8080"][..])];
    let ns = bench("ProxyHandler", 20_000, |_| {
        let backend = Pipe::new(&[b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"]);
        let client = Pipe::new(&[
            b"GET /test HTTP/1.1\r\nHost: api.example.com\r\nConnection: close\r\n\r\n",
        ]);
        let mut env = TestEnv::new(&routes, vec![backend]);
        block_on(serve(client.clone(), "192.0.2.1", &mut env)).unwrap();
        assert_eq!(env.records[0].code, 200);
    });
    // De lat van BENCHMARKS.md: < 50 ms per verzoek.
    assert!(ns < 50_000_000.0, "proxy {ns:.0} ns/op");
}
