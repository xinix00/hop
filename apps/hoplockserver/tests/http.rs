//! `internal/server/server_test.go` over echte sockets: de host-bin zoals hij
//! draait (store-thread, werkers, leanhttp), met een HTTP-client van buiten.
//! De Go-naam staat boven elke test; een test zonder Go-naam zegt waarom.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use hostnet::{Call, Http, Reply};

const T: Duration = Duration::from_secs(5);

/// Go's `mustDo`.
fn must_do(
    method: &'static str,
    url: &str,
    body: &str,
    if_none_match: &str,
    if_match: &str,
) -> Reply {
    must_do_key(method, url, body, if_none_match, if_match, "")
}

fn must_do_key(
    method: &'static str,
    url: &str,
    body: &str,
    if_none_match: &str,
    if_match: &str,
    key: &str,
) -> Reply {
    let mut headers = Vec::new();
    if !if_none_match.is_empty() {
        headers.push(("If-None-Match", if_none_match));
    }
    if !if_match.is_empty() {
        headers.push(("If-Match", if_match));
    }
    if !key.is_empty() {
        headers.push(("X-API-Key", key));
    }
    let call = Call {
        method,
        url,
        headers: &headers,
        body: (method == "PUT").then_some(body.as_bytes()),
        timeout: T,
    };
    Http::new().request(&call, 2 << 20).unwrap()
}

fn etag(r: &Reply) -> String {
    r.header("ETag").unwrap_or_default().to_string()
}

// TestEndToEndCAS
#[test]
fn end_to_end_cas() {
    let srv = common::serve("");
    let u = format!("{}/lease/a", srv.url);

    let get_missing = must_do("GET", &u, "", "", "");
    assert_eq!(get_missing.status, 404, "GET missing");

    let create = must_do("PUT", &u, r#"{"owner":"x"}"#, "*", "");
    assert_eq!(create.status, 200, "create");
    let etag1 = etag(&create);
    assert!(!etag1.is_empty(), "missing ETag");

    let conflict = must_do("PUT", &u, "x", "*", "");
    assert_eq!(conflict.status, 412, "expected 412 on second create");

    let get = must_do("GET", &u, "", "", "");
    assert_eq!(etag(&get), etag1, "ETag mismatch on read");

    let bad = must_do("PUT", &u, r#"{"owner":"y"}"#, "", "\"deadbeef\"");
    assert_eq!(bad.status, 412, "expected 412 on stale if-match");

    let ok = must_do("PUT", &u, r#"{"owner":"y"}"#, "", &etag1);
    assert_eq!(ok.status, 200, "expected 200 on fresh if-match");
    let etag2 = etag(&ok);
    assert_ne!(etag2, etag1, "expected new ETag after update");

    let del_stale = must_do("DELETE", &u, "", "", &etag1);
    assert_eq!(del_stale.status, 412, "expected 412 on stale delete");
    let del_ok = must_do("DELETE", &u, "", "", &etag2);
    assert_eq!(del_ok.status, 204, "expected 204 on delete");
}

// TestAPIKeyEnforced
#[test]
fn api_key_enforced() {
    let srv = common::serve("secret");
    let u = format!("{}/anything", srv.url);
    assert_eq!(must_do_key("GET", &u, "", "", "", "").status, 401);
    assert_eq!(must_do_key("GET", &u, "", "", "", "wrong").status, 401);
    assert_eq!(
        must_do_key("GET", &u, "", "", "", "secret").status,
        404,
        "expected 404 with correct key on missing key"
    );
    let health = must_do("GET", &format!("{}/health", srv.url), "", "", "");
    assert_eq!(health.status, 200, "expected health 200");
    assert_eq!(health.body, b"ok");
}

/// Eén rauw verzoek; het hele antwoord als tekst (de server sluit na `close`).
fn raw(url: &str, request: &str) -> String {
    let addr = url.trim_start_matches("http://");
    let mut s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(T)).unwrap();
    s.write_all(request.as_bytes()).unwrap();
    let mut out = String::new();
    s.read_to_string(&mut out).unwrap();
    out
}

// Geen Go-naam: de bytes op de draad, zoals Hop's client ze leest.
#[test]
fn the_wire_bytes() {
    let srv = common::serve("");
    let put = raw(
        &srv.url,
        "PUT /leases/c HTTP/1.1\r\nHost: t\r\nIf-None-Match: *\r\nContent-Type: application/json\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello",
    );
    assert!(put.starts_with("HTTP/1.1 200 OK\r\n"), "{put}");
    assert!(
        put.contains(
            "\r\nETag: \"2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824\"\r\n"
        ),
        "{put}"
    );
    assert!(put.contains("\r\nContent-Length: 0\r\n"), "{put}");
    assert!(put.ends_with("\r\n\r\n"), "{put}");

    let again = raw(
        &srv.url,
        "PUT /leases/c HTTP/1.1\r\nHost: t\r\nIf-None-Match: *\r\nContent-Length: 1\r\nConnection: close\r\n\r\nx",
    );
    // De reden achter 412 kent leanhttp v3.1.1 niet ("Status" in plaats
    // van Go's "Precondition Failed"); een client leest alleen de code
    // (RFC 9112 §4: de reden negeren), Hop's client ook.
    assert!(again.starts_with("HTTP/1.1 412 "), "{again}");
    assert!(
        again.contains("\r\nContent-Type: text/plain; charset=utf-8\r\n"),
        "{again}"
    );
    assert!(
        again.contains("\r\nX-Content-Type-Options: nosniff\r\n"),
        "{again}"
    );
    assert!(again.ends_with("\r\n\r\nprecondition failed\n"), "{again}");

    let get = raw(
        &srv.url,
        "GET /leases/c HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n",
    );
    assert!(get.starts_with("HTTP/1.1 200 OK\r\n"), "{get}");
    assert!(
        get.contains("\r\nContent-Type: application/json\r\n"),
        "{get}"
    );
    assert!(get.ends_with("\r\n\r\nhello"), "{get}");

    let del = raw(
        &srv.url,
        "DELETE /leases/c HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n",
    );
    assert!(del.starts_with("HTTP/1.1 204 No Content\r\n"), "{del}");
    assert!(!del.contains("Content-Length"), "{del}");

    let post = raw(
        &srv.url,
        "POST /x HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n",
    );
    assert!(
        post.starts_with("HTTP/1.1 405 Method Not Allowed\r\n"),
        "{post}"
    );
    assert!(post.contains("\r\nAllow: GET, PUT, DELETE\r\n"), "{post}");
    assert!(post.ends_with("method not allowed\n"), "{post}");
}

// Geen Go-naam: de lease staat als kaal bestand in de datamap, zoals in Go.
#[test]
fn the_lease_lands_in_the_data_dir() {
    let srv = common::serve("");
    let r = must_do(
        "PUT",
        &format!("{}/leases/prod", srv.url),
        "{\"owner\":\"n1\"}",
        "*",
        "",
    );
    assert_eq!(r.status, 200);
    assert_eq!(
        std::fs::read(srv.data.join("leases/prod")).unwrap(),
        b"{\"owner\":\"n1\"}"
    );
}

// Geen Go-naam: Go's grens van 1 MiB; daarboven weigert leanhttp met 413 (Go: 400).
#[test]
fn the_body_limit_is_one_mib() {
    let srv = common::serve("");
    let u = format!("{}/big", srv.url);
    let full = "a".repeat(1 << 20);
    assert_eq!(must_do("PUT", &u, &full, "", "").status, 200);
    let over = "a".repeat((1 << 20) + 1);
    assert_eq!(must_do("PUT", &u, &over, "", "").status, 413);
    assert_eq!(must_do("GET", &u, "", "", "").body.len(), 1 << 20);
}

// Geen Go-naam: één eigenaar maakt de CAS lineariseerbaar. Acht clients
// hogen samen één teller 200 keer op, elk met GET en PUT If-Match; geen
// ophoging gaat verloren (Go bewees dat met zijn mutex, wij met de thread).
#[test]
fn concurrent_cas_loses_no_update() {
    let srv = common::serve("");
    let u = format!("{}/counter", srv.url);
    assert_eq!(must_do("PUT", &u, "0", "*", "").status, 200);
    let threads: Vec<_> = (0..8)
        .map(|_| {
            let u = u.clone();
            std::thread::spawn(move || {
                let mut done = 0;
                let mut conflicts = 0;
                while done < 25 {
                    let got = must_do("GET", &u, "", "", "");
                    let n: u64 = std::str::from_utf8(&got.body).unwrap().parse().unwrap();
                    let put = must_do("PUT", &u, &(n + 1).to_string(), "", &etag(&got));
                    match put.status {
                        200 => done += 1,
                        412 => conflicts += 1,
                        s => panic!("status {s}"),
                    }
                }
                conflicts
            })
        })
        .collect();
    let conflicts: u64 = threads.into_iter().map(|t| t.join().unwrap()).sum();
    let got = must_do("GET", &u, "", "", "");
    assert_eq!(got.body, b"200", "{conflicts} conflicts");
}

// Geen Go-naam: een niet-canoniek pad is 400 (leanhttp), niet Go's 301.
#[test]
fn a_non_canonical_path_is_refused() {
    let srv = common::serve("");
    let r = raw(
        &srv.url,
        "GET //a/../b HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n",
    );
    assert!(r.starts_with("HTTP/1.1 400 "), "{r}");
}
