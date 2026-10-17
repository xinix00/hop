//! De Go-tests naam voor naam: `internal/store/store_test.go` tegen elke
//! store (geheugen en volume hier, de bestanden in `host`), en
//! `internal/server/server_test.go` tegen de toestandsmachine. De Go-naam
//! staat boven elke test; een test zonder Go-naam zegt waarom hij er is.
//! Dezelfde servertests draaien over echte sockets in `tests/http.rs`.

use alloc::string::String;
use alloc::vec::Vec;
use core::future::Future;
use core::pin::pin;
use core::task::{Context, Poll, Waker};

use crate::server::{Body, Done, Head, Response, Server, Step, respond, run};
use crate::store::{Condition, Error, Store};
use crate::volume::VolumeStore;
use crate::volume::mem::MemVolume;
use crate::{Etag, Key, MemStore};

/// Pollt één keer: elke store in deze tests is meteen klaar.
pub(crate) fn block_on<F: Future>(f: F) -> F::Output {
    let mut f = pin!(f);
    let mut cx = Context::from_waker(Waker::noop());
    match f.as_mut().poll(&mut cx) {
        Poll::Ready(v) => v,
        Poll::Pending => panic!("a test store returned Pending"),
    }
}

/// Go's `s.Put(key string, ...)`: de sleutel als tekst, dus met de toets.
pub(crate) fn put<S: Store>(
    s: &mut S,
    key: &str,
    body: &[u8],
    cond: &Condition,
) -> crate::Result<Etag> {
    let key = Key::parse(key)?;
    block_on(s.put_if(&key, body.to_vec(), cond))
}

pub(crate) fn get<S: Store>(s: &mut S, key: &str) -> crate::Result<crate::Object> {
    let key = Key::parse(key)?;
    block_on(s.get(&key))
}

pub(crate) fn delete<S: Store>(s: &mut S, key: &str, cond: &Condition) -> crate::Result {
    let key = Key::parse(key)?;
    block_on(s.delete(&key, cond))
}

/// De vier tests van `store_test.go` tegen de store die `$new` maakt.
macro_rules! go_store_tests {
    ($new:expr) => {
        use $crate::store::{Condition, Error};
        use $crate::tests::{delete, get, put};

        // TestPutGetDelete
        #[test]
        fn put_get_delete() {
            let mut s = $new;
            assert_eq!(get(&mut s, "lease/foo"), Err(Error::NotFound));

            let got = put(
                &mut s,
                "lease/foo",
                br#"{"owner":"a"}"#,
                &Condition::create(),
            )
            .unwrap();
            assert!(!got.as_str().is_empty());

            assert_eq!(
                put(&mut s, "lease/foo", b"x", &Condition::create()),
                Err(Error::Precondition)
            );

            let got2 = get(&mut s, "lease/foo").unwrap();
            assert_eq!(got2.body, br#"{"owner":"a"}"#);
            assert_eq!(got2.etag, got);

            assert_eq!(
                put(
                    &mut s,
                    "lease/foo",
                    b"y",
                    &Condition::matching("\"deadbeef\"")
                ),
                Err(Error::Precondition)
            );

            let got3 = put(
                &mut s,
                "lease/foo",
                br#"{"owner":"b"}"#,
                &Condition::matching(got.as_str()),
            )
            .unwrap();
            assert_ne!(got3, got);

            assert_eq!(
                delete(&mut s, "lease/foo", &Condition::matching(got.as_str())),
                Err(Error::Precondition)
            );
            assert_eq!(
                delete(&mut s, "lease/foo", &Condition::matching(got3.as_str())),
                Ok(())
            );
            assert_eq!(
                delete(&mut s, "lease/foo", &Condition::default()),
                Err(Error::NotFound)
            );
        }

        // TestEtagIsDeterministic
        #[test]
        fn etag_is_deterministic() {
            let mut s = $new;
            let a = put(&mut s, "k", b"hello", &Condition::create()).unwrap();
            delete(&mut s, "k", &Condition::default()).unwrap();
            let b = put(&mut s, "k", b"hello", &Condition::create()).unwrap();
            assert_eq!(a, b);
        }

        // TestPathTraversalRejected
        #[test]
        fn path_traversal_rejected() {
            let mut s = $new;
            for key in ["../escape", "/abs", "foo/../../escape", ""] {
                assert_eq!(
                    put(&mut s, key, b"x", &Condition::create()),
                    Err(Error::BadKey),
                    "key {key:?}"
                );
            }
        }

        // TestNestedKeyCreatesDirs
        #[test]
        fn nested_key_creates_dirs() {
            let mut s = $new;
            put(&mut s, "a/b/c/lease.json", b"x", &Condition::create()).unwrap();
            let got = get(&mut s, "a/b/c/lease.json").unwrap();
            assert_eq!(got.body, b"x");
        }
    };
}
pub(crate) use go_store_tests;

mod mem_store {
    go_store_tests!(crate::MemStore::new());
}

mod volume_store {
    go_store_tests!(crate::volume::VolumeStore::new(
        crate::volume::mem::MemVolume::default(),
        "/data"
    ));
}

// ---- internal/server: server_test.go ----

/// Go's `mustDo`: één verzoek door de toestandsmachine en de store.
#[expect(
    clippy::too_many_arguments,
    reason = "de argumenten van Go's mustDo, plus server, store en sleutel"
)]
fn call<S: Store>(
    server: &Server,
    store: &mut S,
    method: &str,
    path: &str,
    body: &str,
    if_none_match: &str,
    if_match: &str,
    api_key: &str,
) -> Response {
    let head = Head {
        method,
        path,
        api_key,
        if_match,
        if_none_match,
    };
    let step = match server.begin(&head) {
        Step::ReadBody(p) => server.with_body(p, body.as_bytes().to_vec()),
        other => other,
    };
    match step {
        Step::Respond(r) => r,
        Step::Store(op) => respond(block_on(run(store, op))),
        Step::ReadBody(_) => panic!("asked for the body twice"),
    }
}

fn etag_of(r: &Response) -> String {
    r.etag.map(|e| String::from(e.as_str())).unwrap_or_default()
}

// TestEndToEndCAS
#[test]
fn end_to_end_cas() {
    let srv = Server::new("");
    let mut st = MemStore::new();
    let mut c = |m, p, b, inm, im| call(&srv, &mut st, m, p, b, inm, im, "");

    let get_missing = c("GET", "/lease/a", "", "", "");
    assert_eq!(get_missing.status, 404);

    let create = c("PUT", "/lease/a", r#"{"owner":"x"}"#, "*", "");
    assert_eq!(create.status, 200);
    let etag1 = etag_of(&create);
    assert!(!etag1.is_empty(), "missing ETag");

    let conflict = c("PUT", "/lease/a", "x", "*", "");
    assert_eq!(conflict.status, 412, "expected 412 on second create");

    let got = c("GET", "/lease/a", "", "", "");
    assert_eq!(etag_of(&got), etag1, "ETag mismatch on read");

    let bad = c("PUT", "/lease/a", r#"{"owner":"y"}"#, "", "\"deadbeef\"");
    assert_eq!(bad.status, 412, "expected 412 on stale if-match");

    let ok = c("PUT", "/lease/a", r#"{"owner":"y"}"#, "", &etag1);
    assert_eq!(ok.status, 200, "expected 200 on fresh if-match");
    let etag2 = etag_of(&ok);
    assert_ne!(etag2, etag1, "expected new ETag after update");

    let del_stale = c("DELETE", "/lease/a", "", "", &etag1);
    assert_eq!(del_stale.status, 412, "expected 412 on stale delete");
    let del_ok = c("DELETE", "/lease/a", "", "", &etag2);
    assert_eq!(del_ok.status, 204, "expected 204 on delete");
}

// TestAPIKeyEnforced
#[test]
fn api_key_enforced() {
    let srv = Server::new("secret");
    let mut st = MemStore::new();
    let mut c = |key| call(&srv, &mut st, "GET", "/anything", "", "", "", key);
    assert_eq!(c("").status, 401, "expected 401 without key");
    assert_eq!(c("wrong").status, 401, "expected 401 with wrong key");
    assert_eq!(
        c("secret").status,
        404,
        "expected 404 with correct key on missing key"
    );
    let health = call(&srv, &mut st, "GET", "/health", "", "", "", "");
    assert_eq!(health.status, 200, "expected health 200");
}

// ---- Geen Go-naam: de antwoorden van Go die een client leest ----

// Go's http.Error: platte tekst, nosniff en een regeleinde.
#[test]
fn errors_are_gos_http_error() {
    let srv = Server::new("");
    let mut st = MemStore::new();
    call(&srv, &mut st, "PUT", "/k", "a", "*", "", "");
    let r = call(&srv, &mut st, "PUT", "/k", "b", "*", "", "");
    assert_eq!(r.status, 412);
    let headers: Vec<_> = r.headers().collect();
    assert_eq!(
        headers,
        [
            ("Content-Type", "text/plain; charset=utf-8"),
            ("X-Content-Type-Options", "nosniff")
        ]
    );
    assert_eq!(r.body_parts(), [&b"precondition failed"[..], b"\n"]);
}

// Een GET geeft de body als JSON met de ETag; een PUT alleen de ETag.
#[test]
fn get_and_put_carry_the_etag() {
    let srv = Server::new("");
    let mut st = MemStore::new();
    let put = call(&srv, &mut st, "PUT", "/k", "hello", "*", "", "");
    assert_eq!(put.status, 200);
    assert_eq!(put.body, Body::Empty);
    assert_eq!(put.etag, Some(Etag::of(b"hello")));
    let get = call(&srv, &mut st, "GET", "/k", "", "", "", "");
    assert_eq!(get.status, 200);
    assert_eq!(get.content_type, Some("application/json"));
    assert_eq!(get.etag, Some(Etag::of(b"hello")));
    assert_eq!(get.body, Body::Bytes(b"hello".to_vec()));
}

// Go's volgorde: 401 vóór alles, dan een lege sleutel, dan de methode.
#[test]
fn the_order_of_the_checks_is_gos() {
    let srv = Server::new("k");
    let mut st = MemStore::new();
    assert_eq!(call(&srv, &mut st, "POST", "/", "", "", "", "").status, 401);
    let r = call(&srv, &mut st, "POST", "/", "", "", "", "k");
    assert_eq!((r.status, r.body_parts()[0]), (400, &b"key required"[..]));
    let r = call(&srv, &mut st, "POST", "/x", "", "", "", "k");
    assert_eq!(r.status, 405);
    assert_eq!(r.allow, Some("GET, PUT, DELETE"));
    // HEAD is ook geen methode van een sleutel.
    assert_eq!(
        call(&srv, &mut st, "HEAD", "/x", "", "", "", "k").status,
        405
    );
}

// /health: elke methode, zonder sleutel, "ok" zonder regeleinde.
#[test]
fn health_needs_no_key() {
    let srv = Server::new("secret");
    let mut st = MemStore::new();
    for m in ["GET", "HEAD", "POST", "DELETE"] {
        let r = call(&srv, &mut st, m, "/health", "", "", "", "");
        assert_eq!(r.status, 200, "{m}");
        assert_eq!(r.body_parts()[0], b"ok");
    }
}

// If-None-Match kent alleen `*`, en de body wordt eerst gelezen.
#[test]
fn only_if_none_match_star() {
    let srv = Server::new("");
    let step = srv.begin(&Head {
        method: "PUT",
        path: "/k",
        if_none_match: "\"abc\"",
        ..Head::default()
    });
    let Step::ReadBody(p) = step else {
        panic!("a PUT reads its body first");
    };
    let Step::Respond(r) = srv.with_body(p, b"x".to_vec()) else {
        panic!("expected an answer");
    };
    assert_eq!(r.status, 400);
    assert_eq!(r.body_parts()[0], b"only If-None-Match: * is supported");
}

// Een ongeldige sleutel: 400 voor PUT en DELETE, 500 voor GET (Go's serveGet).
#[test]
fn a_bad_key_is_400_except_for_get() {
    let srv = Server::new("");
    let mut st = MemStore::new();
    let path = "/a\0b";
    assert_eq!(
        call(&srv, &mut st, "PUT", path, "x", "*", "", "").status,
        400
    );
    assert_eq!(
        call(&srv, &mut st, "DELETE", path, "", "", "", "").status,
        400
    );
    let r = call(&srv, &mut st, "GET", path, "", "", "", "");
    assert_eq!(r.status, 500);
    assert_eq!(r.body_parts()[0], b"hoplockserver/store: invalid key");
}

// Een PUT zonder voorwaarde overschrijft (de clusterstaat van Hop).
#[test]
fn an_unconditional_put_overwrites() {
    let srv = Server::new("");
    let mut st = MemStore::new();
    assert_eq!(
        call(&srv, &mut st, "PUT", "/state/c", "1", "", "", "").status,
        200
    );
    assert_eq!(
        call(&srv, &mut st, "PUT", "/state/c", "2", "", "", "").status,
        200
    );
    let r = call(&srv, &mut st, "GET", "/state/c", "", "", "", "");
    assert_eq!(r.body, Body::Bytes(b"2".to_vec()));
}

// If-None-Match wint van If-Match, zoals in Go's switch.
#[test]
fn if_none_match_wins_over_if_match() {
    let mut st = MemStore::new();
    let cond = Condition {
        if_none_match: String::from("*"),
        if_match: String::from("\"whatever\""),
    };
    put(&mut st, "k", b"x", &cond).unwrap();
    assert_eq!(put(&mut st, "k", b"x", &cond), Err(Error::Precondition));
}

// Een DELETE zonder voorwaarde en met een verkeerde If-None-Match: alleen If-Match telt.
#[test]
fn delete_ignores_if_none_match() {
    let srv = Server::new("");
    let mut st = MemStore::new();
    call(&srv, &mut st, "PUT", "/k", "x", "*", "", "");
    assert_eq!(
        call(&srv, &mut st, "DELETE", "/k", "", "\"nope\"", "", "").status,
        204
    );
}

// De sleutel staat in geen Debug van de server.
#[test]
fn the_key_is_not_in_debug() {
    let d = alloc::format!("{:?}", Server::new("hunter2"));
    assert!(!d.contains("hunter2"), "{d}");
}

// ---- Geen Go-naam: de recordvorm op het volume ----

fn volume() -> VolumeStore<MemVolume> {
    VolumeStore::new(MemVolume::default(), "/data")
}

// Het record: magie, lengte, som, body; de ETag is die van de body.
#[test]
fn a_volume_record_is_sealed() {
    let mut s = volume();
    let etag = put(&mut s, "leases/c", b"hello", &Condition::create()).unwrap();
    assert_eq!(etag, Etag::of(b"hello"));
    let rec = &s.volume_mut().files["/data/leases/c"];
    assert_eq!(&rec[..4], b"HLK1");
    assert_eq!(u32::from_le_bytes(rec[4..8].try_into().unwrap()), 5);
    assert_eq!(&rec[8..40], &auth::sha256(b"hello"));
    assert_eq!(&rec[40..], b"hello");
    // Gelezen in stukken van CHUNK (7) bytes.
    assert_eq!(get(&mut s, "leases/c").unwrap().body, b"hello");
}

// Een half record (stroomuitval midden in de schrijf) is afwezig, luid.
#[test]
fn a_torn_volume_record_counts_as_absent() {
    let mut s = volume();
    let etag = put(&mut s, "leases/c", b"hello world", &Condition::create()).unwrap();
    s.volume_mut()
        .files
        .get_mut("/data/leases/c")
        .unwrap()
        .truncate(45);
    assert_eq!(get(&mut s, "leases/c"), Err(Error::NotFound));
    assert_eq!(s.corrupt(), 1);
    assert_eq!(s.take_corrupt().unwrap().as_str(), "leases/c");
    assert_eq!(s.take_corrupt(), None);
    // If-Match op de oude ETag faalt, een aanmaak mag eroverheen.
    assert_eq!(
        put(
            &mut s,
            "leases/c",
            b"x",
            &Condition::matching(etag.as_str())
        ),
        Err(Error::Precondition)
    );
    put(&mut s, "leases/c", b"new", &Condition::create()).unwrap();
    assert_eq!(get(&mut s, "leases/c").unwrap().body, b"new");
}

// Een verminkt record: If-Match faalt, zonder voorwaarde mag het weg.
#[test]
fn a_corrupt_volume_record_can_be_deleted() {
    let mut s = volume();
    put(&mut s, "k", b"hello", &Condition::create()).unwrap();
    s.volume_mut().files.get_mut("/data/k").unwrap()[41] ^= 1;
    assert_eq!(
        delete(
            &mut s,
            "k",
            &Condition::matching(Etag::of(b"hello").as_str())
        ),
        Err(Error::Precondition)
    );
    assert_eq!(delete(&mut s, "k", &Condition::default()), Ok(()));
    assert_eq!(
        delete(&mut s, "k", &Condition::default()),
        Err(Error::NotFound)
    );
}

// De telling bij de start: elk bestand in de boom onder de map.
#[test]
fn volume_keys_are_counted() {
    let mut s = volume();
    assert_eq!(block_on(s.count_keys()), Ok(0));
    for k in ["leases/a", "state/a", "x", "a/b/c/d"] {
        put(&mut s, k, b"1", &Condition::create()).unwrap();
    }
    s.volume_mut()
        .files
        .insert(String::from("/elsewhere/y"), b"1".to_vec());
    assert_eq!(block_on(s.count_keys()), Ok(4));
}

// Een body tot de grens van Go (1 MiB) past in een record, en komt heel terug.
#[test]
fn a_full_body_round_trips_on_the_volume() {
    let mut s = volume();
    let body: Vec<u8> = (0..crate::MAX_BODY).map(|i| (i % 251) as u8).collect();
    let etag = put(&mut s, "big", &body, &Condition::default()).unwrap();
    let got = get(&mut s, "big").unwrap();
    assert_eq!(got.etag, etag);
    assert_eq!(got.body, body);
}

// De uitkomst van run is die van de store, per soort.
#[test]
fn run_maps_each_op() {
    let mut st = MemStore::new();
    let key = Key::parse("k").unwrap();
    let done = block_on(run(&mut st, crate::Op::Get(key)));
    assert_eq!(done, Done::Get(Err(Error::NotFound)));
}
