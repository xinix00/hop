//! De Go-tests naam voor naam: `hoplock/s3` (s3_test.go, ghost_test.go),
//! `hoplockserver/client` (client_test.go, object_test.go) en
//! `internal/discovery` (statestore_test.go, en discovery_test.go voor zover
//! over de backends). De Go-naam staat boven elke test.

mod fake;

use std::time::Duration;

use config::{Config, S3LockConfig};
use discovery::{Backend, Discovery, Error as LeaseError, LeaseState};
use hostnet::{Call, Http};

use crate::{
    Error, FileStateStore, HoplockLease, HoplockStateStore, Lease, S3Lease, S3StateStore,
    StateStore, lock_configured, lock_label, open_lease, open_state_store, wire,
};
use fake::{Fake, Ghost, Objects};

const T: Duration = Duration::from_secs(5);
const SECRET: &str = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";

fn s3_config(endpoint: &str, bucket: &str) -> S3LockConfig {
    S3LockConfig {
        endpoint: endpoint.to_string(),
        bucket: bucket.to_string(),
        region: String::from("us-east-1"),
        access_key_id: String::from("AKIDEXAMPLE"),
        secret_access_key: String::from(SECRET),
        session_token: String::new(),
        use_path_style: true,
    }
}

fn s3_lease(f: &Fake<Objects>) -> S3Lease {
    S3Lease::new(&s3_config(&f.url(), "bkt"), "lock.json", T, 0)
}

fn lease(owner: &str, generation: u64) -> LeaseState {
    LeaseState {
        generation,
        expires_at: 1_790_000_000_000,
        owner: owner.to_string(),
    }
}

fn count(log: &[fake::Seen], method: &str) -> usize {
    log.iter().filter(|s| s.method == method).count()
}

// ---- hoplock/s3: s3_test.go ----

// TestS3_ReadEmpty
#[test]
fn s3_read_empty() {
    let f = Fake::spawn(Objects::default(), fake::objects);
    let mut b = s3_lease(&f);
    assert_eq!(b.read(), Err(LeaseError::NoLease));
    f.stop();
}

// TestS3_WriteCreateAndRead
#[test]
fn s3_write_create_and_read() {
    let f = Fake::spawn(Objects::default(), fake::objects);
    let mut b = s3_lease(&f);
    let h = b.write("", &lease("alice", 1)).unwrap();
    assert!(!h.is_empty());
    let (got, h2) = b.read().unwrap();
    assert_eq!(h2, h);
    assert_eq!(got, lease("alice", 1));
    let (store, _) = f.stop();
    // De body op de draad is Go's hoplock.State.
    let (body, _) = store.data.get("/bkt/lock.json").unwrap();
    let v = types::json::parse(body).unwrap();
    let obj = v.as_object().unwrap();
    for field in ["generation", "expires_at", "owner"] {
        assert!(obj.get(field).is_some(), "missing {field}");
    }
}

// TestS3_WriteCreateRejectedWhenExists
#[test]
fn s3_write_create_rejected_when_exists() {
    let f = Fake::spawn(Objects::default(), fake::objects);
    let mut b = s3_lease(&f);
    b.write("", &lease("alice", 1)).unwrap();
    assert_eq!(b.write("", &lease("alice", 1)), Err(LeaseError::LeaseHeld));
    f.stop();
}

// TestS3_WriteCASMatchAndMismatch
#[test]
fn s3_write_cas_match_and_mismatch() {
    let f = Fake::spawn(Objects::default(), fake::objects);
    let mut b = s3_lease(&f);
    let h1 = b.write("", &lease("alice", 1)).unwrap();
    let h2 = b.write(&h1, &lease("alice", 2)).unwrap();
    assert_ne!(h1, h2, "etag did not advance");
    assert_eq!(b.write(&h1, &lease("alice", 2)), Err(LeaseError::LeaseHeld));
    f.stop();
}

// TestS3_DeleteVariants
#[test]
fn s3_delete_variants() {
    let f = Fake::spawn(Objects::default(), fake::objects);
    let mut b = s3_lease(&f);
    let h = b.write("", &lease("alice", 1)).unwrap();
    assert_eq!(b.delete(""), Err(LeaseError::LeaseHeld));
    assert_eq!(b.delete("wrong"), Err(LeaseError::LeaseHeld));
    assert_eq!(b.delete(&h), Ok(()));
    assert_eq!(b.delete(&h), Err(LeaseError::NoLease));
    f.stop();
}

// TestS3_RequestsAreSigned
#[test]
fn s3_requests_are_signed() {
    let objects = Objects {
        require_sig: true,
        ..Objects::default()
    };
    let f = Fake::spawn(objects, fake::objects);
    let mut b = s3_lease(&f);
    assert_eq!(b.read(), Err(LeaseError::NoLease));
    b.write("", &lease("x", 1)).unwrap();
    let (_, log) = f.stop();
    assert!(count(&log, "GET") > 0);
    assert!(count(&log, "PUT") > 0);
    // Het geheim zelf gaat nooit over de draad, alleen de handtekening.
    for s in &log {
        assert!(s.header.iter().all(|(_, v)| !v.contains(SECRET)));
    }
}

// TestS3_PathStyleAddressing
#[test]
fn s3_path_style_addressing() {
    let f = Fake::spawn(Objects::default(), fake::objects);
    let addr = f.url().trim_start_matches("http://").to_string();
    let mut b = S3Lease::new(&s3_config(&f.url(), "my-bucket"), "leases/lock.json", T, 0);
    let _ = b.read();
    let (_, log) = f.stop();
    let seen = log.first().unwrap();
    assert_eq!(seen.path, "/my-bucket/leases/lock.json");
    assert_eq!(seen.header("Host"), addr);
}

// TestS3_VirtualHostedAddressing: niet overgenomen. Go leidde de dial om naar
// de testserver; hostnet::S3Transport heeft geen dial-haak, en de adressering
// zelf toetst leans3 (Client::url_for) al.

// TestS3_HetznerQuotedETagQuirk
#[test]
fn s3_hetzner_quoted_etag_quirk() {
    let objects = Objects {
        bare_if_match: true,
        ..Objects::default()
    };
    let f = Fake::spawn(objects, fake::objects);
    let mut b = s3_lease(&f);
    let h1 = b.write("", &lease("alice", 1)).unwrap();
    assert_eq!(h1, "\"etag-1\"", "Hetzner returns a quoted ETag");
    let (_, h2) = b.read().unwrap();
    assert_eq!(h2, h1);
    // Renew: eerst geciteerd (412), dan kaal (200).
    let h3 = b.write(&h2, &lease("alice", 2)).unwrap();
    assert_ne!(h3, h1);
    // Een echt verouderde handle faalt na beide pogingen.
    assert_eq!(
        b.write("\"etag-1\"", &lease("alice", 2)),
        Err(LeaseError::LeaseHeld)
    );
    // Ook DELETE probeert opnieuw.
    assert_eq!(b.delete(&h3), Ok(()));
    let (_, log) = f.stop();
    // Aanmaak 1, renew 2, verouderde renew 2.
    assert_eq!(count(&log, "PUT"), 5);
    assert_eq!(count(&log, "DELETE"), 2);
}

// ---- hoplock/s3: ghost_test.go ----

/// Een ghost-lease op `leases/c` met overname na `takeover_ms`.
fn ghost_lease(f: &Fake<Ghost>, takeover_ms: u64) -> S3Lease {
    S3Lease::new(&s3_config(&f.url(), "bkt"), "leases/c", T, takeover_ms)
}

// TestGhostLeaseIsTakenOverAfterTakeoverAfter
#[test]
fn ghost_lease_is_taken_over_after_takeover_after() {
    let g = Ghost {
        etag: String::from("\"ghost-1\""),
        takeable: true,
        takeovers: 0,
    };
    let f = Fake::spawn(g, fake::ghost);
    let mut b = ghost_lease(&f, 120_000);
    let mut now = 1_788_892_620_000; // 2026-09-08 18:37 UTC
    b.set_now_ms(now);
    let state = lease("me", 1);

    // De lees zegt "geen lease", de aanmaak wordt geweigerd: de klok start.
    assert_eq!(b.read(), Err(LeaseError::NoLease));
    assert_eq!(b.write("", &state), Err(LeaseError::LeaseHeld));
    // Binnen de termijn: blijven weigeren.
    now += 60_000;
    b.set_now_ms(now);
    assert_eq!(b.write("", &state), Err(LeaseError::LeaseHeld));
    // Onveranderd na de termijn: overnemen op die ETag.
    now += 120_000;
    b.set_now_ms(now);
    assert_eq!(b.write("", &state), Ok(String::from("\"after-takeover\"")));
    let (g, _) = f.stop();
    assert_eq!(g.takeovers, 1);
}

// TestGhostClockRestartsWhenETagMoves
#[test]
fn ghost_clock_restarts_when_etag_moves() {
    let g = Ghost {
        etag: String::from("\"e1\""),
        takeable: false,
        takeovers: 0,
    };
    let f = Fake::spawn(g, fake::ghost);
    let move_url = format!("{}/__etag", f.url());
    let mut b = ghost_lease(&f, 60_000);
    let mut now = 1_788_892_620_000;
    for i in 0..4 {
        b.set_now_ms(now);
        assert_eq!(
            b.write("", &lease("me", 1)),
            Err(LeaseError::LeaseHeld),
            "write {i}"
        );
        now += 45_000;
        // De eigenaar vernieuwde: de ETag verschuift.
        let etag = format!("\"e{}\"", i + 2);
        let call = Call {
            method: "POST",
            url: &move_url,
            headers: &[],
            body: Some(etag.as_bytes()),
            timeout: T,
        };
        assert_eq!(Http::new().request(&call, 1024).unwrap().status, 204);
    }
    let (g, log) = f.stop();
    assert_eq!(g.takeovers, 0);
    // Nooit een PUT met If-Match: de klok begon elke keer opnieuw.
    assert!(
        log.iter()
            .all(|s| s.method != "PUT" || s.header("If-Match").is_empty())
    );
}

// TestGhostTakeoverDisabledByDefault
#[test]
fn ghost_takeover_disabled_by_default() {
    let g = Ghost {
        etag: String::from("\"ghost-1\""),
        takeable: true,
        takeovers: 0,
    };
    let f = Fake::spawn(g, fake::ghost);
    let mut b = ghost_lease(&f, 0);
    for _ in 0..3 {
        assert_eq!(b.write("", &lease("me", 0)), Err(LeaseError::LeaseHeld));
    }
    let (g, log) = f.stop();
    assert_eq!(g.takeovers, 0);
    assert_eq!(count(&log, "HEAD"), 0, "no HEAD without a takeover term");
}

// ---- hoplockserver/client: client_test.go en object_test.go ----

fn hoplock_server(api_key: Option<&'static str>) -> Fake<Objects> {
    let objects = Objects {
        api_key,
        ..Objects::default()
    };
    Fake::spawn(objects, fake::objects)
}

// TestBackendRoundtrip
#[test]
fn backend_roundtrip() {
    let f = hoplock_server(None);
    let mut b = HoplockLease::new(&f.url(), "", "lease/cluster", T);
    assert_eq!(b.read(), Err(LeaseError::NoLease));

    let handle = b.write("", &lease("node-a", 1)).unwrap();
    assert!(!handle.is_empty());
    assert_eq!(b.write("", &lease("node-a", 1)), Err(LeaseError::LeaseHeld));

    let (got, got_handle) = b.read().unwrap();
    assert_eq!(got.owner, "node-a");
    assert_eq!(got_handle, handle);

    assert_eq!(
        b.write("stale", &lease("node-a", 2)),
        Err(LeaseError::LeaseHeld)
    );
    let handle2 = b.write(&handle, &lease("node-a", 2)).unwrap();
    assert_ne!(handle2, handle);

    assert_eq!(b.delete(&handle), Err(LeaseError::LeaseHeld));
    assert_eq!(b.delete(&handle2), Ok(()));
    assert_eq!(b.delete(&handle2), Err(LeaseError::NoLease));
    let (_, log) = f.stop();
    assert!(log.iter().all(|s| s.path == "/lease/cluster"));
}

// TestBackendAuth
#[test]
fn backend_auth() {
    let f = hoplock_server(Some("secret"));
    let mut no_key = HoplockLease::new(&f.url(), "", "lease/x", T);
    // Een auth-fout is geen "geen lease" maar een onbereikbare opslag.
    assert_eq!(no_key.read(), Err(LeaseError::Unreachable));
    let why = no_key.last_error().unwrap().to_string();
    assert!(why.contains("401"), "{why}");

    let mut good = HoplockLease::new(&f.url(), "secret", "lease/x", T);
    assert_eq!(good.read(), Err(LeaseError::NoLease));
    // De sleutel staat in geen Debug.
    assert!(!format!("{good:?}").contains("secret"));
    f.stop();
}

// TestObjectRoundtrip
#[test]
fn object_roundtrip() {
    let f = hoplock_server(None);
    let mut s = HoplockStateStore::new(&f.url(), "", "cluster", T);
    // Afwezig: None en geen fout (schone start).
    assert_eq!(s.load(), Ok(None));
    let first = br#"{"jobs":["a"]}"#;
    s.save(first).unwrap();
    assert_eq!(s.load().unwrap().as_deref(), Some(&first[..]));
    // Onvoorwaardelijk overschrijven (geen CAS): de tweede wint.
    let second = br#"{"jobs":["a","b"]}"#;
    s.save(second).unwrap();
    assert_eq!(s.load().unwrap().as_deref(), Some(&second[..]));
    let (_, log) = f.stop();
    assert!(
        log.iter()
            .all(|r| r.header("If-Match").is_empty() && r.header("If-None-Match").is_empty())
    );
}

// TestObjectAuth
#[test]
fn object_auth() {
    let f = hoplock_server(Some("secret"));
    let mut no_key = HoplockStateStore::new(&f.url(), "", "x", T);
    let err = no_key.save(b"{}").unwrap_err();
    assert!(matches!(err, Error::Status { code: 401, .. }), "{err}");
    let mut good = HoplockStateStore::new(&f.url(), "secret", "x", T);
    good.save(b"{}").unwrap();
    assert_eq!(good.load().unwrap().as_deref(), Some(&b"{}"[..]));
    f.stop();
}

// TestPutObjectMissingURL
#[test]
fn put_object_missing_url() {
    let mut s = HoplockStateStore::new("", "", "x", T);
    assert!(s.save(b"{}").is_err());
}

// ---- internal/discovery: statestore_test.go ----

// TestStateStoreFromConfigSelection
#[test]
fn state_store_from_config_selection() {
    let kind = |cfg: &Config, standalone: bool| {
        let d = open_state_store(cfg, standalone).describe();
        d.split(' ').next().unwrap().to_string()
    };
    // Standalone: altijd het bestand, ook met een remote config.
    let mut stand = Config::default();
    stand.cluster.lock.url = String::from("http://lock:8090");
    assert_eq!(kind(&stand, true), "file");
    // Leeg en expliciet mem: het bestand (lock in dit proces).
    assert_eq!(kind(&Config::default(), false), "file");
    let mut mem = Config::default();
    mem.cluster.lock.kind = String::from("mem");
    assert_eq!(kind(&mem, false), "file");
    // Expliciet hoplockserver met URL.
    let mut hls = Config::default();
    hls.cluster.name = String::from("prod");
    hls.cluster.lock.kind = String::from("hoplockserver");
    hls.cluster.lock.url = String::from("http://lock:8090");
    assert_eq!(kind(&hls, false), "hoplockserver");
    assert_eq!(
        open_state_store(&hls, false).describe(),
        "hoplockserver http://lock:8090/state/prod"
    );
    // Het standaardtype ("") met URL is ook hoplockserver.
    let mut def = Config::default();
    def.cluster.lock.url = String::from("http://lock:8090");
    assert_eq!(kind(&def, false), "hoplockserver");
    // Een bruikbare S3-sectie wint, ook naast een URL.
    let mut s3 = Config::default();
    s3.cluster.lock.url = String::from("http://lock:8090");
    s3.cluster.lock.s3.endpoint = String::from("https://s3.example.com");
    s3.cluster.lock.s3.bucket = String::from("hop");
    assert_eq!(kind(&s3, false), "s3");
}

/// Een verse map onder de temp-map van het OS, per test en per proces.
fn temp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("store-test-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

// TestFileStateStoreRoundtrip
#[test]
fn file_state_store_roundtrip() {
    let dir = temp_dir("file-roundtrip");
    let path = dir.join("sub").join("state.json");
    let mut s = FileStateStore::new(&path);
    // Afwezig bestand: schone start.
    assert_eq!(s.load(), Ok(None));
    let snap = br#"{"jobs":[{"name":"web"}]}"#;
    s.save(snap).unwrap();
    assert_eq!(s.load().unwrap().as_deref(), Some(&snap[..]));
    // Overschrijven is atomisch en laat geen tijdelijk bestand achter.
    s.save(br#"{"jobs":[]}"#).unwrap();
    assert_eq!(s.load().unwrap().as_deref(), Some(&br#"{"jobs":[]}"#[..]));
    let names: Vec<String> = std::fs::read_dir(dir.join("sub"))
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(names, ["state.json"]);
    let _ = std::fs::remove_dir_all(&dir);
}

// TestHoplockServerStateStoreSaveLoad
#[test]
fn hoplock_server_state_store_save_load() {
    let f = hoplock_server(None);
    let mut cfg = Config::default();
    cfg.cluster.name = String::from("prod");
    cfg.cluster.lock.url = f.url();
    let mut s = open_state_store(&cfg, false);
    assert_eq!(s.load(), Ok(None));
    let snap = br#"{"jobs":[{"name":"web"}]}"#;
    s.save(snap).unwrap();
    assert_eq!(s.load().unwrap().as_deref(), Some(&snap[..]));
    let (store, _) = f.stop();
    // Op state/<cluster>, naast (niet op) de lease.
    assert!(store.data.contains_key("/state/prod"));
    assert!(!store.data.contains_key("/leases/prod"));
}

/// Zoals hierboven, voor de S3-kant (Go: `S3StateStore`, zonder eigen test).
#[test]
fn s3_state_store_save_load() {
    let f = Fake::spawn(Objects::default(), fake::objects);
    let mut s = S3StateStore::new(&s3_config(&f.url(), "bkt"), "prod", T);
    assert_eq!(s.load(), Ok(None));
    s.save(b"{\"jobs\":[]}").unwrap();
    assert_eq!(s.load().unwrap().as_deref(), Some(&b"{\"jobs\":[]}"[..]));
    assert!(!s.describe().contains(SECRET));
    let (store, _) = f.stop();
    assert!(store.data.contains_key("/bkt/state/prod"));
}

// ---- internal/discovery: discovery_test.go, over de backends ----

// TestRenewLeaseDistinguishesDisplacedFromUnreachable, tegen S3 en een dode poort.
#[test]
fn renew_lease_distinguishes_displaced_from_unreachable() {
    let f = Fake::spawn(Objects::default(), fake::objects);
    let mut backend = s3_lease(&f);
    let mut a = Discovery::new(String::from("10.0.0.1:8080"), 30_000);
    let mut b = Discovery::new(String::from("10.0.0.2:8080"), 30_000);
    let now = 1_790_000_000_000;
    assert!(a.try_become_leader(Some(&mut backend), now));
    assert_eq!(b.renew_lease(Some(&mut backend), now), (false, true));
    assert_eq!(a.renew_lease(Some(&mut backend), now + 1), (true, false));
    assert_eq!(
        a.get_leader(Some(&mut backend), now + 2).as_deref(),
        Some("10.0.0.1:8080")
    );
    a.release_leadership(Some(&mut backend));
    assert!(b.try_become_leader(Some(&mut backend), now + 3));
    f.stop();

    // Een poort waar niemand luistert: niet verdrongen, alleen onbereikbaar.
    let dead = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", dead.local_addr().unwrap());
    drop(dead);
    let mut gone = S3Lease::new(&s3_config(&url, "bkt"), "leases/c", T, 0);
    let mut c = Discovery::new(String::from("10.0.0.3:8080"), 30_000);
    assert_eq!(c.renew_lease(Some(&mut gone), now), (false, false));
    let why = gone.last_error().unwrap().to_string();
    assert!(!why.contains(SECRET), "{why}");
}

/// Dezelfde lus tegen de hoplockserver.
#[test]
fn discovery_over_hoplockserver() {
    let f = hoplock_server(Some("k"));
    let mut backend = HoplockLease::new(&f.url(), "k", "leases/c", T);
    let mut a = Discovery::new(String::from("10.0.0.1:8080"), 30_000);
    let mut b = Discovery::new(String::from("10.0.0.2:8080"), 30_000);
    let now = 1_790_000_000_000;
    assert!(a.try_become_leader(Some(&mut backend), now));
    assert!(!b.try_become_leader(Some(&mut backend), now));
    assert_eq!(a.renew_lease(Some(&mut backend), now + 1), (true, false));
    // Verlopen: b neemt over met generatie + 1, en a is verdrongen.
    assert!(b.try_become_leader(Some(&mut backend), now + 40_000));
    assert_eq!(backend.read().unwrap().0.generation, 2);
    assert_eq!(
        a.renew_lease(Some(&mut backend), now + 40_001),
        (false, true)
    );
    f.stop();
}

// ---- cmd/agent: buildBackend, lockConfigured, lockLabel ----

#[test]
fn open_lease_selection() {
    let mut cfg = Config::default();
    cfg.cluster.name = String::from("prod");
    assert!(matches!(open_lease(&cfg, true), Ok(Lease::Mem(_))));
    cfg.cluster.lock.kind = String::from("mem");
    assert!(matches!(open_lease(&cfg, false), Ok(Lease::Mem(_))));

    cfg.cluster.lock.kind = String::from("s3");
    assert!(matches!(
        open_lease(&cfg, false),
        Err(Error::Incomplete { kind: "s3", .. })
    ));
    cfg.cluster.lock.s3 = s3_config("https://s3.example.com", "hop");
    match open_lease(&cfg, false) {
        Ok(Lease::S3(l)) => assert_eq!(l.key(), "leases/prod"),
        other => panic!("{other:?}"),
    }
    cfg.cluster.lock.key = String::from("custom/lease.json");
    match open_lease(&cfg, false) {
        Ok(Lease::S3(l)) => assert_eq!(l.key(), "custom/lease.json"),
        other => panic!("{other:?}"),
    }
    cfg.cluster.lock.key = String::new();

    for kind in ["", "hoplockserver"] {
        cfg.cluster.lock.kind = String::from(kind);
        cfg.cluster.lock.url = String::new();
        assert!(matches!(
            open_lease(&cfg, false),
            Err(Error::Incomplete { .. })
        ));
        cfg.cluster.lock.url = String::from("http://lock:8090");
        match open_lease(&cfg, false) {
            Ok(Lease::Hoplock(l)) => assert_eq!(l.key(), "leases/prod"),
            other => panic!("{other:?}"),
        }
    }

    cfg.cluster.lock.kind = String::from("etcd");
    let err = open_lease(&cfg, false).unwrap_err();
    assert_eq!(
        err.to_string(),
        "store: unknown lock type \"etcd\" (want one of: hoplockserver, s3, mem)"
    );
}

#[test]
fn lock_configured_and_label() {
    let mut cfg = Config::default();
    let lock = &mut cfg.cluster.lock;
    assert!(!lock_configured(lock));
    lock.url = String::from("https://user:pw@lock.example.com:8090/");
    assert!(lock_configured(lock));
    assert_eq!(
        lock_label(lock),
        "hoplockserver (https://lock.example.com:8090/)"
    );
    lock.kind = String::from("mem");
    assert!(lock_configured(lock));
    assert_eq!(lock_label(lock), "mem (in-process)");
    lock.kind = String::from("s3");
    assert!(!lock_configured(lock));
    lock.s3 = s3_config("https://s3.example.com", "hop");
    assert!(lock_configured(lock));
    assert_eq!(lock_label(lock), "s3 (https://s3.example.com/hop)");
    assert!(!format!("{:?}", open_lease(&cfg, false)).contains(SECRET));
}

// ---- de draad: Go's hoplock.State ----

#[test]
fn lease_json_is_gos_hoplock_state() {
    let state = LeaseState {
        generation: 7,
        expires_at: 1_790_000_000_123,
        owner: String::from("10.0.0.1:8080"),
    };
    let body = wire::encode(&state).unwrap();
    assert_eq!(
        String::from_utf8(body.clone()).unwrap(),
        r#"{"generation":7,"expires_at":"2026-09-21T14:13:20.123Z","owner":"10.0.0.1:8080"}"#
    );
    assert_eq!(wire::decode(&body), Ok(state.clone()));
    // Go schrijft de lokale zone met nanoseconden.
    let go = br#"{"generation":7,"expires_at":"2026-09-21T16:13:20.123456789+02:00","owner":"10.0.0.1:8080"}"#;
    assert_eq!(wire::decode(go), Ok(state));
    // Zonder owner (omitempty) en met een onbekend veld.
    let bare = br#"{"generation":1,"expires_at":"0001-01-01T00:00:00Z","extra":true}"#;
    let got = wire::decode(bare).unwrap();
    assert_eq!(
        (got.generation, got.expires_at, got.owner.as_str()),
        (1, 0, "")
    );
    assert!(wire::decode(b"[]").is_err());
    assert!(wire::decode(br#"{"expires_at":"gisteren"}"#).is_err());
}

#[test]
fn strip_quotes_only_strips_a_pair() {
    assert_eq!(wire::strip_quotes("\"abc\""), Some("abc"));
    assert_eq!(wire::strip_quotes("abc"), None);
    assert_eq!(wire::strip_quotes("\""), None);
    assert_eq!(wire::strip_quotes(""), None);
}

#[test]
fn backends_are_send() {
    fn is_send<T: Send>() {}
    is_send::<Lease>();
    is_send::<S3Lease>();
    is_send::<HoplockLease>();
    is_send::<Box<dyn StateStore + Send>>();
    is_send::<Error>();
}

/// Een server die zijn antwoord byte voor byte druppelt: elke byte valt
/// ruim binnen de fasetermijn, het geheel niet binnen het budget.
fn dripping_server() -> String {
    use std::io::{BufRead, BufReader, Write};
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", l.local_addr().unwrap());
    std::thread::spawn(move || {
        let Ok((s, _)) = l.accept() else { return };
        let mut r = BufReader::new(s.try_clone().unwrap());
        loop {
            let mut line = String::new();
            if r.read_line(&mut line).unwrap_or(0) == 0 || line == "\r\n" {
                break;
            }
        }
        let mut s = s;
        let _ = s.write_all(b"HTTP/1.1 200 OK\r\nETag: \"e1\"\r\nContent-Length: 40\r\n\r\n");
        for _ in 0..40 {
            std::thread::sleep(Duration::from_millis(100));
            if s.write_all(b" ").is_err() {
                return;
            }
        }
    });
    url
}

// Geen Go-naam: Go gaf elke backend-aanroep een context met een totale
// termijn (`context.WithTimeout` rond de hele aanroep); dit is die termijn.
#[test]
fn a_dripping_hoplockserver_is_cut_off_at_the_call_budget() {
    let url = dripping_server();
    let mut b = HoplockLease::new(&url, "", "leases/c", Duration::from_millis(400));
    let t0 = std::time::Instant::now();
    assert_eq!(b.read(), Err(LeaseError::Unreachable));
    assert!(t0.elapsed() < Duration::from_secs(2), "{:?}", t0.elapsed());
    let why = b.last_error().unwrap().to_string();
    assert!(why.contains("timed out"), "{why}");
}

// Idem voor S3: één budget voor de hele GET.
#[test]
fn a_dripping_s3_is_cut_off_at_the_call_budget() {
    let url = dripping_server();
    let mut b = S3Lease::new(
        &s3_config(&url, "bkt"),
        "lock.json",
        Duration::from_millis(400),
        0,
    );
    let t0 = std::time::Instant::now();
    assert_eq!(b.read(), Err(LeaseError::Unreachable));
    assert!(t0.elapsed() < Duration::from_secs(2), "{:?}", t0.elapsed());
}
