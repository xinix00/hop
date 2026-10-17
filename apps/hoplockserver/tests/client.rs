//! `client/client_test.go` en `client/object_test.go` met Hop's eigen
//! Rust-client (`store::HoplockLease`, `store::HoplockStateStore` uit hop
//! v3.0.0-alpha.10) tegen deze server: het bewijs dat de host-daemon van Hop
//! er byte voor byte mee praat. De Go-naam staat boven elke test.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]

mod common;

use std::time::Duration;

use discovery::{Backend, Discovery, Error as LeaseError, LeaseState};
use store::{Error, HoplockLease, HoplockStateStore, StateStore};

const T: Duration = Duration::from_secs(5);

fn lease(owner: &str, generation: u64) -> LeaseState {
    LeaseState {
        generation,
        expires_at: 1_790_000_000_000,
        owner: owner.to_string(),
    }
}

// TestBackendRoundtrip
#[test]
fn backend_roundtrip() {
    let srv = common::serve("");
    let mut b = HoplockLease::new(&srv.url, "", "lease/cluster", T);
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
}

// TestBackendAuth
#[test]
fn backend_auth() {
    let srv = common::serve("secret");
    let mut no_key = HoplockLease::new(&srv.url, "", "lease/x", T);
    assert_eq!(no_key.read(), Err(LeaseError::Unreachable));
    let why = no_key.last_error().unwrap().to_string();
    assert!(why.contains("401"), "{why}");

    let mut good = HoplockLease::new(&srv.url, "secret", "lease/x", T);
    assert_eq!(good.read(), Err(LeaseError::NoLease));
}

// TestObjectRoundtrip
#[test]
fn object_roundtrip() {
    let srv = common::serve("");
    let mut s = HoplockStateStore::new(&srv.url, "", "cluster", T);
    assert_eq!(s.load(), Ok(None));
    let first = br#"{"jobs":["a"]}"#;
    s.save(first).unwrap();
    assert_eq!(s.load().unwrap().as_deref(), Some(&first[..]));
    let second = br#"{"jobs":["a","b"]}"#;
    s.save(second).unwrap();
    assert_eq!(s.load().unwrap().as_deref(), Some(&second[..]));
    // Naast de lease, onder state/<cluster>, als kaal bestand.
    assert_eq!(
        std::fs::read(srv.data.join("state/cluster")).unwrap(),
        second
    );
}

// TestObjectAuth
#[test]
fn object_auth() {
    let srv = common::serve("secret");
    let mut no_key = HoplockStateStore::new(&srv.url, "", "x", T);
    let err = no_key.save(b"{}").unwrap_err();
    assert!(matches!(err, Error::Status { code: 401, .. }), "{err}");
    let mut good = HoplockStateStore::new(&srv.url, "secret", "x", T);
    good.save(b"{}").unwrap();
    assert_eq!(good.load().unwrap().as_deref(), Some(&b"{}"[..]));
}

// TestPutObjectMissingURL
#[test]
fn put_object_missing_url() {
    let mut s = HoplockStateStore::new("", "", "x", T);
    assert!(s.save(b"{}").is_err());
}

// Geen Go-naam: de verkiezing van Hop (`discovery`) over deze server, zoals
// `discovery_over_hoplockserver` in hop het tegen een nep-server doet.
#[test]
fn discovery_over_hoplockserver() {
    let srv = common::serve("k");
    let mut backend = HoplockLease::new(&srv.url, "k", "leases/c", T);
    let mut a = Discovery::new(String::from("10.0.0.1:8080"), 30_000);
    let mut b = Discovery::new(String::from("10.0.0.2:8080"), 30_000);
    let now = 1_790_000_000_000;
    assert!(a.try_become_leader(Some(&mut backend), now));
    assert!(!b.try_become_leader(Some(&mut backend), now));
    assert_eq!(a.renew_lease(Some(&mut backend), now + 1), (true, false));
    assert!(b.try_become_leader(Some(&mut backend), now + 40_000));
    assert_eq!(backend.read().unwrap().0.generation, 2);
    assert_eq!(
        a.renew_lease(Some(&mut backend), now + 40_001),
        (false, true)
    );
    // De lease op de schijf is Go's hoplock.State.
    let disk = String::from_utf8(std::fs::read(srv.data.join("leases/c")).unwrap()).unwrap();
    assert!(
        disk.starts_with("{\"generation\":2,\"expires_at\":\""),
        "{disk}"
    );
    assert!(disk.ends_with(",\"owner\":\"10.0.0.2:8080\"}"), "{disk}");
}
