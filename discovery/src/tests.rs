//! De tests uit `discovery_test.go` en `statestore_test.go`, zelfde namen en bedoeling.

use super::*;
use alloc::string::ToString;

/// Een opslag die altijd onbereikbaar is: een geïsoleerde node tijdens een storing.
struct ErrBackend;

impl Backend for ErrBackend {
    fn read(&mut self) -> Result<(LeaseState, String)> {
        Err(Error::Unreachable)
    }
    fn write(&mut self, _: &str, _: &LeaseState) -> Result<String> {
        Err(Error::Unreachable)
    }
    fn delete(&mut self, _: &str) -> Result {
        Err(Error::Unreachable)
    }
}

/// Telt de leesacties op een onderliggende opslag.
struct CountingBackend {
    inner: MemBackend,
    reads: usize,
}

impl Backend for CountingBackend {
    fn read(&mut self) -> Result<(LeaseState, String)> {
        self.reads += 1;
        self.inner.read()
    }
    fn write(&mut self, prev: &str, state: &LeaseState) -> Result<String> {
        self.inner.write(prev, state)
    }
    fn delete(&mut self, handle: &str) -> Result {
        self.inner.delete(handle)
    }
}

const S30: u64 = 30_000;
const NOW: u64 = 1_000_000_000;

fn d(owner: &str, ttl: u64) -> Discovery {
    Discovery::new(owner.to_string(), ttl)
}

#[test]
fn renew_lease_distinguishes_displaced_from_unreachable() {
    let mut be = MemBackend::new();
    let mut a = d("10.0.0.1:8080", S30);
    let mut b = d("10.0.0.2:8080", S30);
    assert!(a.try_become_leader(Some(&mut be), NOW));
    assert_eq!(b.renew_lease(Some(&mut be), NOW), (false, true));
    assert_eq!(a.renew_lease(Some(&mut be), NOW), (true, false));
    let mut c = d("10.0.0.3:8080", S30);
    assert_eq!(c.renew_lease(Some(&mut ErrBackend), NOW), (false, false));
}

#[test]
fn discovery_new() {
    assert_eq!(d("192.168.1.10:8080", S30).node_addr(), "192.168.1.10:8080");
}

#[test]
fn nil_backend_is_harmless() {
    let mut x = d("10.0.0.1:9080", 1_000);
    assert_eq!(x.get_leader::<MemBackend>(None, NOW), None);
    assert!(!x.try_become_leader::<MemBackend>(None, NOW));
    x.release_leadership::<MemBackend>(None);
}

#[test]
fn try_become_leader_creates() {
    let mut be = MemBackend::new();
    let mut x = d("192.168.1.10:8080", S30);
    assert!(x.try_become_leader(Some(&mut be), NOW));
    assert_eq!(
        x.get_leader(Some(&mut be), NOW).as_deref(),
        Some("192.168.1.10:8080")
    );
    assert!(x.is_leader(Some(&mut be), NOW));
}

#[test]
fn try_become_leader_denied_when_held_by_other() {
    let mut be = MemBackend::new();
    let mut other = d("192.168.1.20:8080", S30);
    assert!(other.try_become_leader(Some(&mut be), NOW));
    let mut mine = d("192.168.1.10:8080", S30);
    assert!(!mine.try_become_leader(Some(&mut be), NOW));
    assert_eq!(
        mine.get_leader(Some(&mut be), NOW).as_deref(),
        Some("192.168.1.20:8080")
    );
}

#[test]
fn try_become_leader_takes_over_expired() {
    let mut be = MemBackend::new();
    let mut other = d("192.168.1.20:8080", 10_000);
    assert!(other.try_become_leader(Some(&mut be), NOW));
    let later = NOW + 3_600_000;
    let mut mine = d("192.168.1.10:8080", 10_000);
    assert!(mine.try_become_leader(Some(&mut be), later));
    assert_eq!(
        mine.get_leader(Some(&mut be), later).as_deref(),
        Some("192.168.1.10:8080")
    );
}

#[test]
fn renew_keeps_handle() {
    let mut be = MemBackend::new();
    let mut x = d("192.168.1.10:8080", S30);
    assert!(x.try_become_leader(Some(&mut be), NOW));
    for i in 0..3 {
        assert!(x.renew_lease(Some(&mut be), NOW + i).0, "renew {i}");
    }
}

#[test]
fn release_allows_takeover() {
    let mut be = MemBackend::new();
    let mut a = d("192.168.1.10:8080", S30);
    assert!(a.try_become_leader(Some(&mut be), NOW));
    a.release_leadership(Some(&mut be));
    let mut b = d("192.168.1.20:8080", S30);
    assert!(b.try_become_leader(Some(&mut be), NOW));
    assert_eq!(
        b.get_leader(Some(&mut be), NOW).as_deref(),
        Some("192.168.1.20:8080")
    );
}

#[test]
fn generation_monotonic() {
    let mut be = MemBackend::new();
    let mut a = d("a:1:0", 10_000);
    assert!(a.try_become_leader(Some(&mut be), NOW));
    assert_eq!(be.read().unwrap().0.generation, 1);
    assert!(a.renew_lease(Some(&mut be), NOW).0);
    assert_eq!(be.read().unwrap().0.generation, 1);
    let mut b = d("b:1:0", 10_000);
    assert!(b.try_become_leader(Some(&mut be), NOW + 3_600_000));
    assert_eq!(be.read().unwrap().0.generation, 2);
}

#[test]
fn backend_timeout_scales_with_lease() {
    assert_eq!(backend_timeout_for(30_000), 10_000);
    assert_eq!(backend_timeout_for(15_000), 5_000);
    assert_eq!(backend_timeout_for(120_000), 40_000);
    assert_eq!(d("10.0.0.1:8080", 120_000).timeout_ms(), 40_000);
}

#[test]
fn leader_state_separates_no_leader_from_unreachable() {
    let x = d("10.0.0.1:8080", S30);
    assert_eq!(
        x.leader_state(Some(&mut MemBackend::new()), NOW),
        (None, true)
    );
    assert_eq!(x.leader_state(Some(&mut ErrBackend), NOW), (None, false));
    assert_eq!(x.leader_state::<MemBackend>(None, NOW), (None, true));
}

#[test]
fn renew_lease_does_not_read() {
    let mut be = CountingBackend {
        inner: MemBackend::new(),
        reads: 0,
    };
    let mut x = d("10.0.0.1:8080", S30);
    assert!(x.try_become_leader(Some(&mut be), NOW));
    let before = be.reads;
    for _ in 0..3 {
        assert_eq!(x.renew_lease(Some(&mut be), NOW), (true, false));
    }
    assert_eq!(be.reads, before);
    let mut other = d("10.0.0.2:8080", S30);
    x.release_leadership(Some(&mut be));
    assert!(other.try_become_leader(Some(&mut be), NOW));
    x.set_handle("stale".to_string(), 1);
    assert_eq!(x.renew_lease(Some(&mut be), NOW), (false, true));
}

#[test]
fn state_store_from_config_selection() {
    use StateStoreKind::*;
    assert_eq!(state_store_for(true, "", "http://lock:8090", "", ""), File);
    assert_eq!(state_store_for(false, "", "", "", ""), File);
    assert_eq!(state_store_for(false, "mem", "", "", ""), File);
    assert_eq!(
        state_store_for(false, "hoplockserver", "http://lock:8090", "", ""),
        HoplockServer
    );
    assert_eq!(
        state_store_for(false, "", "http://lock:8090", "", ""),
        HoplockServer
    );
    assert_eq!(
        state_store_for(
            false,
            "",
            "http://lock:8090",
            "https://s3.example.com",
            "hop"
        ),
        S3
    );
}
