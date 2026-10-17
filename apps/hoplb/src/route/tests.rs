//! De tests van `OLD/internal/lb/route_test.go`, naam voor naam, en de
//! benchmarks van `benchmark_test.go` die over de tabel gaan.

use super::*;
use crate::testutil::{bench, ns_per_op};
use alloc::format;
use alloc::string::ToString;
use alloc::vec;

fn route(pattern: &str, addrs: &[(&str, bool)]) -> Route {
    Route::new(
        pattern.to_string(),
        addrs
            .iter()
            .map(|(a, h)| Backend {
                address: a.to_string(),
                healthy: *h,
            })
            .collect(),
    )
}

fn check(rt: &RouteTable, host: &str, want: Option<&str>) {
    let got = rt.match_host(host).map(|r| r.pattern.as_str());
    assert_eq!(got, want, "Match({host:?})");
}

#[test]
fn test_wildcard_route_match() {
    let rt = RouteTable::from_routes(vec![
        route("*.haas.eu", &[("10.0.0.1:80", true)]),
        route("*.example.com", &[("10.0.0.2:80", true)]),
    ])
    .unwrap();
    check(&rt, "app.haas.eu", Some("*.haas.eu"));
    check(&rt, "api.haas.eu", Some("*.haas.eu"));
    check(&rt, "haas.eu", None); // Geen subdomein, geen match.
    check(&rt, "sub.app.haas.eu", None); // Meer niveaus, geen match.
    check(&rt, "test.example.com", Some("*.example.com"));
}

#[test]
fn test_route_table_match() {
    let rt = RouteTable::from_routes(vec![
        route("api.example.com", &[("127.0.0.1:8001", true)]),
        route("*.example.com", &[("127.0.0.1:8002", true)]),
    ])
    .unwrap();
    check(&rt, "api.example.com", Some("api.example.com")); // Exact.
    check(&rt, "app.example.com", Some("*.example.com")); // Wildcard.
    check(&rt, "other.example.com", Some("*.example.com")); // Wildcard.
    check(&rt, "example.com", None); // Niets.
    check(&rt, "api.example.com:443", Some("api.example.com")); // Poort eraf.
}

#[test]
fn test_route_round_robin() {
    let r = route(
        "test",
        &[
            ("127.0.0.1:8001", true),
            ("127.0.0.1:8002", true),
            ("127.0.0.1:8003", true),
        ],
    );
    let mut seen = alloc::collections::BTreeMap::new();
    for _ in 0..9 {
        let b = r.healthy_backend().expect("GetHealthyBackend returned nil");
        *seen.entry(b.address.clone()).or_insert(0) += 1;
    }
    assert_eq!(seen.len(), 3);
    for (addr, count) in seen {
        assert_eq!(count, 3, "backend {addr} hit {count} times, want 3");
    }
}

#[test]
fn test_route_skips_unhealthy() {
    let r = route(
        "test",
        &[
            ("127.0.0.1:8001", false),
            ("127.0.0.1:8002", true),
            ("127.0.0.1:8003", false),
        ],
    );
    for _ in 0..5 {
        let b = r.healthy_backend().expect("GetHealthyBackend returned nil");
        assert_eq!(b.address, "127.0.0.1:8002");
    }
}

#[test]
fn test_route_no_healthy_backends() {
    let r = route(
        "test",
        &[("127.0.0.1:8001", false), ("127.0.0.1:8002", false)],
    );
    assert!(r.healthy_backend().is_none());
}

// Wat Go niet toetste maar de schil wel nodig heeft.

#[test]
fn the_same_pattern_twice_is_one_route_with_both_backends() {
    let rt = RouteTable::from_routes(vec![
        route("a.example.com", &[("10.0.0.1:80", true)]),
        route("a.example.com", &[("10.0.0.2:80", true)]),
    ])
    .unwrap();
    assert_eq!(rt.len(), 1);
    assert_eq!(rt.backends(), 2);
}

#[test]
fn pick_says_no_route_no_backend_or_an_address() {
    let rt = RouteTable::from_routes(vec![
        route("up.example.com", &[("10.0.0.1:80", true)]),
        route("down.example.com", &[("10.0.0.2:80", false)]),
    ])
    .unwrap();
    assert_eq!(rt.pick("nope.example.com"), Pick::NoRoute);
    assert_eq!(rt.pick("down.example.com"), Pick::NoBackend);
    assert_eq!(
        rt.pick("up.example.com:80"),
        Pick::Backend("10.0.0.1:80".into())
    );
}

#[test]
fn a_clone_is_deep_and_keeps_the_counter() {
    let rt = RouteTable::from_routes(vec![route(
        "x.example.com",
        &[("10.0.0.1:80", true), ("10.0.0.2:80", true)],
    )])
    .unwrap();
    assert_eq!(
        rt.pick("x.example.com"),
        Pick::Backend("10.0.0.2:80".into())
    );
    let c = rt.try_clone().unwrap();
    assert_eq!(c.pick("x.example.com"), Pick::Backend("10.0.0.1:80".into()));
    assert_eq!(c.len(), 1);
}

#[test]
fn ports_come_off_and_ipv6_keeps_its_brackets() {
    assert_eq!(strip_port("a.b:8080"), "a.b");
    assert_eq!(strip_port("a.b"), "a.b");
    assert_eq!(strip_port("[::1]:80"), "[::1]");
    assert_eq!(strip_port("[::1]"), "[::1]");
}

fn table(exact: usize, wild: usize, per: usize) -> RouteTable {
    let mut routes = Vec::new();
    for i in 0..exact {
        let backends = (0..per)
            .map(|j| Backend::new(&format!("10.0.{j}.{}:8001", i % 255)).unwrap())
            .collect();
        routes.push(Route::new(format!("api-{i}.example.com"), backends));
    }
    for i in 0..wild {
        let backends = (0..per)
            .map(|j| Backend::new(&format!("10.1.{j}.{}:8001", i % 255)).unwrap())
            .collect();
        routes.push(Route::new(format!("*.domain{i}.com"), backends));
    }
    RouteTable::from_routes(routes).unwrap()
}

#[test]
fn benchmark_route_match() {
    // 70 exacte routes en 30 wildcards, drie backends elk.
    let rt = table(70, 30, 3);
    let hosts = ["api-35.example.com", "app.domain15.com"];
    let ns = bench("RouteMatch (100 routes)", 2_000_000, |i| {
        assert!(rt.match_host(hosts[i % 2]).is_some());
    });
    // Go: 20 ns/op op een M4 Pro; de lat van BENCHMARKS.md is < 1 µs.
    assert!(ns < 10_000.0, "route match {ns:.1} ns/op, lat 10 µs");
}

#[test]
fn benchmark_route_match_scale() {
    for n in [10, 100, 1000] {
        let rt = table(n / 2, n / 2, 1);
        // De wildcard van de laatste: in Go de slechtste zaak vóór de
        // opzoeking op staart.
        let host = format!("app.domain{}.com", n / 2 - 1);
        let ns = ns_per_op(&format!("RouteMatchScale/{n}_routes"), 1_000_000, |_| {
            assert!(rt.match_host(&host).is_some());
        });
        // De lat van BENCHMARKS.md: < 1 µs goed, < 10 µs aanvaardbaar (een
        // test-build zonder optimalisatie, naast de andere tests).
        assert!(ns < 10_000.0, "{n} routes: {ns:.1} ns/op");
    }
}

#[test]
fn benchmark_get_healthy_backend() {
    let r = Route::new(
        "test".to_string(),
        (0..10)
            .map(|i| Backend::new(&format!("10.0.0.{i}:8080")).unwrap())
            .collect(),
    );
    let ns = bench("GetHealthyBackend (10)", 5_000_000, |_| {
        assert!(r.healthy_backend().is_some());
    });
    // Go: 1.6 ns/op; de lat is < 100 ns.
    assert!(ns < 100.0, "backend selection {ns:.1} ns/op");
}

#[test]
fn benchmark_concurrent_route_match() {
    // Go mat hier de RWMutex onder parallelle lezers. Hier heeft elke
    // werker zijn eigen kopie (de vorm van de host-daemon), dus dit meet
    // dat parallel lezen niets deelt behalve de round-robin-teller van de
    // eigen kopie.
    let rt = table(100, 0, 2);
    let threads = 4;
    let per = 500_000;
    let start = std::time::Instant::now();
    std::thread::scope(|s| {
        for _ in 0..threads {
            let mine = rt.try_clone().unwrap();
            s.spawn(move || {
                for i in 0..per {
                    let host = format!("api-{}.example.com", i % 100);
                    assert!(mine.match_host(&host).is_some());
                }
            });
        }
    });
    let ops = f64::from(threads * per);
    let ns = start.elapsed().as_nanos() as f64 / ops;
    std::println!(
        "\nbench ConcurrentRouteMatch ({threads} threads): {ns:.1} ns/op over {ops} ops (Go: 159 ns/op, 1 alloc)"
    );
}
