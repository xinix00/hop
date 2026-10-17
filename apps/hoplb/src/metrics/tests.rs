//! De tests van `OLD/internal/metrics/metrics_test.go`, naam voor naam, en
//! de benchmarks van `benchmark_test.go` als tests met een meetregel.

use super::*;
use crate::testutil::{bench, ns_per_op};
use alloc::format;
use alloc::vec::Vec;

const MS: u64 = 1_000_000;

#[test]
fn test_metrics_record_request() {
    let mut m = Metrics::new();
    m.record_request("api.example.com", "10.0.1.5:8080", 200, 23 * MS)
        .unwrap();
    m.record_request("api.example.com", "10.0.1.5:8080", 200, 45 * MS)
        .unwrap();
    m.record_request("api.example.com", "10.0.1.5:8080", 500, 100 * MS)
        .unwrap();
    m.record_request("api.example.com", "10.0.1.6:8080", 200, 30 * MS)
        .unwrap();

    assert_eq!(m.request_count("api.example.com", "10.0.1.5:8080", 200), 2);
    assert_eq!(m.request_count("api.example.com", "10.0.1.5:8080", 500), 1);
    assert_eq!(m.sample_count("api.example.com", "10.0.1.5:8080"), 3);
}

#[test]
fn test_metrics_percentile() {
    let mut m = Metrics::new();
    for i in 1..=100 {
        m.record_request("api.example.com", "10.0.1.5:8080", 200, i * MS)
            .unwrap();
    }
    let p50 = m.percentile("api.example.com", "10.0.1.5:8080", 0.5);
    assert!(
        (0.050..=0.051).contains(&p50),
        "p50 should be ~0.050s, got {p50:.6}"
    );
    let p99 = m.percentile("api.example.com", "10.0.1.5:8080", 0.99);
    assert!(
        (0.099..=0.100).contains(&p99),
        "p99 should be ~0.099s, got {p99:.6}"
    );
}

#[test]
fn test_metrics_rolling_window() {
    let mut m = Metrics::with_limits(10, MAX_SERIES);
    for i in 0..20 {
        m.record_request("api.example.com", "10.0.1.5:8080", 200, i * MS)
            .unwrap();
    }
    assert_eq!(m.sample_count("api.example.com", "10.0.1.5:8080"), 10);
    // En het zijn de laatste tien: 10 tot en met 19 ms.
    let p0 = m.percentile("api.example.com", "10.0.1.5:8080", 0.0);
    assert!((p0 - 0.010).abs() < 1e-9, "oldest kept sample {p0}");
}

#[test]
fn test_metrics_multiple_domains() {
    let mut m = Metrics::new();
    m.record_request("api.example.com", "10.0.1.5:8080", 200, 10 * MS)
        .unwrap();
    m.record_request("web.example.com", "10.0.1.6:8080", 200, 20 * MS)
        .unwrap();
    m.record_request("admin.example.com", "10.0.1.7:8080", 200, 30 * MS)
        .unwrap();
    let domains: Vec<&str> = m.all_domains().collect();
    assert_eq!(
        domains,
        ["admin.example.com", "api.example.com", "web.example.com"]
    );
}

#[test]
fn test_metrics_no_samples() {
    let mut m = Metrics::new();
    assert_eq!(m.percentile("nonexistent.com", "10.0.1.5:8080", 0.5), 0.0);
    assert_eq!(m.sample_count("nonexistent.com", "10.0.1.5:8080"), 0);
}

// Wat Go niet toetste: de export zelf, de cache, en de grens.

#[test]
fn the_export_is_the_go_text() {
    let mut m = Metrics::new();
    m.record_request("api.example.com", "10.0.1.5:8080", 500, 100 * MS)
        .unwrap();
    m.record_request("api.example.com", "10.0.1.5:8080", 200, 20 * MS)
        .unwrap();
    let mut out = String::new();
    m.render(&mut out).unwrap();
    let want = "# HELP hoplb_requests_total Total HTTP requests\n\
# TYPE hoplb_requests_total counter\n\
hoplb_requests_total{domain=\"api.example.com\",backend=\"10.0.1.5:8080\",code=\"200\"} 1\n\
hoplb_requests_total{domain=\"api.example.com\",backend=\"10.0.1.5:8080\",code=\"500\"} 1\n\
\n\
# HELP hoplb_request_duration_seconds Request duration percentiles\n\
# TYPE hoplb_request_duration_seconds summary\n\
hoplb_request_duration_seconds{domain=\"api.example.com\",backend=\"10.0.1.5:8080\",quantile=\"0.50\"} 0.020000\n\
hoplb_request_duration_seconds{domain=\"api.example.com\",backend=\"10.0.1.5:8080\",quantile=\"0.90\"} 0.020000\n\
hoplb_request_duration_seconds{domain=\"api.example.com\",backend=\"10.0.1.5:8080\",quantile=\"0.95\"} 0.020000\n\
hoplb_request_duration_seconds{domain=\"api.example.com\",backend=\"10.0.1.5:8080\",quantile=\"0.99\"} 0.020000\n\
hoplb_request_duration_seconds_count{domain=\"api.example.com\",backend=\"10.0.1.5:8080\"} 2\n\
hoplb_request_duration_seconds_sum{domain=\"api.example.com\",backend=\"10.0.1.5:8080\"} 0.120000\n";
    assert_eq!(out, want);
}

#[test]
fn an_empty_export_has_both_heads() {
    let mut out = String::new();
    Metrics::new().render(&mut out).unwrap();
    assert!(
        out.contains(
            "# TYPE hoplb_requests_total counter\n\n# HELP hoplb_request_duration_seconds"
        )
    );
}

#[test]
fn label_values_are_escaped() {
    let mut m = Metrics::new();
    m.record_request("a\"b\\c\nd", "", 502, MS).unwrap();
    let mut out = String::new();
    m.render(&mut out).unwrap();
    assert!(
        out.contains(r#"domain="a\"b\\c\nd",backend="",code="502""#),
        "{out}"
    );
}

#[test]
fn series_over_the_limit_fold_into_one() {
    let mut m = Metrics::with_limits(100, 2);
    for d in ["a", "b", "c", "d"] {
        m.record_request(d, "x:1", 200, MS).unwrap();
    }
    assert_eq!(m.all_domains().count(), 3, "a, b and {OVERFLOW_DOMAIN}");
    assert_eq!(m.request_count(OVERFLOW_DOMAIN, "", 200), 2);
    assert_eq!(m.folded(), 2);
    let mut out = String::new();
    m.render(&mut out).unwrap();
    assert!(out.contains("hoplb_series_folded_total 2\n"));
}

#[test]
fn a_new_sample_invalidates_the_sorted_copy() {
    let mut m = Metrics::with_limits(3, 8);
    for ms in [1, 2, 3] {
        m.record_request("d", "b", 200, ms * MS).unwrap();
    }
    assert!((m.percentile("d", "b", 1.0) - 0.003).abs() < 1e-9);
    // Het venster is vol; de nieuwe vervangt de oudste en de lengte blijft.
    m.record_request("d", "b", 200, 9 * MS).unwrap();
    assert!((m.percentile("d", "b", 1.0) - 0.009).abs() < 1e-9);
    assert!((m.percentile("d", "b", 0.0) - 0.002).abs() < 1e-9);
}

#[test]
fn benchmark_record_request() {
    let mut m = Metrics::new();
    let domains = ["api.example.com", "web.example.com", "admin.example.com"];
    let backends = ["10.0.0.1:8080", "10.0.0.2:8080", "10.0.0.3:8080"];
    let codes = [200, 200, 200, 200, 500, 502];
    let ns = bench("RecordRequest", 1_000_000, |i| {
        m.record_request(
            domains[i % 3],
            backends[i % 3],
            codes[i % 6],
            (i % 100) as u64 * MS,
        )
        .unwrap();
    });
    // Go: 54 ns/op; de lat is < 10 µs.
    assert!(ns < 10_000.0, "record {ns:.1} ns/op");
}

#[test]
fn benchmark_concurrent_record_request() {
    // Go mat hier de mutex onder parallelle schrijvers. Hier is er één
    // eigenaar en sturen de werkers een Record over een kanaal: dit meet
    // die weg, van vier zenders naar één eigenaar-thread.
    use std::sync::mpsc;
    let (tx, rx) = mpsc::sync_channel::<Record>(1024);
    let senders = 4;
    let per = 100_000;
    let start = std::time::Instant::now();
    let owner = std::thread::spawn(move || {
        let mut m = Metrics::new();
        let mut n = 0u64;
        while let Ok(r) = rx.recv() {
            m.record(&r).unwrap();
            n += 1;
        }
        n
    });
    std::thread::scope(|s| {
        for _ in 0..senders {
            let tx = tx.clone();
            s.spawn(move || {
                for i in 0..per {
                    tx.send(Record {
                        domain: format!("domain-{}.example.com", i % 10),
                        backend: format!("10.0.0.{}:8080", i % 5),
                        code: 200,
                        nanos: (i % 100) as u64 * MS,
                    })
                    .unwrap();
                }
            });
        }
    });
    drop(tx);
    let n = owner.join().unwrap();
    assert_eq!(n, (senders * per) as u64);
    let ns = start.elapsed().as_nanos() as f64 / n as f64;
    std::println!(
        "\nbench ConcurrentRecordRequest ({senders} senders, 1 owner): {ns:.1} ns/op ({n} ops; Go: 396 ns/op)"
    );
}

fn filled(n: usize) -> Metrics {
    let mut m = Metrics::with_limits(n, MAX_SERIES);
    for i in 0..n {
        m.record_request(
            "api.example.com",
            "10.0.0.1:8080",
            200,
            (i % 500) as u64 * MS,
        )
        .unwrap();
    }
    m
}

#[test]
fn benchmark_percentile() {
    let mut m = filled(10_000);
    let ns = bench("Percentile (10k samples)", 1_000_000, |_| {
        assert!(m.percentile("api.example.com", "10.0.0.1:8080", 0.99) != 0.0);
    });
    // Go: 23 ns/op met de cache; zonder was het 96 µs.
    assert!(ns < 10_000.0, "cached percentile {ns:.1} ns/op");
}

#[test]
fn benchmark_percentile_scale() {
    for n in [100, 1000, 10_000] {
        let mut m = filled(n);
        let ns = ns_per_op(&format!("PercentileScale/{n}_samples"), 500_000, |_| {
            assert!(m.percentile("api.example.com", "10.0.0.1:8080", 0.99) != 0.0);
        });
        assert!(ns < 10_000.0, "{n} samples: {ns:.1} ns/op");
    }
}

#[test]
fn benchmark_exporter() {
    let mut m = Metrics::new();
    for d in 0..10 {
        let domain = format!("service-{d}.example.com");
        for b in 0..3 {
            let addr = format!("10.0.{d}.{b}:8080");
            for s in 0..1000 {
                let code = if s % 20 == 0 { 500 } else { 200 };
                m.record_request(&domain, &addr, code, (s % 500) as u64 * MS)
                    .unwrap();
            }
        }
    }
    let mut out = String::new();
    let ns = bench("Exporter (10 domains)", 2_000, |_| {
        out.clear();
        m.render(&mut out).unwrap();
        assert!(out.len() > 1000);
    });
    // Go: 106 µs/op; de lat van BENCHMARKS.md is < 50 ms.
    assert!(ns < 50_000_000.0, "export {ns:.0} ns/op");
}

#[test]
fn benchmark_all_domains() {
    for n in [10, 100, 1000] {
        let mut m = Metrics::with_limits(10, 2 * n);
        for d in 0..n {
            m.record_request(
                &format!("service-{d}.example.com"),
                "10.0.0.1:8080",
                200,
                MS,
            )
            .unwrap();
        }
        ns_per_op(&format!("AllDomains/{n}_domains"), 20_000, |_| {
            assert_eq!(m.all_domains().count(), n);
        });
    }
}
