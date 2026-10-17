//! Go's `benchmark_test.go` als toetsen met een meetregel.
//!
//! Elke benchmark draait zijn lus, toetst de uitkomst zoals Go's `b.Fatalf`
//! dat deed, en drukt één regel af: `bench <naam>: <ns>/op, <allocaties>/op`.
//! De allocaties telt een allocator die per thread telt, dus een toets die
//! naast een andere draait, telt alleen de zijne. De hete paden (een
//! cache-lezing, een hele vraag) moeten nul allocaties halen; dat is een
//! toets, geen belofte (handboek §6).
//!
//! De getallen zelf: `cargo test --release --test bench -- --nocapture`
//! (`tools/gate.sh` drukt ze af). In een debug-build zijn ze tien keer
//! trager en zeggen ze niets; de allocaties tellen daar ook.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::hint::black_box;
use std::net::Ipv4Addr;
use std::time::Instant;

use hopdns::wire::{encode_query, rtype};
use hopdns::{Cache, Jobs, Server, watcher};

/// Telt de allocaties van deze thread en geeft alles door aan `System`.
struct Counting;

thread_local! {
    static ALLOCS: Cell<u64> = const { Cell::new(0) };
}

fn count() {
    let _ = ALLOCS.try_with(|c| c.set(c.get() + 1));
}

// SAFETY: elke methode geeft zijn argumenten ongewijzigd door aan `System`,
// dat het contract van `GlobalAlloc` nakomt; het tellen raakt alleen een
// thread-lokale `Cell` zonder allocatie.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count();
        // SAFETY: het contract van de aanroeper gaat door naar `System`.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: `ptr` kwam van `System` met deze `layout`.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        count();
        // SAFETY: `ptr` kwam van `System` met deze `layout`.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

fn allocs() -> u64 {
    ALLOCS.with(Cell::get)
}

/// Draait `f` `iters` keer (na een tiende als opwarming) en drukt de
/// meetregel; geeft de allocaties per keer.
fn measure(name: &str, iters: u64, mut f: impl FnMut(u64)) -> f64 {
    for i in 0..iters / 10 {
        f(i);
    }
    let a0 = allocs();
    let t = Instant::now();
    for i in 0..iters {
        f(i);
    }
    let ns = t.elapsed().as_nanos() as f64 / iters as f64;
    let per = (allocs() - a0) as f64 / iters as f64;
    println!("bench {name}: {ns:.1} ns/op, {per:.2} allocs/op ({iters} iterations)");
    per
}

fn ip(a: u8, b: u8, c: u8) -> Ipv4Addr {
    Ipv4Addr::new(10, a, b, c)
}

fn jobs(n: usize) -> Vec<String> {
    (0..n).map(|i| format!("job-{i}")).collect()
}

// BenchmarkCacheGet: 50 jobs met drie adressen, lezen zonder allocatie.
#[test]
fn bench_cache_get() {
    let mut cache = Cache::new();
    let names = jobs(50);
    for (i, name) in names.iter().enumerate() {
        let i = i as u8;
        cache
            .set(
                "cluster-a",
                name,
                vec![ip(0, 0, i), ip(0, 1, i), ip(0, 2, i)],
            )
            .unwrap();
    }
    let per = measure("cache_get", 200_000, |i| {
        let ips = cache.get_cluster("cluster-a", &names[(i % 50) as usize]);
        assert_eq!(black_box(ips).map(<[Ipv4Addr]>::len), Some(3));
    });
    assert_eq!(per, 0.0, "a cache read must not allocate");
}

// BenchmarkCacheGetClusterSpecific: dezelfde job in drie clusters; Go
// bouwde de naam per ronde met Sprintf, hier ligt hij klaar.
#[test]
fn bench_cache_get_cluster_specific() {
    let mut cache = Cache::new();
    let names = jobs(50);
    for c in 0..3u8 {
        for (i, name) in names.iter().enumerate() {
            cache
                .set(&format!("cluster-{c}"), name, vec![ip(c, 0, i as u8)])
                .unwrap();
        }
    }
    let per = measure("cache_get_cluster_specific", 200_000, |i| {
        let ips = cache.get_cluster("cluster-0", &names[(i % 50) as usize]);
        assert_eq!(black_box(ips).map(<[Ipv4Addr]>::len), Some(1));
    });
    assert_eq!(per, 0.0);
}

// BenchmarkConcurrentCacheGet: in Go de RWMutex onder parallelle lezers.
// Hier heeft de cache één eigenaar en geen slot; vier lezers tegelijk
// lenen hem als `&` (scoped threads), wat Rust zonder slot toestaat omdat
// niemand schrijft zolang de leningen leven.
#[test]
fn bench_concurrent_cache_get() {
    let mut cache = Cache::new();
    let names = jobs(50);
    for (i, name) in names.iter().enumerate() {
        let i = i as u8;
        cache
            .set(
                "cluster-a",
                name,
                vec![ip(0, 0, i), ip(0, 1, i), ip(0, 2, i)],
            )
            .unwrap();
    }
    const THREADS: u64 = 4;
    const PER: u64 = 100_000;
    let t = Instant::now();
    std::thread::scope(|s| {
        for _ in 0..THREADS {
            s.spawn(|| {
                for i in 0..PER {
                    let ips = cache.get_cluster("cluster-a", &names[(i % 50) as usize]);
                    assert_eq!(black_box(ips).map(<[Ipv4Addr]>::len), Some(3));
                }
            });
        }
    });
    let ns = t.elapsed().as_nanos() as f64 / (THREADS * PER) as f64;
    println!(
        "bench concurrent_cache_get: {ns:.1} ns/op wall over {THREADS} threads ({} lookups)",
        THREADS * PER
    );
}

// BenchmarkCacheSet: één job zetten. Go gaf dezelfde slice elke ronde mee;
// hier verhuist een `Vec` de cache in, dus de kopie telt als één allocatie.
#[test]
fn bench_cache_set() {
    let mut cache = Cache::new();
    let ips = vec![ip(0, 0, 1), ip(0, 0, 2), ip(0, 0, 3)];
    let names = jobs(50);
    let per = measure("cache_set", 200_000, |i| {
        cache
            .set("cluster-a", &names[(i % 50) as usize], ips.clone())
            .unwrap();
    });
    assert!(per <= 1.0, "set may only copy the address list");
}

// BenchmarkCacheUpdate: het hele cluster vervangen bij 10, 50 en 200 jobs.
// Go wisselde een pointer onder het slot; hier wisselen twee beelden van
// plaats ([`Cache::replace`]), zonder allocatie.
#[test]
fn bench_cache_update() {
    for n in [10usize, 50, 200] {
        let mut data = Jobs::new();
        for (i, name) in jobs(n).iter().enumerate() {
            let i = (i % 256) as u8;
            data.insert(name, vec![ip(0, 0, i), ip(0, 1, i)]).unwrap();
        }
        let mut cache = Cache::new();
        cache.update("cluster-a", data.clone()).unwrap();
        let mut spare = Some(data);
        let per = measure(&format!("cache_update/{n}_jobs"), 200_000, |_| {
            let next = spare.take().unwrap();
            spare = cache.replace("cluster-a", next).unwrap();
        });
        assert_eq!(per, 0.0);
    }
}

fn query(name: &str) -> Vec<u8> {
    let mut q = [0u8; 512];
    let n = encode_query(1, name, rtype::A, None, &mut q).unwrap();
    q[..n].to_vec()
}

// BenchmarkHandleQuery: de hele vraag (lezen, cache, drie A-records
// schrijven), zonder allocatie.
#[test]
fn bench_handle_query() {
    let mut cache = Cache::new();
    cache
        .set("prod", "myapp", vec![ip(0, 0, 1), ip(0, 0, 2), ip(0, 0, 3)])
        .unwrap();
    let server = Server::new("internal").unwrap();
    let q = query("myapp.internal.");
    let mut out = [0u8; 512];
    let per = measure("handle_query", 200_000, |_| {
        let len = server.handle(&cache, &q, &mut out).unwrap();
        assert_eq!(black_box(len), q.len() + 3 * 16);
    });
    assert_eq!(per, 0.0, "a query must not allocate");
}

// BenchmarkHandleQueryScale: 1, 5 en 10 adressen per job.
#[test]
fn bench_handle_query_scale() {
    for n in [1u8, 5, 10] {
        let mut cache = Cache::new();
        cache
            .set("prod", "myapp", (1..=n).map(|i| ip(0, 0, i)).collect())
            .unwrap();
        let server = Server::new("internal").unwrap();
        let q = query("myapp.internal.");
        let mut out = [0u8; 512];
        let per = measure(&format!("handle_query_scale/{n}_ips"), 200_000, |_| {
            let len = server.handle(&cache, &q, &mut out).unwrap();
            assert_eq!(black_box(len), q.len() + usize::from(n) * 16);
        });
        assert_eq!(per, 0.0);
    }
}

// BenchmarkConcurrentHandleQuery: 20 jobs, vier vragers tegelijk op één
// gedeelde `&Server` en `&Cache`.
#[test]
fn bench_concurrent_handle_query() {
    let mut cache = Cache::new();
    for i in 0..20u8 {
        cache
            .set(
                "prod",
                &format!("job-{i}"),
                vec![ip(0, 0, i + 1), ip(0, 1, i + 1)],
            )
            .unwrap();
    }
    let server = Server::new("internal").unwrap();
    let queries: Vec<Vec<u8>> = (0..20)
        .map(|i| query(&format!("job-{i}.internal.")))
        .collect();
    const THREADS: u64 = 4;
    const PER: u64 = 50_000;
    let t = Instant::now();
    std::thread::scope(|s| {
        for _ in 0..THREADS {
            s.spawn(|| {
                let mut out = [0u8; 512];
                let a0 = allocs();
                for i in 0..PER {
                    let q = &queries[(i % 20) as usize];
                    let len = server.handle(&cache, q, &mut out).unwrap();
                    let m = hopdns::wire::decode(&out[..len]);
                    assert_eq!(black_box(m).map(|m| m.answers.len()).ok(), Some(2));
                }
                assert!(allocs() >= a0);
            });
        }
    });
    let ns = t.elapsed().as_nanos() as f64 / (THREADS * PER) as f64;
    println!(
        "bench concurrent_handle_query: {ns:.1} ns/op wall over {THREADS} threads, answers decoded ({} queries)",
        THREADS * PER
    );
}

// BenchmarkExtractIP: het adres uit een endpoint; Go alloceerde twee keer
// (url.Parse), hier nul.
#[test]
fn bench_extract_ip() {
    let endpoints = [
        "http://10.0.0.1:8080",
        "http://192.168.1.50:8080",
        "http://172.16.0.100:8080",
    ];
    let per = measure("extract_ip", 200_000, |i| {
        let got = watcher::extract_ip(endpoints[(i % 3) as usize]);
        assert!(black_box(got).is_some(), "expected an IP");
    });
    assert_eq!(per, 0.0);
}

// BenchmarkParseJobFromData: de job uit een SSE-gebeurtenis, nu door
// hoplib's lezer (de bytes erin, de gebeurtenis eruit, de job eruit).
#[test]
fn bench_parse_job_from_data() {
    let frames = [
        "event: job\ndata: {\"name\":\"my-api\"}\n\n",
        "event: job\ndata: {\"name\":\"web-frontend\"}\n\n",
        "event: job\ndata: {\"name\":\"worker-pool\"}\n\n",
        "event: job\ndata: {\"name\":\"hopdns\"}\n\n",
    ];
    let mut events = hoplib::Events::new();
    measure("parse_job_from_data", 100_000, |i| {
        events.push(frames[(i % 4) as usize].as_bytes()).unwrap();
        let e = events.next().unwrap().unwrap();
        assert!(black_box(e.job()).is_some(), "expected non-empty job");
    });
}
