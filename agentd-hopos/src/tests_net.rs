//! De netwerk-kant van de bewoner zonder net: SNTP tegen een nep-server,
//! en de artifact-dialer (URL, http of https, de foutpaden) tegen nep-
//! verbindingen uit het geheugen.

use alloc::collections::BTreeMap;
use alloc::string::String;
use alloc::vec::Vec;
use core::future::Future;
use core::pin::pin;
use core::task::{Context, Poll, Waker};
use std::cell::RefCell;
use std::rc::Rc;

use leanhttp::{AsyncRead, AsyncWrite, Close, IoError};

use crate::entropy::Pool;
use crate::fetch::{Clock, Connect, HttpImages, IpOnly, Resolve, Scheme, scheme_of};
use crate::sntp::{self, NtpLink, PACKET, SntpError};
use crate::{Images, Sink};

fn block_on<F: Future>(f: F) -> F::Output {
    let mut f = pin!(f);
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..100_000 {
        if let Poll::Ready(v) = f.as_mut().poll(&mut cx) {
            return v;
        }
    }
    panic!("future bleef hangen");
}

// ---- Namen ----

/// Een resolver uit een tabel.
#[derive(Clone, Default)]
struct Table(BTreeMap<&'static str, [u8; 4]>);

impl Resolve for Table {
    async fn resolve(&mut self, host: &str) -> Result<[u8; 4], String> {
        self.0
            .get(host)
            .copied()
            .ok_or_else(|| format!("dns: no such name (NXDOMAIN) for {host}"))
    }
}

fn names() -> Table {
    let mut t = BTreeMap::new();
    t.insert("pool.ntp.org", [162, 159, 200, 1]);
    t.insert("artifacts.local", [10, 0, 2, 2]);
    t.insert("github.com", [140, 82, 121, 4]);
    t.insert("objects.githubusercontent.com", [185, 199, 108, 133]);
    Table(t)
}

// ---- SNTP ----

/// Wat de nep-server met de zoveelste vraag doet; `None` is stilte.
type Answer = fn(usize, &[u8; PACKET]) -> Option<Vec<u8>>;

/// NTP-seconden van 2026-09-29T00:00:00Z.
const NTP_NOW: u64 = 1_790_640_000 + 2_208_988_800;

/// Een nep-NTP-server: antwoordt per vraag met wat `answer` van de vraag
/// maakt; `None` is een verloren datagram. Telt de vragen en onthoudt het
/// adres.
struct FakeNtp {
    answer: Answer,
    asked: usize,
    server: Option<[u8; 4]>,
}

impl NtpLink for FakeNtp {
    async fn exchange(
        &mut self,
        server: [u8; 4],
        req: &[u8; PACKET],
        resp: &mut [u8],
    ) -> Result<usize, String> {
        self.server = Some(server);
        let i = self.asked;
        self.asked += 1;
        let a = (self.answer)(i, req).ok_or_else(|| String::from("timed out after 3 s"))?;
        resp[..a.len()].copy_from_slice(&a);
        Ok(a.len())
    }
}

/// Een goed serverantwoord op `req`: stratum 2, T2 = T3 - 1 ms.
fn server_reply(req: &[u8; PACKET]) -> Vec<u8> {
    let mut p = vec![0u8; PACKET];
    p[0] = 0x24; // LI 0, versie 4, mode 4.
    p[1] = 2;
    p[24..32].copy_from_slice(&req[40..48]);
    let t3 = (NTP_NOW << 32) | (1 << 31); // + 0,5 s
    let t2 = t3 - ((1u64 << 32) / 1000); // 1 ms eerder
    p[32..40].copy_from_slice(&t2.to_be_bytes());
    p[40..48].copy_from_slice(&t3.to_be_bytes());
    p
}

fn run_sntp(answer: Answer) -> (Result<sntp::Sample, SntpError>, FakeNtp) {
    let mut link = FakeNtp {
        answer,
        asked: 0,
        server: None,
    };
    let mono = std::cell::Cell::new(5_000_000_000u64);
    let clock = || {
        // Elke lezing 11 ms later: de rondreis is dan 11 ms.
        mono.set(mono.get() + 11_000_000);
        mono.get()
    };
    let mut n = 0u64;
    let r = block_on(sntp::sync(
        &mut names(),
        &mut link,
        sntp::SERVER,
        clock,
        || {
            n += 1;
            0x1234_0000 + n
        },
    ));
    (r, link)
}

#[test]
fn sntp_against_a_fake_server() {
    let (r, link) = run_sntp(|_, req| Some(server_reply(req)));
    let s = r.unwrap();
    assert_eq!(link.server, Some([162, 159, 200, 1]), "via de resolver");
    assert_eq!(link.asked, 1);
    // Rondreis 11 ms min 1 ms bij de server: 10 ms, de helft terug.
    assert_eq!(s.delay_ns, 10_000_000);
    assert_eq!(
        s.unix_ns,
        1_790_640_000 * 1_000_000_000 + 500_000_000 + 5_000_000
    );
    assert_eq!(s.stratum, 2);
    assert_eq!(s.unix_at(s.at + 7), s.unix_ns + 7);
}

#[test]
fn sntp_retries_silence_and_refuses_crooked_answers() {
    // Twee keer stilte, dan antwoord: drie pogingen is genoeg.
    let (r, link) = run_sntp(|i, req| (i == 2).then(|| server_reply(req)));
    assert!(r.is_ok(), "{r:?}");
    assert_eq!(link.asked, 3);
    // Altijd stilte: de laatste fout, na drie vragen.
    let (r, link) = run_sntp(|_, _| None);
    assert_eq!(r, Err(SntpError::Link(String::from("timed out after 3 s"))));
    assert_eq!(link.asked, 3);

    let cases: [(Answer, SntpError); 6] = [
        (
            |_, r| Some(server_reply(r)[..40].to_vec()),
            SntpError::Short(40),
        ),
        (
            |_, r| {
                let mut p = server_reply(r);
                p[0] = 0x23; // een vraag
                Some(p)
            },
            SntpError::NotServer(3),
        ),
        (
            |_, r| {
                let mut p = server_reply(r);
                p[24] ^= 1; // niet ons transmit-veld
                Some(p)
            },
            SntpError::Mismatch,
        ),
        (
            |_, r| {
                let mut p = server_reply(r);
                p[0] = 0xe4; // LI 3: alarm
                Some(p)
            },
            SntpError::Unsynchronized,
        ),
        (
            |_, r| {
                let mut p = server_reply(r);
                p[40..48].copy_from_slice(&((2_208_988_800u64 + 5) << 32).to_be_bytes());
                Some(p)
            },
            SntpError::TooEarly(5),
        ),
        (
            |_, r| {
                let mut p = server_reply(r);
                p[1] = 0;
                p[12..16].copy_from_slice(b"RATE");
                Some(p)
            },
            SntpError::Kiss(*b"RATE"),
        ),
    ];
    for (answer, want) in cases {
        let (r, link) = run_sntp(answer);
        assert_eq!(r, Err(want.clone()), "{want}");
        // Een Kiss-o'-Death stopt meteen; de rest krijgt zijn drie kansen.
        let asked = if matches!(want, SntpError::Kiss(_)) {
            1
        } else {
            3
        };
        assert_eq!(link.asked, asked, "{want}");
    }
    // Een naam die niet bestaat: geen vraag.
    let mut link = FakeNtp {
        answer: |_, r| Some(server_reply(r)),
        asked: 0,
        server: None,
    };
    let r = block_on(sntp::sync(
        &mut IpOnly,
        &mut link,
        "pool.ntp.org",
        || 0,
        || 1,
    ));
    assert!(
        matches!(r, Err(SntpError::Resolve(ref e)) if e.contains("no resolver")),
        "{r:?}"
    );
    assert_eq!(link.asked, 0);
}

// ---- Artifacts ----

/// Een verbinding uit het geheugen: leest `input`, bewaart wat er
/// geschreven wordt.
struct Mem {
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

/// Wat de nep-netstack zag: per verbinding het adres en de geschreven bytes.
type Seen = Rc<RefCell<Vec<([u8; 4], u16, Rc<RefCell<Vec<u8>>>)>>>;

/// Een netstack die per verbinding het volgende antwoord uit `script`
/// geeft.
struct FakeNet {
    script: Vec<Vec<u8>>,
    seen: Seen,
}

impl Connect for FakeNet {
    type Conn = Mem;

    async fn connect(&mut self, ip: [u8; 4], port: u16) -> Result<Mem, String> {
        if self.script.is_empty() {
            return Err(String::from("refused"));
        }
        let out = Rc::new(RefCell::new(Vec::new()));
        self.seen.borrow_mut().push((ip, port, out.clone()));
        Ok(Mem {
            input: self.script.remove(0),
            at: 0,
            out,
        })
    }
}

/// Een klok met of zonder vertrouwde tijd.
struct TestClock(Option<u64>);

impl Clock for TestClock {
    fn trusted_unix_secs(&self) -> Option<u64> {
        self.0
    }
    fn mono_ns(&self) -> u64 {
        42
    }
}

/// Een sink die alles bewaart.
#[derive(Default)]
struct Keep {
    len: Option<u64>,
    bytes: Vec<u8>,
}

impl Sink for Keep {
    async fn begin(&mut self, size: u64) -> Result<(), String> {
        self.len = Some(size);
        Ok(())
    }
    async fn chunk(&mut self, bytes: &[u8]) -> Result<(), String> {
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }
}

/// Haalt `url` op met een nep-net dat `script` speelt; de uitkomst, de
/// sink en wat het net zag.
fn fetch(url: &str, script: &[&[u8]], clock: Option<u64>) -> (Result<(), String>, Keep, Seen) {
    let seen: Seen = Rc::default();
    let net = FakeNet {
        script: script.iter().map(|s| s.to_vec()).collect(),
        seen: seen.clone(),
    };
    let mut images = HttpImages::new(net, names(), TestClock(clock), Pool::new(b"test"));
    assert_eq!(images.root_count(), 119, "de ingebakken Mozilla-set leest");
    let mut sink = Keep::default();
    let r = block_on(images.fetch(url, &mut sink));
    (r, sink, seen)
}

const OK: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\n\x7fELF-image";
const NOW: Option<u64> = Some(1_790_640_000);

#[test]
fn urls_and_schemes() {
    assert_eq!(scheme_of("http://10.0.2.2:8000/a.elf"), Ok(Scheme::Http));
    assert_eq!(scheme_of("HTTPS://github.com/x"), Ok(Scheme::Https));
    for bad in ["ftp://x/y", "github.com/x", "https://", "https:///x"] {
        assert!(scheme_of(bad).is_err(), "{bad}");
    }
    let (r, _, seen) = fetch("ftp://artifacts.local/a.elf", &[OK], NOW);
    assert!(r.unwrap_err().contains("not supported"));
    assert!(seen.borrow().is_empty());
}

#[test]
fn http_by_name_goes_plain_through_the_resolver() {
    let (r, sink, seen) = fetch("http://artifacts.local:8000/a.elf", &[OK], None);
    r.unwrap();
    assert_eq!(
        (sink.len, sink.bytes.as_slice()),
        (Some(10), &b"\x7fELF-image"[..])
    );
    let seen = seen.borrow();
    assert_eq!((seen[0].0, seen[0].1), ([10, 0, 2, 2], 8000));
    assert!(seen[0].2.borrow().starts_with(b"GET /a.elf HTTP/1.1\r\n"));
}

#[test]
fn the_same_url_is_fetched_whole_every_time() {
    // De server vervangt het bestand tussen twee plaatsingen. De downloader
    // onthoudt niets: de tweede keer weer een kale GET (geen If-None-Match,
    // geen gecachet image) en de nieuwe bytes.
    const NEW: &[u8] = b"HTTP/1.1 200 OK\r\nETag: \"v2\"\r\nContent-Length: 8\r\n\r\n\x7fELF-new";
    let seen: Seen = Rc::default();
    let net = FakeNet {
        script: vec![OK.to_vec(), NEW.to_vec()],
        seen: seen.clone(),
    };
    let mut images = HttpImages::new(net, names(), TestClock(NOW), Pool::new(b"test"));
    let url = "http://artifacts.local/a.elf";
    let mut first = Keep::default();
    block_on(images.fetch(url, &mut first)).unwrap();
    let mut second = Keep::default();
    block_on(images.fetch(url, &mut second)).unwrap();
    assert_eq!(first.bytes, b"\x7fELF-image");
    assert_eq!(second.bytes, b"\x7fELF-new");
    let seen = seen.borrow();
    assert_eq!(seen.len(), 2, "twee verbindingen, twee volledige downloads");
    for (_, _, req) in seen.iter() {
        let req = req.borrow();
        assert!(req.starts_with(b"GET /a.elf HTTP/1.1\r\n"));
        assert!(
            !req.windows(13)
                .any(|w| w.eq_ignore_ascii_case(b"If-None-Match"))
        );
    }
}

#[test]
fn https_goes_through_tls_with_the_name_as_sni() {
    // De "server" praat geen TLS: de handshake faalt, maar pas nadat de
    // dialer de naam opzocht, poort 443 opende en een ClientHello met de
    // naam erin stuurde. Geen GET in platte tekst.
    let (r, _, seen) = fetch(
        "https://github.com/xinix00/HopOS/releases/download/apps/welcome.elf",
        &[b"HTTP/1.1 400 Bad Request\r\n\r\n"],
        NOW,
    );
    let e = r.unwrap_err();
    assert!(e.contains("tls github.com"), "{e}");
    let seen = seen.borrow();
    assert_eq!((seen[0].0, seen[0].1), ([140, 82, 121, 4], 443));
    let hello = seen[0].2.borrow();
    assert_eq!(hello[0], 0x16, "een TLS-handshakerecord");
    assert!(hello.windows(10).any(|w| w == b"github.com"), "SNI");
    assert!(!hello.windows(4).any(|w| w == b"GET "));
}

#[test]
fn https_is_refused_without_clock_or_name_and_says_why() {
    let (r, _, seen) = fetch("https://github.com/x.elf", &[OK], None);
    let e = r.unwrap_err();
    assert!(e.contains("SNTP") && e.contains("https refused"), "{e}");
    assert!(seen.borrow().is_empty(), "geen verbinding zonder klok");

    let (r, _, seen) = fetch("https://10.0.2.2/x.elf", &[OK], NOW);
    assert!(r.unwrap_err().contains("bare address"));
    assert!(seen.borrow().is_empty());

    let (r, _, _) = fetch("http://nowhere.example/x.elf", &[OK], NOW);
    let e = r.unwrap_err();
    assert!(
        e.contains("resolve nowhere.example") && e.contains("NXDOMAIN"),
        "{e}"
    );

    let (r, _, _) = fetch("http://artifacts.local/x.elf", &[], NOW);
    let e = r.unwrap_err();
    assert!(
        e.contains("connect artifacts.local (10.0.2.2:80): refused"),
        "{e}"
    );
}

#[test]
fn a_redirect_to_https_switches_to_tls_per_hop() {
    // Zoals een GitHub-release: 302 naar een andere host op https.
    const REDIRECT: &[u8] = b"HTTP/1.1 302 Found\r\nLocation: https://objects.githubusercontent.com/o/1?sig=x\r\nContent-Length: 0\r\n\r\n";
    let (r, _, seen) = fetch("http://artifacts.local/a.elf", &[REDIRECT, b"not tls"], NOW);
    let e = r.unwrap_err();
    assert!(e.contains("tls objects.githubusercontent.com"), "{e}");
    {
        let seen = seen.borrow();
        assert_eq!(seen.len(), 2);
        assert_eq!((seen[1].0, seen[1].1), ([185, 199, 108, 133], 443));
        assert_eq!(seen[1].2.borrow()[0], 0x16);
    }
    // Zonder klok: de tweede hop weigert met de reden, na de eerste.
    let (r, _, seen) = fetch("http://artifacts.local/a.elf", &[REDIRECT, OK], None);
    let e = r.unwrap_err();
    assert!(e.contains("SNTP"), "{e}");
    assert_eq!(seen.borrow().len(), 1);
}

// ---- De echte download (met net, op verzoek) ----

/// Een blokkerende std-TCP-verbinding als leanhttp-verbinding: elke poll is
/// meteen klaar, dus `block_on` met een lege waker volstaat.
struct StdConn(std::net::TcpStream);

impl AsyncRead for StdConn {
    fn poll_read(&mut self, _: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize, IoError>> {
        use std::io::Read;
        Poll::Ready(self.0.read(buf).map_err(|_| IoError::Reset))
    }
}

impl AsyncWrite for StdConn {
    fn poll_write(&mut self, _: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, IoError>> {
        use std::io::Write;
        Poll::Ready(self.0.write(buf).map_err(|_| IoError::Reset))
    }
}

impl Close for StdConn {
    fn poll_close(&mut self, _: &mut Context<'_>) -> Poll<Result<(), IoError>> {
        let _ = self.0.shutdown(std::net::Shutdown::Both);
        Poll::Ready(Ok(()))
    }
}

struct StdNet;

impl Connect for StdNet {
    type Conn = StdConn;

    async fn connect(&mut self, ip: [u8; 4], port: u16) -> Result<StdConn, String> {
        let addr = std::net::SocketAddr::from((ip, port));
        let s = std::net::TcpStream::connect_timeout(&addr, std::time::Duration::from_secs(10))
            .map_err(|e| format!("{e}"))?;
        s.set_read_timeout(Some(std::time::Duration::from_secs(30)))
            .map_err(|e| format!("{e}"))?;
        Ok(StdConn(s))
    }
}

struct StdDns;

impl Resolve for StdDns {
    async fn resolve(&mut self, host: &str) -> Result<[u8; 4], String> {
        use std::net::ToSocketAddrs;
        (host, 0)
            .to_socket_addrs()
            .map_err(|e| format!("{e}"))?
            .find_map(|a| match a.ip() {
                std::net::IpAddr::V4(v4) => Some(v4.octets()),
                std::net::IpAddr::V6(_) => None,
            })
            .ok_or_else(|| format!("{host}: no IPv4 address"))
    }
}

struct StdClock;

impl Clock for StdClock {
    fn trusted_unix_secs(&self) -> Option<u64> {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .map(|d| d.as_secs())
    }
    fn mono_ns(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos() as u64)
    }
}

/// De echte GitHub-download: dezelfde `HttpImages` als op de node (TLS 1.3,
/// de ingebakken Mozilla-wortels, de redirect naar
/// objects.githubusercontent.com), met std-sockets eronder. Draait alleen
/// op verzoek, met net: `sh tools/github-download.sh [url]`.
#[test]
#[ignore = "heeft internet nodig; tools/github-download.sh"]
fn real_github_release_download() {
    let url = std::env::var("HOP_TEST_URL").unwrap_or_else(|_| {
        String::from(
            "https://github.com/xinix00/HopOS/releases/download/apps/welcome-arm64-tamago.elf",
        )
    });
    let mut pool = Pool::new(b"real-download");
    pool.harvest(|| StdClock.mono_ns(), 512);
    let mut images = HttpImages::new(StdNet, StdDns, StdClock, pool);
    let mut sink = Keep::default();
    let r = block_on(images.fetch(&url, &mut sink));
    std::println!(
        "download {url}: {r:?}, length {:?}, {} bytes",
        sink.len,
        sink.bytes.len()
    );
    r.unwrap();
    assert_eq!(sink.len, Some(sink.bytes.len() as u64));
    assert!(sink.bytes.starts_with(b"\x7fELF"), "geen ELF");
}

// ---- De pool onder druk ----

/// Geen stromen: de doorgifte-lus zonder eigenaar.
struct NoStreams;

impl hop_http::Streams for NoStreams {
    async fn poll(&mut self, _: hop_http::Ask) -> hop_http::Chunk {
        hop_http::Chunk::default()
    }
    async fn nap(&mut self, _: core::time::Duration) {}
    fn now(&self) -> u64 {
        0
    }
    fn done(&mut self) {}
}

fn no_clock() -> Option<u64> {
    None
}

/// Twee verzoeken achter elkaar op één verbinding door `forward::serve`,
/// met `crowded` als stand van de pool; wat er op de draad kwam.
fn serve_two(crowded: bool) -> String {
    use crate::forward::{Routed, serve};
    use hop_http::Reply;

    let out = Rc::new(RefCell::new(Vec::new()));
    let conn = Mem {
        input: b"GET /a HTTP/1.1\r\nHost: n\r\n\r\nGET /b HTTP/1.1\r\nHost: n\r\n\r\n".to_vec(),
        at: 0,
        out: out.clone(),
    };
    let net = FakeNet {
        script: Vec::new(),
        seen: Rc::new(RefCell::new(Vec::new())),
    };
    let mut client = crate::client::Client::new(net, names(), Pool::new(b"test"), no_clock);
    let mut none = NoStreams;
    block_on(serve(
        conn,
        async |req: api::Request| {
            let mut r = api::Response::empty(200);
            r.body = req.path.into_bytes();
            Routed::Reply(Reply::Plain(r))
        },
        &mut none,
        &mut client,
        crate::Port::Leader,
        || crowded,
    ))
    .unwrap();
    String::from_utf8(out.borrow().clone()).unwrap()
}

#[test]
fn a_crowded_pool_closes_after_the_answer() {
    // Ruimte genoeg: keep-alive, beide verzoeken op dezelfde verbinding.
    let text = serve_two(false);
    assert_eq!(text.matches("HTTP/1.1 200").count(), 2, "{text}");
    assert!(!text.contains("Connection: close"), "{text}");

    // Elke werker bezet: het eerste antwoord zegt close, en de verbinding
    // gaat dicht voordat het tweede verzoek aan de beurt is. Een wachtende
    // verbinding krijgt deze werker dus na één verzoek, niet na de
    // leestermijn.
    let text = serve_two(true);
    assert_eq!(text.matches("HTTP/1.1 200").count(), 1, "{text}");
    assert!(text.contains("Connection: close"), "{text}");
    assert!(text.ends_with("/a"), "{text}");
}
