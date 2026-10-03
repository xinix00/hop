//! De cluster van de bewoner zonder net: de lock-config, de lease op een
//! nep-hoplockserver en een nep-S3, de relay van de leader, de verkiezing
//! van de node (leider worden, volgen, registreren), en de doorgiftes.
//!
//! De nep-server is een handler achter een verbinding uit het geheugen: de
//! client van de bewoner (leanhttp) schrijft een echt verzoek, de handler
//! leest het en antwoordt met echte bytes. Zo toetst de test het protocol op
//! de draad (koppen, voorwaarden, sleutels), niet een nagebootste functie.

use alloc::collections::BTreeMap;
use alloc::string::String;
use alloc::vec::Vec;
use core::future::Future;
use core::pin::pin;
use core::sync::atomic::{AtomicU64, Ordering::Relaxed};
use core::task::{Context, Poll, Waker};
use std::cell::RefCell;
use std::rc::Rc;

use abi::systemapi::SlotInfo;
use agent::{LeaseOp, LeaseReply, LinkError, Request as LinkRequest};
use api::{Method, Request, TasksScope};
use discovery::{Discovery, LeaseState};
use hopos_runner::KernSys;
use hopos_runner::fake::FakeKern;
use leader::RunReply;
use leanhttp::{AsyncRead, AsyncWrite, Close, IoError};
use sync::mpsc::Mailbox;

use crate::client::Client;
use crate::entropy::Pool;
use crate::env::{BootConfig, BootError, S3Config};
use crate::fetch::{Connect, IpOnly};
use crate::forward::{self, Forward, Routed, Source};
use crate::lease;
use crate::lock::{self, ClusterConfig, LeaseBackend, LockKind, StateBackend};
use crate::mail::{
    DispatchQueue, Inbox, LeaseQueue, LinkJob, LinkQueue, Mail, Remote, StateOp, StateQueue,
};
use crate::relay::Relay;
use crate::{ClusterParts, Images, Node, Port, Sink};

const KEY: &[u8] = b"test-key";
const T0: u64 = 1_790_640_000 * types::time::SECOND;
const SECOND: u64 = types::time::SECOND;

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

fn env(pairs: &[(&'static str, &'static str)]) -> impl Fn(&str) -> Option<String> {
    let m: BTreeMap<&str, &str> = pairs.iter().copied().collect();
    move |k| m.get(k).map(|v| String::from(*v))
}

// ---- De nep-server ----

/// Een verzoek zoals de nep-server het las.
#[derive(Clone, Debug)]
struct Raw {
    ip: [u8; 4],
    port: u16,
    method: String,
    target: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Raw {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    fn path(&self) -> &str {
        self.target.split('?').next().unwrap()
    }
}

/// Een antwoord: status, koppen en body.
type Out = (u16, Vec<(&'static str, String)>, Vec<u8>);

type Handler = Box<dyn FnMut(&Raw) -> Out>;

struct Server {
    handler: Handler,
    seen: Vec<Raw>,
}

/// Een netstack waar elke verbinding bij dezelfde handler uitkomt.
#[derive(Clone)]
struct FakeNet(Rc<RefCell<Server>>);

impl FakeNet {
    fn new(handler: impl FnMut(&Raw) -> Out + 'static) -> Self {
        Self(Rc::new(RefCell::new(Server {
            handler: Box::new(handler),
            seen: Vec::new(),
        })))
    }

    fn seen(&self) -> Vec<Raw> {
        self.0.borrow().seen.clone()
    }

    fn client(&self) -> Client<FakeNet, IpOnly> {
        Client::new(self.clone(), IpOnly, Pool::new(b"test"), || None)
    }
}

/// Eén verbinding: verzamelt het verzoek, en geeft bij de eerste lees het antwoord.
struct Pipe {
    server: Rc<RefCell<Server>>,
    ip: [u8; 4],
    port: u16,
    req: Vec<u8>,
    resp: Option<Vec<u8>>,
    at: usize,
}

impl Connect for FakeNet {
    type Conn = Pipe;

    async fn connect(&mut self, ip: [u8; 4], port: u16) -> Result<Pipe, String> {
        Ok(Pipe {
            server: self.0.clone(),
            ip,
            port,
            req: Vec::new(),
            resp: None,
            at: 0,
        })
    }
}

/// Leest een compleet verzoek uit `buf`, of `None` als het nog niet af is.
fn parse(buf: &[u8], ip: [u8; 4], port: u16) -> Option<Raw> {
    let text = core::str::from_utf8(buf).ok()?;
    let (head, rest) = text.split_once("\r\n\r\n")?;
    let mut lines = head.split("\r\n");
    let mut first = lines.next()?.split(' ');
    let method = String::from(first.next()?);
    let target = String::from(first.next()?);
    let headers: Vec<(String, String)> = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (String::from(k.trim()), String::from(v.trim())))
        .collect();
    let len = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("Content-Length"))
        .map_or(0, |(_, v)| v.parse::<usize>().unwrap());
    if rest.len() < len {
        return None;
    }
    Some(Raw {
        ip,
        port,
        method,
        target,
        headers,
        body: rest.as_bytes()[..len].to_vec(),
    })
}

impl AsyncRead for Pipe {
    fn poll_read(&mut self, _: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize, IoError>> {
        if self.resp.is_none() {
            let Some(raw) = parse(&self.req, self.ip, self.port) else {
                return Poll::Ready(Err(IoError::Closed));
            };
            let mut s = self.server.borrow_mut();
            let (status, headers, body) = (s.handler)(&raw);
            s.seen.push(raw);
            let mut out = format!("HTTP/1.1 {status} X\r\nContent-Length: {}\r\n", body.len());
            for (k, v) in headers {
                out.push_str(&format!("{k}: {v}\r\n"));
            }
            out.push_str("\r\n");
            let mut bytes = out.into_bytes();
            bytes.extend_from_slice(&body);
            self.resp = Some(bytes);
        }
        let resp = self.resp.as_ref().unwrap();
        let n = (resp.len() - self.at).min(buf.len());
        buf[..n].copy_from_slice(&resp[self.at..self.at + n]);
        self.at += n;
        Poll::Ready(Ok(n))
    }
}

impl AsyncWrite for Pipe {
    fn poll_write(&mut self, _: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, IoError>> {
        self.req.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }
}

impl Close for Pipe {
    fn poll_close(&mut self, _: &mut Context<'_>) -> Poll<Result<(), IoError>> {
        Poll::Ready(Ok(()))
    }
}

/// Een hoplockserver in het geheugen: GET, PUT en DELETE met If-Match en
/// If-None-Match, de ETag een teller, één API-sleutel.
fn hoplockserver(api_key: &'static str) -> FakeNet {
    let mut store: BTreeMap<String, (Vec<u8>, String)> = BTreeMap::new();
    let mut next = 0u64;
    FakeNet::new(move |r| {
        if r.header("X-API-Key") != Some(api_key) {
            return (401, vec![], b"unauthorized".to_vec());
        }
        let key = String::from(r.path().trim_start_matches('/'));
        let current = store.get(&key).map(|(_, e)| e.clone());
        match r.method.as_str() {
            "GET" => match store.get(&key) {
                Some((b, e)) => (200, vec![("ETag", e.clone())], b.clone()),
                None => (404, vec![], vec![]),
            },
            "PUT" => {
                let ok = match (r.header("If-None-Match"), r.header("If-Match")) {
                    (Some("*"), _) => current.is_none(),
                    (_, Some(m)) => current.as_deref() == Some(m),
                    _ => true,
                };
                if !ok {
                    return (412, vec![], vec![]);
                }
                next += 1;
                let etag = format!("\"e{next}\"");
                store.insert(key, (r.body.clone(), etag.clone()));
                (200, vec![("ETag", etag)], vec![])
            }
            "DELETE" => {
                if current.as_deref() != r.header("If-Match") {
                    return (412, vec![], vec![]);
                }
                store.remove(&key);
                (204, vec![], vec![])
            }
            _ => (405, vec![], vec![]),
        }
    })
}

fn hoplock_cfg() -> ClusterConfig {
    ClusterConfig::from_env(
        env(&[
            (lock::ENV_LOCK_URL, "http://10.0.2.2:8090"),
            (lock::ENV_LOCK_APIKEY, "lock-secret"),
        ]),
        "c1",
        None,
    )
    .unwrap()
    .unwrap()
}

fn wall_secs() -> u64 {
    1_790_640_000
}

// ---- De config ----

#[test]
fn the_lock_comes_from_the_env_and_s3_wins() {
    // Zonder lock: standalone.
    assert_eq!(ClusterConfig::from_env(env(&[]), "c1", None), Ok(None));
    // Een hoplockserver, met de sleutels van de host als standaard.
    let c = hoplock_cfg();
    assert_eq!(
        c.lock,
        LockKind::Hoplock {
            url: String::from("http://10.0.2.2:8090"),
            api_key: String::from("lock-secret"),
        }
    );
    assert_eq!(c.lease_key, "leases/c1");
    assert_eq!(c.state_key, "state/c1");
    assert_eq!(c.ttl_ms, lock::DEFAULT_LEASE_TTL_MS);
    assert!(!format!("{c:?}").contains("lock-secret"), "{c:?}");
    // Een eigen lease-sleutel en TTL.
    let c = ClusterConfig::from_env(
        env(&[
            (lock::ENV_LOCK_URL, "http://lock:8090"),
            (lock::ENV_LOCK_KEY, "clusters/c1/lease.json"),
            (lock::ENV_LEASE_TTL, "45"),
        ]),
        "c1",
        None,
    )
    .unwrap()
    .unwrap();
    assert_eq!(c.lease_key, "clusters/c1/lease.json");
    assert_eq!(c.ttl_ms, 45_000);
    // S3 alleen is geen cluster: het is ook de object-store van de apps.
    let s3 = S3Config {
        endpoint: String::from("https://s3.example.com"),
        bucket: String::from("hop"),
        region: String::new(),
        key: String::from("AKID"),
        secret: String::from("s3cr3t"),
        path_style: true,
    };
    assert_eq!(ClusterConfig::from_env(env(&[]), "c1", Some(&s3)), Ok(None));
    // De S3-lock met zijn type: lease en staat in dezelfde bucket.
    let c = ClusterConfig::from_env(env(&[(lock::ENV_LOCK_TYPE, "s3")]), "c1", Some(&s3))
        .unwrap()
        .unwrap();
    assert!(matches!(
        (&c.lock, &c.state),
        (LockKind::S3(_), LockKind::S3(_))
    ));
    assert_eq!(c.label(), "s3 (https://s3.example.com/hop)");
    // Een hoplockserver naast een bruikbare S3-sectie: de lease op de server,
    // de staat in de bucket, zoals `discovery::state_store_for` op de host.
    let c = ClusterConfig::from_env(
        env(&[(lock::ENV_LOCK_URL, "http://lock:8090")]),
        "c1",
        Some(&s3),
    )
    .unwrap()
    .unwrap();
    assert!(matches!(
        (&c.lock, &c.state),
        (LockKind::Hoplock { .. }, LockKind::S3(_))
    ));
    // `mem` is standalone; een onbekend type, of s3 zonder bucket, weigert.
    assert_eq!(
        ClusterConfig::from_env(
            env(&[
                (lock::ENV_LOCK_TYPE, "mem"),
                (lock::ENV_LOCK_URL, "http://lock")
            ]),
            "c1",
            None
        ),
        Ok(None)
    );
    for kind in ["etcd", "s3"] {
        let got = ClusterConfig::from_env(
            |k| (k == lock::ENV_LOCK_TYPE).then(|| String::from(kind)),
            "c1",
            None,
        );
        assert!(
            matches!(
                got,
                Err(BootError::Bad {
                    var: lock::ENV_LOCK_TYPE,
                    ..
                })
            ),
            "{kind}: {got:?}"
        );
    }
    // Een URL die geen URL is, en een lease onder de tik van de verkiezing.
    assert!(matches!(
        ClusterConfig::from_env(env(&[(lock::ENV_LOCK_URL, "lock:8090")]), "c1", None),
        Err(BootError::Bad {
            var: lock::ENV_LOCK_URL,
            ..
        })
    ));
    assert!(matches!(
        ClusterConfig::from_env(
            env(&[
                (lock::ENV_LOCK_URL, "http://lock"),
                (lock::ENV_LEASE_TTL, "9")
            ]),
            "c1",
            None
        ),
        Err(BootError::Bad {
            var: lock::ENV_LEASE_TTL,
            ..
        })
    ));
}

fn boot_cfg() -> BootConfig {
    BootConfig::from_env(
        env(&[
            ("HOPOS_APIKEY", "test-key"),
            ("HOPOS_NODE", "n1"),
            ("HOPOS_NODE_IP", "10.0.0.5"),
            ("HOPOS_CORES", "4"),
            ("HOPOS_MEMORY", "1073741824"),
        ]),
        2,
        "10.100.0.2",
    )
    .unwrap()
}

#[test]
fn advertise_replaces_the_address_the_cluster_sees() {
    let cfg = boot_cfg();
    assert_eq!(lock::advertised(&cfg, env(&[])).unwrap(), cfg);
    let a = lock::advertised(&cfg, env(&[(lock::ENV_ADVERTISE, "127.0.0.1:18080")])).unwrap();
    assert_eq!(
        (a.node_ip.as_str(), a.port, a.leader_port()),
        ("127.0.0.1", 18080, 19080)
    );
    let a = lock::advertised(&cfg, env(&[(lock::ENV_ADVERTISE, "192.168.1.7")])).unwrap();
    assert_eq!((a.node_ip.as_str(), a.port), ("192.168.1.7", 8080));
    for bad in ["1.2.3.4:x", ":80", "1.2.3.4:65000"] {
        let got = lock::advertised(&cfg, |k| {
            (k == lock::ENV_ADVERTISE).then(|| String::from(bad))
        });
        assert!(matches!(got, Err(BootError::Bad { .. })), "{bad}");
    }
}

#[test]
fn the_time_server_comes_from_the_env() {
    use crate::sntp::{PORT, SERVER, server_from};
    assert_eq!(server_from(None), (String::from(SERVER), PORT));
    assert_eq!(server_from(Some(" ")), (String::from(SERVER), PORT));
    assert_eq!(
        server_from(Some("10.0.2.2:12345")),
        (String::from("10.0.2.2"), 12345)
    );
    assert_eq!(
        server_from(Some("ntp.lan")),
        (String::from("ntp.lan"), PORT)
    );
    assert_eq!(
        server_from(Some("ntp.lan:0")),
        (String::from("ntp.lan:0"), PORT)
    );
}

// ---- De lease en de staat ----

#[test]
fn the_hoplock_lease_speaks_the_cas_protocol_of_the_host() {
    let net = hoplockserver("lock-secret");
    let cfg = hoplock_cfg();
    let mut a = lock::open_lease(&cfg, net.client(), wall_secs);
    let mut b = lock::open_lease(&cfg, net.client(), wall_secs);
    let mut da = Discovery::new(String::from("10.0.0.5:9080"), cfg.ttl_ms);
    let mut db = Discovery::new(String::from("10.0.0.6:9080"), cfg.ttl_ms);
    let now = 1_000_000;

    // Niemand leidt: node a maakt de lease aan (If-None-Match: *).
    assert_eq!(block_on(lease::leader_state(&mut a, now)), (None, true));
    assert_eq!(block_on(lease::claim(&mut da, &mut a, now)), Ok(()));
    let put = net.seen().into_iter().find(|r| r.method == "PUT").unwrap();
    assert_eq!(put.target, "/leases/c1");
    assert_eq!((put.ip, put.port), ([10, 0, 2, 2], 8090));
    assert_eq!(put.header("If-None-Match"), Some("*"));
    // De bytes van de host: dezelfde lease als `store::HoplockLease` schrijft.
    let state = discovery::wire::decode(&put.body).unwrap();
    assert_eq!(state.owner, "10.0.0.5:9080");
    assert_eq!(state.generation, 1);

    // Node b ziet node a als leider en krijgt de lease niet.
    assert_eq!(
        block_on(lease::leader_state(&mut b, now + 1)),
        (Some(String::from("10.0.0.5:9080")), true)
    );
    assert_eq!(
        block_on(lease::claim(&mut db, &mut b, now + 1)),
        Err(discovery::Error::LeaseHeld)
    );

    // Een renew is één PUT met If-Match op de handle, zonder lees.
    let before = net.seen().len();
    assert_eq!(
        block_on(lease::renew(&mut da, &mut a, now + 5_000)),
        (true, false)
    );
    let seen = net.seen();
    assert_eq!(seen.len(), before + 1);
    assert_eq!(seen.last().unwrap().header("If-Match"), Some("\"e1\""));

    // Na de TTL neemt node b over (generatie + 1), en node a is verdrongen.
    let later = now + 5_000 + cfg.ttl_ms + 1;
    assert_eq!(block_on(lease::claim(&mut db, &mut b, later)), Ok(()));
    let (got, _) = block_on(b.read()).unwrap();
    assert_eq!((got.owner.as_str(), got.generation), ("10.0.0.6:9080", 2));
    assert_eq!(
        block_on(lease::renew(&mut da, &mut a, later + 1)),
        (false, true)
    );

    // Loslaten verwijdert de lease met If-Match; daarna is er niemand.
    block_on(lease::release(&mut db, &mut b));
    assert_eq!(block_on(a.read()), Err(discovery::Error::NoLease));
}

#[test]
fn a_refused_lock_is_unreachable_and_says_why_without_the_key() {
    let net = hoplockserver("the-right-key");
    let mut l = lock::open_lease(&hoplock_cfg(), net.client(), wall_secs);
    assert_eq!(block_on(l.read()), Err(discovery::Error::Unreachable));
    let why = l.last_error().unwrap();
    assert!(why.contains("status 401"), "{why}");
    assert!(!why.contains("lock-secret"), "{why}");
}

#[test]
fn the_cluster_state_lives_next_to_the_lease() {
    let net = hoplockserver("lock-secret");
    let mut st = lock::open_state(&hoplock_cfg(), net.client(), wall_secs);
    assert_eq!(block_on(st.load()), Ok(None));
    assert_eq!(block_on(st.save(b"{\"jobs\":[]}")), Ok(()));
    assert_eq!(block_on(st.load()), Ok(Some(b"{\"jobs\":[]}".to_vec())));
    // Onvoorwaardelijk: geen If-Match op de staat.
    let put = net.seen().into_iter().find(|r| r.method == "PUT").unwrap();
    assert_eq!(put.target, "/state/c1");
    assert_eq!(put.header("If-Match"), None);
    assert_eq!(st.describe(), "hoplockserver http://10.0.2.2:8090/state/c1");
}

#[test]
fn the_s3_lease_signs_and_retries_a_bare_etag() {
    // Hetzner en Ceph: een geciteerde ETag terug, maar If-Match vergelijkt kaal.
    let net = FakeNet::new(|r| {
        assert!(
            r.header("Authorization")
                .is_some_and(|a| a.starts_with("AWS4-HMAC-SHA256 Credential=AKID/")),
            "{r:?}"
        );
        assert_eq!(r.path(), "/hop/leases/c1", "path-style");
        match (
            r.method.as_str(),
            r.header("If-None-Match"),
            r.header("If-Match"),
        ) {
            ("GET", _, _) => (
                404,
                vec![],
                b"<Error><Code>NoSuchKey</Code></Error>".to_vec(),
            ),
            ("PUT", Some("*"), _) => (200, vec![("ETag", String::from("\"e1\""))], vec![]),
            ("PUT", _, Some("\"e1\"")) => (412, vec![], vec![]),
            ("PUT", _, Some("e1")) => (200, vec![("ETag", String::from("\"e2\""))], vec![]),
            _ => (500, vec![], vec![]),
        }
    });
    let s3 = S3Config {
        endpoint: String::from("http://10.0.2.2:9000"),
        bucket: String::from("hop"),
        region: String::from("auto"),
        key: String::from("AKID"),
        secret: String::from("s3cr3t"),
        path_style: true,
    };
    let cfg = ClusterConfig::from_env(env(&[(lock::ENV_LOCK_TYPE, "s3")]), "c1", Some(&s3))
        .unwrap()
        .unwrap();
    let mut l = lock::open_lease(&cfg, net.client(), wall_secs);
    assert_eq!(block_on(l.read()), Err(discovery::Error::NoLease));
    let state = LeaseState {
        generation: 1,
        expires_at: 5_000,
        owner: String::from("10.0.0.5:9080"),
    };
    assert_eq!(block_on(l.write("", &state)).as_deref(), Ok("\"e1\""));
    assert_eq!(block_on(l.write("\"e1\"", &state)).as_deref(), Ok("\"e2\""));
    // Het geheim staat in geen enkele kop op de draad.
    for r in net.seen() {
        assert!(r.headers.iter().all(|(_, v)| !v.contains("s3cr3t")));
    }
}

// ---- De relay van de leader ----

fn agent(id: &str) -> types::Agent {
    types::Agent {
        id: String::from(id),
        endpoint: format!("http://{id}:8080"),
        ..types::Agent::default()
    }
}

fn job(name: &str) -> types::Job {
    types::Job::from_value(
        &types::json::parse_str(&format!(r#"{{"name":"{name}","command":"sleep 1"}}"#)).unwrap(),
        false,
    )
    .unwrap()
}

#[test]
fn the_relay_answers_now_and_learns_from_the_agent() {
    let mut r = Relay::new();
    let n2 = agent("n2");
    // Nog niets geleerd: aangenomen, en de aanroep in de rij.
    assert_eq!(r.run(T0, &n2, &job("web"), false), RunReply::Accepted);
    let out = r.take();
    assert!(
        matches!(&out[..], [Remote::Run { agent, job, replace: false, .. }] if agent == "n2" && job == "web")
    );
    // De agent weigerde: tot de TTL komt die weigering meteen terug.
    r.refuse(T0, "n2", "web", RunReply::NoCapacity);
    assert_eq!(
        r.run(T0 + SECOND, &n2, &job("web"), false),
        RunReply::NoCapacity
    );
    assert!(r.take().is_empty());
    assert_eq!(
        r.run(T0 + crate::relay::REFUSAL_TTL + 1, &n2, &job("web"), false),
        RunReply::Accepted
    );
    // Stoppen en verwijderen gaan in de rij; verwijderen wist de weigeringen.
    r.refuse(T0, "n2", "web", RunReply::AffinityMismatch);
    assert!(r.stop_job(&n2, "web"));
    r.delete_job(&n2, "web");
    assert_eq!(r.run(T0, &n2, &job("web"), false), RunReply::Accepted);
    // De takenlijst: één vraag per agent tegelijk, en het antwoord uit de boeken.
    r.take();
    r.refresh([n2.clone(), agent("n3")].iter());
    r.refresh([n2.clone()].iter());
    assert_eq!(r.take().len(), 2);
    assert_eq!(r.tasks(&n2), None);
    r.on_tasks("n2", Some(Vec::new()));
    assert_eq!(r.tasks(&n2), Some(Vec::new()));
    // Een volle rij: een `run` die niet past, komt terug als niet verstuurd.
    let q: &'static DispatchQueue = Box::leak(Box::new(Mailbox::new()));
    for _ in 0..crate::mail::DISPATCH_Q + 1 {
        r.run(T0, &n2, &job("big"), false);
    }
    let unsent = r.flush(q);
    assert_eq!(unsent, vec![(String::from("n2"), String::from("big"))]);
    assert_eq!(r.take_dropped(), 1);
}

// ---- De node in een cluster ----

/// Geen artifacts nodig: de jobs van deze tests zijn nooit voor de eigen kern.
struct NoImages;

impl Images for NoImages {
    async fn fetch<K: Sink>(&mut self, url: &str, _sink: &mut K) -> Result<(), String> {
        Err(format!("404 {url}"))
    }
}

type TestNode = Node<
    KernSys<applib::sys::Client<hopos_runner::fake::FakeDial, hopos_runner::fake::NeverTimer>>,
    NoImages,
>;

/// De wandklok van de elector in deze tests (ms); elke test zet hem niet
/// terug, want ze lezen hem alleen relatief.
static WALL: AtomicU64 = AtomicU64::new(1_790_640_000_000);

fn wall_ms() -> u64 {
    WALL.load(Relaxed)
}

/// De rijen van één node, elk met zijn eigen `'static`.
struct Queues {
    lease: &'static LeaseQueue,
    link: &'static LinkQueue,
    state: &'static StateQueue,
    dispatch: &'static DispatchQueue,
    _inbox: &'static Inbox,
}

fn queues() -> Queues {
    Queues {
        lease: Box::leak(Box::new(Mailbox::new())),
        link: Box::leak(Box::new(Mailbox::new())),
        state: Box::leak(Box::new(Mailbox::new())),
        dispatch: Box::leak(Box::new(Mailbox::new())),
        _inbox: Box::leak(Box::new(Mailbox::new())),
    }
}

fn clustered(clock: fn() -> bool) -> (TestNode, Queues) {
    clustered_on(&FakeKern::new(4), clock)
}

fn clustered_on(k: &FakeKern, clock: fn() -> bool) -> (TestNode, Queues) {
    let sys = KernSys::new(k.client(), 4);
    let q = queues();
    let parts = ClusterParts {
        cfg: hoplock_cfg(),
        clock_ok: clock,
        lease: q.lease,
        link: q.link,
        state: q.state,
        dispatch: q.dispatch,
        wall_ms,
        init_jobs: None,
        node_dead: 20 * SECOND,
    };
    (
        Node::new_clustered(&boot_cfg(), sys, NoImages, T0, parts),
        q,
    )
}

fn drain<T, const N: usize>(q: &Mailbox<T, N>) -> Vec<T> {
    core::iter::from_fn(|| q.try_recv()).collect()
}

fn signed_body(method: Method, target: &str, body: &str) -> Request {
    let path = target.split('?').next().unwrap();
    let sig = auth::sign(KEY, method.as_str(), path, body.as_bytes());
    let mut r = Request::new(method, target, body.as_bytes());
    r.headers.push((
        auth::AUTH_HEADER.into(),
        String::from_utf8(sig.to_vec()).unwrap(),
    ));
    r
}

fn routed(n: &mut TestNode, port: Port, req: &Request, now: u64) -> Routed {
    block_on(n.handle_routed(port, req, now))
}

fn status(r: &Routed) -> u16 {
    match r {
        Routed::Reply(hop_http::Reply::Plain(resp)) => resp.status,
        other => panic!("geen gewoon antwoord: {other:?}"),
    }
}

fn said(n: &mut TestNode, marker: &str) -> bool {
    n.take_lines().iter().any(|l| l.contains(marker))
}

#[test]
fn without_a_synced_clock_the_node_does_not_join() {
    let (mut n, q) = clustered(|| false);
    n.cluster_boot(T0);
    assert!(said(&mut n, "HOP_CLUSTER_NO_CLOCK"));
    block_on(n.tick(T0 + 11 * SECOND));
    assert!(
        drain(q.lease).is_empty(),
        "geen claim en geen lees zonder klok"
    );
    // De leader-API is dicht, en zegt waarom.
    let r = routed(
        &mut n,
        Port::Leader,
        &signed_body(Method::Get, "/v1/jobs", ""),
        T0,
    );
    assert_eq!(status(&r), 503);
}

#[test]
fn a_free_lock_makes_the_node_leader_with_the_committed_state() {
    let (mut n, q) = clustered(|| true);
    n.cluster_boot(T0);
    assert!(said(&mut n, "HOP_CLUSTER_JOIN"));
    assert_eq!(drain(q.lease), vec![LeaseOp::Claim]);

    // De boot-claim is raak: de staat-taak leest eerst de gecommitte staat,
    // en tot die er is, is de leader-API dicht.
    n.on_mail(Mail::Lease(LeaseReply::Claimed(true)), T0);
    assert_eq!(drain(q.state), vec![StateOp::Load]);
    assert!(said(&mut n, "HOP_LEADER_LOADING"));
    let jobs = signed_body(Method::Get, "/v1/jobs", "");
    let r = routed(&mut n, Port::Leader, &jobs, T0);
    assert_eq!(status(&r), 503);

    // De staat is er (leeg, een schone cluster): leider.
    n.on_mail(Mail::Loaded(Ok(None)), T0);
    assert!(said(&mut n, "HOP_LEADER leader=10.0.0.5:9080"));
    assert_eq!(status(&routed(&mut n, Port::Leader, &jobs, T0)), 200);

    // Een agent op een andere node registreert over het LAN.
    let reg = r#"{"id":"n2","endpoint":"http://10.0.0.6:8080","version":"3.0.0","placed":{}}"#;
    let r = routed(
        &mut n,
        Port::Leader,
        &signed_body(Method::Post, "/v1/agents", reg),
        T0 + SECOND,
    );
    assert_eq!(status(&r), 200);

    // Een job voor n2: in de settle-periode alleen bewaard, daarna geplaatst
    // via de relay: de dispatch-taak krijgt de `POST /run`.
    let web = r#"{"name":"web","command":"sleep 1","affinity":{"node.id":"n2"}}"#;
    let r = routed(
        &mut n,
        Port::Leader,
        &signed_body(Method::Post, "/v1/jobs", web),
        T0 + SECOND,
    );
    assert!(matches!(status(&r), 200..=202), "{r:?}");
    let hb = r#"{"id":"n2","endpoint":"http://10.0.0.6:8080","version":"3.0.0","temp_milli_c":0}"#;
    routed(
        &mut n,
        Port::Leader,
        &signed_body(Method::Post, "/v1/heartbeat", hb),
        T0 + 21 * SECOND,
    );
    block_on(n.tick(T0 + 21 * SECOND));
    let calls = drain(q.dispatch);
    assert!(
        calls
            .iter()
            .any(|c| matches!(c, Remote::Run { agent, job, .. } if agent == "n2" && job == "web")),
        "{calls:?}"
    );
    // De leader vraagt ook de takenlijst van n2 (voor zijn boeken).
    assert!(
        calls
            .iter()
            .any(|c| matches!(c, Remote::Tasks { agent, .. } if agent == "n2"))
    );
    // De snapshot gaat naar de staat-taak (na het debounce-venster van de
    // leader: de eerste tik na een mutatie start het, de volgende schrijft).
    block_on(n.tick(T0 + 23 * SECOND));
    let saves = drain(q.state);
    let Some(StateOp::Save(snap)) = saves.last() else {
        panic!("geen snapshot: {saves:?}");
    };
    assert!(String::from_utf8_lossy(snap).contains("\"web\""));
    drain(q.dispatch);

    // n2 weigerde (vol): de plaatsing is afgeboekt, en de volgende reconcile
    // stuurt hem niet meteen weer naar n2.
    n.on_mail(
        Mail::Ran {
            agent: String::from("n2"),
            job: String::from("web"),
            reply: RunReply::NoCapacity,
        },
        T0 + 22 * SECOND,
    );
    assert!(said(&mut n, "HOP_RELAY_REFUSED"));
    assert!(
        !drain(q.dispatch)
            .iter()
            .any(|c| matches!(c, Remote::Run { .. }))
    );

    // /v1/tasks: de eigen taken in-proces, die van n2 vraagt de verbindingstaak.
    let r = routed(
        &mut n,
        Port::Leader,
        &signed_body(Method::Get, "/v1/tasks", ""),
        T0 + 22 * SECOND,
    );
    let Routed::Forward(Forward::Tasks {
        agents,
        auth,
        scope,
    }) = r
    else {
        panic!("geen rondgang: {r:?}");
    };
    assert!(matches!(scope, TasksScope::All { .. }));
    assert!(auth.is_some());
    assert!(
        agents
            .iter()
            .any(|(id, s)| id == "n1" && matches!(s, Source::Known(Some(_))))
    );
    assert!(
        agents
            .iter()
            .any(|(id, s)| id == "n2" && *s == Source::Ask(String::from("http://10.0.0.6:8080")))
    );

    // De log van een taak op n2: een ondertekende doorgifte, als stroom geteld.
    let logs = signed_body(Method::Get, "/v1/agents/n2/logs/t1/stdout?follow=1", "");
    let r = routed(&mut n, Port::Leader, &logs, T0 + 22 * SECOND);
    let Routed::Forward(Forward::Agent {
        endpoint,
        path,
        auth,
        stream,
    }) = r
    else {
        panic!("geen doorgifte: {r:?}");
    };
    assert_eq!(endpoint, "http://10.0.0.6:8080");
    assert!(path.starts_with("/logs/t1/stdout"), "{path}");
    assert!(stream);
    assert_eq!(
        auth.as_deref(),
        Some(
            core::str::from_utf8(&auth::sign(
                KEY,
                "GET",
                path.split('?').next().unwrap(),
                b""
            ))
            .unwrap()
        )
    );
}

#[test]
fn a_held_lock_makes_the_node_follow_and_register_over_the_lan() {
    let (mut n, q) = clustered(|| true);
    n.cluster_boot(T0);
    drain(q.lease);
    n.on_mail(Mail::Lease(LeaseReply::Claimed(false)), T0);
    assert!(drain(q.state).is_empty(), "geen leider zonder lease");

    // De eerste tik van de verkiezing vraagt wie leidt.
    block_on(n.tick(T0 + 11 * SECOND));
    assert_eq!(drain(q.lease), vec![LeaseOp::Read]);
    n.on_mail(
        Mail::Lease(LeaseReply::Read {
            leader: Some(String::from("10.0.0.9:9080")),
            ok: true,
        }),
        T0 + 11 * SECOND,
    );
    // De volgende tik registreert bij die leader, over de link-taak.
    block_on(n.tick(T0 + 22 * SECOND));
    let jobs = drain(q.link);
    let [LinkJob::Election { req, url, body }] = &jobs[..] else {
        panic!("geen register: {jobs:?}");
    };
    assert_eq!(url, "http://10.0.0.9:9080/v1/agents");
    assert!(body.contains(r#""id":"n1""#) && body.contains(r#""endpoint":"http://10.0.0.5:8080""#));
    n.on_mail(
        Mail::Link {
            req: req.clone(),
            result: Ok(()),
        },
        T0 + 22 * SECOND,
    );

    // /v1/* op de agent-poort gaat nu ongewijzigd naar de leader.
    let list = signed_body(Method::Get, "/v1/jobs?x=1", "");
    let r = routed(&mut n, Port::Agent, &list, T0 + 23 * SECOND);
    let Routed::Forward(Forward::Leader { addr, req, stream }) = r else {
        panic!("geen doorgifte naar de leader: {r:?}");
    };
    assert_eq!(addr, "10.0.0.9:9080");
    assert_eq!(req.query, "x=1");
    assert!(!stream);
    // De leader-API van deze node zegt wie wel leidt.
    let r = routed(&mut n, Port::Leader, &list, T0 + 23 * SECOND);
    let Routed::Reply(hop_http::Reply::Plain(resp)) = r else {
        panic!("{r:?}");
    };
    assert_eq!(resp.status, 503);
    assert!(String::from_utf8_lossy(&resp.body).contains("10.0.0.9:9080"));

    // De volgende tik is een heartbeat; een leader die ons vergat (404)
    // krijgt een nieuwe registratie.
    block_on(n.tick(T0 + 33 * SECOND));
    let jobs = drain(q.link);
    let [LinkJob::Election { req, url, .. }] = &jobs[..] else {
        panic!("geen heartbeat: {jobs:?}");
    };
    assert_eq!(url, "http://10.0.0.9:9080/v1/heartbeat");
    assert!(matches!(req, LinkRequest::Heartbeat { .. }));
    n.on_mail(
        Mail::Link {
            req: req.clone(),
            result: Err(LinkError::NotRegistered),
        },
        T0 + 33 * SECOND,
    );
    block_on(n.tick(T0 + 44 * SECOND));
    assert!(matches!(
        &drain(q.link)[..],
        [LinkJob::Election {
            req: LinkRequest::Register { .. },
            ..
        }]
    ));
}

/// De kern in slot 0 en Hop in zijn eigen slot (2 in [`boot_cfg`]) zoals
/// de nep-kern ze meldt op kernklok `at_s` (seconden, vanaf 100): de kern
/// 80 % idle (dus 20 % cpu), 3 MiB van 64 MiB RAM zonder partitie; Hop 90 %
/// idle, 4 MiB in een partitie van 32 MiB.
fn system_slots(k: &FakeKern, at_s: u64) {
    let span = (at_s - 100) * SECOND;
    let slot = |idle_ns, mem_sys, ram_size, partition| SlotInfo {
        state: 2,
        core_on: 1,
        cores: 1,
        at_ns: at_s * SECOND,
        idle_ns,
        mem_sys,
        ram_size,
        partition,
        ..SlotInfo::default()
    };
    let mut s = k.0.borrow_mut();
    s.system.insert(0, slot(span / 5 * 4, 3 << 20, 64 << 20, 0));
    s.system
        .insert(2, slot(span / 10 * 9, 4 << 20, 1 << 20, 32 << 20));
}

fn json(body: &[u8]) -> types::json::Value {
    types::json::parse(body).unwrap()
}

fn num(v: &types::json::Value, key: &str) -> f64 {
    let f = v.as_object().unwrap().get(key);
    types::de::float(f.unwrap_or_else(|| panic!("geen {key} in {v:?}")), key).unwrap()
}

#[test]
fn the_heartbeat_to_the_leader_carries_the_kernel_and_hop() {
    let k = FakeKern::new(4);
    system_slots(&k, 100);
    let (mut n, q) = clustered_on(&k, || true);
    n.cluster_boot(T0);
    drain(q.lease);
    n.on_mail(Mail::Lease(LeaseReply::Claimed(false)), T0);
    // Elke tik meet (het monitor-interval is 5 s); de eerste stand geeft
    // geheugen maar nog geen cpu.
    block_on(n.tick(T0 + 11 * SECOND));
    drain(q.lease);
    n.on_mail(
        Mail::Lease(LeaseReply::Read {
            leader: Some(String::from("10.0.0.9:9080")),
            ok: true,
        }),
        T0 + 11 * SECOND,
    );
    system_slots(&k, 105);
    block_on(n.tick(T0 + 22 * SECOND));
    let [LinkJob::Election { req, .. }] = &drain(q.link)[..] else {
        panic!("geen register");
    };
    n.on_mail(
        Mail::Link {
            req: req.clone(),
            result: Ok(()),
        },
        T0 + 22 * SECOND,
    );
    system_slots(&k, 110);
    block_on(n.tick(T0 + 33 * SECOND));
    let jobs = drain(q.link);
    let [LinkJob::Election { url, body, .. }] = &jobs[..] else {
        panic!("geen heartbeat: {jobs:?}");
    };
    assert_eq!(url, "http://10.0.0.9:9080/v1/heartbeat");
    let v = json(body.as_bytes());
    assert_eq!(
        types::json::Value::as_str(v.as_object().unwrap().get("id").unwrap()),
        Some("n1")
    );
    assert_eq!(num(&v, "kern_cpu_percent"), 20.0);
    assert_eq!(num(&v, "kern_mem_bytes"), f64::from(3u32 << 20));
    assert_eq!(num(&v, "kern_ram_bytes"), f64::from(64u32 << 20));
    assert_eq!(num(&v, "hop_cpu_percent"), 10.0);
    assert_eq!(num(&v, "hop_mem_bytes"), f64::from(4u32 << 20));
    // De partitie van Hop is zijn limiet, niet de RAM-maat die hij meldt.
    assert_eq!(num(&v, "hop_ram_bytes"), f64::from(32u32 << 20));
    // Zonder kern-temperatuur (geen applib op de host) geen temp_milli_c.
    assert!(!body.contains("temp_milli_c"), "{body}");
}

#[test]
fn a_kernel_without_slot_0_leaves_the_kernel_out() {
    let k = FakeKern::new(4);
    system_slots(&k, 100);
    // Een kern van vóór slot 0: de stand van slot 0 is leeg.
    k.0.borrow_mut().system.remove(&0);
    let (mut n, q) = clustered_on(&k, || true);
    n.cluster_boot(T0);
    n.on_mail(Mail::Lease(LeaseReply::Claimed(true)), T0);
    n.on_mail(Mail::Loaded(Ok(None)), T0);
    drain(q.state);
    block_on(n.tick(T0 + SECOND));
    let agents = signed_body(Method::Get, "/v1/agents", "");
    let Routed::Reply(hop_http::Reply::Plain(r)) =
        routed(&mut n, Port::Leader, &agents, T0 + SECOND)
    else {
        panic!("geen antwoord");
    };
    let body = String::from_utf8(r.body).unwrap();
    assert!(!body.contains("kern_"), "{body}");
    assert!(body.contains(r#""hop_mem_bytes":4194304"#), "{body}");
}

#[test]
fn the_leader_shows_the_kernel_and_hop_of_every_agent() {
    let k = FakeKern::new(4);
    system_slots(&k, 100);
    let (mut n, q) = clustered_on(&k, || true);
    n.cluster_boot(T0);
    n.on_mail(Mail::Lease(LeaseReply::Claimed(true)), T0);
    n.on_mail(Mail::Loaded(Ok(None)), T0);
    drain(q.state);
    // Twee standen, 5 s uit elkaar: de eigen heartbeat draagt de cpu.
    block_on(n.tick(T0 + SECOND));
    system_slots(&k, 105);
    block_on(n.tick(T0 + 6 * SECOND));

    // n2 is een nieuwe Hop, n3 een oude zonder de velden.
    for (id, ip) in [("n2", "10.0.0.6"), ("n3", "10.0.0.7")] {
        let reg = format!(r#"{{"id":"{id}","endpoint":"http://{ip}:8080","placed":{{}}}}"#);
        let r = routed(
            &mut n,
            Port::Leader,
            &signed_body(Method::Post, "/v1/agents", &reg),
            T0 + 6 * SECOND,
        );
        assert_eq!(status(&r), 200);
    }
    let hb = r#"{"id":"n2","endpoint":"http://10.0.0.6:8080","version":"3.0.7","temp_milli_c":51000,"kern_cpu_percent":1.5,"kern_mem_bytes":2097152,"kern_ram_bytes":16777216,"hop_cpu_percent":3,"hop_mem_bytes":1048576,"hop_ram_bytes":8388608}"#;
    let r = routed(
        &mut n,
        Port::Leader,
        &signed_body(Method::Post, "/v1/heartbeat", hb),
        T0 + 7 * SECOND,
    );
    assert_eq!(status(&r), 200);
    let hb = r#"{"id":"n3","endpoint":"http://10.0.0.7:8080","version":"3.0.6","temp_milli_c":0}"#;
    let r = routed(
        &mut n,
        Port::Leader,
        &signed_body(Method::Post, "/v1/heartbeat", hb),
        T0 + 7 * SECOND,
    );
    assert_eq!(status(&r), 200);

    // /v1/agents: naast temp_milli_c de vier (en de noemers).
    let r = routed(
        &mut n,
        Port::Leader,
        &signed_body(Method::Get, "/v1/agents", ""),
        T0 + 7 * SECOND,
    );
    let Routed::Reply(hop_http::Reply::Plain(r)) = r else {
        panic!("geen antwoord: {r:?}");
    };
    let v = json(&r.body);
    let list = v.as_array().unwrap();
    let agent = |id: &str| {
        list.iter()
            .find(|a| a.as_object().unwrap().get("id").and_then(|v| v.as_str()) == Some(id))
            .unwrap()
            .clone()
    };
    let n1 = agent("n1");
    assert_eq!(num(&n1, "kern_cpu_percent"), 20.0);
    assert_eq!(num(&n1, "kern_mem_bytes"), f64::from(3u32 << 20));
    assert_eq!(num(&n1, "hop_cpu_percent"), 10.0);
    assert_eq!(num(&n1, "hop_mem_bytes"), f64::from(4u32 << 20));
    let n2 = agent("n2");
    assert_eq!(num(&n2, "temp_milli_c"), 51_000.0);
    assert_eq!(num(&n2, "kern_cpu_percent"), 1.5);
    assert_eq!(num(&n2, "hop_ram_bytes"), 8_388_608.0);
    let n3 = agent("n3");
    assert!(n3.as_object().unwrap().get("kern_mem_bytes").is_none());

    // /v1/tasks: de rondgang, en per agent die antwoordt zijn systeemtaken.
    let r = routed(
        &mut n,
        Port::Leader,
        &signed_body(Method::Get, "/v1/tasks", ""),
        T0 + 7 * SECOND,
    );
    let Routed::Forward(Forward::Tasks {
        agents,
        auth,
        scope,
    }) = r
    else {
        panic!("geen rondgang: {r:?}");
    };
    let net = FakeNet::new(|_| (200, vec![], b"[]".to_vec()));
    let resp = block_on(forward::tasks(
        &mut net.client(),
        agents,
        auth.as_deref(),
        &scope,
    ));
    assert_eq!(resp.status, 200);
    let v = json(&resp.body);
    let by = v.as_object().unwrap().get("tasks_by_agent").unwrap();
    let tasks = |id: &str| {
        by.as_object()
            .unwrap()
            .get(id)
            .unwrap()
            .as_array()
            .unwrap()
            .to_vec()
    };
    let n1 = tasks("n1");
    assert_eq!(n1.len(), 2, "{n1:?}");
    let t = |v: &types::json::Value| types::Task::from_value(v).unwrap();
    let (kern, hop) = (t(&n1[0]), t(&n1[1]));
    assert_eq!(
        (
            kern.job_name.as_str(),
            kern.driver.as_str(),
            kern.pid,
            kern.state
        ),
        ("kern", "hop", 0, types::TaskState::System)
    );
    assert_eq!((kern.cpu_percent, kern.mem_percent), (20.0, 4.6));
    assert_eq!(
        (hop.job_name.as_str(), hop.pid, hop.state),
        ("hop", 1, types::TaskState::System)
    );
    assert_eq!((hop.cpu_percent, hop.mem_percent), (10.0, 12.5));
    let n2 = tasks("n2");
    let (kern, hop) = (t(&n2[0]), t(&n2[1]));
    assert_eq!((kern.cpu_percent, kern.mem_percent), (1.5, 12.5));
    assert_eq!((hop.cpu_percent, hop.mem_percent), (3.0, 12.5));
    assert!(
        tasks("n3").is_empty(),
        "een oude agent heeft geen systeemtaken"
    );

    // En nergens in de boeken: geen plaatsing, en de status telt niets.
    let r = routed(
        &mut n,
        Port::Leader,
        &signed_body(Method::Get, "/v1/status", ""),
        T0 + 7 * SECOND,
    );
    let Routed::Reply(hop_http::Reply::Plain(r)) = r else {
        panic!("geen antwoord: {r:?}");
    };
    let v = json(&r.body);
    assert_eq!(num(&v, "total_placed"), 0.0);
    assert!(n.agent().tasks().next().is_none());
}

#[test]
fn a_displaced_leader_steps_down_and_forgets_its_calls() {
    let (mut n, q) = clustered(|| true);
    n.cluster_boot(T0);
    n.on_mail(Mail::Lease(LeaseReply::Claimed(true)), T0);
    n.on_mail(Mail::Loaded(Ok(None)), T0);
    assert!(said(&mut n, "HOP_LEADER"));
    drain(q.lease);
    // Een aanroep van de leader die nog in de rij staat.
    q.dispatch
        .try_send(Remote::Tasks {
            agent: String::from("n2"),
            endpoint: String::from("http://10.0.0.6:8080"),
        })
        .unwrap();
    // De renew zegt: iemand anders houdt de lease. De volgende tik treedt af.
    block_on(n.tick(T0 + 11 * SECOND));
    n.on_mail(
        Mail::Lease(LeaseReply::Renewed(false, true)),
        T0 + 11 * SECOND,
    );
    assert!(said(&mut n, "HOP_LEASE_DISPLACED"));
    block_on(n.tick(T0 + 22 * SECOND));
    assert!(said(&mut n, "HOP_LEADER_STOPPED"));
    assert!(
        drain(q.dispatch).is_empty(),
        "een ex-leader stuurt niets meer"
    );
    let r = routed(
        &mut n,
        Port::Leader,
        &signed_body(Method::Get, "/v1/jobs", ""),
        T0 + 22 * SECOND,
    );
    assert_eq!(status(&r), 503);
}

// ---- De doorgiftes ----

#[test]
fn a_forward_to_the_leader_carries_the_callers_signature() {
    let net = FakeNet::new(|r| {
        assert_eq!(r.header("X-Hop-Auth"), Some("sig"));
        assert_eq!(r.header("Content-Type"), Some("application/json"));
        (
            201,
            vec![("Content-Type", String::from("application/json"))],
            b"{\"ok\":true}".to_vec(),
        )
    });
    let mut req = Request::new(Method::Post, "/v1/jobs?dry=1", b"{\"name\":\"web\"}");
    req.headers
        .push((String::from("X-Hop-Auth"), String::from("sig")));
    req.headers.push((
        String::from("Content-Type"),
        String::from("application/json"),
    ));
    let resp = block_on(forward::to_leader(&mut net.client(), "10.0.0.9:9080", &req));
    assert_eq!(resp.status, 201);
    assert_eq!(resp.header("Content-Type"), Some("application/json"));
    assert_eq!(resp.body, b"{\"ok\":true}");
    let seen = net.seen();
    assert_eq!((seen[0].ip, seen[0].port), ([10, 0, 0, 9], 9080));
    assert_eq!(
        (seen[0].method.as_str(), seen[0].target.as_str()),
        ("POST", "/v1/jobs?dry=1")
    );
    assert_eq!(seen[0].body, b"{\"name\":\"web\"}");
}

#[test]
fn the_tasks_round_asks_the_other_nodes_and_skips_the_silent_ones() {
    let net = FakeNet::new(|r| match r.ip {
        [10, 0, 0, 6] => (
            200,
            vec![],
            br#"[{"id":"t2","job_name":"web","state":"running"}]"#.to_vec(),
        ),
        _ => (500, vec![], vec![]),
    });
    let agents = vec![
        (String::from("n1"), Source::Known(Some(Vec::new()))),
        (
            String::from("n2"),
            Source::Ask(String::from("http://10.0.0.6:8080")),
        ),
        (
            String::from("n3"),
            Source::Ask(String::from("http://10.0.0.7:8080")),
        ),
    ];
    let resp = block_on(forward::tasks(
        &mut net.client(),
        agents,
        Some("sig"),
        &TasksScope::All { agents: Vec::new() },
    ));
    assert_eq!(resp.status, 200);
    let body = String::from_utf8(resp.body).unwrap();
    assert!(body.contains("\"n1\"") && body.contains("\"t2\""), "{body}");
    assert!(
        !body.contains("\"n3\":["),
        "een agent die niet antwoordt, ontbreekt: {body}"
    );
    assert!(
        net.seen()
            .iter()
            .all(|r| r.header("X-Hop-Auth") == Some("sig") && r.path() == "/tasks")
    );
}

#[test]
fn a_failed_forward_is_a_loud_502() {
    #[derive(Clone)]
    struct Refused;
    impl Connect for Refused {
        type Conn = Pipe;
        async fn connect(&mut self, _ip: [u8; 4], _port: u16) -> Result<Pipe, String> {
            Err(String::from("connection refused"))
        }
    }
    let mut c = Client::new(Refused, IpOnly, Pool::new(b"t"), || None);
    let resp = block_on(forward::to_agent(
        &mut c,
        "http://10.0.0.6:8080",
        "/capacity",
        None,
    ));
    assert_eq!(resp.status, 502);
    let body = String::from_utf8(resp.body).unwrap();
    assert!(
        body.contains("10.0.0.6") && body.contains("connection refused"),
        "{body}"
    );
}
