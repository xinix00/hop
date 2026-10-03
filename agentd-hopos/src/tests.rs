//! De keten van HTTP-verzoek tot `SystemApi`-frames, met een nep-kern.
//!
//! `POST /v1/jobs` gaat als bytes door leanhttp en `hop-http` naar de
//! leader van de node; die plaatst op de eigen agent, de agent geeft een
//! start, de runner vraagt de kern om een slot en stroomt het image erin
//! (echte frames door `applib::sys::Client`, gelezen met de decoders van
//! `abi`), en de kern zegt `Placed`. Daarna zegt `GET /tasks` "running".

use alloc::collections::BTreeMap;
use alloc::string::String;
use alloc::vec::Vec;
use core::future::{Future, poll_fn};
use core::pin::pin;
use core::task::{Context, Poll, Waker};
use std::cell::RefCell;
use std::rc::Rc;

use abi::systemapi::PrivOp;
use api::{Method, Request};
use hop_http::{Ask, Chunk, Reply, Streams};
use hopos_runner::KernSys;
use hopos_runner::fake::FakeKern;
use leanhttp::{AsyncRead, AsyncWrite, Close, IoError};
use types::json::Value;

use crate::env::{BootConfig, BootError};
use crate::{Answer, Handoff, Hub, Images, Node, Port, Question, Sink};

const KEY: &[u8] = b"test-key";
const T0: u64 = 1_788_220_800 * types::time::SECOND;
const ELF: &[u8] = b"\x7fELF-the-web-app-image-bytes";
const JOB: &str = r#"{"name":"web","artifacts":[{"url":"http://images/web.elf"}],"cpu_shares":1024,"memory_limit":33554432}"#;

/// Een server in het geheugen: per URL de bytes; de test kan ze vervangen.
type Files = Rc<RefCell<BTreeMap<String, Vec<u8>>>>;

/// Artifacts uit het geheugen, in brokken van `.1` bytes.
struct MemImages(Files, usize);

impl MemImages {
    /// Brokken van 8 bytes: veel brokken voor een klein image.
    fn new(files: BTreeMap<String, Vec<u8>>) -> Self {
        Self(Rc::new(RefCell::new(files)), 8)
    }
}

impl Images for MemImages {
    async fn fetch<K: Sink>(&mut self, url: &str, sink: &mut K) -> Result<(), String> {
        let bytes = self.0.borrow().get(url).cloned();
        let bytes = bytes.ok_or_else(|| format!("404 {url}"))?;
        sink.begin(bytes.len() as u64).await?;
        for c in bytes.chunks(self.1) {
            sink.chunk(c).await?;
        }
        Ok(())
    }
}

type TestNode = Node<
    KernSys<applib::sys::Client<hopos_runner::fake::FakeDial, hopos_runner::fake::NeverTimer>>,
    MemImages,
>;

fn cfg() -> BootConfig {
    let mut env = BTreeMap::new();
    env.insert("HOPOS_APIKEY", "test-key");
    env.insert("HOPOS_NODE", "n1");
    env.insert("HOPOS_NODE_IP", "10.0.0.5");
    env.insert("HOPOS_CORES", "4");
    env.insert("HOPOS_MEMORY", "1073741824");
    BootConfig::from_env(|k| env.get(k).map(|v| String::from(*v)), 2, "10.100.0.2").unwrap()
}

fn node() -> (TestNode, FakeKern) {
    let k = FakeKern::new(4);
    let sys = KernSys::new(k.client(), 4);
    let mut images = BTreeMap::new();
    images.insert(String::from("http://images/web.elf"), ELF.to_vec());
    (Node::new(&cfg(), sys, MemImages::new(images), T0), k)
}

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

/// Een ondertekend verzoek als bytes op de draad.
fn wire(method: &str, path: &str, body: &str) -> Vec<u8> {
    let sig = auth::sign(KEY, method, path, body.as_bytes());
    let sig = core::str::from_utf8(&sig).unwrap();
    format!(
        "{method} {path} HTTP/1.1\r\nHost: n1\r\nX-Hop-Auth: {sig}\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    )
    .into_bytes()
}

/// Geen stromen in deze tests: wie er toch een opent, krijgt meteen het einde.
struct NoStreams;

impl Streams for NoStreams {
    async fn poll(&mut self, _ask: Ask) -> Chunk {
        Chunk {
            done: true,
            ..Chunk::default()
        }
    }
    async fn nap(&mut self, _d: core::time::Duration) {}
    fn now(&self) -> u64 {
        0
    }
    fn done(&mut self) {}
}

/// Stuurt `raw` door leanhttp en hop-http naar `port` van de node; het antwoord als (status, body).
fn http(node: &mut TestNode, port: Port, raw: Vec<u8>) -> (u16, String) {
    let out = Rc::new(RefCell::new(Vec::new()));
    let conn = Mem {
        input: raw,
        at: 0,
        out: out.clone(),
    };
    block_on(hop_http::serve(
        conn,
        async |req: Request| node.handle(port, &req, T0).await,
        &mut NoStreams,
    ))
    .unwrap();
    let text = String::from_utf8(out.borrow().clone()).unwrap();
    let status = text[9..12].parse().unwrap();
    let body = text
        .split_once("\r\n\r\n")
        .map(|(_, b)| String::from(b))
        .unwrap_or_default();
    (status, body)
}

#[test]
fn post_a_job_and_the_kernel_places_it() {
    let (mut n, k) = node();
    assert!(n.take_lines().iter().any(|l| l.contains("HOP_LEADER")));

    let (status, body) = http(&mut n, Port::Leader, wire("POST", "/v1/jobs", JOB));
    assert!((200..300).contains(&status), "{status} {body}");

    // De kern kreeg START_SLOT en de brokken van het image, en plaatste het.
    {
        let st = k.0.borrow();
        assert_eq!(st.ops[0], PrivOp::StartSlot.op());
        let streams = st
            .ops
            .iter()
            .filter(|&&o| o == PrivOp::StreamImage.op())
            .count();
        assert_eq!(streams, ELF.len().div_ceil(8), "{:?}", st.ops);
        let slot = &st.slots[&1];
        assert!(slot.placed);
        assert_eq!(slot.image, ELF);
        assert_eq!(slot.job, "web");
        assert_eq!(slot.memory_limit, 32 << 20);
    }
    let lines = n.take_lines();
    assert!(
        lines.iter().any(|l| l.contains("HOP_JOB_PLACED slot=1")),
        "{lines:?}"
    );

    // De agent-API zegt "running".
    let (status, body) = http(&mut n, Port::Agent, wire("GET", "/tasks", ""));
    assert_eq!(status, 200, "{body}");
    let tasks = types::json::parse(body.as_bytes()).unwrap();
    let t = &tasks.as_array().unwrap()[0];
    let field = |k: &str| {
        t.as_object()
            .unwrap()
            .get(k)
            .cloned()
            .unwrap_or(Value::Null)
    };
    assert_eq!(field("job_name").as_str(), Some("web"));
    assert_eq!(field("state").as_str(), Some("running"));
    assert_eq!(field("pid").as_i64(), Some(1));

    // Via de agent-poort gaat /v1/status in-proces naar de eigen leader.
    let (status, body) = http(&mut n, Port::Agent, wire("GET", "/v1/status", ""));
    assert_eq!(status, 200, "{body}");
    assert!(body.contains("\"web\":1"), "{body}");

    // Een tik pollt de kooi bij de kern (SLOT_STATUS) en laat hem draaien.
    block_on(n.tick(T0 + 6 * types::time::SECOND));
    assert!(k.0.borrow().ops.contains(&PrivOp::SlotStatus.op()));
    assert_eq!(
        n.agent().tasks().next().unwrap().state,
        types::TaskState::Running
    );
}

/// Een vervangen artifact onder dezelfde URL: de volgende plaatsing krijgt het nieuwe image.
///
/// Hop houdt geen image vast; elke plaatsing haalt de URL opnieuw (GEMETEN
/// 03-10 op de LicheeRV: een vervangen welcome-riscv64.elf in de rollende
/// release kwam minutenlang oud binnen, maar dat was de redirect van
/// GitHub, niet Hop). Deze toets houdt dat zo: DELETE en POST van dezelfde
/// job plaatsen wat de server nu heeft.
#[test]
fn a_replaced_artifact_is_what_the_next_placement_gets() {
    const NEW: &[u8] = b"\x7fELF-the-new-web-app";
    let k = FakeKern::new(4);
    let sys = KernSys::new(k.client(), 4);
    let mut files = BTreeMap::new();
    files.insert(String::from("http://images/web.elf"), ELF.to_vec());
    let images = MemImages::new(files);
    let server = images.0.clone();
    let mut n = Node::new(&cfg(), sys, images, T0);
    http(&mut n, Port::Leader, wire("POST", "/v1/jobs", JOB));
    assert_eq!(k.0.borrow().slots[&1].image, ELF);

    server
        .borrow_mut()
        .insert(String::from("http://images/web.elf"), NEW.to_vec());
    let (status, body) = http(&mut n, Port::Leader, wire("DELETE", "/v1/jobs/web", ""));
    assert_eq!(status, 204, "{body}");
    block_on(n.tick(T0 + types::time::SECOND));
    assert!(k.0.borrow().slots.is_empty(), "de oude bewoner is weg");
    let (status, body) = http(&mut n, Port::Leader, wire("POST", "/v1/jobs", JOB));
    assert!((200..300).contains(&status), "{status} {body}");
    let st = k.0.borrow();
    let placed: Vec<_> = st.slots.values().filter(|s| s.placed).collect();
    assert_eq!(placed.len(), 1, "{:?}", st.slots.keys());
    assert_eq!(placed[0].image, NEW);
}

#[test]
fn an_unsigned_job_never_reaches_the_kernel() {
    let (mut n, k) = node();
    let raw = format!(
        "POST /v1/jobs HTTP/1.1\r\nHost: n1\r\nContent-Length: {}\r\n\r\n{JOB}",
        JOB.len()
    );
    let (status, _) = http(&mut n, Port::Leader, raw.into_bytes());
    assert_eq!(status, 401);
    assert!(k.0.borrow().ops.is_empty());
}

#[test]
fn a_full_kernel_gives_the_task_back() {
    let k = FakeKern::new(0);
    let sys = KernSys::new(k.client(), 4);
    let mut images = BTreeMap::new();
    images.insert(String::from("http://images/web.elf"), ELF.to_vec());
    let mut n = Node::new(&cfg(), sys, MemImages::new(images), T0);
    let (status, body) = http(&mut n, Port::Leader, wire("POST", "/v1/jobs", JOB));
    assert!(status < 500, "{status} {body}");
    let lines = n.take_lines();
    assert!(
        lines
            .iter()
            .any(|l| l.contains("HOP_JOB_FAILED") && l.contains("no free run")),
        "{lines:?}"
    );
    assert!(k.0.borrow().slots.is_empty());
}

#[test]
fn a_missing_artifact_frees_the_reservation() {
    let k = FakeKern::new(4);
    let sys = KernSys::new(k.client(), 4);
    let mut n = Node::new(&cfg(), sys, MemImages::new(BTreeMap::new()), T0);
    http(&mut n, Port::Leader, wire("POST", "/v1/jobs", JOB));
    let lines = n.take_lines();
    assert!(
        lines
            .iter()
            .any(|l| l.contains("404 http://images/web.elf")),
        "{lines:?}"
    );
    assert!(k.0.borrow().slots.is_empty());
}

#[test]
fn boot_config_fails_closed_without_a_key() {
    let none = |_: &str| None::<String>;
    assert_eq!(
        BootConfig::from_env(none, 3, "10.100.0.3"),
        Err(BootError::NoKey)
    );
    let insecure = |k: &str| (k == "HOPOS_INSECURE").then(|| String::from("1"));
    let c = BootConfig::from_env(insecure, 3, "10.100.0.3").unwrap();
    assert!(c.insecure && c.api_key.is_empty());
    assert_eq!(c.node_id, "hopos-3");
    assert_eq!((c.port, c.leader_port()), (8080, 9080));
    assert_eq!(c.node_ip, "10.100.0.3");
    assert!(c.memory_defaulted);
    let bad = |k: &str| match k {
        "HOPOS_INSECURE" => Some(String::from("1")),
        "HOPOS_CORES" => Some(String::from("many")),
        _ => None,
    };
    assert!(matches!(
        BootConfig::from_env(bad, 3, "x"),
        Err(BootError::Bad {
            var: "HOPOS_CORES",
            ..
        })
    ));
    assert_eq!(cfg().api_key, KEY);
}

#[test]
fn the_system_core_comes_from_the_env() {
    let with = |pairs: &[(&str, &'static str)]| {
        let get = |k: &str| {
            pairs
                .iter()
                .find(|(n, _)| *n == k)
                .map(|(_, v)| String::from(*v))
        };
        let c = BootConfig::from_env(get, 2, "x").unwrap();
        (c.cores, c.system_core)
    };
    let open = ("HOPOS_INSECURE", "1");
    // De kern zegt het: 0 eigen cores is dan de waarheid.
    assert_eq!(
        with(&[open, ("HOPOS_CORES", "0"), ("HOPOS_SYSTEM_CORE", "1")]),
        (0, true)
    );
    assert_eq!(
        with(&[open, ("HOPOS_CORES", "2"), ("HOPOS_SYSTEM_CORE", "1")]),
        (2, true)
    );
    // Een oude kern: geen system-core, en minstens één core zoals altijd.
    assert_eq!(with(&[open, ("HOPOS_CORES", "0")]), (1, false));
    assert_eq!(
        with(&[open, ("HOPOS_CORES", "3"), ("HOPOS_SYSTEM_CORE", "0")]),
        (3, false)
    );
}

#[test]
fn the_hub_carries_a_request_and_its_answer() {
    let hub = Hub::new(2);
    let req = Request::new(Method::Get, "/health", b"");
    let mut ask = pin!(hub.ask(1, Port::Agent, req.clone()));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(ask.as_mut().poll(&mut cx).is_pending());
    assert!(block_on_ready(hub.wait()));
    let (slot, got) = hub.next().unwrap();
    assert_eq!((slot, got), (1, Question::Http(Port::Agent, req)));
    hub.answer(1, Answer::Reply(Reply::Plain(api::Response::empty(204))));
    match ask.as_mut().poll(&mut cx) {
        Poll::Ready(Reply::Plain(r)) => assert_eq!(r.status, 204),
        other => panic!("{other:?}"),
    }
    assert!(hub.next().is_none());
}

/// Eén poll: is `f` meteen klaar?
fn block_on_ready<F: Future>(f: F) -> bool {
    let mut f = pin!(f);
    f.as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
        .is_ready()
}

#[test]
fn a_fresh_resident_stops_what_the_kernel_still_runs() {
    let (mut n, k) = node();
    // Slot 1 is Hop zelf; de veger blijft daar vanaf.
    k.0.borrow_mut().slots.insert(
        1,
        hopos_runner::fake::FakeSlot {
            placed: true,
            ..Default::default()
        },
    );
    http(&mut n, Port::Leader, wire("POST", "/v1/jobs", JOB));
    block_on(n.tick(T0 + types::time::SECOND));
    assert_eq!(k.0.borrow().slots.len(), 2);
    // Een nieuwe Hop over dezelfde kern kent niets: geen staat op hopfs,
    // dus de bewoner in slot 2 is van niemand en gaat weg.
    let sys = KernSys::new(k.client(), 4);
    let mut again = Node::new(&cfg(), sys, MemImages::new(BTreeMap::new()), T0);
    block_on(again.sweep_strays());
    assert_eq!(k.0.borrow().slots.keys().copied().collect::<Vec<_>>(), [1]);
    assert!(
        again
            .take_lines()
            .iter()
            .any(|l| l.contains("HOP_STRAY_STOPPED slot=2"))
    );
    assert_eq!(again.agent().tasks().count(), 0);
}

/// Twee taken om de beurt, zoals de executor van de app-core ze pollt.
///
/// Geen executor in een host-test, en ook geen geneste ronde: deze lus is de
/// enige die pollt. `a` is klaar als hij klaar is; `b` loopt eeuwig.
fn run_two<A: Future, B: Future<Output = ()>>(a: A, b: B) -> A::Output {
    let mut a = pin!(a);
    let mut b = pin!(b);
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..10_000_000 {
        if let Poll::Ready(v) = a.as_mut().poll(&mut cx) {
            return v;
        }
        let _ = b.as_mut().poll(&mut cx);
    }
    panic!("de eigenaar kwam nooit klaar");
}

#[test]
fn a_long_stream_does_not_block_the_other_tasks() {
    // 8 MiB in brokken van 64 KiB, over een verbinding die bij elke lees
    // eerst de core teruggeeft (zoals TCP waar het antwoord nog onderweg
    // is). De eigenaar-taak stroomt; een tweede taak moet intussen gewoon
    // aan de beurt komen, tussen de brokken door.
    const SIZE: usize = 8 << 20;
    const CHUNK: usize = 64 << 10;
    let mut image = alloc::vec![0u8; SIZE];
    image[..4].copy_from_slice(b"\x7fELF");
    let k = FakeKern::new(4);
    k.0.borrow_mut().yield_reads = true;
    let sys = KernSys::new(k.client(), 4);
    let mut files = BTreeMap::new();
    files.insert(String::from("http://images/web.elf"), image);
    let mut n = Node::new(
        &cfg(),
        sys,
        MemImages(Rc::new(RefCell::new(files)), CHUNK),
        T0,
    );

    let out = Rc::new(RefCell::new(Vec::new()));
    let conn = Mem {
        input: wire("POST", "/v1/jobs", JOB),
        at: 0,
        out: out.clone(),
    };
    let mut none = NoStreams;
    let owner = hop_http::serve(
        conn,
        async |req: Request| n.handle(Port::Leader, &req, T0).await,
        &mut none,
    );

    // De tweede taak telt zijn beurten terwijl slot 1 half gestroomd is.
    let during = Rc::new(RefCell::new(0u64));
    let other = {
        let (k, during) = (k.clone(), during.clone());
        poll_fn(move |_| {
            let streaming =
                k.0.borrow()
                    .slots
                    .get(&1)
                    .is_some_and(|s| !s.placed && !s.image.is_empty());
            if streaming {
                *during.borrow_mut() += 1;
            }
            Poll::<()>::Pending
        })
    };
    run_two(owner, other).unwrap();

    let st = k.0.borrow();
    let slot = &st.slots[&1];
    assert!(slot.placed);
    assert_eq!(slot.image.len(), SIZE);
    let streams = st
        .ops
        .iter()
        .filter(|&&o| o == PrivOp::StreamImage.op())
        .count();
    assert_eq!(streams, SIZE / CHUNK);
    // Elke brok wachtte op de kern, en in elk van die wachten kwam de andere
    // taak aan de beurt: tussen de eerste en de laatste brok minstens één
    // beurt per brok.
    assert!(st.yields >= streams as u64, "{} {streams}", st.yields);
    assert!(
        *during.borrow() >= (streams - 1) as u64,
        "de tweede taak kwam {} keer aan de beurt tijdens {streams} brokken",
        during.borrow()
    );
    drop(st);
    let text = String::from_utf8(out.borrow().clone()).unwrap();
    assert!(text.starts_with("HTTP/1.1 2"), "{text}");
    assert!(
        n.take_lines()
            .iter()
            .any(|l| l.contains("HOP_JOB_PLACED slot=1"))
    );
}

#[test]
fn a_clean_boot_seeds_the_init_jobs_once() {
    let (mut n, k) = node();
    // De headless-vorm: één artifact en geen driver is de hop-driver.
    let specs =
        r#"[{"name":"web","artifacts":[{"url":"http://images/web.elf"}],"memory_limit":33554432}]"#;
    assert_eq!(block_on(n.seed_init_jobs(specs, T0)), Ok(1));
    let lines = n.take_lines();
    assert!(
        lines
            .iter()
            .any(|l| l.contains("seeded 1 init job(s): web HOP_INIT_SEEDED")),
        "{lines:?}"
    );
    assert!(
        lines.iter().any(|l| l.contains("HOP_JOB_PLACED slot=1")),
        "{lines:?}"
    );
    assert!(k.0.borrow().slots[&1].placed);
    // Een leader met jobs is geen schone boot: niets nog eens.
    assert_eq!(block_on(n.seed_init_jobs(specs, T0)), Ok(0));
    assert_eq!(k.0.borrow().slots.len(), 1);
}

#[test]
fn broken_init_jobs_are_loud_and_seed_nothing() {
    for bad in [
        "not json",
        r#"{"name":"web"}"#,
        r#"[{"name":"web","bogus_field":1,"artifacts":[{"url":"http://images/web.elf"}]}]"#,
        r#"[{"artifacts":[{"url":"http://images/web.elf"}]}]"#,
    ] {
        let (mut n, k) = node();
        let r = block_on(n.seed_init_jobs(bad, T0));
        assert!(r.is_err(), "{bad}: {r:?}");
        assert!(k.0.borrow().ops.is_empty(), "{bad}");
    }
}

#[test]
fn the_key_wins_over_insecure_and_secrets_stay_out_of_debug() {
    let mut env = BTreeMap::new();
    env.insert("HOPOS_APIKEY", "s3cr3t-api-key");
    env.insert("HOPOS_INSECURE", "1");
    env.insert("HOPOS_S3_ENDPOINT", "https://s3.example.com");
    env.insert("HOPOS_S3_BUCKET", "hop-prod");
    env.insert("HOPOS_S3_KEY", "AKIA1");
    env.insert("HOPOS_S3_SECRET", "very-secret-value");
    env.insert("HOPOS_S3_PATHSTYLE", "1");
    env.insert("HOPOS_INIT_JOBS", r#"[{"name":"a"}]"#);
    let c =
        BootConfig::from_env(|k| env.get(k).map(|v| String::from(*v)), 1, "10.100.0.1").unwrap();
    assert!(!c.insecure && c.insecure_ignored);
    assert_eq!(c.api_key, b"s3cr3t-api-key");
    let s3 = c.s3.clone().unwrap();
    assert_eq!(
        (
            s3.endpoint.as_str(),
            s3.bucket.as_str(),
            s3.key.as_str(),
            s3.path_style
        ),
        ("https://s3.example.com", "hop-prod", "AKIA1", true)
    );
    assert_eq!(c.init_jobs.as_deref(), Some(r#"[{"name":"a"}]"#));
    let shown = format!("{c:?}");
    assert!(
        !shown.contains("s3cr3t") && !shown.contains("very-secret"),
        "{shown}"
    );
    assert!(
        shown.contains("<14 bytes>") && shown.contains("<17 bytes>"),
        "{shown}"
    );
    // Zonder bucket geen S3; zonder jobs geen init-jobs.
    env.remove("HOPOS_S3_BUCKET");
    env.insert("HOPOS_INIT_JOBS", " ");
    let c = BootConfig::from_env(|k| env.get(k).map(|v| String::from(*v)), 1, "x").unwrap();
    assert!(c.s3.is_none() && c.init_jobs.is_none());
}

#[test]
fn the_hub_carries_a_poll_and_a_stream_done() {
    let hub = Hub::new(2);
    let ask = Ask::Events { seq: 3 };
    let mut poll = pin!(hub.poll(0, ask.clone()));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(poll.as_mut().poll(&mut cx).is_pending());
    assert_eq!(hub.next(), Some((0, Question::Poll(ask))));
    let c = Chunk {
        text: "event: status\ndata: {}\n\n".into(),
        seq: 4,
        done: false,
    };
    hub.answer(0, Answer::Chunk(c.clone()));
    assert_eq!(poll.as_mut().poll(&mut cx), Poll::Ready(c));
    // Afmelden wacht nergens op.
    hub.stream_done(1);
    assert_eq!(hub.next(), Some((1, Question::StreamDone)));
}

#[test]
fn the_handoff_gives_each_connection_to_a_free_worker() {
    let pool: Handoff<u32> = Handoff::new(2);
    let mut cx = Context::from_waker(Waker::noop());
    let mut w0 = pin!(pool.take(0));
    assert!(w0.as_mut().poll(&mut cx).is_pending());
    assert_eq!(pool.give(10), Ok(0));
    assert_eq!(pool.give(11), Ok(1));
    // Beide bezig: de derde komt terug bij de acceptor.
    assert_eq!(pool.give(12), Err(12));
    assert_eq!(w0.as_mut().poll(&mut cx), Poll::Ready(10));
    assert_eq!(block_on(pool.take(1)), 11);
    pool.free(1);
    assert_eq!(pool.give(12), Ok(1));
}

/// Een ondertekend verzoek zonder de draad (de query telt niet mee in de HMAC).
fn signed(method: Method, target: &str) -> Request {
    let path = target.split('?').next().unwrap();
    let sig = auth::sign(KEY, method.as_str(), path, b"");
    let mut r = Request::new(method, target, b"");
    r.headers.push((
        auth::AUTH_HEADER.into(),
        String::from_utf8(sig.to_vec()).unwrap(),
    ));
    r
}

/// Zet een logregel klaar in slot 1 van de nep-kern.
fn app_says(k: &FakeKern, line: &str) {
    k.0.borrow_mut()
        .slots
        .get_mut(&1)
        .unwrap()
        .logs
        .push_back(line.as_bytes().to_vec());
}

#[test]
fn a_log_is_followed_live_through_the_leader() {
    let (mut n, k) = node();
    http(&mut n, Port::Leader, wire("POST", "/v1/jobs", JOB));
    let task = n.agent().tasks().next().unwrap().id.clone();
    app_says(&k, "one");
    block_on(n.tick(T0 + types::time::SECOND));

    // hop logs --follow: via de leader naar de eigen agent, in-proces.
    let req = signed(
        Method::Get,
        &format!("/v1/agents/n1/logs/{task}/stdout?follow=1"),
    );
    let Reply::Stream { ask, first, .. } = block_on(n.handle(Port::Leader, &req, T0)) else {
        panic!("no stream");
    };
    assert!(first.is_empty());
    let c = n.poll(&ask, T0);
    assert_eq!(
        (c.text.as_str(), c.seq, c.done),
        ("data: one\n\n", 1, false)
    );
    // Niets nieuws: niets, hetzelfde nummer.
    let again = Ask::Logs {
        task_id: task.clone(),
        stream: api::LogStream::Stdout,
        seq: c.seq,
    };
    assert_eq!(n.poll(&again, T0).text, "");
    // Een nieuwe regel van de app komt er als enige bij.
    app_says(&k, "two");
    block_on(n.tick(T0 + 2 * types::time::SECOND));
    let c2 = n.poll(&again, T0);
    assert_eq!((c2.text.as_str(), c2.seq), ("data: two\n\n", 2));
    // Met follow=0: de momentopname (`hop logs` zonder --follow).
    let snap = signed(
        Method::Get,
        &format!("/v1/agents/n1/logs/{task}/stdout?follow=0"),
    );
    let Reply::Events { lines, .. } = block_on(n.handle(Port::Leader, &snap, T0)) else {
        panic!("no snapshot");
    };
    assert_eq!(lines, ["one", "two"]);
    n.stream_done();
    // Zonder query: de levende tail, Go's contract (het dashboard vraagt zo).
    let dash = signed(Method::Get, &format!("/v1/agents/n1/logs/{task}/stdout"));
    let Reply::Stream { ask, .. } = block_on(n.handle(Port::Leader, &dash, T0)) else {
        panic!("the dashboard's log route is no live tail");
    };
    assert_eq!(n.poll(&ask, T0).text, "data: one\n\ndata: two\n\n");
    // Een onbekende agent: 404; een onbekende taak: geen stroom.
    let Reply::Plain(r) = block_on(n.handle(
        Port::Leader,
        &signed(Method::Get, "/v1/agents/zz/logs/t/stdout"),
        T0,
    )) else {
        panic!("no plain reply");
    };
    assert_eq!(r.status, 404);
    n.stream_done();
}

#[test]
fn events_and_tasks_through_the_leader() {
    let (mut n, _k) = node();
    // hop events: de stroom begint met een ping en het huidige nummer.
    let Reply::Stream { ask, first, .. } =
        block_on(n.handle(Port::Leader, &signed(Method::Get, "/v1/events"), T0))
    else {
        panic!("no stream");
    };
    assert_eq!(first, api::PING);
    // Een job plaatsen geeft meldingen: de job, en de taak met zijn event.
    http(&mut n, Port::Leader, wire("POST", "/v1/jobs", JOB));
    let c = n.poll(&ask, T0);
    assert!(
        c.text.contains(r#""name":"web""#) || c.text.contains(r#""job":"web""#),
        "{}",
        c.text
    );
    assert!(!c.done);

    // hop jobs: /v1/tasks met de taken van de eigen agent.
    let Reply::Plain(r) = block_on(n.handle(Port::Leader, &signed(Method::Get, "/v1/tasks"), T0))
    else {
        panic!("no plain reply");
    };
    assert_eq!(r.status, 200);
    let body = String::from_utf8(r.body).unwrap();
    assert!(body.contains(r#""tasks_by_agent":{"n1":[{"#), "{body}");
    assert!(body.contains(r#""job_name":"web""#), "{body}");
    // En de capaciteit via de leader.
    let Reply::Plain(r) = block_on(n.handle(
        Port::Leader,
        &signed(Method::Get, "/v1/agents/n1/capacity"),
        T0,
    )) else {
        panic!("no plain reply");
    };
    assert_eq!(r.status, 200);

    // Het plafond: tot `MAX_STREAMS` stromen mag, de volgende niet.
    for _ in 1..crate::node::MAX_STREAMS {
        let more = block_on(n.handle(Port::Leader, &signed(Method::Get, "/v1/events"), T0));
        assert!(matches!(more, Reply::Stream { .. }));
    }
    let Reply::Plain(r) = block_on(n.handle(Port::Leader, &signed(Method::Get, "/v1/events"), T0))
    else {
        panic!("a stream over the ceiling was admitted");
    };
    assert_eq!(r.status, 503);
    // Na een afmelding weer wel.
    n.stream_done();
    let again = block_on(n.handle(Port::Leader, &signed(Method::Get, "/v1/events"), T0));
    assert!(matches!(again, Reply::Stream { .. }));
}

/// De CORS-koppen op een antwoord, voor elke soort `Reply`.
fn allow_origin(r: &Reply) -> Option<&str> {
    match r {
        Reply::Plain(h) | Reply::Events { head: h, .. } | Reply::Stream { head: h, .. } => {
            h.header("Access-Control-Allow-Origin")
        }
    }
}

#[test]
fn the_dashboard_gets_cors_on_every_agent_answer() {
    let (mut n, _k) = node();
    http(&mut n, Port::Leader, wire("POST", "/v1/jobs", JOB));
    // Een /v1/-route op de agent-poort gaat in-proces naar de leader; ook
    // dat antwoord draagt de koppen, anders gooit de browser het weg.
    for target in [
        "/v1/status",
        "/v1/agents",
        "/v1/jobs",
        "/v1/jobs/web/status",
        "/v1/agents/n1/capacity",
        "/leader",
    ] {
        let r = block_on(n.handle(Port::Agent, &signed(Method::Get, target), T0));
        assert_eq!(allow_origin(&r), Some("*"), "{target}: {r:?}");
        let Reply::Plain(p) = &r else {
            panic!("{target}: not a plain answer");
        };
        assert_eq!(
            p.status,
            200,
            "{target}: {:?}",
            core::str::from_utf8(&p.body)
        );
    }
    // De takentabel van het dashboard: de taak van web op n1.
    let Reply::Plain(p) =
        block_on(n.handle(Port::Agent, &signed(Method::Get, "/v1/jobs/web/status"), T0))
    else {
        panic!("no plain reply");
    };
    let v = types::json::parse(&p.body).unwrap();
    let by = v.as_object().unwrap().get("tasks_by_agent").unwrap();
    assert_eq!(
        by.as_object()
            .unwrap()
            .get("n1")
            .unwrap()
            .as_array()
            .unwrap()
            .len(),
        1
    );
    // De kop van een stroom (SSE) ook.
    let r = block_on(n.handle(Port::Agent, &signed(Method::Get, "/v1/events"), T0));
    assert!(matches!(r, Reply::Stream { .. }), "{r:?}");
    assert_eq!(allow_origin(&r), Some("*"));
    n.stream_done();
    // En een weigering: ongetekend is 401, met de koppen.
    let r = block_on(n.handle(Port::Agent, &Request::new(Method::Get, "/v1/jobs", b""), T0));
    assert_eq!(allow_origin(&r), Some("*"));
    // De leader-poort is geen browserpoort: daar geen koppen, zoals in Go.
    let r = block_on(n.handle(Port::Leader, &signed(Method::Get, "/v1/status"), T0));
    assert_eq!(allow_origin(&r), None);
}
