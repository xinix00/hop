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
use core::future::Future;
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

use core::sync::atomic::{AtomicU64, Ordering::Relaxed};
use core::time::Duration;
use sync::mpsc::Mailbox;
use sync::spsc::Channel;

use crate::download::{Orders, Pieces, download_task};
use crate::env::{BootConfig, BootError, ColdFlip};
use crate::{Answer, Handoff, Hub, Images, Kept, Node, Port, Question, Sink};

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
    async fn fetch<K: Sink>(&mut self, a: &types::Artifact, sink: &mut K) -> Result<(), String> {
        let url = a.url.as_str();
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
    cfg_with(&[])
}

/// [`cfg`] met extra sleutels van de kern erbij.
fn cfg_with(extra: &[(&'static str, &'static str)]) -> BootConfig {
    let mut env = BTreeMap::new();
    env.insert("HOPOS_APIKEY", "test-key");
    env.insert("HOPOS_NODE", "n1");
    env.insert("HOPOS_NODE_IP", "10.0.0.5");
    env.insert("HOPOS_CORES", "4");
    env.insert("HOPOS_MEMORY", "1073741824");
    env.extend(extra.iter().copied());
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

/// Stuurt `raw` door leanhttp en hop-http naar `port` van de node, en haalt
/// daarna de downloads op die het verzoek gaf (zonder downloadtaak, zie
/// `Node::settle`); het antwoord als (status, body).
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
    block_on(node.settle(T0));
    reply_of(&out)
}

/// Het antwoord op de draad als (status, body).
fn reply_of(out: &Rc<RefCell<Vec<u8>>>) -> (u16, String) {
    let text = String::from_utf8(out.borrow().clone()).unwrap();
    let status = text[9..12].parse().unwrap();
    let body = text
        .split_once("\r\n\r\n")
        .map(|(_, b)| String::from(b))
        .unwrap_or_default();
    (status, body)
}

/// Een tik, en dan de downloads die hij gaf (een herstart).
fn tick(node: &mut TestNode, now: u64) {
    block_on(node.tick(now));
    block_on(node.settle(now));
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
    tick(&mut n, T0 + 6 * types::time::SECOND);
    assert!(k.0.borrow().ops.contains(&PrivOp::SlotStatus.op()));
    assert_eq!(
        n.agent().tasks().next().unwrap().state,
        types::TaskState::Running
    );
}

/// Een node met web geplaatst en een kernbundel op de server; de nep-kern
/// neemt de FLIP aan als `flip_ok`.
fn placed_for_a_flip(flip_ok: bool) -> (TestNode, FakeKern) {
    let (mut n, k) = ready_for_a_flip(flip_ok, &[]);
    http(&mut n, Port::Leader, wire("POST", "/v1/jobs", JOB));
    assert!(k.0.borrow().slots[&1].placed);
    n.take_lines();
    (n, k)
}

/// Een node met `env` erbij en een kernbundel op de server, nog zonder
/// taak; de nep-kern neemt de FLIP aan als `flip_ok`.
fn ready_for_a_flip(flip_ok: bool, env: &[(&'static str, &'static str)]) -> (TestNode, FakeKern) {
    let k = FakeKern::new(4);
    k.0.borrow_mut().flip_ok = flip_ok;
    let sys = KernSys::new(k.client(), 4);
    let mut images = BTreeMap::new();
    images.insert(String::from("http://images/web.elf"), ELF.to_vec());
    images.insert(
        String::from("http://images/k.flip"),
        b"\x7fELF-a-kernel-bundle".to_vec(),
    );
    let mut n = Node::new(&cfg_with(env), sys, MemImages::new(images), T0);
    n.take_lines();
    (n, k)
}

/// Hop weigerde de koude flip zelf: een 502 met `why`, één regel, en de
/// kern zag geen stop, geen bundel en geen FLIP.
fn refused_by_hop(n: &mut TestNode, k: &FakeKern, why: &str) {
    let ops_before = k.0.borrow().ops.len();
    let (status, body) = cold_flip(n);
    assert_eq!(status, 502, "{body}");
    assert!(body.contains(why), "{body}");
    let lines = n.take_lines();
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert!(
        lines[0].contains("HOP_FLIP_FAIL") && lines[0].contains(why),
        "{lines:?}"
    );
    let st = k.0.borrow();
    assert!(
        st.ops[ops_before..]
            .iter()
            .all(|&o| o != PrivOp::StopSlot.op()
                && o != PrivOp::StartSlot.op()
                && o != PrivOp::Flip.op()),
        "{:?}",
        &st.ops[ops_before..]
    );
}

/// De M4 (`HOPOS_COLD_FLIP=no`): geen PSCI, dus Hop weigert koud meteen,
/// en de taak draait door.
#[test]
fn a_board_without_psci_refuses_cold_before_any_stop() {
    let (mut n, k) = ready_for_a_flip(true, &[("HOPOS_COLD_FLIP", "no")]);
    http(&mut n, Port::Leader, wire("POST", "/v1/jobs", JOB));
    n.take_lines();
    refused_by_hop(&mut n, &k, "cold flip: no PSCI on this board, ask warm");
    assert!(k.0.borrow().slots[&1].placed);
    assert_eq!(
        n.agent().tasks().next().unwrap().state,
        types::TaskState::Running
    );
    placed_matches_the_agent(&mut n);
    // Warm mag wel.
    let sum = "ab".repeat(32);
    let body = format!(r#"{{"url":"http://images/k.flip","sha256":"{sum}"}}"#);
    let (status, body) = http(&mut n, Port::Agent, wire("POST", "/flip", &body));
    assert_eq!(status, 202, "{body}");
}

/// De Pi 5 (`HOPOS_COLD_FLIP=fresh`) met Hop op de OS-core: zonder
/// plaatsing op een app-core gaat de koude flip door, een job in `system`
/// telt niet, en na de eerste plaatsing daarbuiten weigert Hop zelf.
#[test]
fn a_fresh_board_flips_cold_until_an_app_core_ran() {
    let fresh = [
        ("HOPOS_COLD_FLIP", "fresh"),
        ("HOPOS_HOP_GROUP", "system"),
        ("HOPOS_SYSTEM_CORE", "1"),
    ];
    let (mut n, k) = ready_for_a_flip(true, &fresh);
    let (status, body) = cold_flip(&mut n);
    assert_eq!(status, 202, "{body}");
    assert!(k.0.borrow().ops.contains(&PrivOp::Flip.op()));

    let (mut n, k) = ready_for_a_flip(true, &fresh);
    let sys_job = r#"{"name":"sys","artifacts":[{"url":"http://images/web.elf"}],"cpu_shares":1024,"memory_limit":33554432,"tags":{"sharegroup":"system"}}"#;
    let (status, body) = http(&mut n, Port::Leader, wire("POST", "/v1/jobs", sys_job));
    assert!((200..300).contains(&status), "{status} {body}");
    assert!(k.0.borrow().slots[&1].placed);
    n.take_lines();
    let (status, body) = cold_flip(&mut n);
    assert_eq!(status, 202, "{body}");

    let (mut n, k) = ready_for_a_flip(true, &fresh);
    http(&mut n, Port::Leader, wire("POST", "/v1/jobs", JOB));
    assert!(k.0.borrow().slots[&1].placed);
    n.take_lines();
    refused_by_hop(
        &mut n,
        &k,
        "cold flip: CPU_OFF has no way back on this board and an app core ran, ask warm",
    );
    assert_eq!(
        n.agent().tasks().next().unwrap().state,
        types::TaskState::Running
    );
    // Ook na het stoppen van de job: de core draaide, en dat blijft zo.
    http(&mut n, Port::Leader, wire("DELETE", "/v1/jobs/web", ""));
    refused_by_hop(
        &mut n,
        &k,
        "cold flip: CPU_OFF has no way back on this board and an app core ran, ask warm",
    );
}

/// `fresh` met Hop op een eigen app-core: die core draait al, dus nooit koud.
#[test]
fn a_fresh_board_with_hop_on_an_app_core_never_flips_cold() {
    let (mut n, k) = ready_for_a_flip(true, &[("HOPOS_COLD_FLIP", "fresh")]);
    refused_by_hop(
        &mut n,
        &k,
        "cold flip: CPU_OFF has no way back on this board and an app core ran, ask warm",
    );
}

/// `yes` (en een oude kern zonder de sleutel): zoals voorheen, Hop stopt
/// en vraagt; de kern beslist.
#[test]
fn a_board_that_flips_cold_stops_and_asks_as_before() {
    for env in [&[("HOPOS_COLD_FLIP", "yes")][..], &[]] {
        let (mut n, k) = ready_for_a_flip(false, env);
        http(&mut n, Port::Leader, wire("POST", "/v1/jobs", JOB));
        n.take_lines();
        let (status, body) = cold_flip(&mut n);
        assert_eq!(status, 502, "{body}");
        assert!(body.contains("kernel refused the flip"), "{body}");
        let lines = n.take_lines();
        assert!(
            lines
                .iter()
                .any(|l| l.contains("HOP_FLIP_COLD_STOP stopped=1")),
            "{lines:?}"
        );
        assert!(k.0.borrow().ops.contains(&PrivOp::Flip.op()));
    }
}

fn cold_flip(n: &mut TestNode) -> (u16, String) {
    let sum = "ab".repeat(32);
    let body = format!(r#"{{"url":"http://images/k.flip","sha256":"{sum}","cold":true}}"#);
    http(n, Port::Agent, wire("POST", "/flip", &body))
}

/// Wat de leader geplaatst telt en wat de agent heeft, moet hetzelfde zijn.
fn placed_matches_the_agent(n: &mut TestNode) {
    let (_, body) = http(n, Port::Agent, wire("GET", "/v1/status", ""));
    assert!(body.contains("\"web\":1"), "{body}");
    assert_eq!(n.agent().placed_task_counts().get("web"), Some(&1));
}

/// De Pi 5 (03-10): de kern weigert de koude flip ná de stop van de
/// taken. Ze komen terug, en de leader telt niets wat de agent niet heeft.
#[test]
fn a_refused_cold_flip_restarts_the_stopped_tasks() {
    let (mut n, k) = placed_for_a_flip(false);
    let (status, body) = cold_flip(&mut n);
    assert_eq!(status, 502, "{body}");
    let lines = n.take_lines();
    let at = |m: &str| lines.iter().position(|l| l.contains(m));
    assert!(at("HOP_FLIP_COLD_STOP stopped=1").is_some(), "{lines:?}");
    assert!(at("HOP_FLIP_FAIL").is_some(), "{lines:?}");
    let back = lines
        .iter()
        .find(|l| l.contains("HOP_FLIP_COLD_BACK"))
        .unwrap_or_else(|| panic!("{lines:?}"));
    assert!(back.contains("1 stopped task(s) restart"), "{back}");
    // Gestopt, en weer geplaatst; het bundelslot is opgeruimd.
    assert!(k.0.borrow().ops.contains(&PrivOp::StopSlot.op()));
    let slots: Vec<String> = k.0.borrow().slots.values().map(|s| s.job.clone()).collect();
    assert_eq!(slots, ["web"]);
    assert_eq!(
        n.agent().tasks().next().unwrap().state,
        types::TaskState::Running
    );
    placed_matches_the_agent(&mut n);
}

/// De kern nam de koude flip aan (202) maar sprong niet: een Hop die na
/// de wachttijd nog leeft, herstart de gestopte taken.
#[test]
fn an_accepted_cold_flip_that_never_jumps_brings_the_tasks_back() {
    let (mut n, k) = placed_for_a_flip(true);
    let (status, body) = cold_flip(&mut n);
    assert_eq!(status, 202, "{body}");
    let lines = n.take_lines();
    assert!(
        lines.iter().any(|l| l.contains("HOP_FLIP_ACCEPTED")),
        "{lines:?}"
    );
    // Gestopt bij de kern, maar het record blijft: de leader telt hem nog,
    // en terecht.
    assert!(k.0.borrow().slots.is_empty());
    assert_eq!(
        n.agent().tasks().next().unwrap().state,
        types::TaskState::Stopping
    );
    placed_matches_the_agent(&mut n);
    // Binnen de wachttijd: niets.
    tick(&mut n, T0 + 29 * types::time::SECOND);
    assert!(k.0.borrow().slots.is_empty());
    assert!(
        !n.take_lines()
            .iter()
            .any(|l| l.contains("HOP_FLIP_COLD_BACK"))
    );
    // Erna: de kern sprong niet, dus de taak herstart.
    tick(&mut n, T0 + 31 * types::time::SECOND);
    let lines = n.take_lines();
    assert!(
        lines.iter().any(|l| l.contains("HOP_FLIP_COLD_BACK")),
        "{lines:?}"
    );
    assert!(
        k.0.borrow()
            .slots
            .values()
            .any(|s| s.job == "web" && s.placed)
    );
    assert_eq!(
        n.agent().tasks().next().unwrap().state,
        types::TaskState::Running
    );
    placed_matches_the_agent(&mut n);
    // Eén keer: de volgende tik zegt het niet opnieuw.
    tick(&mut n, T0 + 62 * types::time::SECOND);
    assert!(
        !n.take_lines()
            .iter()
            .any(|l| l.contains("HOP_FLIP_COLD_BACK"))
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
    tick(&mut n, T0 + types::time::SECOND);
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
    // Het board-contract van de koude flip; zonder sleutel `yes`.
    let cold = |v: Option<&'static str>| {
        let mut pairs = Vec::from([open]);
        pairs.extend(v.map(|v| ("HOPOS_COLD_FLIP", v)));
        let get = |k: &str| {
            pairs
                .iter()
                .find(|(n, _)| *n == k)
                .map(|(_, v)| String::from(*v))
        };
        BootConfig::from_env(get, 2, "x").unwrap().cold_flip
    };
    assert_eq!(cold(None), ColdFlip::Yes);
    assert_eq!(cold(Some("yes")), ColdFlip::Yes);
    assert_eq!(cold(Some("no")), ColdFlip::No);
    assert_eq!(cold(Some("fresh")), ColdFlip::Fresh);
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
    // Een kern zonder de staat-ops (v3.0.14): er is niets over te nemen.
    k.0.borrow_mut().no_state_ops = true;
    // Slot 1 is Hop zelf; de veger blijft daar vanaf.
    k.0.borrow_mut().slots.insert(
        1,
        hopos_runner::fake::FakeSlot {
            placed: true,
            ..Default::default()
        },
    );
    http(&mut n, Port::Leader, wire("POST", "/v1/jobs", JOB));
    tick(&mut n, T0 + types::time::SECOND);
    assert_eq!(k.0.borrow().slots.len(), 2);
    // Een nieuwe Hop over dezelfde kern kent niets: de kern hield geen
    // staat, dus de bewoner in slot 2 is van niemand en gaat weg.
    let sys = KernSys::new(k.client(), 4);
    let mut again = Node::new(&cfg(), sys, MemImages::new(BTreeMap::new()), T0);
    assert_eq!(block_on(again.restore(T0)), Kept::Unsupported);
    assert!(Kept::Unsupported.is_clean(), "the init jobs seed as before");
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

/// Artifacts die vóór elke brok de core teruggeven, zoals TCP waar de
/// volgende bytes nog onderweg zijn.
struct SlowImages(MemImages);

impl Images for SlowImages {
    async fn fetch<K: Sink>(&mut self, a: &types::Artifact, sink: &mut K) -> Result<(), String> {
        let url = a.url.as_str();
        let bytes = self.0.0.borrow().get(url).cloned();
        let bytes = bytes.ok_or_else(|| format!("404 {url}"))?;
        sink.begin(bytes.len() as u64).await?;
        for c in bytes.chunks(self.0.1) {
            sync::yield_now().await;
            sink.chunk(c).await?;
        }
        Ok(())
    }
}

/// Stuurt `raw` naar de node en eist het antwoord in één poll: geen wacht
/// op de kern, op het net of op een download. Haalt geen downloads op.
fn http_once(node: &mut TestNode, port: Port, raw: Vec<u8>) -> (u16, String) {
    let out = Rc::new(RefCell::new(Vec::new()));
    let conn = Mem {
        input: raw,
        at: 0,
        out: out.clone(),
    };
    let mut none = NoStreams;
    let served = pin!(hop_http::serve(
        conn,
        async |req: Request| node.handle(port, &req, T0).await,
        &mut none,
    ))
    .poll(&mut Context::from_waker(Waker::noop()));
    let Poll::Ready(r) = served else {
        panic!("the API did not answer within one poll");
    };
    r.unwrap();
    reply_of(&out)
}

/// 03-10: de API mag nooit stilstaan tijdens een download.
///
/// Een image van 4 MiB in 64 brokken van 64 KiB, over een server die vóór
/// elke brok de core teruggeeft, en een kern die bij elke call eerst de
/// core teruggeeft. De downloadtaak en de eigenaar gaan om de beurt, zoals
/// op de app-core: per ronde hoogstens één brok de kern in. Halverwege
/// antwoordt `GET /v1/status` in één poll (ruim binnen 100 ms), en een
/// tweede `POST /v1/jobs` wordt aangenomen; zijn download komt na de eerste.
#[test]
fn the_api_answers_while_an_image_streams() {
    const SIZE: usize = 4 << 20;
    const CHUNK: usize = 64 << 10;
    const API: &str =
        r#"{"name":"api","artifacts":[{"url":"http://images/api.elf"}],"memory_limit":33554432}"#;
    let mut image = alloc::vec![0u8; SIZE];
    image[..4].copy_from_slice(b"\x7fELF");
    let k = FakeKern::new(4);
    k.0.borrow_mut().yield_reads = true;
    let sys = KernSys::new(k.client(), 4);
    let mut files = BTreeMap::new();
    files.insert(String::from("http://images/web.elf"), image);
    files.insert(String::from("http://images/api.elf"), ELF.to_vec());
    let files = Rc::new(RefCell::new(files));
    let mut n = Node::new(&cfg(), sys, MemImages(files.clone(), CHUNK), T0);

    // Aangenomen in één poll; de download is een opdracht, nog geen brok.
    let (status, body) = http_once(&mut n, Port::Leader, wire("POST", "/v1/jobs", JOB));
    assert!((200..300).contains(&status), "{status} {body}");
    assert!(!k.0.borrow().ops.contains(&PrivOp::StreamImage.op()));

    let orders: Orders = Mailbox::new();
    let pieces: Pieces = Channel::new();
    let wanted = AtomicU64::new(0);
    let (tx, mut rx) = pieces.split().unwrap();
    let images = SlowImages(MemImages(files, CHUNK));
    let mut task = pin!(download_task(images, &orders, tx, &wanted));
    let mut cx = Context::from_waker(Waker::noop());
    let mut fed = 0;
    let mut asked = None;
    for _ in 0..100_000 {
        // De flush van de eigenaar: een opdracht, en wat hij nog wil.
        if let Some(o) = n.take_order() {
            orders.try_send(o).unwrap();
        }
        wanted.store(n.wanted(), Relaxed);
        let _ = task.as_mut().poll(&mut cx);
        // Eén brok per ronde, zoals de lus van de eigenaar.
        if let Some((seq, p)) = rx.try_recv() {
            block_on(n.on_piece(seq, p, T0));
            fed += 1;
        }
        if fed == SIZE / CHUNK / 2 && asked.is_none() {
            let streaming =
                k.0.borrow()
                    .slots
                    .get(&1)
                    .is_some_and(|s| !s.placed && !s.image.is_empty());
            assert!(streaming, "slot 1 streams halfway");
            let t = std::time::Instant::now();
            let (status, body) = http_once(&mut n, Port::Leader, wire("GET", "/v1/status", ""));
            asked = Some(t.elapsed());
            assert_eq!(status, 200, "{body}");
            let (status, body) = http_once(&mut n, Port::Leader, wire("POST", "/v1/jobs", API));
            assert_eq!(status, 201, "{body}");
            assert!(body.contains("dispatched"), "{body}");
        }
        let placed = k.0.borrow().slots.values().filter(|s| s.placed).count();
        if placed == 2 && n.wanted() == 0 {
            break;
        }
    }
    let elapsed = asked.expect("the API was never asked during the download");
    assert!(elapsed < Duration::from_millis(100), "{elapsed:?}");
    let st = k.0.borrow();
    assert_eq!(st.slots[&1].job, "web");
    assert_eq!(st.slots[&1].image.len(), SIZE);
    assert_eq!(st.slots[&2].job, "api");
    assert_eq!(st.slots[&2].image, ELF);
    assert!(st.slots.values().all(|s| s.placed));
    drop(st);
    let lines = n.take_lines();
    for slot in ["slot=1", "slot=2"] {
        assert!(
            lines
                .iter()
                .any(|l| l.contains("HOP_JOB_PLACED") && l.contains(slot)),
            "{slot}: {lines:?}"
        );
    }
}

/// Een download die de node opgeeft (de taak stopt halverwege), stopt bij
/// zijn volgende brok; de kooi is weg en de volgende download loopt gewoon.
#[test]
fn a_stopped_task_cancels_its_download() {
    const SIZE: usize = 1 << 20;
    const CHUNK: usize = 64 << 10;
    let mut image = alloc::vec![0u8; SIZE];
    image[..4].copy_from_slice(b"\x7fELF");
    let k = FakeKern::new(4);
    let sys = KernSys::new(k.client(), 4);
    let mut files = BTreeMap::new();
    files.insert(String::from("http://images/web.elf"), image);
    let files = Rc::new(RefCell::new(files));
    let mut n = Node::new(&cfg(), sys, MemImages(files.clone(), CHUNK), T0);
    http_once(&mut n, Port::Leader, wire("POST", "/v1/jobs", JOB));

    let orders: Orders = Mailbox::new();
    let pieces: Pieces = Channel::new();
    let wanted = AtomicU64::new(0);
    let (tx, mut rx) = pieces.split().unwrap();
    let images = SlowImages(MemImages(files, CHUNK));
    let mut task = pin!(download_task(images, &orders, tx, &wanted));
    let mut cx = Context::from_waker(Waker::noop());
    let mut fed = 0;
    for _ in 0..10_000 {
        if let Some(o) = n.take_order() {
            orders.try_send(o).unwrap();
        }
        wanted.store(n.wanted(), Relaxed);
        let _ = task.as_mut().poll(&mut cx);
        if let Some((seq, p)) = rx.try_recv() {
            block_on(n.on_piece(seq, p, T0));
            fed += 1;
        }
        if fed == 4 {
            break;
        }
    }
    assert_eq!(n.wanted(), 1);
    // De job weg terwijl hij stroomt: de kooi gaat terug.
    let (status, _) = http_once(&mut n, Port::Leader, wire("DELETE", "/v1/jobs/web", ""));
    assert_eq!(status, 204);
    tick(&mut n, T0 + types::time::SECOND);
    for _ in 0..10_000 {
        wanted.store(n.wanted(), Relaxed);
        let _ = task.as_mut().poll(&mut cx);
        if let Some((seq, p)) = rx.try_recv() {
            block_on(n.on_piece(seq, p, T0));
        }
    }
    assert_eq!(n.wanted(), 0, "nothing is downloading");
    assert!(
        k.0.borrow().slots.is_empty(),
        "{:?}",
        k.0.borrow().slots.keys()
    );
    // De downloadtaak gaf het op: geen brokken meer onderweg.
    assert!(rx.try_recv().is_none());
    let lines = n.take_lines();
    assert!(
        lines.iter().any(|l| l.contains("HOP_JOB_FAILED")),
        "{lines:?}"
    );
}

#[test]
fn a_clean_boot_seeds_the_init_jobs_once() {
    let (mut n, k) = node();
    // De headless-vorm: één artifact en geen driver is de hop-driver.
    let specs =
        r#"[{"name":"web","artifacts":[{"url":"http://images/web.elf"}],"memory_limit":33554432}]"#;
    assert_eq!(block_on(n.seed_init_jobs(specs, T0)), Ok(1));
    block_on(n.settle(T0));
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
    tick(&mut n, T0 + types::time::SECOND);

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
    tick(&mut n, T0 + 2 * types::time::SECOND);
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

// ---- De herstart van Hop: de staat bij de kern ----------------------------------

const SECOND: u64 = types::time::SECOND;

/// Een node met web geplaatst in slot 1, en een nieuw image van Hop op de server.
fn placed_for_a_restart() -> (TestNode, FakeKern) {
    let k = FakeKern::new(4);
    let sys = KernSys::new(k.client(), 4);
    let mut n = Node::new(&cfg(), sys, MemImages::new(restart_images()), T0);
    http(&mut n, Port::Leader, wire("POST", "/v1/jobs", JOB));
    assert!(k.0.borrow().slots[&1].placed);
    n.take_lines();
    (n, k)
}

fn restart_images() -> BTreeMap<String, Vec<u8>> {
    let mut images = BTreeMap::new();
    images.insert(String::from("http://images/web.elf"), ELF.to_vec());
    images.insert(
        String::from("http://images/agentd-hopos"),
        b"\x7fELF-a-new-hop".to_vec(),
    );
    images
}

/// De volgende Hop over dezelfde kern, zoals de kern hem in slot 2 zet.
fn next_hop(k: &FakeKern) -> TestNode {
    let sys = KernSys::new(k.client(), 4);
    let mut n = Node::new(&cfg(), sys, MemImages::new(restart_images()), T0);
    n.take_lines();
    n
}

fn puts(k: &FakeKern) -> u32 {
    k.0.borrow().state_puts
}

#[test]
fn the_state_reaches_the_kernel_after_a_change_and_on_the_beat() {
    let (mut n, k) = placed_for_a_restart();
    assert_eq!(puts(&k), 0, "a request alone writes nothing yet");
    // De tik is de debounce: wat in de vorige seconde veranderde, gaat nu.
    tick(&mut n, T0 + SECOND);
    assert_eq!(puts(&k), 1);
    let blob = String::from_utf8(k.0.borrow().state.clone()).unwrap();
    assert!(blob.contains("\"web\""), "{blob}");
    assert!(
        blob.contains("\"leader\""),
        "standalone: the leader's jobs travel along"
    );
    // Niets veranderd: geen nieuwe schrijf, tot het ritme.
    tick(&mut n, T0 + 2 * SECOND);
    tick(&mut n, T0 + 3 * SECOND);
    let quiet = puts(&k);
    assert!(
        quiet <= 2,
        "{quiet}: one more at most (the leader's own debounce)"
    );
    // Een warme kern-flip: de nieuwe kern heeft niets. Het ritme legt het
    // blob opnieuw neer.
    k.0.borrow_mut().state.clear();
    tick(&mut n, T0 + 9 * SECOND);
    assert!(k.0.borrow().state.is_empty(), "not yet: the beat is 10 s");
    tick(&mut n, T0 + 14 * SECOND);
    assert!(!k.0.borrow().state.is_empty(), "the beat put it back");
}

#[test]
fn a_restarted_hop_adopts_what_the_kernel_still_runs() {
    let (mut n, k) = placed_for_a_restart();
    tick(&mut n, T0 + SECOND);
    let id = n.agent().tasks().next().unwrap().id.clone();
    drop(n);

    let mut again = next_hop(&k);
    let kept = block_on(again.restore(T0 + 2 * SECOND));
    assert_eq!(kept, Kept::Adopted(1));
    assert!(!kept.is_clean(), "no init jobs over an adopted state");
    let lines = again.take_lines();
    assert!(
        lines
            .iter()
            .any(|l| l == "hop: adopted 1 running cage(s) from the kernel's state HOP_ADOPTED n=1"),
        "{lines:?}"
    );
    // De veger laat de overgenomen kooi staan.
    block_on(again.sweep_strays());
    assert_eq!(k.0.borrow().slots.len(), 1);
    assert_eq!(again.runner().slot_of(&id), Some(runner::Slot(1)));
    let t = again.agent().task(&id).unwrap();
    assert_eq!((t.state, t.pid), (types::TaskState::Running, 1));
    // De standalone leader kent zijn job weer en telt de taak als geplaatst:
    // geen tweede plaatsing, ook niet na zijn eigen tik.
    let (_, jobs) = http(&mut again, Port::Leader, wire("GET", "/v1/jobs", ""));
    assert!(jobs.contains("\"web\""), "{jobs}");
    tick(&mut again, T0 + 12 * SECOND);
    tick(&mut again, T0 + 23 * SECOND);
    assert_eq!(k.0.borrow().slots.len(), 1, "no second web");
    assert!(
        !k.0.borrow().ops.contains(&PrivOp::StopSlot.op()),
        "nothing was stopped"
    );
    placed_matches_the_agent(&mut again);
    // De init-jobs zaaien niet in een leader met jobs.
    let specs = format!("[{JOB}]");
    assert_eq!(block_on(again.seed_init_jobs(&specs, T0)), Ok(0));
}

#[test]
fn a_task_whose_slot_stopped_is_not_adopted_and_placed_again() {
    let (mut n, k) = placed_for_a_restart();
    tick(&mut n, T0 + SECOND);
    drop(n);
    // De app viel weg terwijl Hop herstartte: het slot is leeg.
    k.0.borrow_mut().slots.clear();

    let mut again = next_hop(&k);
    assert_eq!(block_on(again.restore(T0 + 2 * SECOND)), Kept::Adopted(0));
    let lines = again.take_lines();
    assert!(
        lines.iter().any(|l| l.contains("HOP_ADOPTED n=0")),
        "{lines:?}"
    );
    assert!(
        lines.iter().any(|l| l.contains("HOP_ADOPT_SKIPPED n=1")),
        "{lines:?}"
    );
    // De leader kent de job nog, ziet hem niet geplaatst, en plaatst hem
    // meteen opnieuw (de registratie na de overname reconcilet): een nieuwe
    // taak die op zijn image wacht, geen overgenomen kooi.
    assert!(
        again.agent().tasks().all(|t| t.pid == 0),
        "{:?}",
        again.agent().tasks().collect::<Vec<_>>()
    );
    tick(&mut again, T0 + 12 * SECOND);
    assert!(k.0.borrow().slots[&1].placed, "web placed again");
    placed_matches_the_agent(&mut again);
}

#[test]
fn the_own_slot_of_hop_is_never_adopted() {
    // Een staat die het slot van Hop zelf noemt (slot 2 in deze config):
    // dat draait altijd, en is nooit een taak. Een slot dat stroomt of al
    // van een ander is, toetst de runner (`runner/src/tests.rs`).
    let (mut n, k) = placed_for_a_restart();
    tick(&mut n, T0 + SECOND);
    let id = n.agent().tasks().next().unwrap().id.clone();
    drop(n);
    let blob = String::from_utf8(k.0.borrow().state.clone()).unwrap();
    assert_eq!(blob.matches("\"pid\":1").count(), 1, "{blob}");
    k.0.borrow_mut().state = blob.replace("\"pid\":1", "\"pid\":2").into_bytes();
    let mut again = next_hop(&k);
    assert_eq!(block_on(again.restore(T0 + 2 * SECOND)), Kept::Adopted(0));
    assert_eq!(again.runner().slot_of(&id), None);
    assert!(again.agent().tasks().all(|t| t.pid != 2));
}

#[test]
fn an_unreadable_state_is_not_a_clean_one() {
    let k = FakeKern::new(4);
    k.0.borrow_mut().state = b"{\"version\":1,\"tasks\":{}}".to_vec();
    let mut n = next_hop(&k);
    let kept = block_on(n.restore(T0));
    assert_eq!(kept, Kept::Unreadable);
    assert!(!kept.is_clean());
    assert!(
        n.take_lines()
            .iter()
            .any(|l| l.contains("HOP_KERNEL_STATE_FAIL"))
    );
    // Niets: een schone start.
    let k = FakeKern::new(4);
    assert_eq!(block_on(next_hop(&k).restore(T0)), Kept::Clean);
}

#[test]
fn an_older_kernel_keeps_no_state_and_refuses_a_restart() {
    let (mut n, k) = placed_for_a_restart();
    k.0.borrow_mut().no_state_ops = true;
    k.0.borrow_mut().restart_ok = true;
    tick(&mut n, T0 + SECOND);
    let lines = n.take_lines();
    let none: Vec<&String> = lines
        .iter()
        .filter(|l| l.contains("HOP_KERNEL_STATE_NONE"))
        .collect();
    assert_eq!(none.len(), 1, "{lines:?}");
    // Niet elke tik opnieuw: alleen op het ritme.
    let calls = |k: &FakeKern| {
        k.0.borrow()
            .ops
            .iter()
            .filter(|&&o| o == PrivOp::StatePut.op())
            .count()
    };
    let before = calls(&k);
    for s in 2..9 {
        tick(&mut n, T0 + s * SECOND);
    }
    assert_eq!(calls(&k), before, "no put per tick on an older kernel");
    // Een herstart zonder staat zou elke app stoppen: 502, en Hop draait door.
    let (status, body) = http(&mut n, Port::Agent, wire("POST", "/restart", ""));
    assert_eq!(status, 502, "{body}");
    assert!(body.contains("keeps no state"), "{body}");
    let lines = n.take_lines();
    assert!(
        lines
            .iter()
            .any(|l| l == "hop: restart of Hop requested (same image) HOP_RESTART"),
        "{lines:?}"
    );
    assert!(
        lines.iter().any(|l| l.contains("HOP_RESTART_FAIL")),
        "{lines:?}"
    );
    assert!(
        !k.0.borrow().ops.contains(&PrivOp::HopRestart.op()),
        "no HOP_RESTART asked"
    );
    assert!(k.0.borrow().restarts.is_empty());
}

#[test]
fn the_stop_bell_hands_the_last_state_to_the_kernel() {
    let (mut n, k) = placed_for_a_restart();
    // Geen tik sinds de plaatsing: alleen de stop-bel legt hem nog neer.
    assert!(k.0.borrow().state.is_empty());
    block_on(n.on_stop(T0 + SECOND / 2));
    let blob = String::from_utf8(k.0.borrow().state.clone()).unwrap();
    assert!(blob.contains("\"web\""), "{blob}");
    let lines = n.take_lines();
    assert!(
        lines
            .iter()
            .any(|l| l.contains("state of 1 task(s) handed to the kernel")
                && l.ends_with("HOP_STOPPING")),
        "{lines:?}"
    );
    // En de volgende neemt hem over.
    assert_eq!(
        block_on(next_hop(&k).restore(T0 + SECOND)),
        Kept::Adopted(1)
    );
}

#[test]
fn restart_with_the_same_image() {
    let (mut n, k) = placed_for_a_restart();
    k.0.borrow_mut().restart_ok = true;
    let (status, body) = http(&mut n, Port::Agent, wire("POST", "/restart", ""));
    assert_eq!(status, 202, "{body}");
    {
        let st = k.0.borrow();
        assert_eq!(st.restarts, [(0, String::new(), 0)]);
        // De staat ging vlak vóór de vraag naar de kern, zonder tik.
        let at = |op: u8| st.ops.iter().rposition(|&o| o == op).unwrap();
        assert!(at(PrivOp::StatePut.op()) < at(PrivOp::HopRestart.op()));
        assert!(String::from_utf8_lossy(&st.state).contains("\"web\""));
    }
    let lines = n.take_lines();
    assert!(
        lines
            .iter()
            .any(|l| l == "hop: restart of Hop requested (same image) HOP_RESTART"),
        "{lines:?}"
    );
    assert!(
        lines.iter().any(|l| l
            == "hop: the kernel took the restart; this Hop stops now and the next one adopts its cages HOP_RESTART_ACCEPTED"),
        "{lines:?}"
    );
}

#[test]
fn restart_with_a_new_image_streams_it_raw_first() {
    let (mut n, k) = placed_for_a_restart();
    k.0.borrow_mut().restart_ok = true;
    let sum = "ab".repeat(32);
    let body = format!(r#"{{"url":"http://images/agentd-hopos","sha256":"{sum}"}}"#);
    let (status, reply) = http(&mut n, Port::Agent, wire("POST", "/restart", &body));
    assert_eq!(status, 202, "{reply}");
    let st = k.0.borrow();
    // Het image kwam in slot 2 onder de jobnaam van de kern, en de kern nam
    // het over (het slot is weer vrij).
    assert_eq!(st.restarts, [(2, sum, 0)]);
    assert_eq!(st.slots.keys().copied().collect::<Vec<_>>(), [1]);
    let start = PrivOp::StartSlot.op();
    let raw = st
        .frames
        .iter()
        .filter(|f| f.windows(15).any(|w| w == b"hopos.hop-image"));
    assert_eq!(raw.count(), 1, "one raw slot for the image");
    assert!(st.ops.contains(&start));
    drop(st);
    assert!(n.take_lines().iter().any(
        |l| l == "hop: restart of Hop requested (image http://images/agentd-hopos) HOP_RESTART"
    ));
}

#[test]
fn a_refused_restart_is_a_502_and_cleans_the_image_slot() {
    let (mut n, k) = placed_for_a_restart();
    // restart_ok staat uit: de kern weigert.
    let sum = "cd".repeat(32);
    let body = format!(r#"{{"url":"http://images/agentd-hopos","sha256":"{sum}"}}"#);
    let (status, reply) = http(&mut n, Port::Agent, wire("POST", "/restart", &body));
    assert_eq!(status, 502, "{reply}");
    assert!(reply.contains("kernel refused the restart"), "{reply}");
    assert!(n.take_lines().iter().any(|l| {
        l.starts_with("hop: restart of Hop failed: kernel refused the restart")
            && l.ends_with("HOP_RESTART_FAIL")
    }));
    assert_eq!(
        k.0.borrow().slots.keys().copied().collect::<Vec<_>>(),
        [1],
        "the image slot is gone, web runs on"
    );
    // Een image dat niet bestaat: geen staat en geen vraag aan de kern.
    let before = k.0.borrow().ops.len();
    let body = format!(r#"{{"url":"http://images/nope","sha256":"{sum}"}}"#);
    let (status, _) = http(&mut n, Port::Agent, wire("POST", "/restart", &body));
    assert_eq!(status, 502);
    assert!(!k.0.borrow().ops[before..].contains(&PrivOp::HopRestart.op()));
}

/// Geen net: een doorgifte kan hier niet (de herstart is een gewoon antwoord).
struct NoNet;

impl crate::fetch::Connect for NoNet {
    type Conn = Mem;
    async fn connect(&mut self, _ip: [u8; 4], _port: u16) -> Result<Mem, String> {
        Err(String::from("no network in this test"))
    }
}

/// 22-10, `qemu-test-hop-restart.sh`: het 202 op `POST /restart` kwam nooit
/// aan, want de kern stopte Hop 500 ms na zijn OK. Nu vertrekt Hop zelf, en
/// pas als zijn antwoord de deur uit is: de werker en de eigenaar om de
/// beurt zoals op de app-core, met een bel die nooit gaat.
#[test]
fn the_restart_reply_is_out_before_hop_leaves() {
    use crate::restart::{Departure, depart};
    let (mut n, k) = placed_for_a_restart();
    k.0.borrow_mut().restart_ok = true;
    // Twee werkers: de pool is niet vol, dus alleen de vlag sluit.
    let pool: Handoff<Mem> = Handoff::new(2);
    let hub = Hub::new(1);
    let leaving = core::cell::Cell::new(false);
    let out = Rc::new(RefCell::new(Vec::new()));
    // Keep-alive: zonder de vlag bleef de verbinding open tot de leestermijn.
    let mut raw = wire("POST", "/restart", "");
    let at = raw.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
    raw.splice(at..at, b"\r\nConnection: keep-alive".iter().copied());
    pool.give(Mem {
        input: raw,
        at: 0,
        out: out.clone(),
    })
    .unwrap_or_else(|_| panic!("a free worker"));

    let mut client = crate::client::Client::new(
        NoNet,
        crate::fetch::IpOnly,
        applib::rand::Rng::from_seed(b"t", || 0),
        || None,
    );
    let mut none = NoStreams;
    let written_at_leave = Rc::new(RefCell::new(None));
    let why = {
        let mut worker = pin!(async {
            let conn = pool.take(0).await;
            crate::forward::serve(
                conn,
                async |req: Request| hub.ask_routed(0, Port::Agent, req).await,
                &mut none,
                &mut client,
                Port::Agent,
                || pool.none_free() || leaving.get(),
            )
            .await
            .unwrap();
            pool.free(0);
        });
        let mut owner = pin!(async {
            hub.wait().await;
            let (slot, q) = hub.next().unwrap();
            let Question::Http(port, req) = q else {
                panic!("{q:?}")
            };
            let answer = match n.handle_routed(port, &req, T0).await {
                crate::forward::Routed::Reply(r) => Answer::Reply(r),
                crate::forward::Routed::Forward(f) => Answer::Forward(f),
            };
            assert!(n.take_leaving(), "the kernel took it: leave");
            leaving.set(true);
            hub.answer(slot, answer);
            let why = depart(
                pool.freed(slot),
                core::future::pending::<()>(),
                core::future::pending::<()>(),
            )
            .await;
            *written_at_leave.borrow_mut() = Some(String::from_utf8(out.borrow().clone()).unwrap());
            n.leave(T0 + SECOND / 10, why).await;
            why
        });
        let mut cx = Context::from_waker(Waker::noop());
        let mut why = None;
        let mut worker_done = false;
        for _ in 0..10_000 {
            if !worker_done {
                worker_done = worker.as_mut().poll(&mut cx).is_ready();
            }
            if let Poll::Ready(w) = owner.as_mut().poll(&mut cx) {
                why = Some(w);
                break;
            }
        }
        why
    };
    assert_eq!(
        why,
        Some(Departure::Replied),
        "left without a bell, after the reply"
    );
    let text = written_at_leave.borrow().clone().unwrap();
    assert!(text.starts_with("HTTP/1.1 202"), "{text}");
    assert!(text.contains("Connection: close"), "{text}");
    let lines = n.take_lines();
    assert!(
        lines.iter().any(|l| l
            == "hop: the restart reply is out; leaving for the new Hop HOP_RESTART_LEAVING why=replied"),
        "{lines:?}"
    );
    assert!(
        lines.iter().any(|l| l
            .starts_with("hop: leaving for the restart; state of 1 task(s) handed to the kernel")
            && l.ends_with("HOP_STOPPING")),
        "{lines:?}"
    );
    assert_eq!(k.0.borrow().restarts.len(), 1);
    assert!(String::from_utf8_lossy(&k.0.borrow().state).contains("\"web\""));
}

#[test]
fn the_bell_or_the_grace_ends_the_wait_for_the_reply() {
    use crate::restart::{Departure, depart};
    let never = || core::future::pending::<()>();
    let now = || core::future::ready(());
    assert_eq!(block_on(depart(never(), never(), now())), Departure::Bell);
    assert_eq!(
        block_on(depart(never(), now(), never())),
        Departure::GraceOver
    );
    assert_eq!(
        block_on(depart(now(), never(), now())),
        Departure::Bell,
        "the bell wins"
    );
}

// ---- De logs van de kern en van Hop zelf ----------------------------------------

#[test]
fn the_kernel_log_reaches_kern_on_the_agent_and_through_the_leader() {
    let (mut n, k) = node();
    k.0.borrow_mut().kern_logs.extend([
        b"HOPOS_BOOT v3.0.17".to_vec(),
        b"HOPOS_TICK 1 late_ms=0".to_vec(),
    ]);
    tick(&mut n, T0 + SECOND);
    assert!(k.0.borrow().kern_logs.is_empty(), "the tick pumped slot 0");
    let (status, body) = http(&mut n, Port::Agent, wire("GET", "/logs/kern/stdout", ""));
    assert_eq!(status, 200, "{body}");
    assert!(
        body.contains("HOPOS_BOOT v3.0.17") && body.contains("HOPOS_TICK 1"),
        "{body}"
    );
    // Via de leader, zoals het dashboard hem vraagt: de levende tail.
    let dash = signed(Method::Get, "/v1/agents/n1/logs/kern/stdout");
    let Reply::Stream { ask, .. } = block_on(n.handle(Port::Leader, &dash, T0)) else {
        panic!("the dashboard's kern log is no live tail");
    };
    let c = n.poll(&ask, T0);
    assert_eq!(
        (c.text.as_str(), c.seq, c.done),
        (
            "data: HOPOS_BOOT v3.0.17\n\ndata: HOPOS_TICK 1 late_ms=0\n\n",
            2,
            false
        )
    );
    // En hij loopt door: een nieuwe regel van de kern komt er als enige bij.
    k.0.borrow_mut()
        .kern_logs
        .push_back(b"HOPOS_TICK 2".to_vec());
    tick(&mut n, T0 + 2 * SECOND);
    let again = Ask::Logs {
        task_id: String::from("kern"),
        stream: api::LogStream::Stdout,
        seq: c.seq,
    };
    let c2 = n.poll(&again, T0 + 2 * SECOND);
    assert_eq!(
        (c2.text.as_str(), c2.done),
        ("data: HOPOS_TICK 2\n\n", false)
    );
    n.stream_done();
}

#[test]
fn an_older_kernel_without_its_own_log_says_so_once() {
    let (mut n, k) = node();
    k.0.borrow_mut().no_kern_logs = true;
    for s in 1..=10 {
        tick(&mut n, T0 + s * SECOND);
    }
    let lines = n.take_lines();
    let none: Vec<&String> = lines
        .iter()
        .filter(|l| l.contains("HOP_KERN_LOG_NONE"))
        .collect();
    assert_eq!(none.len(), 1, "{lines:?}");
    // Eén vraag naar slot 0 in tien tikken: daarna pas na een minuut.
    let asked =
        k.0.borrow()
            .frames
            .iter()
            .filter_map(|f| {
                abi::hopabi::decode_req(&f[abi::systemapi::HEADER_LEN..])
                    .ok()
                    .map(|r| (r.op, r.off))
            })
            .filter(|&(op, off)| op == PrivOp::NextLog.op() && off == 0)
            .count();
    assert_eq!(asked, 1);
    // Een flip naar een nieuwe kern: na de minuut komen de regels, met één regel.
    k.0.borrow_mut().no_kern_logs = false;
    k.0.borrow_mut()
        .kern_logs
        .push_back(b"HOPOS_FLIP_SETTLED".to_vec());
    tick(&mut n, T0 + 70 * SECOND);
    assert!(n.take_lines().iter().any(|l| l.contains("HOP_KERN_LOG_ON")));
    let (_, body) = http(&mut n, Port::Agent, wire("GET", "/logs/kern/stdout", ""));
    assert!(body.contains("HOPOS_FLIP_SETTLED"), "{body}");
}

#[test]
fn hop_keeps_its_own_lines_in_the_ring_of_hop() {
    let (mut n, _k) = node();
    // De regels van de node (HOP_LEADER bij de start) gaan erin zodra de
    // binary ze op de console zet (take_lines), en de regels van de andere
    // taken via hop_line.
    let lines = n.take_lines();
    assert!(lines.iter().any(|l| l.contains("HOP_LEADER")), "{lines:?}");
    n.hop_line("hop: agent up node=n1 HOP_UP");
    let (status, body) = http(&mut n, Port::Agent, wire("GET", "/logs/hop/stdout", ""));
    assert_eq!(status, 200, "{body}");
    assert!(
        body.contains("HOP_LEADER") && body.contains("HOP_UP"),
        "{body}"
    );
    let snap = signed(Method::Get, "/v1/agents/n1/logs/hop/stdout?follow=0");
    let Reply::Events { lines, .. } = block_on(n.handle(Port::Leader, &snap, T0)) else {
        panic!("no snapshot of hop");
    };
    assert_eq!(
        lines.last().map(String::as_str),
        Some("hop: agent up node=n1 HOP_UP")
    );
}
