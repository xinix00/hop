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
use hop_http::Reply;
use hopos_runner::KernSys;
use hopos_runner::fake::{FakeKern, Spin};
use leanhttp::{AsyncRead, AsyncWrite, Close, IoError};
use types::json::Value;

use crate::env::{BootConfig, BootError};
use crate::{Hub, Images, Node, Port, Sink};

const KEY: &[u8] = b"test-key";
const T0: u64 = 1_788_220_800 * types::time::SECOND;
const ELF: &[u8] = b"\x7fELF-the-web-app-image-bytes";
const JOB: &str = r#"{"name":"web","artifacts":[{"url":"http://images/web.elf"}],"cpu_shares":1024,"memory_limit":33554432}"#;

/// Artifacts uit het geheugen, in brokken van 8 bytes.
struct MemImages(BTreeMap<String, Vec<u8>>);

impl Images for MemImages {
    fn fetch(&mut self, url: &str, sink: &mut dyn Sink) -> Result<(), String> {
        let bytes = self.0.get(url).ok_or_else(|| format!("404 {url}"))?;
        sink.begin(bytes.len() as u64)?;
        for c in bytes.chunks(8) {
            sink.chunk(c)?;
        }
        Ok(())
    }
}

type TestNode = Node<
    KernSys<
        applib::sys::Client<hopos_runner::fake::FakeDial, hopos_runner::fake::NeverTimer>,
        Spin,
    >,
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
    let sys = KernSys::new(k.client(), Spin, 4);
    let mut images = BTreeMap::new();
    images.insert(String::from("http://images/web.elf"), ELF.to_vec());
    (Node::new(&cfg(), sys, MemImages(images), T0), k)
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

/// Stuurt `raw` door leanhttp en hop-http naar `port` van de node; het antwoord als (status, body).
fn http(node: &mut TestNode, port: Port, raw: Vec<u8>) -> (u16, String) {
    let out = Rc::new(RefCell::new(Vec::new()));
    let conn = Mem {
        input: raw,
        at: 0,
        out: out.clone(),
    };
    block_on(hop_http::serve(conn, async |req: Request| {
        node.handle(port, &req, T0)
    }))
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
    n.tick(T0 + 6 * types::time::SECOND);
    assert!(k.0.borrow().ops.contains(&PrivOp::SlotStatus.op()));
    assert_eq!(
        n.agent().tasks().next().unwrap().state,
        types::TaskState::Running
    );
    // De tik schreef de veranderde agent-staat naar hopfs.
    let saved =
        k.0.borrow()
            .files
            .get(hopos_runner::STATE_PATH)
            .cloned()
            .unwrap();
    assert!(String::from_utf8(saved).unwrap().contains("\"web\""));
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
    let sys = KernSys::new(k.client(), Spin, 4);
    let mut images = BTreeMap::new();
    images.insert(String::from("http://images/web.elf"), ELF.to_vec());
    let mut n = Node::new(&cfg(), sys, MemImages(images), T0);
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
    let sys = KernSys::new(k.client(), Spin, 4);
    let mut n = Node::new(&cfg(), sys, MemImages(BTreeMap::new()), T0);
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
fn the_hub_carries_a_request_and_its_answer() {
    let hub = Hub::new(2);
    let req = Request::new(Method::Get, "/health", b"");
    let mut ask = pin!(hub.ask(1, Port::Agent, req.clone()));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(ask.as_mut().poll(&mut cx).is_pending());
    assert!(block_on_ready(hub.wait()));
    let (slot, port, got) = hub.next().unwrap();
    assert_eq!((slot, port, got), (1, Port::Agent, req));
    hub.answer(1, Reply::Plain(api::Response::empty(204)));
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
fn a_new_resident_adopts_the_cages_of_the_old_one() {
    let (mut n, k) = node();
    http(&mut n, Port::Leader, wire("POST", "/v1/jobs", JOB));
    n.tick(T0 + types::time::SECOND);
    // Een nieuwe bewoner over dezelfde kern: de staat komt uit hopfs.
    let sys = KernSys::new(k.client(), Spin, 4);
    let mut again = Node::new(&cfg(), sys, MemImages(BTreeMap::new()), T0);
    assert_eq!(again.restore().unwrap(), 1);
    assert_eq!(again.runner().cages_in_use(), 1);
    assert_eq!(again.agent().tasks().next().unwrap().job_name, "web");
}
