//! De tests van de host-backends: de Go-tests naam voor naam, plus wat de Rust-vorm nieuw maakt.
//!
//! Alles draait zonder Docker (een nep-daemon op een tijdelijke unix-socket)
//! en zonder netwerk (een server-thread op 127.0.0.1). Wat echt Docker
//! nodig heeft, draait alleen met `HOP_TEST_DOCKER=1`; wat root of
//! `sandbox-exec` nodig heeft, toetst dat eerst.

mod docker;
mod download;
mod extract;
mod isolation;
mod process;

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use super::{HostConfig, HostRunner, TaskSpec};
use crate::{LogPolicy, Stream};

/// Een tijdelijke map die bij `Drop` weggaat.
pub(super) struct TempDir(PathBuf);

impl TempDir {
    pub(super) fn new(name: &str) -> Self {
        static N: AtomicU32 = AtomicU32::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        let p = std::env::temp_dir().join(format!("hop-rt-{}-{n}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        // macOS: /var is een symlink naar /private/var; het echte pad houdt
        // sandbox-profielen en vergelijkingen eerlijk.
        Self(std::fs::canonicalize(&p).unwrap())
    }

    pub(super) fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Een config zonder isolatie onder `base`.
pub(super) fn config(base: &Path, isolate: bool) -> HostConfig {
    HostConfig {
        rootfs_base: base.to_path_buf(),
        isolate,
        docker_socket: base.join("no-docker.sock"),
        logs: LogPolicy::DEFAULT,
    }
}

/// Een exec-spec met `command`.
pub(super) fn spec(id: &str, command: &str) -> TaskSpec {
    TaskSpec {
        task_id: id.to_string(),
        job_name: id.to_string(),
        command: command.to_string(),
        ..TaskSpec::default()
    }
}

/// Milliseconden sinds `t0`: de klok die de tests aan de runner geven.
pub(super) fn ms(t0: Instant) -> u64 {
    u64::try_from(t0.elapsed().as_millis()).unwrap()
}

/// Tikt tot `done` waar is of `limit` verstreken is; geeft of het lukte.
pub(super) fn tick_until(
    r: &mut HostRunner,
    t0: Instant,
    limit: Duration,
    mut done: impl FnMut(&mut HostRunner, u64) -> bool,
) -> bool {
    let end = Instant::now() + limit;
    while Instant::now() < end {
        let now = ms(t0);
        r.tick(now);
        if done(r, now) {
            return true;
        }
        thread::sleep(Duration::from_millis(10));
    }
    false
}

/// De regels van een ring als één tekst.
pub(super) fn text(r: &HostRunner, now: u64, id: &str, s: Stream) -> String {
    r.logs(now, id, s)
        .map(|ring| ring.tail().collect::<Vec<_>>().join("\n"))
        .unwrap_or_default()
}

/// Een HTTP-server die één verzoek aanneemt, `answer` schrijft en de kop teruggeeft.
pub(super) fn one_shot(answer: Vec<u8>) -> (String, thread::JoinHandle<String>) {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap().to_string();
    let h = thread::spawn(move || {
        let (s, _) = l.accept().unwrap();
        let mut r = BufReader::new(s.try_clone().unwrap());
        let mut head = String::new();
        loop {
            let mut line = String::new();
            if r.read_line(&mut line).unwrap() == 0 || line == "\r\n" {
                break;
            }
            head.push_str(&line);
        }
        let mut s = s;
        s.write_all(&answer).unwrap();
        head
    });
    (addr, h)
}

/// Een `200 OK` met `body`.
pub(super) fn ok(body: &[u8]) -> Vec<u8> {
    let mut v = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();
    v.extend_from_slice(body);
    v
}

/// Een tar met `files` (ustar, alleen gewone bestanden).
pub(super) fn tar(files: &[(&str, &[u8])]) -> Vec<u8> {
    let mut out = Vec::new();
    for (name, data) in files {
        out.extend_from_slice(&tar_header(name, data.len() as u64, b'0'));
        out.extend_from_slice(data);
        out.resize(out.len().div_ceil(512) * 512, 0);
    }
    out.resize(out.len() + 1024, 0);
    out
}

/// Eén ustar-kop.
pub(super) fn tar_header(name: &str, size: u64, kind: u8) -> [u8; 512] {
    let mut h = [0u8; 512];
    h[..name.len()].copy_from_slice(name.as_bytes());
    h[100..107].copy_from_slice(b"0000644");
    h[108..115].copy_from_slice(b"0000000");
    h[116..123].copy_from_slice(b"0000000");
    h[124..135].copy_from_slice(format!("{size:011o}").as_bytes());
    h[136..147].copy_from_slice(b"00000000000");
    h[156] = kind;
    h[257..263].copy_from_slice(b"ustar\0");
    h[263..265].copy_from_slice(b"00");
    h[148..156].copy_from_slice(b"        ");
    let sum: u32 = h.iter().map(|&b| u32::from(b)).sum();
    h[148..155].copy_from_slice(format!("{sum:06o}\0").as_bytes());
    h
}

/// Voert `data` door een systeemcommando en geeft de uitvoer.
pub(super) fn pipe(cmd: &str, args: &[&str], data: &[u8]) -> Vec<u8> {
    let mut c = Command::new(cmd)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = c.stdin.take().unwrap();
    let data = data.to_vec();
    let w = thread::spawn(move || stdin.write_all(&data).unwrap());
    let mut out = Vec::new();
    c.stdout.take().unwrap().read_to_end(&mut out).unwrap();
    w.join().unwrap();
    assert!(c.wait().unwrap().success());
    out
}

/// gzip via het systeem (`-9`: dynamische Huffman-blokken).
pub(super) fn gzip(data: &[u8]) -> Vec<u8> {
    pipe("gzip", &["-9", "-n", "-c"], data)
}

/// Rauwe deflate: gzip zonder zijn kop van 10 bytes (`-n`: geen naam) en trailer van 8.
pub(super) fn deflate(data: &[u8]) -> Vec<u8> {
    let gz = gzip(data);
    gz[10..gz.len() - 8].to_vec()
}

/// De CRC-32 van `data` via de eigen implementatie.
pub(super) fn crc(data: &[u8]) -> u32 {
    let mut c = super::extract::Crc32::new();
    c.update(data);
    c.sum()
}

/// Een zip met `files`; `deflated` kiest methode 8, anders 0.
pub(super) fn zip(files: &[(&str, &[u8])], deflated: bool) -> Vec<u8> {
    let mut out = Vec::new();
    let mut central = Vec::new();
    for (name, data) in files {
        let offset = out.len() as u32;
        let (method, body) = if deflated {
            (8u16, deflate(data))
        } else {
            (0u16, data.to_vec())
        };
        let c = crc(data);
        let mut local = Vec::new();
        local.extend_from_slice(&0x0403_4b50u32.to_le_bytes());
        local.extend_from_slice(&20u16.to_le_bytes());
        local.extend_from_slice(&0u16.to_le_bytes());
        local.extend_from_slice(&method.to_le_bytes());
        local.extend_from_slice(&[0, 0, 0, 0]);
        local.extend_from_slice(&c.to_le_bytes());
        local.extend_from_slice(&(body.len() as u32).to_le_bytes());
        local.extend_from_slice(&(data.len() as u32).to_le_bytes());
        local.extend_from_slice(&(name.len() as u16).to_le_bytes());
        local.extend_from_slice(&0u16.to_le_bytes());
        local.extend_from_slice(name.as_bytes());
        out.extend_from_slice(&local);
        out.extend_from_slice(&body);
        central.extend_from_slice(&0x0201_4b50u32.to_le_bytes());
        central.extend_from_slice(&0x0314u16.to_le_bytes()); // Unix, 2.0
        central.extend_from_slice(&20u16.to_le_bytes());
        central.extend_from_slice(&0u16.to_le_bytes());
        central.extend_from_slice(&method.to_le_bytes());
        central.extend_from_slice(&[0, 0, 0, 0]);
        central.extend_from_slice(&c.to_le_bytes());
        central.extend_from_slice(&(body.len() as u32).to_le_bytes());
        central.extend_from_slice(&(data.len() as u32).to_le_bytes());
        central.extend_from_slice(&(name.len() as u16).to_le_bytes());
        central.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0]);
        central.extend_from_slice(&(0o100_644u32 << 16).to_le_bytes());
        central.extend_from_slice(&offset.to_le_bytes());
        central.extend_from_slice(name.as_bytes());
    }
    let cd_off = out.len() as u32;
    out.extend_from_slice(&central);
    out.extend_from_slice(&0x0605_4b50u32.to_le_bytes());
    out.extend_from_slice(&[0, 0, 0, 0]);
    out.extend_from_slice(&(files.len() as u16).to_le_bytes());
    out.extend_from_slice(&(files.len() as u16).to_le_bytes());
    out.extend_from_slice(&(central.len() as u32).to_le_bytes());
    out.extend_from_slice(&cd_off.to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes());
    out
}

/// Een antwoord van de nep-daemon.
pub(super) struct Reply {
    pub(super) status: u16,
    pub(super) body: Vec<u8>,
}

impl Reply {
    pub(super) fn new(status: u16, body: &str) -> Self {
        Self {
            status,
            body: body.as_bytes().to_vec(),
        }
    }
}

/// Een nep-Docker-daemon op een tijdelijke unix-socket (Go: de `do`-naad).
///
/// Elke verbinding krijgt een eigen thread; `handler` krijgt methode en
/// pad en geeft het antwoord. Elk verzoek gaat ook als `"METHODE pad"` over
/// het kanaal, zodat een test kan zien wat er gevraagd werd.
pub(super) fn fake_docker(
    dir: &Path,
    handler: impl Fn(&str, &str) -> Reply + Send + Sync + Copy + 'static,
) -> (PathBuf, mpsc::Receiver<String>) {
    let sock = dir.join("d.sock");
    let l = UnixListener::bind(&sock).unwrap();
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        for s in l.incoming() {
            let Ok(s) = s else { return };
            let tx = tx.clone();
            thread::spawn(move || {
                let mut r = BufReader::new(s.try_clone().unwrap());
                let mut first = String::new();
                if r.read_line(&mut first).unwrap_or(0) == 0 {
                    return;
                }
                let mut len = 0usize;
                loop {
                    let mut line = String::new();
                    if r.read_line(&mut line).unwrap_or(0) == 0 || line == "\r\n" {
                        break;
                    }
                    if let Some(v) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                        len = v.trim().parse().unwrap_or(0);
                    }
                }
                let mut body = vec![0u8; len];
                let _ = r.read_exact(&mut body);
                let mut parts = first.split(' ');
                let (method, path) = (parts.next().unwrap_or(""), parts.next().unwrap_or(""));
                let _ = tx.send(format!("{method} {path}"));
                let reply = handler(method, path);
                let mut s = s;
                let head = format!(
                    "HTTP/1.1 {} X\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    reply.status,
                    reply.body.len()
                );
                let _ = s.write_all(head.as_bytes());
                let _ = s.write_all(&reply.body);
            });
        }
    });
    (sock, rx)
}

/// Een lege env-map (voor leesbaarheid in de tests).
pub(super) fn map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}
