//! De host-daemon van begin tot eind: een nep-Hop over echte HTTP, de
//! daemon met zijn threads, en vragen over echte UDP.
//!
//! Go's SSE-toetsen draaiden tegen een `httptest`-server; hun logica staat
//! naam voor naam in de simulatie van `src/watcher/tests.rs`. Deze toets
//! bewijst de draad eromheen: hoplib's host-agent (status, agents, tasks,
//! de terugval van `/v1/jobs/{naam}/status` op een 404 zoals Hop t/m
//! alpha.10), de SSE-lezer, de berichten naar de server-thread, en het
//! antwoord op de socket.

#![cfg(feature = "std")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::io::{Read, Write};
use std::net::{Ipv4Addr, TcpListener, TcpStream, UdpSocket};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;
use std::time::{Duration, Instant};

use hopdns::Flags;
use hopdns::host::Daemon;
use hopdns::wire::{Message, decode, encode_query, rtype};

/// Wat de toets de nep-Hop laat doen.
enum Cmd {
    /// Zet de taken van een job: (id, staat).
    SetJob(&'static str, Vec<(&'static str, &'static str)>),
    /// Stuur een gebeurtenis naar elke open stroom.
    Event(&'static str, String),
}

/// De nep-Hop: één thread bezit de jobs en de open stromen; de toets stuurt
/// opdrachten.
fn fake_hop() -> (u16, Sender<Cmd>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    listener.set_nonblocking(true).unwrap();
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || serve_hop(&listener, &rx));
    (port, tx)
}

fn serve_hop(listener: &TcpListener, rx: &Receiver<Cmd>) {
    let mut jobs: Vec<(&str, Vec<(&str, &str)>)> = Vec::new();
    let mut streams: Vec<TcpStream> = Vec::new();
    loop {
        while let Ok(cmd) = rx.try_recv() {
            match cmd {
                Cmd::SetJob(name, tasks) => {
                    jobs.retain(|(n, _)| *n != name);
                    jobs.push((name, tasks));
                }
                Cmd::Event(kind, data) => {
                    let frame = format!("event: {kind}\ndata: {data}\n\n");
                    streams.retain_mut(|s| s.write_all(frame.as_bytes()).is_ok());
                }
            }
        }
        match listener.accept() {
            Ok((mut conn, _)) => {
                conn.set_nonblocking(false).unwrap();
                if let Some(path) = read_path(&mut conn) {
                    if path == "/v1/events" {
                        let head = "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\r\nevent: ping\ndata: {}\n\n";
                        if conn.write_all(head.as_bytes()).is_ok() {
                            streams.push(conn);
                        }
                    } else {
                        let (status, body) = route(&path, &jobs);
                        let reply = format!(
                            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        );
                        let _ = conn.write_all(reply.as_bytes());
                    }
                }
            }
            Err(_) => thread::sleep(Duration::from_millis(5)),
        }
    }
}

/// Het pad van een GET.
fn read_path(conn: &mut TcpStream) -> Option<String> {
    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    while !buf.ends_with(b"\r\n\r\n") {
        if conn.read(&mut byte).ok()? == 0 {
            return None;
        }
        buf.push(byte[0]);
    }
    let head = String::from_utf8(buf).ok()?;
    head.split(' ').nth(1).map(String::from)
}

fn route(path: &str, jobs: &[(&str, Vec<(&str, &str)>)]) -> (&'static str, String) {
    match path {
        "/v1/status" => (
            "200 OK",
            r#"{"cluster_name":"test-cluster","agents":1}"#.into(),
        ),
        "/v1/agents" => (
            "200 OK",
            r#"[{"id":"agent1","endpoint":"http://10.9.8.7:8080"}]"#.into(),
        ),
        "/v1/tasks" => {
            let tasks: Vec<String> = jobs
                .iter()
                .flat_map(|(job, ts)| {
                    ts.iter().map(move |(id, state)| {
                        format!(r#"{{"id":"{id}","job_name":"{job}","state":"{state}"}}"#)
                    })
                })
                .collect();
            (
                "200 OK",
                format!(r#"{{"tasks_by_agent":{{"agent1":[{}]}}}}"#, tasks.join(",")),
            )
        }
        _ => ("404 Not Found", r#"{"error":"not found"}"#.into()),
    }
}

fn ask(sock: &UdpSocket, name: &str) -> Message {
    let mut q = [0u8; 512];
    let n = encode_query(7, name, rtype::A, None, &mut q).unwrap();
    sock.send(&q[..n]).unwrap();
    let mut buf = [0u8; 1500];
    let len = sock.recv(&mut buf).unwrap();
    decode(&buf[..len]).unwrap()
}

/// Vraagt tot `ok` het antwoord goedkeurt, hooguit tien seconden.
fn until(sock: &UdpSocket, name: &str, ok: impl Fn(&Message) -> bool) -> Message {
    let start = Instant::now();
    loop {
        let m = ask(sock, name);
        if ok(&m) {
            return m;
        }
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "{name}: last answer {m:?}"
        );
        thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn host_daemon_end_to_end() {
    let (port, hop) = fake_hop();
    hop.send(Cmd::SetJob(
        "web",
        vec![("t1", "running"), ("t2", "running")],
    ))
    .unwrap();
    let peer = format!("http://127.0.0.1:{port}");
    let flags = Flags::parse(["-listen", "127.0.0.1:0", "-peer", &peer]).unwrap();
    let daemon = Daemon::start(&flags).unwrap();
    let addr = daemon.local_addr().unwrap();
    thread::spawn(move || daemon.serve());

    let sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    sock.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    sock.connect(addr).unwrap();
    let agent_ip = Ipv4Addr::new(10, 9, 8, 7);

    // De zaai na de ping: web op het adres van zijn agent, één keer.
    let m = until(&sock, "web.hop.local", |m| m.answers.len() == 1);
    assert_eq!(m.a_records().collect::<Vec<_>>(), [agent_ip]);
    let m = ask(&sock, "web.test-cluster.hop.local");
    assert_eq!(m.a_records().collect::<Vec<_>>(), [agent_ip]);
    assert_eq!(ask(&sock, "nope.hop.local").rcode(), 3);

    // Een nieuwe job via een melding: de job-route geeft 404, hoplib valt
    // terug op agents plus tasks.
    hop.send(Cmd::SetJob("api", vec![("t3", "running")]))
        .unwrap();
    hop.send(Cmd::Event("job", r#"{"name":"api"}"#.into()))
        .unwrap();
    until(&sock, "api.hop.local", |m| m.answers.len() == 1);

    // Een crash leegt de job: bekend, zonder adres (NOERROR met SOA).
    hop.send(Cmd::SetJob("web", vec![("t1", "failed"), ("t2", "failed")]))
        .unwrap();
    hop.send(Cmd::Event(
        "task",
        r#"{"job":"web","event":"crash"}"#.into(),
    ))
    .unwrap();
    let m = until(&sock, "web.hop.local", |m| m.answers.is_empty());
    assert_eq!((m.rcode(), m.authority.len()), (0, 1));
}
