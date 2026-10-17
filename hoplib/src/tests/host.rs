//! De host-module tegen een echte socket: een kleine HTTP-server op
//! 127.0.0.1 die de echte leader-API van hop draait (met de HMAC-toets).

use super::*;
use crate::host::Agent;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::string::String;
use std::sync::mpsc;
use std::thread;

/// Leest één verzoek (kop en body) van `s` als `api::Request`.
fn read_request(s: &mut TcpStream) -> Option<api::Request> {
    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    while !buf.ends_with(b"\r\n\r\n") {
        if s.read(&mut byte).ok()? == 0 {
            return None;
        }
        buf.push(byte[0]);
    }
    let head = String::from_utf8(buf).ok()?;
    let mut lines = head.split("\r\n");
    let mut first = lines.next()?.split(' ');
    let (method, target) = (first.next()?, first.next()?);
    let mut headers = Vec::new();
    let mut len = 0usize;
    for l in lines.filter(|l| !l.is_empty()) {
        let (k, v) = l.split_once(':')?;
        let v = v.trim();
        if k.eq_ignore_ascii_case("content-length") {
            len = v.parse().ok()?;
        }
        headers.push((k.to_string(), v.to_string()));
    }
    let mut body = vec![0u8; len];
    s.read_exact(&mut body).ok()?;
    let mut r = api::Request::new(Method::parse(method), target, &body);
    r.headers = headers;
    Some(r)
}

fn write_response(s: &mut TcpStream, status: u16, ctype: &str, body: &[u8]) {
    let head = alloc::format!(
        "HTTP/1.1 {status} X\r\nContent-Type: {ctype}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let _ = s.write_all(head.as_bytes());
    let _ = s.write_all(body);
}

/// Start een server; elke verbinding gaat naar `handle`. Geeft het adres.
fn serve(handle: impl Fn(api::Request, &mut TcpStream) + Send + 'static) -> String {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap().to_string();
    thread::spawn(move || {
        for s in l.incoming() {
            let Ok(mut s) = s else { continue };
            if let Some(req) = read_request(&mut s) {
                handle(req, &mut s);
            }
        }
    });
    addr
}

/// Een server die de leader-API van hop draait; `/v1/tasks` geeft 404 (een
/// oude leader), en `/tasks` antwoordt als agent.
fn leader_server() -> String {
    serve(|req, s| {
        if req.path == "/tasks" {
            let list = vec![task("t9", "api", 32000).to_value().unwrap()];
            let body = types::json::to_string(&types::json::Value::Array(list)).unwrap();
            return write_response(s, 200, "application/json", body.as_bytes());
        }
        let mut cl = cluster();
        // De agents staan op dit adres zelf, zodat de terugval hier landt.
        let me = s.local_addr().unwrap().to_string();
        for a in &mut cl.agents {
            a.endpoint = alloc::format!("http://{me}");
        }
        let api = LeaderApi::new(KEY.as_bytes(), "prod");
        let (r, effect) = api.handle(&mut cl, 0, &req);
        let r = match effect {
            LeaderEffect::Tasks { .. } => api::Response::error(404, "not found"),
            _ => r,
        };
        write_response(s, r.status, "application/json", &r.body);
    })
}

#[test]
fn host_agent_talks_to_the_leader_api() {
    let addr = leader_server();
    let agent = Agent::new(Client::new(&addr, Some(KEY)).unwrap());
    let st = agent.status().unwrap();
    assert_eq!(st.cluster_name, "prod");
    assert_eq!(agent.jobs().unwrap().len(), 2);
    assert_eq!(agent.job("api").unwrap().unwrap().command, "./api");
    let web = Job::from_json(br#"{"name":"web","command":"./web"}"#).unwrap();
    assert_eq!(agent.apply(&web).unwrap().status, "dispatched");
    assert_eq!(agent.delete("db"), Ok(()));

    let bad = Agent::new(Client::new(&addr, Some("wrong")).unwrap());
    assert!(matches!(bad.status(), Err(Error::Status { code: 401, .. })));
}

#[test]
fn host_tasks_fall_back_to_the_agents() {
    let addr = leader_server();
    let agent = Agent::new(Client::new(&addr, Some(KEY)).unwrap());
    let all = agent.tasks().unwrap();
    assert_eq!(all.len(), 2);
    for at in &all {
        assert_eq!(at.tasks.as_ref().unwrap()[0].id, "t9");
    }
    // Ook de takentabel van één job valt terug (alpha.10 kent de route niet).
    let st = agent.job_status("api").unwrap();
    assert_eq!(st.agents.len(), 2);
    assert_eq!(st.tasks.len(), 2);
    assert!(agent.job_status("db").unwrap().tasks.is_empty());
}

#[test]
fn host_net_errors_are_errors() {
    // Een poort waar niemand luistert.
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap().to_string();
    drop(l);
    let agent = Agent::new(Client::new(&addr, None).unwrap());
    assert!(matches!(agent.status(), Err(Error::Net(_))));
}

#[test]
fn host_stream_reads_events_and_reconnects() {
    let (tx, rx) = mpsc::channel::<u32>();
    let addr = serve(move |req, s| {
        assert_eq!(req.path, "/v1/events");
        let n = rx.recv().unwrap_or(0);
        if n == 2 {
            return write_response(s, 401, "application/json", br#"{"error":"unauthorized"}"#);
        }
        let head =
            "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n";
        let _ = s.write_all(head.as_bytes());
        let _ = s.write_all(event_log_bytes().as_bytes());
        // De stroom sluit: de lezer moet opnieuw verbinden.
    });
    for n in [1, 2, 3] {
        tx.send(n).unwrap();
    }
    let agent = Agent::new(Client::new(&addr, Some(KEY)).unwrap());
    let fast = Backoff::new(Duration::from_millis(1), Duration::from_millis(4));
    let mut s = agent.stream().with_backoff(fast);
    let kinds: Vec<String> = (0..5).map(|_| s.next().unwrap().kind).collect();
    assert_eq!(kinds, ["ping", "job", "task", "agent", "status"]);
    assert_eq!(s.next(), Err(Error::Closed));
    let err = s.next().unwrap_err();
    assert_eq!(err.to_string(), "status 401: unauthorized");
    assert_eq!(s.next().unwrap().kind, "ping");

    // De lus: tot de callback stopt, met de breuken in on_lost.
    tx.send(4).unwrap();
    let mut seen = Vec::new();
    let mut lost = 0;
    let agent2 = Agent::new(Client::new(&addr, Some(KEY)).unwrap());
    agent2.events(
        |e| {
            seen.push(e.kind.clone());
            if e.kind == "status" {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        },
        |_, _| lost += 1,
    );
    assert_eq!(seen.len(), 5);
    assert_eq!(lost, 0);
}

use core::ops::ControlFlow;
