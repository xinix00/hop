//! De client tegen een echte socket op 127.0.0.1: een server-thread die één
//! verzoek leest en een vast antwoord schrijft.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::thread;
use std::time::Duration;

use crate::{Call, Error, Http};

const T: Duration = Duration::from_secs(5);

/// Start een server die één verzoek aanneemt, de kop (en body) teruggeeft
/// via de join-handle, en `answer` schrijft.
fn one_shot(answer: &'static str) -> (String, thread::JoinHandle<String>) {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap().to_string();
    let h = thread::spawn(move || {
        let (s, _) = l.accept().unwrap();
        let mut r = BufReader::new(s.try_clone().unwrap());
        let mut head = String::new();
        let mut len = 0usize;
        loop {
            let mut line = String::new();
            r.read_line(&mut line).unwrap();
            if let Some(v) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                len = v.trim().parse().unwrap();
            }
            head.push_str(&line);
            if line == "\r\n" {
                break;
            }
        }
        let mut body = vec![0u8; len];
        r.read_exact(&mut body).unwrap();
        head.push_str(&String::from_utf8(body).unwrap());
        let mut s = s;
        s.write_all(answer.as_bytes()).unwrap();
        head
    });
    (addr, h)
}

#[test]
fn request_reads_status_headers_and_body() {
    let (addr, h) = one_shot(
        "HTTP/1.1 201 Created\r\nContent-Type: application/json\r\nContent-Length: 11\r\n\r\n{\"ok\":true}",
    );
    let url = format!("http://{addr}/v1/jobs");
    let call = Call {
        method: "POST",
        url: &url,
        headers: &[("X-Hop-Auth", "abc")],
        body: Some(b"{\"name\":\"a\"}"),
        timeout: T,
    };
    let r = Http::new().request(&call, 1 << 20).unwrap();
    assert_eq!(r.status, 201);
    assert_eq!(r.header("content-type"), Some("application/json"));
    assert_eq!(r.body, b"{\"ok\":true}");
    let seen = h.join().unwrap();
    assert!(seen.starts_with("POST /v1/jobs HTTP/1.1\r\n"));
    assert!(seen.contains("X-Hop-Auth: abc\r\n"));
    assert!(seen.ends_with("{\"name\":\"a\"}"));
}

#[test]
fn stream_writes_body_and_rejects_error_status() {
    let (addr, _h) = one_shot("HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello");
    let url = format!("http://{addr}/a");
    let mut out = Vec::new();
    let mut seen = 0;
    let n = Http::new()
        .stream(&Call::get(&url, T), &mut out, &mut |got, _| seen = got)
        .unwrap();
    assert_eq!((n, seen), (5, 5));
    assert_eq!(out, b"hello");

    let (addr, _h) = one_shot("HTTP/1.1 404 Not Found\r\nContent-Length: 4\r\n\r\ngone");
    let url = format!("http://{addr}/a");
    let err = Http::new()
        .stream(&Call::get(&url, T), &mut Vec::new(), &mut |_, _| {})
        .unwrap_err();
    assert_eq!(
        err,
        Error::Status {
            code: 404,
            body: "gone".to_string()
        }
    );
}

#[test]
fn dial_failure_says_where() {
    // Een poort waar niemand luistert: de zin noemt de host.
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap().to_string();
    drop(l);
    let url = format!("http://{addr}/");
    let err = Http::new()
        .request(&Call::get(&url, Duration::from_secs(1)), 1024)
        .unwrap_err();
    assert!(
        matches!(err, Error::Dial(ref w) if w.contains("127.0.0.1")),
        "{err}"
    );
}

#[test]
fn roots_parse() {
    assert!(Http::new().root_count() > 100);
}

/// Een server die de kop meteen stuurt en de body van `len` bytes daarna
/// byte voor byte druppelt, één per `every`.
fn dripping(len: usize, every: Duration) -> String {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap().to_string();
    thread::spawn(move || {
        let (s, _) = l.accept().unwrap();
        let mut r = BufReader::new(s.try_clone().unwrap());
        loop {
            let mut line = String::new();
            if r.read_line(&mut line).unwrap_or(0) == 0 || line == "\r\n" {
                break;
            }
        }
        let mut s = s;
        let head = format!("HTTP/1.1 200 OK\r\nContent-Length: {len}\r\n\r\n");
        if s.write_all(head.as_bytes()).is_err() {
            return;
        }
        for _ in 0..len {
            thread::sleep(every);
            if s.write_all(b"x").is_err() {
                return;
            }
        }
    });
    addr
}

#[test]
fn a_dripping_server_is_cut_off_at_the_total_deadline() {
    // Elke byte komt ruim binnen de fasetermijn (5 s), dus zonder grens
    // duurt deze aanroep 30 x 100 ms; met een grens van 300 ms niet.
    let addr = dripping(30, Duration::from_millis(100));
    let url = format!("http://{addr}/slow");
    let t0 = std::time::Instant::now();
    let until = t0 + Duration::from_millis(300);
    let err = Http::new()
        .request_until(&Call::get(&url, T), 1024, until)
        .unwrap_err();
    assert_eq!(
        err,
        Error::Http(leanhttp::Error::Io(leanhttp::IoError::TimedOut)),
        "{err}"
    );
    assert!(t0.elapsed() < Duration::from_secs(2), "{:?}", t0.elapsed());
}

#[test]
fn a_total_deadline_that_is_ample_changes_nothing() {
    let (addr, _h) = one_shot("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok");
    let url = format!("http://{addr}/");
    let until = std::time::Instant::now() + T;
    let r = Http::new()
        .request_until(&Call::get(&url, T), 1024, until)
        .unwrap();
    assert_eq!((r.status, r.body.as_slice()), (200, &b"ok"[..]));
}

#[test]
fn open_gives_the_head_before_the_body_is_done() {
    // Een chunked stroom: de eerste gebeurtenis, dan wacht de server op de
    // test. Leest de client de eerste vóór de tweede er is, dan stroomt hij.
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap().to_string();
    let (go, wait) = std::sync::mpsc::channel::<()>();
    let h = thread::spawn(move || {
        let (s, _) = l.accept().unwrap();
        let mut r = BufReader::new(s.try_clone().unwrap());
        loop {
            let mut line = String::new();
            r.read_line(&mut line).unwrap();
            if line == "\r\n" {
                break;
            }
        }
        let mut s = s;
        s.write_all(
            b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n",
        )
        .unwrap();
        s.write_all(b"b\r\ndata: one\n\n\r\n").unwrap();
        wait.recv().unwrap();
        s.write_all(b"b\r\ndata: two\n\n\r\n0\r\n\r\n").unwrap();
    });
    let url = format!("http://{addr}/v1/events");
    let mut o = Http::new().open(&Call::get(&url, T)).unwrap();
    assert_eq!(o.status(), 200);
    assert_eq!(o.header("content-type"), Some("text/event-stream"));
    let mut buf = [0u8; 64];
    let n = o.read(&mut buf).unwrap();
    assert_eq!(&buf[..n], b"data: one\n\n");
    go.send(()).unwrap();
    let rest = o.read_to_end(1024).unwrap();
    assert_eq!(rest, b"data: two\n\n");
    h.join().unwrap();
}
