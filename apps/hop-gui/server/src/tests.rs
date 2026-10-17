//! De tabel, de keuze van het antwoord, en één verzoek door leanhttp in het geheugen.

use super::*;
use std::string::String;
use std::vec::Vec;

use core::future::Future;
use core::pin::pin;
use core::task::{Context, Poll, Waker};
use leanhttp::{AsyncRead, AsyncWrite, Close, IoError};

#[test]
fn the_dashboard_is_in_the_table() {
    for p in ["/index.html", "/app.js", "/appearance.js", "/style.css"] {
        assert!(find(p).is_some(), "{p} missing");
    }
    // Alles uit vendor/, ook het font.
    assert!(find("/vendor/tactile-dialog.js").is_some());
    assert!(find("/vendor/material-symbols-outlined.woff2").is_some());
    assert!(
        FILES
            .iter()
            .filter(|f| f.path.starts_with("/vendor/"))
            .count()
            >= 9
    );
    assert_eq!(find("/"), find("/index.html"));
    assert!(find("/nope.js").is_none());
    assert!(find("/../Cargo.toml").is_none());
}

#[test]
fn the_bytes_are_the_files_in_the_repo() {
    let root = concat!(env!("CARGO_MANIFEST_DIR"), "/..");
    for f in FILES {
        let on_disk = std::fs::read(std::format!("{root}{}", f.path)).unwrap();
        assert_eq!(f.body, on_disk.as_slice(), "{}", f.path);
    }
    assert_eq!(total_bytes(), FILES.iter().map(|f| f.body.len()).sum());
}

#[test]
fn content_types_by_extension() {
    let ct = |p| find(p).unwrap().content_type;
    assert_eq!(ct("/"), "text/html; charset=utf-8");
    assert_eq!(ct("/app.js"), "text/javascript; charset=utf-8");
    assert_eq!(ct("/style.css"), "text/css; charset=utf-8");
    assert_eq!(ct("/vendor/material-symbols-outlined.woff2"), "font/woff2");
}

#[test]
fn etags_are_quoted_and_differ_per_file() {
    for f in FILES {
        assert!(
            f.etag.starts_with('"') && f.etag.ends_with('"'),
            "{}",
            f.etag
        );
        assert_eq!(f.etag.len(), 18);
    }
    assert_ne!(
        find("/app.js").unwrap().etag,
        find("/style.css").unwrap().etag
    );
}

#[test]
fn if_none_match_lists_and_weak_tags() {
    let e = "\"00ff\"";
    assert!(etag_matches("\"00ff\"", e));
    assert!(etag_matches("\"aa\", W/\"00ff\"", e));
    assert!(etag_matches("*", e));
    assert!(!etag_matches("\"00fe\"", e));
    assert!(!etag_matches("", e));
}

#[test]
fn answers() {
    let app = find("/app.js").unwrap();
    assert_eq!(answer("GET", "/app.js", None), Answer::File(app));
    assert_eq!(answer("HEAD", "/app.js", None), Answer::File(app));
    assert_eq!(
        answer("GET", "/app.js", Some(app.etag)),
        Answer::NotModified(app)
    );
    assert_eq!(answer("GET", "/app.js", Some("\"x\"")), Answer::File(app));
    assert_eq!(answer("GET", "/v1/status", None), Answer::NotFound);
    assert_eq!(answer("POST", "/", None), Answer::MethodNotAllowed);
}

#[test]
fn decimals() {
    let mut b = [0u8; 20];
    assert_eq!(decimal(0, &mut b), "0");
    assert_eq!(decimal(3_963_852, &mut b), "3963852");
    assert_eq!(decimal(usize::MAX, &mut b), std::format!("{}", usize::MAX));
}

/// Pollt tot klaar; een verbinding in het geheugen is nooit `Pending`.
fn block_on<F: Future>(f: F) -> F::Output {
    let mut f = pin!(f);
    let mut cx = Context::from_waker(Waker::noop());
    loop {
        if let Poll::Ready(v) = f.as_mut().poll(&mut cx) {
            return v;
        }
    }
}

/// Stuurt `requests` door [`serve`] over een verbinding in het geheugen,
/// en geeft wat er op de draad kwam.
fn exchange(requests: &str) -> String {
    use std::cell::RefCell;
    use std::rc::Rc;
    struct Shared {
        input: Vec<u8>,
        at: usize,
        out: Rc<RefCell<Vec<u8>>>,
    }
    impl AsyncRead for Shared {
        fn poll_read(
            &mut self,
            _: &mut Context<'_>,
            buf: &mut [u8],
        ) -> Poll<Result<usize, IoError>> {
            let rest = &self.input[self.at..];
            let n = rest.len().min(buf.len());
            buf[..n].copy_from_slice(&rest[..n]);
            self.at += n;
            Poll::Ready(Ok(n))
        }
    }
    impl AsyncWrite for Shared {
        fn poll_write(&mut self, _: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, IoError>> {
            self.out.borrow_mut().extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }
    }
    impl Close for Shared {
        fn poll_close(&mut self, _: &mut Context<'_>) -> Poll<Result<(), IoError>> {
            Poll::Ready(Ok(()))
        }
    }
    let out = Rc::new(RefCell::new(Vec::new()));
    let conn = Shared {
        input: requests.as_bytes().to_vec(),
        at: 0,
        out: out.clone(),
    };
    let _ = block_on(leanhttp::serve(
        conn,
        async |ex: &mut Exchange<'_, Shared>| serve(ex).await,
    ));
    String::from_utf8_lossy(&out.borrow()).into_owned()
}

#[test]
fn the_index_on_the_wire() {
    let text = exchange("GET / HTTP/1.1\r\nHost: n\r\nConnection: close\r\n\r\n");
    let index = find("/").unwrap();
    assert!(text.starts_with("HTTP/1.1 200"), "{text}");
    assert!(
        text.contains("Content-Type: text/html; charset=utf-8"),
        "{text}"
    );
    assert!(
        text.contains(&std::format!("ETag: {}", index.etag)),
        "{text}"
    );
    assert!(text.contains("Cache-Control: no-cache"), "{text}");
    assert!(
        text.contains(&std::format!("Content-Length: {}", index.body.len())),
        "{text}"
    );
    assert!(text.ends_with(core::str::from_utf8(index.body).unwrap()));
}

#[test]
fn revalidation_head_and_refusals_on_the_wire() {
    let app = find("/app.js").unwrap();
    let req = std::format!(
        "GET /app.js HTTP/1.1\r\nHost: n\r\nIf-None-Match: {}\r\n\r\n\
         HEAD /app.js HTTP/1.1\r\nHost: n\r\n\r\n\
         POST / HTTP/1.1\r\nHost: n\r\nContent-Length: 0\r\n\r\n\
         GET /missing HTTP/1.1\r\nHost: n\r\nConnection: close\r\n\r\n",
        app.etag
    );
    let text = exchange(&req);
    let parts: Vec<&str> = text.split("HTTP/1.1 ").filter(|s| !s.is_empty()).collect();
    assert_eq!(parts.len(), 4, "{text}");
    assert!(parts[0].starts_with("304"), "{}", parts[0]);
    assert!(parts[0].contains(app.etag));
    assert!(parts[1].starts_with("200"), "{}", parts[1]);
    // HEAD: de lengte als metadata, geen body.
    assert!(parts[1].contains(&std::format!("Content-Length: {}", app.body.len())));
    assert!(parts[1].ends_with("\r\n\r\n"), "{}", parts[1]);
    assert!(parts[2].starts_with("405") && parts[2].contains("Allow: GET, HEAD"));
    assert!(parts[3].starts_with("404"), "{}", parts[3]);
}
