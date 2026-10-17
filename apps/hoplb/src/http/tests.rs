//! De leeskant en het doorzetten van bodies.

use super::*;
use crate::testutil::{Op, Pipe, block_on};

fn head_of(bytes: &[u8]) -> Result<RequestHead> {
    parse_request(bytes)
}

#[test]
fn a_request_head_with_its_fields() {
    let h = head_of(b"GET /a?b=1 HTTP/1.1\r\nHost: x.example.com\r\nX-A:  1 \r\nx-a: 2\r\n\r\n")
        .unwrap();
    assert_eq!(
        (h.method.as_str(), h.target.as_str(), h.minor),
        ("GET", "/a?b=1", 1)
    );
    assert_eq!(h.headers.get("host"), Some("x.example.com"));
    assert_eq!(h.headers.all("X-A").collect::<Vec<_>>(), ["1", "2"]);
}

#[test]
fn crooked_requests_are_refused() {
    for bad in [
        &b"GET / HTTP/1.1\nHost: x\r\n\r\n"[..],
        b"GET / HTTP/1.1\r\nHost : x\r\n\r\n",
        b"GET / HTTP/1.1\r\nHost: x\r\n folded\r\n\r\n",
        b"GET  / HTTP/1.1\r\n\r\n",
        b"GET / HTTP/1.1\r\nX: a\x01b\r\n\r\n",
    ] {
        assert!(head_of(bad).is_err(), "{:?}", core::str::from_utf8(bad));
    }
    assert_eq!(
        head_of(b"GET / HTTP/2.0\r\n\r\n").unwrap_err(),
        Error::Unsupported("http version")
    );
}

#[test]
fn framing_follows_rfc_9112() {
    let h = |s: &[u8]| head_of(s).unwrap().headers;
    assert_eq!(
        request_framing(&h(b"GET / HTTP/1.1\r\n\r\n")),
        Ok(Framing::Empty)
    );
    assert_eq!(
        request_framing(&h(b"POST / HTTP/1.1\r\nContent-Length: 5\r\n\r\n")),
        Ok(Framing::Length(5))
    );
    assert_eq!(
        request_framing(&h(b"POST / HTTP/1.1\r\nContent-Length: 5, 5\r\n\r\n")),
        Ok(Framing::Length(5))
    );
    assert!(
        request_framing(&h(
            b"POST / HTTP/1.1\r\nContent-Length: 5\r\nContent-Length: 6\r\n\r\n"
        ))
        .is_err()
    );
    assert_eq!(
        request_framing(&h(b"POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n")),
        Ok(Framing::Chunked)
    );
    assert!(
        request_framing(&h(
            b"POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\nContent-Length: 3\r\n\r\n"
        ))
        .is_err()
    );
    assert_eq!(
        request_framing(&h(
            b"POST / HTTP/1.1\r\nTransfer-Encoding: gzip, chunked\r\n\r\n"
        )),
        Err(Error::Unsupported("transfer coding"))
    );
    let r = parse_response(b"HTTP/1.1 200 OK\r\n\r\n").unwrap();
    assert_eq!(response_framing("GET", 200, &r.headers), Ok(Framing::Eof));
    assert_eq!(
        response_framing("HEAD", 200, &r.headers),
        Ok(Framing::Empty)
    );
    assert_eq!(response_framing("GET", 204, &r.headers), Ok(Framing::Empty));
    assert_eq!(response_framing("GET", 304, &r.headers), Ok(Framing::Empty));
}

#[test]
fn a_response_head() {
    let r = parse_response(b"HTTP/1.0 404 Not Found\r\nA: b\r\n\r\n").unwrap();
    assert_eq!(
        (r.minor, r.status, r.reason.as_str()),
        (0, 404, "Not Found")
    );
    let r = parse_response(b"HTTP/1.1 200\r\n\r\n").unwrap();
    assert_eq!((r.status, r.reason.as_str()), (200, ""));
    assert!(parse_response(b"HTTP/1.1 20 OK\r\n\r\n").is_err());
    assert!(parse_response(b"HTTP/1.1 700 OK\r\n\r\n").is_err());
}

#[test]
fn the_reader_finds_a_head_across_reads_and_keeps_the_rest() {
    let mut r = Reader::new(Pipe::new(&[
        b"GET / HT",
        b"TP/1.1\r\nHost: a\r",
        b"\n\r\nBODY",
    ]))
    .unwrap();
    let n = block_on(r.read_head()).unwrap();
    assert_eq!(&r.buffered()[..n], b"GET / HTTP/1.1\r\nHost: a\r\n\r\n");
    r.consume(n);
    let mut out = [0u8; 16];
    let k = block_on(r.read_some(&mut out)).unwrap();
    assert_eq!(&out[..k], b"BODY");
}

#[test]
fn eof_before_a_head_is_eof_and_halfway_is_unexpected() {
    let mut r = Reader::new(Pipe::new(&[])).unwrap();
    assert_eq!(block_on(r.read_head()), Err(Error::Eof));
    let mut r = Reader::new(Pipe::new(&[b"GET / HTTP/1.1\r\n"])).unwrap();
    assert_eq!(block_on(r.read_head()), Err(Error::UnexpectedEof));
}

#[test]
fn a_head_that_does_not_fit_is_too_large() {
    let big = alloc::vec![b'a'; BUF_SIZE + 10];
    let mut r = Reader::new(Pipe::new(&[b"GET / HTTP/1.1\r\nX: ", &big])).unwrap();
    assert_eq!(
        block_on(r.read_head()),
        Err(Error::HeadTooLarge { limit: BUF_SIZE })
    );
}

fn how(framing: Framing, dechunk: bool, flush: bool) -> Copy {
    Copy {
        framing,
        dechunk,
        flush,
        read_idle: None,
        write_timeout: None,
        watch: None,
    }
}

#[test]
fn a_length_body_goes_through_in_pieces() {
    let mut from = Reader::new(Pipe::new(&[b"0123456789", b"abcdef", b"NEXT"])).unwrap();
    let to = Pipe::new(&[]);
    let mut sink = Reader::new(to.clone()).unwrap();
    let mut piece = [0u8; 4];
    let n = block_on(copy_body(
        &mut from,
        &mut sink,
        how(Framing::Length(16), false, true),
        &mut piece,
    ))
    .unwrap();
    assert_eq!(n, 16);
    assert_eq!(to.text(), "0123456789abcdef");
    assert!(to.ops().iter().all(|op| match op {
        Op::Write(b) => b.len() <= 4,
        _ => true,
    }));
    // Wat na de body komt, blijft staan voor het volgende bericht.
    let mut rest = [0u8; 8];
    let k = block_on(from.read_some(&mut rest)).unwrap();
    assert_eq!(&rest[..k], b"NEXT");
}

#[test]
fn a_short_length_body_is_the_source_failing() {
    let mut from = Reader::new(Pipe::new(&[b"abc"])).unwrap();
    let mut to = Reader::new(Pipe::new(&[])).unwrap();
    let mut piece = [0u8; 16];
    let r = block_on(copy_body(
        &mut from,
        &mut to,
        how(Framing::Length(5), false, false),
        &mut piece,
    ));
    assert_eq!(r, Err(Fault::Source(Error::UnexpectedEof)));
}

#[test]
fn chunked_passes_through_verbatim_with_trailers() {
    let wire: &[u8] = b"4\r\nWiki\r\n5;x=y\r\npedia\r\n0\r\nX-T: 1\r\n\r\nNEXT";
    let mut from = Reader::new(Pipe::new(&[&wire[..7], &wire[7..20], &wire[20..]])).unwrap();
    let to = Pipe::new(&[]);
    let mut sink = Reader::new(to.clone()).unwrap();
    let mut piece = [0u8; 3];
    let n = block_on(copy_body(
        &mut from,
        &mut sink,
        how(Framing::Chunked, false, false),
        &mut piece,
    ))
    .unwrap();
    assert_eq!(n, 9);
    assert_eq!(to.text().as_bytes(), &wire[..wire.len() - 4]);
    let mut rest = [0u8; 8];
    let k = block_on(from.read_some(&mut rest)).unwrap();
    assert_eq!(&rest[..k], b"NEXT");
}

#[test]
fn chunked_can_be_unpacked_for_an_old_client() {
    let mut from = Reader::new(Pipe::new(&[
        b"4\r\nWiki\r\n5\r\npedia\r\n0\r\nX-T: 1\r\n\r\n",
    ]))
    .unwrap();
    let to = Pipe::new(&[]);
    let mut sink = Reader::new(to.clone()).unwrap();
    let mut piece = [0u8; 64];
    block_on(copy_body(
        &mut from,
        &mut sink,
        how(Framing::Chunked, true, false),
        &mut piece,
    ))
    .unwrap();
    assert_eq!(to.text(), "Wikipedia");
}

#[test]
fn crooked_chunks_are_refused() {
    for bad in [
        &b"x\r\n"[..],
        b"4\r\nWikiXX",
        b"-1\r\n",
        b"11111111111111111\r\n",
    ] {
        let mut from = Reader::new(Pipe::new(&[bad])).unwrap();
        let mut to = Reader::new(Pipe::new(&[])).unwrap();
        let mut piece = [0u8; 64];
        let r = block_on(copy_body(
            &mut from,
            &mut to,
            how(Framing::Chunked, false, false),
            &mut piece,
        ));
        assert!(matches!(r, Err(Fault::Source(_))), "{bad:?}: {r:?}");
    }
}

#[test]
fn eof_framing_reads_until_the_end() {
    let mut from = Reader::new(Pipe::new(&[b"abc", b"def"])).unwrap();
    let to = Pipe::new(&[]);
    let mut sink = Reader::new(to.clone()).unwrap();
    let mut piece = [0u8; 64];
    let n = block_on(copy_body(
        &mut from,
        &mut sink,
        how(Framing::Eof, false, true),
        &mut piece,
    ))
    .unwrap();
    assert_eq!((n, to.text().as_str()), (6, "abcdef"));
}

#[test]
fn a_sink_that_fails_is_the_sink() {
    let mut from = Reader::new(Pipe::new(&[b"abc"])).unwrap();
    let pipe = Pipe::new(&[]);
    pipe.0.borrow_mut().closed = true;
    let mut to = Reader::new(pipe).unwrap();
    let mut piece = [0u8; 64];
    let r = block_on(copy_body(
        &mut from,
        &mut to,
        how(Framing::Length(3), false, false),
        &mut piece,
    ));
    assert!(matches!(r, Err(Fault::Sink(_))), "{r:?}");
}

#[test]
fn a_watched_copy_stops_when_the_reader_left() {
    let mut from = Reader::new(Pipe::new(&[
        b"4\r\nWiki\r\n",
        b"5\r\npedia\r\n",
        b"0\r\n\r\n",
    ]))
    .unwrap();
    // Een lezer die al weg is: EOF op zijn leeskant.
    let to = Pipe::new(&[]);
    let mut sink = Reader::new(to.clone()).unwrap();
    let mut piece = [0u8; 16];
    let mut how = how(Framing::Chunked, false, true);
    how.watch = Some(Duration::from_millis(1));
    let r = block_on(copy_body(&mut from, &mut sink, how, &mut piece));
    assert_eq!(r, Err(Fault::Sink(Error::Eof)));
    assert_eq!(to.text(), "", "nothing goes to a reader that left");
}

#[test]
fn a_watched_copy_keeps_what_the_reader_sent() {
    let mut from = Reader::new(Pipe::new(&[b"hello"])).unwrap();
    // Een lezer die er is en alvast zijn volgende verzoek stuurt.
    let to = Pipe::new(&[b"GET /next HTTP/1.1\r\n"]);
    to.linger();
    let mut sink = Reader::new(to.clone()).unwrap();
    let mut piece = [0u8; 16];
    let mut how = how(Framing::Length(5), false, true);
    how.watch = Some(Duration::from_millis(1));
    let n = block_on(copy_body(&mut from, &mut sink, how, &mut piece)).unwrap();
    assert_eq!(n, 5);
    assert_eq!(to.text(), "hello");
    // Wat de sondering las, staat klaar voor het volgende verzoek.
    assert_eq!(sink.buffered(), b"GET /next HTTP/1.1\r\n");
}
