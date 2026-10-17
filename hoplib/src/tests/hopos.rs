//! De hopos-module op de host: dezelfde async code door leanhttp, over een
//! nep-verbinding die per pad een vast antwoord geeft en de verzoeken
//! doorgeeft aan de echte leader-API van hop.

use super::*;
use crate::hopos::{Agent, Dialer, Idle, Nap};
use core::future::Future;
use core::ops::ControlFlow;
use core::pin::pin;
use core::task::{Context, Poll, Waker};
use leanhttp::{AsyncRead, AsyncWrite, Close, IoError, Target};

/// Draait een future die nooit echt wacht (de nep-verbinding is altijd klaar).
fn block_on<F: Future>(f: F) -> F::Output {
    let mut f = pin!(f);
    let mut cx = Context::from_waker(Waker::noop());
    loop {
        if let Poll::Ready(v) = f.as_mut().poll(&mut cx) {
            return v;
        }
    }
}

/// Het antwoord op een verzoek: de echte leader-API, of een SSE-stroom.
fn answer(req: &[u8]) -> Vec<u8> {
    let text = core::str::from_utf8(req).unwrap();
    let (head, body) = text.split_once("\r\n\r\n").unwrap();
    let mut lines = head.split("\r\n");
    let mut first = lines.next().unwrap().split(' ');
    let (method, target) = (first.next().unwrap(), first.next().unwrap());
    if target == "/v1/events" {
        let mut out = String::from("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\r\n");
        out.push_str(&event_log_bytes());
        return out.into_bytes();
    }
    let mut r = api::Request::new(Method::parse(method), target, body.as_bytes());
    for l in lines {
        let (k, v) = l.split_once(':').unwrap();
        r.headers.push((k.to_string(), v.trim().to_string()));
    }
    let mut cl = cluster();
    let (resp, effect) = LeaderApi::new(KEY.as_bytes(), "prod").handle(&mut cl, 0, &r);
    let resp = match effect {
        LeaderEffect::Tasks { .. } => api::Response::error(404, "not found"),
        _ => resp,
    };
    let mut out = alloc::format!(
        "HTTP/1.1 {} X\r\nContent-Length: {}\r\n\r\n",
        resp.status,
        resp.body.len()
    )
    .into_bytes();
    out.extend_from_slice(&resp.body);
    out
}

/// Een verbinding die het verzoek verzamelt en daarna het antwoord geeft.
#[derive(Default)]
struct FakeConn {
    sent: Vec<u8>,
    reply: Option<Vec<u8>>,
    at: usize,
    timeouts: Vec<Option<Duration>>,
}

impl FakeConn {
    /// Het verzoek is af als de kop af is en de body er is.
    fn request_done(&self) -> bool {
        let Some(i) = self.sent.windows(4).position(|w| w == b"\r\n\r\n") else {
            return false;
        };
        let head = core::str::from_utf8(&self.sent[..i])
            .unwrap()
            .to_ascii_lowercase();
        let len = head
            .split("\r\n")
            .find_map(|l| l.strip_prefix("content-length:"))
            .map_or(0, |v| v.trim().parse::<usize>().unwrap());
        self.sent.len() >= i + 4 + len
    }
}

impl AsyncRead for FakeConn {
    fn poll_read(&mut self, _: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize, IoError>> {
        if self.reply.is_none() {
            assert!(self.request_done(), "read before the request was complete");
            self.reply = Some(answer(&self.sent));
        }
        let reply = self.reply.as_ref().unwrap();
        let n = buf.len().min(reply.len() - self.at).min(7);
        buf[..n].copy_from_slice(&reply[self.at..self.at + n]);
        self.at += n;
        Poll::Ready(Ok(n))
    }

    fn set_read_timeout(&mut self, t: Option<Duration>) -> Result<(), IoError> {
        self.timeouts.push(t);
        Ok(())
    }
}

impl AsyncWrite for FakeConn {
    fn poll_write(&mut self, _: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, IoError>> {
        self.sent.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }
}

impl Close for FakeConn {
    fn poll_close(&mut self, _: &mut Context<'_>) -> Poll<Result<(), IoError>> {
        Poll::Ready(Ok(()))
    }
}

#[derive(Clone, Default)]
struct FakeDial {
    refuse: bool,
}

impl leanhttp::Dial for FakeDial {
    type Conn = Idle<FakeConn>;

    async fn dial(&mut self, _: Target<'_>) -> leanhttp::Result<Self::Conn> {
        if self.refuse {
            return Err(leanhttp::Error::Connect);
        }
        Ok(Idle::new(FakeConn::default(), Duration::from_secs(60)))
    }
}

impl Dialer for FakeDial {
    fn why(&self) -> Option<Reason> {
        self.refuse.then(|| Reason::new("connection refused"))
    }
}

#[derive(Clone, Copy)]
struct NoNap;

impl Nap for NoNap {
    fn nap(&self, _: Duration) -> impl Future<Output = ()> {
        core::future::ready(())
    }
}

fn agent(key: &str) -> Agent<FakeDial, NoNap> {
    Agent::with(
        Client::new("10.0.0.1:7878", Some(key)).unwrap(),
        FakeDial::default(),
        NoNap,
    )
}

#[test]
fn hopos_agent_talks_to_the_leader_api() {
    let a = agent(KEY);
    let st = block_on(a.status()).unwrap();
    assert_eq!(st.cluster_name, "prod");
    assert_eq!(block_on(a.agents()).unwrap().len(), 2);
    assert!(block_on(a.job("db")).unwrap().is_some());
    let web = Job::from_json(br#"{"name":"web","command":"./web"}"#).unwrap();
    assert_eq!(block_on(a.apply(&web)).unwrap().status, "dispatched");
    assert_eq!(block_on(a.delete("db")), Ok(()));
    let bad = agent("wrong");
    assert!(matches!(
        block_on(bad.jobs()),
        Err(Error::Status { code: 401, .. })
    ));
}

#[test]
fn hopos_dial_failure_carries_the_reason() {
    let a = Agent::with(
        Client::new("10.0.0.1:7878", None).unwrap(),
        FakeDial { refuse: true },
        NoNap,
    );
    let err = block_on(a.status()).unwrap_err();
    assert_eq!(err.to_string(), "net: connection refused");
}

#[test]
fn hopos_stream_and_events_loop() {
    let a = agent(KEY);
    let mut s = a.stream();
    let kinds: Vec<String> = (0..5).map(|_| block_on(s.next()).unwrap().kind).collect();
    assert_eq!(kinds, ["ping", "job", "task", "agent", "status"]);
    // Zonder lengte eindigt de stroom als de verbinding leeg is: een breuk.
    assert!(block_on(s.next()).is_err());
    assert_eq!(block_on(s.next()).unwrap().kind, "ping");

    // De lus met een async callback die zelf de agent aanroept.
    let mut jobs_seen = 0;
    let mut n = 0;
    block_on(a.events(
        async |e| {
            n += 1;
            if e.is_resync() {
                jobs_seen += a.jobs().await.unwrap().len();
            }
            if n == 5 {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        },
        |_, _| {},
    ));
    assert_eq!(jobs_seen, 4);
}

#[test]
fn idle_turns_no_timeout_into_the_idle_bound() {
    let mut c = Idle::new(FakeConn::default(), Duration::from_secs(60));
    c.set_read_timeout(None).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    assert_eq!(
        c.get_mut().timeouts,
        [Some(Duration::from_secs(60)), Some(Duration::from_secs(5))]
    );
}
