//! De reverse proxy: één clientverbinding, verzoek na verzoek, elk naar een
//! backend uit de routetabel (Go: `internal/lb/proxy.go`, met
//! `httputil.ReverseProxy` als het deel dat Go uit de standaardbibliotheek
//! haalde).
//!
//! Een verzoek loopt door vaste toestanden, en elke toestand zegt wat er bij
//! een fout overblijft:
//!
//! | Toestand | Wat er gebeurt | Fout |
//! | --- | --- | --- |
//! | kop | lezen, ontleden, `Host` | 400/431/501/505, dicht |
//! | route | [`Env::pick`]: patroon, round-robin | 502 `no route for host`, 503 `no healthy backend` |
//! | bellen | [`Env::dial`] | 502 `backend error` |
//! | doorgeven | kop naar de backend: zonder hop-by-hop-velden, met `X-Forwarded-For`; de body in happen | 502, dicht |
//! | antwoord | kop van de backend lezen (1xx overslaan) | 502 `backend error` |
//! | terug | kop naar de client; de body hap voor hap, doorgespoeld | dicht (de status is al weg) |
//!
//! Na elk verzoek gaat er één [`Record`] naar [`Env::record`]: domein
//! (de `Host` van de client, zoals Go `r.Host`), backend, status en de tijd
//! van kop tot laatste byte. Wat er met die meting gebeurt, is van de schil.
//!
//! De verbinding naar de backend draagt `Connection: close`: één verbinding
//! per verzoek. Go hield een pool per backend; die komt pas terug met een
//! meting die zegt dat het bellen telt (de backends staan op hetzelfde LAN,
//! op HopOS zelfs op dezelfde node).
//!
//! Wat hier niet staat: sockets, de klok, de routetabel zelf. Dat zijn de
//! methodes van [`Env`], en die zijn van de schil.

use alloc::string::String;
use alloc::vec::Vec;
use core::future::Future;
use core::time::Duration;

use leanhttp::{AsyncRead, AsyncWrite, Conn, IoError};

use crate::error::{Error, Result, try_string};
use crate::http::{
    self, Copy, Fault, Framing, HeadBuf, Headers, PIECE, Reader, RequestHead, ResponseHead,
};
use crate::metrics::Record;
use crate::route::Pick;

/// De termijnen van de proxy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    /// De eerste kop op een verse verbinding (Go: `ReadHeaderTimeout`, 10 s).
    pub head: Duration,
    /// De stilte tussen twee verzoeken op een keep-alive-verbinding. Go had
    /// 120 s; met een vaste pool werkers houdt een stille browser anders een
    /// werker vast (zie `READ_CAP` in welcome en agentd).
    pub idle: Duration,
    /// De stilte per lees van een verzoekbody. Go had geen `ReadTimeout`
    /// (een grote upload moet door); dit is geen totale grens maar een
    /// stiltegrens, dus een upload die stroomt, haalt het.
    pub body_idle: Duration,
    /// Per schrijf naar de client of de backend (leanhttp's `WRITE_TIMEOUT`):
    /// een lange stroom overleeft, een lezer die niet leest niet.
    pub write: Duration,
    /// De stilte per lees van de backend, ook voor zijn kop. Go had er
    /// geen; een SSE-stroom zwijgt lang, maar een backend die tien minuten
    /// niets zegt, houdt een werker niet voor altijd vast.
    pub backend_idle: Duration,
    /// Eén kijkje op de leeskant van de client terwijl het antwoord loopt
    /// ([`http::Reader::peer_gone`]): vóór elke hap en elke
    /// [`http::WATCH_EVERY`] stilte van de backend. Een client die wegging
    /// maakt het antwoord af en geeft de werker terug, in plaats van te
    /// wachten op een mislukte schrijf (op HopOS: lang niet) of op
    /// `backend_idle`.
    pub probe: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            head: Duration::from_secs(10),
            idle: Duration::from_secs(5),
            body_idle: Duration::from_secs(60),
            write: Duration::from_secs(30),
            backend_idle: Duration::from_secs(600),
            probe: Duration::from_millis(1),
        }
    }
}

/// Wat de proxy van zijn schil vraagt.
pub trait Env {
    /// De verbinding naar een backend.
    type Conn: Conn;

    /// Kiest voor `host`: geen route, geen gezonde backend, of een adres.
    fn pick(&mut self, host: &str) -> Pick;

    /// Belt `addr` (`host:poort`).
    fn dial(&mut self, addr: &str) -> impl Future<Output = Result<Self::Conn>>;

    /// Nanoseconden op een monotone klok.
    fn now_ns(&self) -> u64;

    /// Eén afgehandeld verzoek.
    fn record(&mut self, r: Record);

    /// Een backend faalde (Go logde `Proxy error for <host> -> <addr>: <err>`).
    fn backend_error(&mut self, host: &str, addr: &str, err: Error) {
        let _ = (host, addr, err);
    }

    /// De termijnen.
    fn limits(&self) -> Limits {
        Limits::default()
    }

    /// Of elke werker van de pool een verbinding heeft. Dan zegt het
    /// antwoord `Connection: close`: de volgende verbinding wacht op het
    /// einde van dit verzoek, niet op de stilte ([`Limits::idle`]) van een
    /// keep-alive-client die misschien niets meer vraagt. Een browser doet
    /// zijn parallelle verzoeken op eigen verbindingen; zonder dit wachtte
    /// de derde op de eerste.
    fn crowded(&self) -> bool {
        false
    }
}

/// Bedient één clientverbinding tot hij sluit; `peer` is het IP van de
/// client, voor `X-Forwarded-For` (leeg: geen).
///
/// De verbinding is dicht als dit terugkomt.
pub async fn serve<C: Conn, E: Env>(client: C, peer: &str, env: &mut E) -> Result {
    let mut client = Reader::new(client)?;
    let mut piece = Vec::new();
    if piece.try_reserve_exact(PIECE).is_err() {
        let _ = leanhttp::close(client.get_mut()).await;
        return Err(Error::OutOfMemory { bytes: PIECE });
    }
    piece.resize(PIECE, 0);
    let limits = env.limits();
    let mut wait = limits.head;
    let out = loop {
        match one(&mut client, peer, env, &mut piece, wait).await {
            Ok(true) => wait = limits.idle,
            Ok(false) => break Ok(()),
            Err(e) => break Err(e),
        }
    };
    let _ = leanhttp::close(client.get_mut()).await;
    out
}

/// Eén verzoek; `Ok(true)` als de verbinding het volgende mag dragen.
async fn one<C: Conn, E: Env>(
    client: &mut Reader<C>,
    peer: &str,
    env: &mut E,
    piece: &mut [u8],
    wait: Duration,
) -> Result<bool> {
    let limits = env.limits();
    client.get_mut().set_read_timeout(Some(wait))?;
    let n = match client.read_head().await {
        Ok(n) => n,
        // Een client die tussen verzoeken sluit of zwijgt: klaar.
        Err(Error::Eof | Error::Io(_)) => return Ok(false),
        Err(e) => {
            let status = if matches!(e, Error::HeadTooLarge { .. }) {
                431
            } else {
                400
            };
            reply_error(
                client.get_mut(),
                status,
                reason_text(status),
                false,
                &limits,
            )
            .await;
            return Ok(false);
        }
    };
    let parsed = http::parse_request(client.buffered().get(..n).unwrap_or(&[]));
    client.consume(n);
    let req = match parsed.and_then(|r| check_request(&r).map(|f| (r, f))) {
        Ok(r) => r,
        Err(e) => {
            let status = status_of(&e);
            reply_error(
                client.get_mut(),
                status,
                reason_text(status),
                false,
                &limits,
            )
            .await;
            return Ok(false);
        }
    };
    let start = env.now_ns();
    exchange(client, &req.0, req.1, peer, env, piece, start).await
}

/// De framing van een verzoek, en de regels die Go's server afdwong.
fn check_request(r: &RequestHead) -> Result<Framing> {
    if r.method == "CONNECT" {
        return Err(Error::Unsupported("CONNECT"));
    }
    let hosts = r.headers.all("Host").count();
    if hosts > 1 || (r.minor == 1 && hosts == 0) {
        return Err(Error::BadRequest("missing or repeated Host header"));
    }
    http::request_framing(&r.headers)
}

/// Het gesprek met de backend voor één verzoek.
async fn exchange<C: Conn, E: Env>(
    client: &mut Reader<C>,
    req: &RequestHead,
    framing: Framing,
    peer: &str,
    env: &mut E,
    piece: &mut [u8],
    start: u64,
) -> Result<bool> {
    let limits = env.limits();
    let (domain, target) = host_and_target(req);
    // Go: `http.Error` en daarna klaar; een body die nog op de draad staat,
    // maakt de verbinding onbruikbaar, dus dan dicht.
    let clean = framing == Framing::Empty && !wants_close(req);
    let addr = match env.pick(domain) {
        Pick::Backend(a) => a,
        pick => {
            let (status, msg) = match pick {
                Pick::NoRoute => (502, "no route for host"),
                _ => (503, "no healthy backend"),
            };
            reply_error(client.get_mut(), status, msg, !clean, &limits).await;
            record(env, domain, "", status, start);
            return Ok(clean);
        }
    };
    let backend = match env.dial(&addr).await {
        Ok(b) => b,
        Err(e) => {
            env.backend_error(domain, &addr, e);
            reply_error(client.get_mut(), 502, "backend error", !clean, &limits).await;
            record(env, domain, &addr, 502, start);
            return Ok(clean);
        }
    };
    let mut backend = match Reader::new(backend) {
        Ok(b) => b,
        Err(e) => {
            reply_error(client.get_mut(), 503, "no healthy backend", true, &limits).await;
            record(env, domain, &addr, 503, start);
            return Err(e);
        }
    };
    let out = forward(
        client,
        &mut backend,
        req,
        framing,
        domain,
        target,
        peer,
        env,
        piece,
    )
    .await;
    let _ = leanhttp::close(backend.get_mut()).await;
    match out {
        Ok((status, keep)) => {
            record(env, domain, &addr, status, start);
            Ok(keep)
        }
        Err(Stage::Before(e)) => {
            // Nog niets naar de client: een eerlijke 502.
            env.backend_error(domain, &addr, e);
            reply_error(client.get_mut(), 502, "backend error", true, &limits).await;
            record(env, domain, &addr, 502, start);
            Ok(false)
        }
        Err(Stage::After(status, e)) => {
            // De status is al weg; alleen nog dichtdoen.
            env.backend_error(domain, &addr, e);
            record(env, domain, &addr, status, start);
            Ok(false)
        }
        Err(Stage::Client) => {
            // De client zelf viel weg of stuurde een kapotte body; Go's
            // `ReverseProxy` telde dat als 502.
            record(env, domain, &addr, 502, start);
            Ok(false)
        }
    }
}

/// Waar een doorgifte faalde.
enum Stage {
    /// Vóór er iets naar de client ging (de backend faalde).
    Before(Error),
    /// Nadat de kop met deze status naar de client ging.
    After(u16, Error),
    /// De client zelf (lezen van zijn body, schrijven naar hem vóór de kop).
    Client,
}

/// Kop en body naar de backend, antwoord terug; `(status, keep-alive)`.
#[expect(clippy::too_many_arguments, reason = "één verzoek, plat doorgegeven")]
async fn forward<C: Conn, B: Conn, E: Env>(
    client: &mut Reader<C>,
    backend: &mut Reader<B>,
    req: &RequestHead,
    framing: Framing,
    domain: &str,
    target: &str,
    peer: &str,
    env: &mut E,
    piece: &mut [u8],
) -> core::result::Result<(u16, bool), Stage> {
    let limits = env.limits();
    let head = request_head(req, framing, domain, target, peer).map_err(|_| Stage::Client)?;
    write(backend.get_mut(), &head, limits.write)
        .await
        .map_err(Stage::Before)?;
    if framing != Framing::Empty {
        if req.headers.has_token("Expect", "100-continue") {
            // De client wacht op ons oordeel; de backend kreeg geen `Expect`.
            write(
                client.get_mut(),
                b"HTTP/1.1 100 Continue\r\n\r\n",
                limits.write,
            )
            .await
            .map_err(|_| Stage::Client)?;
        }
        let how = Copy {
            framing,
            dechunk: false,
            flush: false,
            read_idle: Some(limits.body_idle),
            write_timeout: Some(limits.write),
            watch: None,
        };
        http::copy_body(client, backend, how, piece)
            .await
            .map_err(|f| match f {
                Fault::Source(_) => Stage::Client,
                Fault::Sink(e) => Stage::Before(e),
            })?;
    }
    let resp = read_response(backend, client, &limits).await?;
    let rframing =
        http::response_framing(&req.method, resp.status, &resp.headers).map_err(Stage::Before)?;
    // Een HTTP/1.0-client kent geen chunked: uitpakken, en dan sluit het
    // einde van de verbinding de body af.
    let dechunk = rframing == Framing::Chunked && req.minor == 0;
    let keep = !wants_close(req)
        && req.minor == 1
        && rframing != Framing::Eof
        && !dechunk
        && !env.crowded();
    let head = response_head(&resp, rframing, dechunk, keep).map_err(|_| Stage::Client)?;
    write(client.get_mut(), &head, limits.write)
        .await
        .map_err(|e| Stage::After(resp.status, e))?;
    let how = Copy {
        framing: rframing,
        dechunk,
        flush: true,
        read_idle: Some(limits.backend_idle),
        write_timeout: Some(limits.write),
        watch: Some(limits.probe),
    };
    match http::copy_body(backend, client, how, piece).await {
        Ok(_) => {
            leanhttp::flush(client.get_mut())
                .await
                .map_err(|e| Stage::After(resp.status, e.into()))?;
            Ok((resp.status, keep))
        }
        Err(Fault::Source(e) | Fault::Sink(e)) => Err(Stage::After(resp.status, e)),
    }
}

/// Leest de kop van het antwoord; tussenantwoorden (1xx) vallen weg, want
/// de client kreeg zijn `100 Continue` al van ons en `Upgrade` gaat niet door.
/// De kop van het antwoord van de backend, tot de eerste eindstatus. De
/// wacht op een trage backend gaat in stukken van [`http::WATCH_EVERY`],
/// met tussendoor een kijkje of de client er nog is: een client die
/// wegging is [`Stage::Client`], en de backend gaat mee dicht.
async fn read_response<B: Conn, C: Conn>(
    backend: &mut Reader<B>,
    client: &mut Reader<C>,
    limits: &Limits,
) -> core::result::Result<ResponseHead, Stage> {
    let mut waited = Duration::ZERO;
    loop {
        let left = limits.backend_idle.saturating_sub(waited);
        let slice = left.min(http::WATCH_EVERY);
        backend
            .get_mut()
            .set_read_timeout(Some(slice))
            .map_err(|e| Stage::Before(e.into()))?;
        let n = match backend.read_head().await {
            Ok(n) => n,
            Err(Error::Io(IoError::TimedOut)) if left > http::WATCH_EVERY => {
                waited = waited.saturating_add(slice);
                if client.peer_gone(limits.probe).await {
                    return Err(Stage::Client);
                }
                continue;
            }
            Err(Error::Eof) => return Err(Stage::Before(Error::UnexpectedEof)),
            Err(e) => return Err(Stage::Before(e)),
        };
        let head = http::parse_response(backend.buffered().get(..n).unwrap_or(&[]));
        backend.consume(n);
        let head = head.map_err(Stage::Before)?;
        if head.status >= 200 {
            return Ok(head);
        }
        if head.status == 101 {
            return Err(Stage::Before(Error::Unsupported("protocol upgrade")));
        }
    }
}

/// Velden die alleen voor deze ene verbinding gelden (RFC 9110 §7.6.1), en
/// die een proxy dus nooit doorgeeft; `Content-Length` en `Expect` zet de
/// proxy zelf. Dezelfde lijst als Go's `httputil.ReverseProxy`.
const HOP_BY_HOP: &[&str] = &[
    "Connection",
    "Proxy-Connection",
    "Keep-Alive",
    "Proxy-Authenticate",
    "Proxy-Authorization",
    "Te",
    "Trailer",
    "Transfer-Encoding",
    "Upgrade",
    "Content-Length",
];

/// Geeft een veld door? Niet als het hop-by-hop is of in `Connection` staat.
fn passes(name: &str, h: &Headers) -> bool {
    !HOP_BY_HOP.iter().any(|x| x.eq_ignore_ascii_case(name)) && !h.has_token("Connection", name)
}

/// De kop naar de backend.
fn request_head(
    req: &RequestHead,
    framing: Framing,
    domain: &str,
    target: &str,
    peer: &str,
) -> Result<Vec<u8>> {
    let mut b = HeadBuf::new()?;
    b.fmt(format_args!("{} {} HTTP/1.1\r\n", req.method, target))?;
    let mut has_host = false;
    let mut xff = String::new();
    for (k, v) in req.headers.iter() {
        if k.eq_ignore_ascii_case("X-Forwarded-For") {
            if !xff.is_empty() {
                push_str(&mut xff, ", ")?;
            }
            push_str(&mut xff, v)?;
            continue;
        }
        if k.eq_ignore_ascii_case("Expect") || !passes(k, &req.headers) {
            continue;
        }
        has_host |= k.eq_ignore_ascii_case("Host");
        b.field(k, v)?;
    }
    if !has_host && !domain.is_empty() {
        b.field("Host", domain)?;
    }
    // Go (`ReverseProxy.ServeHTTP`): het IP van de client achter een
    // bestaande keten.
    if !peer.is_empty() {
        if !xff.is_empty() {
            push_str(&mut xff, ", ")?;
        }
        push_str(&mut xff, peer)?;
    }
    if !xff.is_empty() {
        b.field("X-Forwarded-For", &xff)?;
    }
    match framing {
        Framing::Length(n) => b.fmt(format_args!("Content-Length: {n}\r\n"))?,
        Framing::Chunked => b.field("Transfer-Encoding", "chunked")?,
        Framing::Empty | Framing::Eof => {}
    }
    b.push("Connection: close\r\n\r\n")?;
    Ok(b.0)
}

/// De kop naar de client.
fn response_head(
    resp: &ResponseHead,
    framing: Framing,
    dechunk: bool,
    keep: bool,
) -> Result<Vec<u8>> {
    let mut b = HeadBuf::new()?;
    b.fmt(format_args!("HTTP/1.1 {} {}\r\n", resp.status, resp.reason))?;
    for (k, v) in resp.headers.iter() {
        // Een lengte bij HEAD of 304 is informatief en mag mee.
        let informative = framing == Framing::Empty && k.eq_ignore_ascii_case("Content-Length");
        if informative || passes(k, &resp.headers) {
            b.field(k, v)?;
        }
    }
    match framing {
        Framing::Length(n) => b.fmt(format_args!("Content-Length: {n}\r\n"))?,
        Framing::Chunked if !dechunk => b.field("Transfer-Encoding", "chunked")?,
        _ => {}
    }
    if !keep {
        b.push("Connection: close\r\n")?;
    }
    b.push("\r\n")?;
    Ok(b.0)
}

/// Het domein voor de routetabel en het doel voor de backend.
///
/// Een doel in absolute vorm (`http://host/pad`) wordt een pad, en zijn
/// host wint van `Host`, zoals in Go's server.
fn host_and_target(req: &RequestHead) -> (&str, &str) {
    let t = req.target.as_str();
    let abs = t
        .strip_prefix("http://")
        .or_else(|| t.strip_prefix("https://"));
    if let Some(rest) = abs {
        let slash = rest.find('/').unwrap_or(rest.len());
        let host = rest.get(..slash).unwrap_or("");
        let path = rest.get(slash..).filter(|p| !p.is_empty()).unwrap_or("/");
        return (host, path);
    }
    (req.headers.get("Host").unwrap_or(""), t)
}

/// Vraagt de client om sluiten na dit verzoek?
fn wants_close(req: &RequestHead) -> bool {
    req.headers.has_token("Connection", "close")
        || (req.minor == 0 && !req.headers.has_token("Connection", "keep-alive"))
}

/// Schrijft `bytes` met een schrijftermijn.
async fn write<W: AsyncWrite>(w: &mut W, bytes: &[u8], timeout: Duration) -> Result {
    w.set_write_timeout(Some(timeout))?;
    leanhttp::write_all(w, bytes).await?;
    Ok(())
}

/// Een fout zoals Go's `http.Error`: `msg` plus regeleinde, platte tekst.
async fn reply_error<W: AsyncWrite + AsyncRead>(
    w: &mut W,
    status: u16,
    msg: &str,
    close: bool,
    limits: &Limits,
) {
    let Ok(mut b) = HeadBuf::new() else {
        return;
    };
    let ok = b
        .fmt(format_args!(
            "HTTP/1.1 {status} {}\r\nContent-Type: text/plain; charset=utf-8\r\n\
             X-Content-Type-Options: nosniff\r\nContent-Length: {}\r\n",
            http::reason(status),
            msg.len() + 1
        ))
        .and_then(|()| {
            if close {
                b.push("Connection: close\r\n")
            } else {
                Ok(())
            }
        })
        .and_then(|()| b.push("\r\n"))
        .and_then(|()| b.push(msg))
        .and_then(|()| b.push("\n"));
    if ok.is_ok() {
        let _ = write(w, &b.0, limits.write).await;
        let _ = leanhttp::flush(w).await;
    }
}

fn record<E: Env>(env: &mut E, domain: &str, backend: &str, code: u16, start: u64) {
    let nanos = env.now_ns().saturating_sub(start);
    let (Ok(domain), Ok(backend)) = (try_string(domain), try_string(backend)) else {
        // Zonder geheugen voor twee korte strings valt de meting weg; het
        // verzoek zelf is al afgehandeld.
        return;
    };
    env.record(Record {
        domain,
        backend,
        code,
        nanos,
    });
}

/// De status bij een kapot verzoek.
fn status_of(e: &Error) -> u16 {
    match e {
        Error::HeadTooLarge { .. } => 431,
        Error::Unsupported("http version") => 505,
        Error::Unsupported(_) => 501,
        _ => 400,
    }
}

fn reason_text(status: u16) -> &'static str {
    match status {
        431 => "request header fields too large",
        501 => "not implemented",
        505 => "http version not supported",
        _ => "bad request",
    }
}

fn push_str(s: &mut String, t: &str) -> Result {
    s.try_reserve(t.len())
        .map_err(|_| Error::OutOfMemory { bytes: t.len() })?;
    s.push_str(t);
    Ok(())
}

#[cfg(test)]
mod tests;
