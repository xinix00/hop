//! De HTTP-servers van de daemon: een vaste pool verbindingsthreads per poort, leanhttp over std-sockets.
//!
//! Bezit per thread één verbinding tegelijk en een kloon van de listener;
//! de staat van de node bezit hij niet. Elk verzoek gaat als [`Msg::Http`]
//! naar de eigenaar, het antwoord komt terug over een eigen kanaal.
//! Handboek §2: een verbinding is een taak uit een vaste pool, de grootte is
//! een constante ([`WORKERS`]).
//!
//! Een [`Reply::Proxy`] voert de thread zelf uit (zie [`crate::msg::Reply`]):
//! hetzelfde verzoek, met dezelfde `X-Hop-Auth`, naar de leader.

use std::net::{TcpListener, TcpStream};
use std::sync::mpsc::{self, Sender};
use std::task::{Context, Poll};
use std::time::Duration;

use api::{Method, Request, Response};
use hostnet::{Call, Http, StdConn, block_on};
use leanhttp::{AsyncRead, AsyncWrite, Close, Exchange, IoError};

use crate::msg::{Msg, Port, Reply};

/// Verbindingsthreads per poort. Een CLI-aanroep, een heartbeat van elke
/// agent per 10 s en een GUI: acht tegelijk is ruim voor een cluster van
/// tientallen nodes.
pub(crate) const WORKERS: usize = 8;

/// De langste stilte op een verbinding voordat de thread hem sluit. De
/// keep-alive van leanhttp (60 s) zou een thread uit de pool een minuut
/// vasthouden voor een client die niets meer vraagt.
const READ_CAP: Duration = Duration::from_secs(5);

/// Hoe lang een verbinding op de eigenaar wacht. Een apply met een rolling
/// update wacht op de uitrol (2 s per stap), dus ruim.
const OWNER_TIMEOUT: Duration = Duration::from_secs(300);

/// Hoe lang de doorgifte naar de leader mag duren.
const PROXY_TIMEOUT: Duration = Duration::from_secs(120);

/// De grootste body die de doorgifte leest (Go: `proxyMaxBody`, 8 MiB).
const PROXY_MAX_BODY: usize = api::PROXY_MAX_BODY;

/// Een std-socket met een plafond op elke leestermijn.
struct Capped(StdConn<TcpStream>);

impl AsyncRead for Capped {
    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize, IoError>> {
        self.0.poll_read(cx, buf)
    }

    fn set_read_timeout(&mut self, t: Option<Duration>) -> Result<(), IoError> {
        let t = Some(t.map_or(READ_CAP, |t| t.min(READ_CAP)));
        self.0.set_read_timeout(t)
    }
}

impl AsyncWrite for Capped {
    fn poll_write(&mut self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, IoError>> {
        self.0.poll_write(cx, buf)
    }

    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), IoError>> {
        self.0.poll_flush(cx)
    }

    fn set_write_timeout(&mut self, t: Option<Duration>) -> Result<(), IoError> {
        self.0.set_write_timeout(t)
    }
}

impl Close for Capped {
    fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), IoError>> {
        self.0.poll_close(cx)
    }
}

/// Start [`WORKERS`] threads op `listener`; elke thread bezit een kloon.
pub(crate) fn spawn_pool(
    listener: &TcpListener,
    port: Port,
    owner: &Sender<Msg>,
) -> std::io::Result<()> {
    for i in 0..WORKERS {
        let l = listener.try_clone()?;
        let owner = owner.clone();
        std::thread::Builder::new()
            .name(format!("http-{port:?}-{i}"))
            .spawn(move || worker(&l, port, &owner))?;
    }
    Ok(())
}

fn worker(l: &TcpListener, port: Port, owner: &Sender<Msg>) {
    let http = Http::new();
    loop {
        let stream = match l.accept() {
            Ok((s, _)) => s,
            Err(e) => {
                eprintln!("hop: accept on {port:?}: {e}");
                std::thread::sleep(Duration::from_millis(100));
                continue;
            }
        };
        let _ = stream.set_nodelay(true);
        let conn = Capped(StdConn::new(stream, Some(READ_CAP)));
        // Een verbinding die eindigt met een termijn of een reset is een
        // client die wegging; geen logregel waard.
        let _ = block_on(serve(conn, port, owner, &http));
    }
}

async fn serve(conn: Capped, port: Port, owner: &Sender<Msg>, http: &Http) -> leanhttp::Result {
    let out = leanhttp::serve(conn, async |ex: &mut Exchange<'_, Capped>| {
        let req = read_request(ex).await?;
        let reply = ask(owner, port, &req);
        let reply = match reply {
            Reply::Proxy { leader } => Reply::Plain(proxy(http, &leader, &req)),
            r => r,
        };
        write_reply(ex, &reply).await
    })
    .await?;
    // Geen route neemt de verbinding over; komt er toch een terug, dan valt hij hier.
    drop(out);
    Ok(())
}

/// Stuurt het verzoek naar de eigenaar en wacht op het antwoord.
fn ask(owner: &Sender<Msg>, port: Port, req: &Request) -> Reply {
    let (tx, rx) = mpsc::sync_channel(1);
    let msg = Msg::Http {
        port,
        req: req.clone(),
        reply: tx,
    };
    if owner.send(msg).is_err() {
        return Reply::Plain(Response::error(503, "node is shutting down"));
    }
    rx.recv_timeout(OWNER_TIMEOUT)
        .unwrap_or_else(|_| Reply::Plain(Response::error(504, "node did not answer in time")))
}

/// Geeft `req` door aan de leader op `leader` en geeft zijn antwoord terug.
pub(crate) fn proxy(http: &Http, leader: &str, req: &Request) -> Response {
    let mut url = format!("http://{leader}{}", req.path);
    if !req.query.is_empty() {
        url.push('?');
        url.push_str(&req.query);
    }
    let mut headers: Vec<(&str, &str)> = Vec::new();
    for name in [auth::AUTH_HEADER, "Content-Type"] {
        if let Some(v) = req.header(name) {
            headers.push((name, v));
        }
    }
    let call = Call {
        method: req.method.as_str(),
        url: &url,
        headers: &headers,
        body: (!req.body.is_empty() || matches!(req.method, Method::Post | Method::Patch))
            .then_some(req.body.as_slice()),
        timeout: PROXY_TIMEOUT,
    };
    match http.request(&call, PROXY_MAX_BODY) {
        Ok(r) => {
            let mut resp = Response::empty(r.status);
            if let Some(ct) = r.header("Content-Type") {
                resp.set_header("Content-Type", ct);
            }
            resp.body = r.body;
            resp
        }
        Err(e) => Response::error(502, &format!("leader {leader}: {e}")),
    }
}

/// Leest het verzoek van een [`Exchange`] als [`api::Request`], met de body.
///
/// De body is al begrensd: leanhttp weigert alles boven
/// [`leanhttp::MAX_BODY_BYTES`] (1 MiB) met een 413 voordat dit draait.
async fn read_request<C: leanhttp::Conn>(ex: &mut Exchange<'_, C>) -> leanhttp::Result<Request> {
    let headers = ex
        .req
        .header
        .iter()
        .map(|(k, v)| (String::from(k), String::from(v)))
        .collect();
    let body = ex.read_body_to_end().await?;
    Ok(Request {
        method: Method::parse(&ex.req.method),
        path: ex.req.path.clone(),
        query: ex.req.raw_query.clone(),
        headers,
        body,
    })
}

/// Zet status en headers van `r` op het antwoord.
fn write_head<C: leanhttp::Conn>(ex: &mut Exchange<'_, C>, r: &Response) -> leanhttp::Result {
    for (k, v) in &r.headers {
        // leanhttp rekent de lengte zelf.
        if k.eq_ignore_ascii_case("Content-Length") {
            continue;
        }
        ex.header_mut().set(k, v)?;
    }
    ex.write_header(r.status)
}

/// Schrijft een antwoord op de draad.
async fn write_reply<C: leanhttp::Conn>(
    ex: &mut Exchange<'_, C>,
    reply: &Reply,
) -> leanhttp::Result {
    match reply {
        Reply::Plain(r) => {
            write_head(ex, r)?;
            if !r.body.is_empty() {
                ex.write(&r.body).await?;
            }
            Ok(())
        }
        Reply::Events { head, lines } => {
            write_head(ex, head)?;
            ex.header_mut().set("Content-Type", "text/event-stream")?;
            ex.header_mut().set("Cache-Control", "no-cache")?;
            for line in lines {
                ex.write(b"data: ").await?;
                ex.write(line.as_bytes()).await?;
                ex.write(b"\n\n").await?;
            }
            ex.flush().await
        }
        // De thread voerde de doorgifte al uit; hier komt hij niet.
        Reply::Proxy { .. } => {
            write_head(ex, &Response::error(500, "unexecuted proxy"))?;
            Ok(())
        }
    }
}
