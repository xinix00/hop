//! De HTTP-servers van de daemon: een vaste pool verbindingsthreads per poort, leanhttp over std-sockets.
//!
//! Bezit per thread één verbinding tegelijk en een kloon van de listener;
//! de staat van de node bezit hij niet. Elk verzoek gaat als [`Msg::Http`]
//! naar de eigenaar, het antwoord komt terug over een eigen kanaal.
//! Handboek §2: een verbinding is een taak uit een vaste pool, de grootte is
//! een constante ([`WORKERS`]).
//!
//! Een [`Reply::Proxy`] voert de thread zelf uit (zie [`crate::msg::Reply`]):
//! hetzelfde verzoek, met dezelfde `X-Hop-Auth`, naar de leader. Zo ook de
//! rondgang van `/v1/tasks` en de doorgifte naar één agent
//! ([`Reply::Tasks`], [`Reply::Agent`]): de eigenaar wacht nooit op het net.
//!
//! Op de agent-poort zet de thread de CORS-koppen ([`api::cors_headers`])
//! op elk antwoord voordat hij het verzoek doorgeeft: ook een doorgifte en
//! een stroom, die hun kop hier krijgen en niet in de handler.
//!
//! Een stroom (SSE van `/v1/events`, een log-tail, een doorgegeven stroom)
//! houdt zijn thread vast tot hij af is of de lezer weg. De thread vraagt
//! de eigenaar elke [`STREAM_POLL`] wat er bij kwam ([`Msg::Poll`]); de
//! eigenaar heeft zo geen tabel van wachtende verbindingen. Hoeveel stromen
//! er tegelijk open staan, telt de eigenaar ([`crate::node::MAX_STREAMS`]);
//! de thread meldt zijn stroom af in `Drop` ([`StreamGuard`]).

use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Sender};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use api::{Method, Request, Response};
use hostnet::{Call, Http, StdConn, block_on};
use leanhttp::{AsyncRead, AsyncWrite, Close, Exchange, IoError, Next, Source};
use types::json;

use crate::msg::{Chunk, Msg, Poll as Ask, Port, Reply};

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

/// Hoe vaak een open stroom de eigenaar vraagt wat er bij kwam. De
/// logregels komen per tik van de eigenaar (1 s) in de ring, dus vaker
/// vragen levert niets op; minder vaak maakt een tail stroef.
const STREAM_POLL: Duration = Duration::from_millis(500);

/// Na zoveel stilte schrijft een stroom een [`api::KEEPALIVE`]. Die houdt
/// proxies wakker, en de schrijf is hoe een stroom merkt dat zijn lezer
/// weg is (de tweede schrijf na een gesloten verbinding faalt).
const KEEPALIVE_EVERY: Duration = Duration::from_secs(15);

/// De stiltetermijn van een doorgegeven stroom: ruim boven de keepalive
/// van de bron, dus een bron die zo lang zwijgt, is dood.
const STREAM_IDLE: Duration = Duration::from_secs(60);

/// Het budget van de hele rondgang van `/v1/tasks`, zoals Go's
/// `HTTPClientTimeout` rond `GetClusterStatus`: een dode agent kost de
/// aanroeper niet meer dan dit.
const TASKS_BUDGET: Duration = Duration::from_secs(5);

/// De termijn per agent binnen die rondgang: één trage agent eet niet het
/// hele budget op voordat de rest gevraagd is.
const TASKS_EACH: Duration = Duration::from_secs(2);

/// De termijn van een gewone doorgifte naar een agent (`/capacity`).
const AGENT_TIMEOUT: Duration = Duration::from_secs(10);

/// De grootste takenlijst die de rondgang van één agent leest.
const MAX_TASKS_BODY: usize = 8 << 20;

/// Wat elke verbindingsthread van zijn poort meekrijgt.
struct Worker {
    port: Port,
    owner: Sender<Msg>,
    http: Http,
    /// De clustersleutel, voor de aanroepen die de thread zelf doet.
    key: Vec<u8>,
    /// Hoeveel threads van deze poort een verbinding hebben. Zijn het er
    /// [`WORKERS`], dan wacht de volgende verbinding in de backlog, en dan
    /// sluit een thread zijn verbinding na het antwoord in plaats van tot
    /// [`READ_CAP`] op een keep-alive-client te wachten die misschien niets
    /// meer vraagt. Een browser doet zijn parallelle verzoeken op eigen
    /// verbindingen; Go had een goroutine per verbinding en kende dit
    /// wachten niet.
    busy: Arc<AtomicUsize>,
}

/// Meldt een open stroom af bij de eigenaar, op elk pad.
struct StreamGuard(Sender<Msg>);

impl Drop for StreamGuard {
    fn drop(&mut self) {
        let _ = self.0.send(Msg::StreamDone);
    }
}

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
    key: &[u8],
) -> std::io::Result<()> {
    let busy = Arc::new(AtomicUsize::new(0));
    for i in 0..WORKERS {
        let l = listener.try_clone()?;
        let w = Worker {
            port,
            owner: owner.clone(),
            http: Http::new(),
            key: key.to_vec(),
            busy: busy.clone(),
        };
        std::thread::Builder::new()
            .name(format!("http-{port:?}-{i}"))
            .spawn(move || worker(&l, &w))?;
    }
    Ok(())
}

fn worker(l: &TcpListener, w: &Worker) {
    let port = w.port;
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
        w.busy.fetch_add(1, Ordering::AcqRel);
        // Een verbinding die eindigt met een termijn of een reset is een
        // client die wegging; geen logregel waard.
        let _ = block_on(serve(conn, w));
        w.busy.fetch_sub(1, Ordering::AcqRel);
    }
}

async fn serve(conn: Capped, w: &Worker) -> leanhttp::Result {
    let out = leanhttp::serve(conn, async |ex: &mut Exchange<'_, Capped>| {
        let req = read_request(ex).await?;
        if w.busy.load(Ordering::Acquire) >= WORKERS {
            // Elke thread bezet: na dit antwoord dicht, zodat de wachtende
            // verbinding een thread krijgt zodra dit verzoek af is.
            ex.header_mut().set("Connection", "close")?;
        }
        if w.port == Port::Agent {
            // De browser van het dashboard praat met deze poort: élk antwoord
            // draagt de CORS-koppen, ook een doorgifte naar de leader en een
            // stroom, die hun kop hier en niet in de handler krijgen.
            for (k, v) in api::cors_headers(&req) {
                ex.header_mut().set(k, v)?;
            }
        }
        let reply = ask(&w.owner, w.port, &req);
        // Een stroom is al geteld; vanaf hier meldt de guard hem af, ook als
        // de eerste schrijf al faalt.
        let _guard = reply.is_stream().then(|| StreamGuard(w.owner.clone()));
        execute(ex, w, &req, reply).await
    })
    .await?;
    // Geen route neemt de verbinding over; komt er toch een terug, dan valt hij hier.
    drop(out);
    Ok(())
}

/// Voert het antwoord van de eigenaar uit op de verbinding.
async fn execute<C: leanhttp::Conn>(
    ex: &mut Exchange<'_, C>,
    w: &Worker,
    req: &Request,
    reply: Reply,
) -> leanhttp::Result {
    match reply {
        Reply::Proxy {
            leader,
            stream: false,
        } => write_reply(ex, &Reply::Plain(proxy(&w.http, &leader, req))).await,
        Reply::Proxy {
            leader,
            stream: true,
        } => {
            let url = with_query(&format!("http://{leader}{}", req.path), &req.query);
            let auth = req.header(auth::AUTH_HEADER).map(String::from);
            relay(ex, &w.http, &url, auth.as_deref()).await
        }
        Reply::Tasks { agents, scope } => {
            let r = fan_out(&w.http, &w.key, &agents, &scope);
            write_reply(ex, &Reply::Plain(r)).await
        }
        Reply::Agent {
            endpoint,
            path,
            stream: false,
        } => {
            let r = agent_call(&w.http, &w.key, &endpoint, &path);
            write_reply(ex, &Reply::Plain(r)).await
        }
        Reply::Agent {
            endpoint,
            path,
            stream: true,
        } => {
            let url = format!("{}{path}", endpoint.trim_end_matches('/'));
            let sig = signature(&w.key, &url);
            relay(ex, &w.http, &url, sig.as_deref()).await
        }
        Reply::Follow {
            head,
            task_id,
            stream,
        } => {
            let ask = |seq| Ask::Logs {
                task_id: task_id.clone(),
                stream,
                seq,
            };
            pump(ex, &w.owner, &head, "", 0, ask).await
        }
        Reply::Subscribe { head, seq } => {
            pump(ex, &w.owner, &head, api::PING, seq, |seq| Ask::Events {
                seq,
            })
            .await
        }
        r @ (Reply::Plain(_) | Reply::Events { .. }) => write_reply(ex, &r).await,
    }
}

/// Een URL met de query van de aanroeper erachter.
fn with_query(url: &str, query: &str) -> String {
    if query.is_empty() {
        return String::from(url);
    }
    format!("{url}?{query}")
}

/// De `X-Hop-Auth` van een GET op `url`, of `None` zonder sleutel.
fn signature(key: &[u8], url: &str) -> Option<String> {
    auth::sign_call(key, "GET", url, b"").map(|s| String::from_utf8_lossy(&s).into_owned())
}

/// Zet de kop van een SSE-stroom op het antwoord.
fn stream_head<C: leanhttp::Conn>(ex: &mut Exchange<'_, C>, head: &Response) -> leanhttp::Result {
    ex.header_mut().set("Content-Type", "text/event-stream")?;
    ex.header_mut().set("Cache-Control", "no-cache")?;
    write_head(ex, head)
}

/// De bron van een stroom uit de eigenaar voor [`Exchange::stream`]: elke
/// vraag blokkerend (deze thread is van deze ene stroom), een
/// [`api::KEEPALIVE`] na [`KEEPALIVE_EVERY`] stilte, en [`STREAM_POLL`]
/// slaap als er niets is.
struct OwnerSource<'a, F: Fn(u64) -> Ask> {
    owner: &'a Sender<Msg>,
    ask: F,
    seq: u64,
    quiet: Instant,
    /// Wat meteen na de kop komt (de ping van `/v1/events`), één keer.
    first: Option<&'a str>,
    /// De eigenaar zei dat het af is; de volgende beurt is het einde.
    ended: bool,
}

impl<F: Fn(u64) -> Ask> Source for OwnerSource<'_, F> {
    async fn next(&mut self) -> Next {
        if let Some(f) = self.first.take()
            && !f.is_empty()
        {
            return Next::Data(f.as_bytes().to_vec());
        }
        if self.ended {
            return Next::End;
        }
        // De eigenaar is weg: de node stopt, de stroom ook.
        let Some(c) = poll_owner(self.owner, (self.ask)(self.seq)) else {
            return Next::End;
        };
        self.seq = c.seq;
        self.ended = c.done;
        if !c.text.is_empty() {
            self.quiet = Instant::now();
            return Next::Data(c.text.into_bytes());
        }
        if c.done {
            return Next::End;
        }
        if self.quiet.elapsed() >= KEEPALIVE_EVERY {
            self.quiet = Instant::now();
            return Next::Data(api::KEEPALIVE.as_bytes().to_vec());
        }
        Next::Nothing
    }

    async fn nap(&mut self) {
        // Deze thread is van deze ene stroom: slapen is hier wachten.
        std::thread::sleep(STREAM_POLL);
    }
}

/// Een open stroom uit de eigenaar via [`Exchange::stream`]: kop, `first`,
/// en dan elke [`STREAM_POLL`] wat er sinds `seq` bij kwam, tot de eigenaar
/// zegt dat het af is of de lezer weg is (leanhttp sondeert de leeskant
/// elke beurt; op deze blokkerende socket is dat één `read` van 1 ms).
async fn pump<C: leanhttp::Conn>(
    ex: &mut Exchange<'_, C>,
    owner: &Sender<Msg>,
    head: &Response,
    first: &str,
    seq: u64,
    ask: impl Fn(u64) -> Ask,
) -> leanhttp::Result {
    stream_head(ex, head)?;
    let mut src = OwnerSource {
        owner,
        ask,
        seq,
        quiet: Instant::now(),
        first: Some(first),
        ended: false,
    };
    ex.stream(head.status, &mut src).await
}

/// Vraagt de eigenaar één [`Ask`]; `None` als hij niet (meer) antwoordt.
fn poll_owner(owner: &Sender<Msg>, poll: Ask) -> Option<Chunk> {
    let (tx, rx) = mpsc::sync_channel(1);
    owner.send(Msg::Poll { poll, reply: tx }).ok()?;
    rx.recv_timeout(OWNER_TIMEOUT).ok()
}

/// Geeft een stroom door: `GET url` met `auth` als `X-Hop-Auth`, en elke
/// hap van de bron meteen naar de lezer, tot een van beide kanten stopt.
///
/// Een bron die geen 200 geeft, gaat als gewoon antwoord terug (status en
/// het begin van de body), zoals Go's `proxyToAgent`.
async fn relay<C: leanhttp::Conn>(
    ex: &mut Exchange<'_, C>,
    http: &Http,
    url: &str,
    auth: Option<&str>,
) -> leanhttp::Result {
    let mut headers: Vec<(&str, &str)> = Vec::new();
    if let Some(a) = auth {
        headers.push((auth::AUTH_HEADER, a));
    }
    let call = Call {
        method: "GET",
        url,
        headers: &headers,
        body: None,
        timeout: STREAM_IDLE,
    };
    let mut src = match http.open(&call) {
        Ok(o) => o,
        Err(e) => {
            let r = Response::error(502, &format!("{}: {e}", auth::path_of(url)));
            return write_reply(ex, &Reply::Plain(r)).await;
        }
    };
    if src.status() != 200 {
        let mut r = Response::empty(src.status());
        if let Some(ct) = src.header("Content-Type") {
            r.set_header("Content-Type", ct);
        }
        r.body = src.read_to_end(PROXY_MAX_BODY).unwrap_or_default();
        return write_reply(ex, &Reply::Plain(r)).await;
    }
    // De leeskant vóór de kop, zodat de lus tussen twee happen kan zien dat
    // de lezer wegging (de bron gaat dan mee dicht).
    ex.claim_done().await?;
    stream_head(ex, &Response::empty(200))?;
    ex.flush().await?;
    let mut buf = vec![0u8; 16 << 10];
    loop {
        // Een bron die dichtgaat of zwijgt tot zijn termijn, is het einde.
        let n = match src.read(&mut buf) {
            Ok(0) | Err(_) => return Ok(()),
            Ok(n) => n,
        };
        if ex.reader_gone().await {
            return Ok(());
        }
        ex.write(buf.get(..n).unwrap_or_default()).await?;
        ex.flush().await?;
    }
}

/// De rondgang van `/v1/tasks` en `/v1/jobs/{naam}/status`: elke agent
/// `GET /tasks`, ondertekend, met één totaal budget ([`TASKS_BUDGET`]);
/// wie niet antwoordt, ontbreekt. `scope` kiest de vorm van het antwoord.
fn fan_out(
    http: &Http,
    key: &[u8],
    agents: &[(String, String)],
    scope: &api::TasksScope,
) -> Response {
    let until = Instant::now() + TASKS_BUDGET;
    let results: Vec<(String, Option<Vec<types::Task>>)> = agents
        .iter()
        .map(|(id, endpoint)| (id.clone(), agent_tasks(http, key, endpoint, until)))
        .collect();
    scope.reply(&results)
}

/// De taken van één agent, of `None` als hij niet (op tijd) antwoordde.
fn agent_tasks(
    http: &Http,
    key: &[u8],
    endpoint: &str,
    until: Instant,
) -> Option<Vec<types::Task>> {
    let left = until.saturating_duration_since(Instant::now());
    if left.is_zero() {
        return None;
    }
    let url = format!("{}/tasks", endpoint.trim_end_matches('/'));
    let sig = signature(key, &url);
    let mut headers: Vec<(&str, &str)> = Vec::new();
    if let Some(s) = &sig {
        headers.push((auth::AUTH_HEADER, s));
    }
    let call = Call {
        method: "GET",
        url: &url,
        headers: &headers,
        body: None,
        timeout: TASKS_EACH.min(left),
    };
    let until = until.min(Instant::now() + TASKS_EACH);
    let r = http.request_until(&call, MAX_TASKS_BODY, until).ok()?;
    if r.status != 200 {
        return None;
    }
    let v = json::parse(&r.body).ok()?;
    v.as_array()?
        .iter()
        .map(|t| types::Task::from_value(t).ok())
        .collect()
}

/// Een gewone doorgifte naar een agent: `GET {endpoint}{path}`, ondertekend.
fn agent_call(http: &Http, key: &[u8], endpoint: &str, path: &str) -> Response {
    let url = format!("{}{path}", endpoint.trim_end_matches('/'));
    let sig = signature(key, &url);
    let mut headers: Vec<(&str, &str)> = Vec::new();
    if let Some(s) = &sig {
        headers.push((auth::AUTH_HEADER, s));
    }
    let call = Call {
        method: "GET",
        url: &url,
        headers: &headers,
        body: None,
        timeout: AGENT_TIMEOUT,
    };
    match http.request_until(&call, PROXY_MAX_BODY, Instant::now() + AGENT_TIMEOUT) {
        Ok(r) => {
            let mut resp = Response::empty(r.status);
            if let Some(ct) = r.header("Content-Type") {
                resp.set_header("Content-Type", ct);
            }
            resp.body = r.body;
            resp
        }
        // Go: "failed to contact agent", met het adres erbij.
        Err(e) => Response::error(502, &format!("failed to contact agent {endpoint}: {e}")),
    }
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
    let url = with_query(&format!("http://{leader}{}", req.path), &req.query);
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
        // De rest voert `execute` uit; hier komt hij niet.
        _ => {
            write_head(ex, &Response::error(500, "unexecuted reply"))?;
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    //! De doorgifte van een stroom en de rondgang van `/v1/tasks`, tegen
    //! echte sockets op 127.0.0.1 en een verbinding in het geheugen.

    use std::cell::RefCell;
    use std::io::{BufRead, BufReader, Write};
    use std::rc::Rc;
    use std::thread;

    use super::*;

    /// Een verbinding in het geheugen: het verzoek erin, het antwoord eruit.
    /// De client blijft na zijn verzoek lezen (een lezer van een stroom): na
    /// de invoer `Pending`, en een lees met termijn (de sondering van
    /// `reader_gone`) verloopt meteen.
    struct Mem {
        input: Vec<u8>,
        at: usize,
        out: Rc<RefCell<Vec<u8>>>,
        probing: bool,
    }

    impl AsyncRead for Mem {
        fn poll_read(
            &mut self,
            _: &mut Context<'_>,
            buf: &mut [u8],
        ) -> Poll<Result<usize, IoError>> {
            let rest = &self.input[self.at..];
            if rest.is_empty() {
                return if self.probing {
                    Poll::Ready(Err(IoError::TimedOut))
                } else {
                    Poll::Pending
                };
            }
            let n = rest.len().min(buf.len());
            buf[..n].copy_from_slice(&rest[..n]);
            self.at += n;
            Poll::Ready(Ok(n))
        }

        fn set_read_timeout(&mut self, t: Option<Duration>) -> Result<(), IoError> {
            self.probing = t.is_some();
            Ok(())
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

    /// Een bron die één verzoek leest (en de kop teruggeeft) en `answer` schrijft.
    fn source(answer: &'static [u8]) -> (String, thread::JoinHandle<String>) {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap().to_string();
        let h = thread::spawn(move || {
            let (s, _) = l.accept().unwrap();
            let mut r = BufReader::new(s.try_clone().unwrap());
            let mut head = String::new();
            loop {
                let mut line = String::new();
                r.read_line(&mut line).unwrap();
                head.push_str(&line);
                if line == "\r\n" {
                    break;
                }
            }
            let mut s = s;
            s.write_all(answer).unwrap();
            head
        });
        (addr, h)
    }

    /// Eén verzoek door leanhttp; `f` doet het werk; wat er op de draad kwam.
    fn exchange(f: impl AsyncFn(&mut Exchange<'_, Mem>) -> leanhttp::Result) -> String {
        let out = Rc::new(RefCell::new(Vec::new()));
        let conn = Mem {
            input: b"GET /x HTTP/1.1\r\nHost: n\r\n\r\n".to_vec(),
            at: 0,
            out: out.clone(),
            probing: false,
        };
        block_on(leanhttp::serve(conn, async |ex: &mut Exchange<'_, Mem>| {
            f(ex).await
        }))
        .unwrap();
        String::from_utf8(out.borrow().clone()).unwrap()
    }

    #[test]
    fn a_stream_is_relayed_as_it_comes() {
        let (addr, h) = source(
            b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n\
              b\r\ndata: one\n\n\r\nb\r\ndata: two\n\n\r\n0\r\n\r\n",
        );
        let url = format!("http://{addr}/logs/t1/stdout?follow=1");
        let http = Http::new();
        let text = exchange(async |ex| relay(ex, &http, &url, Some("sig")).await);
        assert!(text.contains("Content-Type: text/event-stream"), "{text}");
        assert!(text.contains("data: one\n\n"), "{text}");
        assert!(text.contains("data: two\n\n"), "{text}");
        // De bron kreeg het pad met de query en de handtekening.
        let seen = h.join().unwrap();
        assert!(
            seen.starts_with("GET /logs/t1/stdout?follow=1 HTTP/1.1"),
            "{seen}"
        );
        assert!(seen.contains("X-Hop-Auth: sig"), "{seen}");
    }

    #[test]
    fn a_refusing_source_passes_its_status_through() {
        let (addr, _h) = source(
            b"HTTP/1.1 404 Not Found\r\nContent-Type: application/json\r\nContent-Length: 28\r\n\r\n{\"error\":\"no logs for task\"}",
        );
        let url = format!("http://{addr}/logs/t9/stdout");
        let http = Http::new();
        let text = exchange(async |ex| relay(ex, &http, &url, None).await);
        assert!(text.starts_with("HTTP/1.1 404"), "{text}");
        assert!(text.contains("no logs for task"), "{text}");
    }

    /// Eén verzoek over `s`; de kop van het antwoord (tot de lege regel).
    fn ask_over(s: &mut TcpStream) -> String {
        s.write_all(b"GET /health HTTP/1.1\r\nHost: n\r\n\r\n")
            .unwrap();
        let mut r = BufReader::new(s.try_clone().unwrap());
        let mut head = String::new();
        loop {
            let mut line = String::new();
            r.read_line(&mut line).unwrap();
            head.push_str(&line);
            if line == "\r\n" || line.is_empty() {
                return head;
            }
        }
    }

    #[test]
    fn a_full_pool_closes_after_the_answer_so_the_next_connection_never_waits() {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap();
        // De eigenaar: elk verzoek een leeg 200.
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            for m in rx {
                if let Msg::Http { reply, .. } = m {
                    let _ = reply.send(Reply::Plain(Response::empty(200)));
                }
            }
        });
        spawn_pool(&l, Port::Agent, &tx, b"").unwrap();

        // Keep-alive-clients die na hun verzoek stil blijven, één per thread.
        let mut held = Vec::new();
        for i in 1..=WORKERS {
            let mut s = TcpStream::connect(addr).unwrap();
            let head = ask_over(&mut s);
            assert!(head.starts_with("HTTP/1.1 200"), "{head}");
            if i < WORKERS {
                assert!(!head.contains("Connection: close"), "#{i}: {head}");
            } else {
                // De laatste vrije thread: dicht na het antwoord.
                assert!(head.contains("Connection: close"), "#{i}: {head}");
            }
            held.push(s);
        }
        // De volgende verbinding krijgt die thread meteen, niet na READ_CAP.
        let t = Instant::now();
        let mut s = TcpStream::connect(addr).unwrap();
        let head = ask_over(&mut s);
        assert!(head.starts_with("HTTP/1.1 200"), "{head}");
        assert!(
            t.elapsed() < Duration::from_secs(2),
            "waited {:?} on an idle keep-alive client",
            t.elapsed()
        );
        drop(held);
    }

    #[test]
    fn a_reader_that_leaves_frees_its_stream_within_a_poll() {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap();
        // De eigenaar: `/v1/events` is een stroom die nooit af is, en elke
        // vraag levert niets nieuws; een afmelding gaat naar de test.
        let (tx, rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        thread::spawn(move || {
            for m in rx {
                match m {
                    Msg::Http { reply, .. } => {
                        let _ = reply.send(Reply::Subscribe {
                            head: Response::empty(200),
                            seq: 0,
                        });
                    }
                    Msg::Poll { reply, .. } => {
                        let _ = reply.send(Chunk::default());
                    }
                    Msg::StreamDone => {
                        let _ = done_tx.send(());
                    }
                    _ => {}
                }
            }
        });
        spawn_pool(&l, Port::Agent, &tx, b"").unwrap();

        let mut s = TcpStream::connect(addr).unwrap();
        s.write_all(b"GET /v1/events HTTP/1.1\r\nHost: n\r\n\r\n")
            .unwrap();
        let mut r = BufReader::new(s.try_clone().unwrap());
        let mut line = String::new();
        r.read_line(&mut line).unwrap();
        assert!(line.starts_with("HTTP/1.1 200"), "{line}");
        // De lezer gaat weg; de stroom moet dat binnen een paar polls zien,
        // niet pas na twee keepalives (30 s).
        drop(r);
        drop(s);
        let t = Instant::now();
        done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("the stream did not notice its reader leaving");
        assert!(t.elapsed() < Duration::from_secs(5));
    }

    #[test]
    fn the_tasks_round_names_the_silent_agent() {
        let (addr, h) = source(
            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 38\r\n\r\n[{\"id\":\"t1\",\"job_name\":\"web\",\"pid\":7}]",
        );
        // Een poort waar niemand luistert: die agent ontbreekt.
        let dead = TcpListener::bind("127.0.0.1:0").unwrap();
        let dead_addr = dead.local_addr().unwrap().to_string();
        drop(dead);
        let agents = [
            (String::from("a1"), format!("http://{addr}")),
            (String::from("a2"), format!("http://{dead_addr}")),
        ];
        let r = fan_out(&Http::new(), b"key", &agents, &api::TasksScope::All);
        assert_eq!(r.status, 200);
        let body = String::from_utf8(r.body).unwrap();
        assert!(body.contains(r#""tasks_by_agent":{"a1":[{"#), "{body}");
        assert!(body.contains(r#""unreachable":["a2"]"#), "{body}");
        // Ondertekend met de clustersleutel, over `GET /tasks`.
        let seen = h.join().unwrap();
        assert!(seen.starts_with("GET /tasks HTTP/1.1"), "{seen}");
        assert!(seen.contains("X-Hop-Auth: "), "{seen}");
    }
}
