//! De doorgiftes over het LAN: de verbindingstaak geeft een verzoek door aan de leader of aan een agent.
//!
//! De eigenaar wacht nooit op het net (zie [`crate::mail`]). Moet een verzoek
//! naar een andere node, dan antwoordt hij met een [`Forward`] in plaats van
//! een antwoord, en de verbindingstaak die het verzoek bracht, voert hem zelf
//! uit ([`serve`]), zoals de verbindingsthread van de daemon
//! (`agentd/src/http.rs`):
//!
//! - [`Forward::Leader`]: deze node leidt niet; `/v1/*` op de agent-poort gaat
//!   ongewijzigd naar de leader (methode, pad, query, body en de
//!   `X-Hop-Auth` van de aanroeper: de handtekening dekt alleen methode, pad
//!   en body, en het hele cluster deelt één sleutel). Een stroom
//!   (`/v1/events`, een log-tail) spoelt per brok door;
//! - [`Forward::Agent`]: deze node leidt, en `/v1/agents/{id}/logs/...` of
//!   `/capacity` is voor een agent op een andere node: een ondertekende `GET`
//!   daar, gebufferd of als stroom;
//! - [`Forward::Tasks`]: de rondgang van `/v1/tasks` en
//!   `/v1/jobs/{naam}/status` langs de agents op andere nodes, met de taken
//!   van de eigen agent die de eigenaar al meegaf.
//!
//! Een stroom houdt zijn verbindingstaak vast zolang hij loopt; de eigenaar
//! telde hem bij het antwoord, en [`serve`] meldt hem af met
//! [`Streams::done`], op elk pad.

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use core::time::Duration;

use api::{Method, Request, Response, TasksScope};
use hop_http::{Reply, Streams};
use leanhttp::{AsyncRead, AsyncWrite, Close, Exchange};
use types::Task;

use crate::Port;
use crate::client::{Client, Req, redact};
use crate::fetch::{Connect, Resolve};

/// Hoe lang de doorgifte naar de leader mag duren; een apply met een
/// rolling update wacht op de uitrol (zoals de daemon: 120 s).
pub const PROXY_TIMEOUT: Duration = Duration::from_secs(120);

/// De termijn van een gewone doorgifte naar een agent (`/capacity`, een
/// momentopname van de log).
pub const AGENT_TIMEOUT: Duration = Duration::from_secs(10);

/// De termijn per agent in de rondgang van `/v1/tasks`: één trage agent
/// eet niet het budget van de rest op (de daemon: 2 s per agent).
pub const TASKS_EACH: Duration = Duration::from_secs(2);

/// De stiltetermijn van een doorgegeven stroom: ruim boven de keepalive van
/// de bron (15 s), dus een bron die zo lang zwijgt, is dood.
pub const STREAM_IDLE: Duration = Duration::from_secs(60);

/// De grootste takenlijst die de rondgang van één agent leest.
const MAX_TASKS_BODY: usize = 8 << 20;

/// Een brok van een doorgegeven stroom.
const RELAY_CHUNK: usize = 4 << 10;

/// Een verzoek dat de verbindingstaak zelf doorgeeft.
#[derive(Debug, PartialEq)]
pub enum Forward {
    /// Hetzelfde verzoek naar de leader op `addr` (`ip:poort`).
    Leader {
        /// De leader.
        addr: String,
        /// Het verzoek van de aanroeper.
        req: Request,
        /// Een stroom: per brok doorspoelen.
        stream: bool,
    },
    /// `GET {endpoint}{path}` bij een agent, ondertekend met de clustersleutel.
    Agent {
        /// Het endpoint van de agent (`http://ip:poort`).
        endpoint: String,
        /// Pad en query.
        path: String,
        /// De `X-Hop-Auth` voor die `GET`, als er een sleutel is.
        auth: Option<String>,
        /// Een stroom: per brok doorspoelen.
        stream: bool,
    },
    /// De rondgang van `/v1/tasks` langs de agents op andere nodes.
    Tasks {
        /// Per agent in de volgorde van de leader: het id, en óf de taken
        /// (de eigen agent, die kent de eigenaar al) óf het endpoint om te vragen.
        agents: Vec<(String, Source)>,
        /// De `X-Hop-Auth` van `GET /tasks` (voor elke agent dezelfde: de
        /// handtekening dekt alleen methode, pad en body).
        auth: Option<String>,
        /// De vorm van het antwoord.
        scope: TasksScope,
    },
}

/// Waar de taken van één agent in een rondgang vandaan komen.
#[derive(Debug, PartialEq)]
pub enum Source {
    /// De eigenaar gaf ze mee (de eigen agent); `None` zonder geheugen.
    Known(Option<Vec<Task>>),
    /// Te vragen bij dit endpoint.
    Ask(String),
}

impl Forward {
    /// Of dit een stroom is die de eigenaar als open telt.
    pub fn is_stream(&self) -> bool {
        matches!(
            self,
            Self::Leader { stream: true, .. } | Self::Agent { stream: true, .. }
        )
    }
}

/// Wat de eigenaar op een verzoek terugstuurt: een antwoord, of een doorgifte.
#[derive(Debug)]
pub enum Routed {
    /// Een antwoord van de eigenaar zelf.
    Reply(Reply),
    /// Een doorgifte voor de verbindingstaak.
    Forward(Forward),
}

/// Een URL met de query van de aanroeper erachter.
fn with_query(url: &str, query: &str) -> String {
    if query.is_empty() {
        return String::from(url);
    }
    format!("{url}?{query}")
}

/// Een gebufferd antwoord van de bron als [`Response`], met zijn inhoudstype.
fn plain(status: u16, content_type: Option<&str>, body: Vec<u8>) -> Response {
    let mut r = Response::empty(status);
    if let Some(ct) = content_type {
        r.set_header("Content-Type", ct);
    }
    r.body = body;
    r
}

/// De koppen van de aanroeper die mee gaan naar de leader.
fn passed(req: &Request) -> Vec<(&str, &str)> {
    let mut out = Vec::new();
    for name in [auth::AUTH_HEADER, "Content-Type"] {
        if let Some(v) = req.header(name) {
            out.push((name, v));
        }
    }
    out
}

/// Geeft `req` gebufferd door aan de leader op `addr`.
pub async fn to_leader<C: Connect, R: Resolve>(
    client: &mut Client<C, R>,
    addr: &str,
    req: &Request,
) -> Response {
    let url = with_query(&format!("http://{addr}{}", req.path), &req.query);
    let headers = passed(req);
    let body = (!req.body.is_empty() || matches!(req.method, Method::Post | Method::Patch))
        .then_some(req.body.as_slice());
    let r = Req {
        method: req.method.as_str(),
        url: &url,
        headers: &headers,
        body,
        timeout: PROXY_TIMEOUT,
    };
    match client.request(r, api::PROXY_MAX_BODY).await {
        Ok(got) => plain(got.status, got.content_type.as_deref(), got.body),
        Err(e) => Response::error(502, &format!("leader {addr}: {e}")),
    }
}

/// Een gewone doorgifte naar een agent: `GET {endpoint}{path}`, ondertekend.
pub async fn to_agent<C: Connect, R: Resolve>(
    client: &mut Client<C, R>,
    endpoint: &str,
    path: &str,
    auth: Option<&str>,
) -> Response {
    let url = format!("{}{path}", endpoint.trim_end_matches('/'));
    let mut headers: Vec<(&str, &str)> = Vec::new();
    if let Some(a) = auth {
        headers.push((auth::AUTH_HEADER, a));
    }
    let r = Req {
        method: "GET",
        url: &url,
        headers: &headers,
        body: None,
        timeout: AGENT_TIMEOUT,
    };
    match client.request(r, api::PROXY_MAX_BODY).await {
        Ok(got) => plain(got.status, got.content_type.as_deref(), got.body),
        // Go: "failed to contact agent", met het adres erbij.
        Err(e) => Response::error(502, &format!("failed to contact agent {endpoint}: {e}")),
    }
}

/// De taken van één agent, of `None` als hij niet (op tijd) antwoordde.
async fn agent_tasks<C: Connect, R: Resolve>(
    client: &mut Client<C, R>,
    endpoint: &str,
    auth: Option<&str>,
) -> Option<Vec<Task>> {
    let url = format!("{}/tasks", endpoint.trim_end_matches('/'));
    let mut headers: Vec<(&str, &str)> = Vec::new();
    if let Some(a) = auth {
        headers.push((auth::AUTH_HEADER, a));
    }
    let r = Req {
        method: "GET",
        url: &url,
        headers: &headers,
        body: None,
        timeout: TASKS_EACH,
    };
    let got = client.request(r, MAX_TASKS_BODY).await.ok()?;
    if got.status != 200 {
        return None;
    }
    let v = types::json::parse(&got.body).ok()?;
    v.as_array()?
        .iter()
        .map(|t| Task::from_value(t).ok())
        .collect()
}

/// De rondgang: elke agent op een andere node `GET /tasks`; wie niet antwoordt, ontbreekt.
pub async fn tasks<C: Connect, R: Resolve>(
    client: &mut Client<C, R>,
    agents: Vec<(String, Source)>,
    auth: Option<&str>,
    scope: &TasksScope,
) -> Response {
    let mut results: Vec<(String, Option<Vec<Task>>)> = Vec::new();
    if results.try_reserve_exact(agents.len()).is_err() {
        return Response::empty(500);
    }
    for (id, src) in agents {
        let t = match src {
            Source::Known(t) => t,
            Source::Ask(endpoint) => agent_tasks(client, &endpoint, auth).await,
        };
        results.push((id, t));
    }
    scope.reply(&results)
}

/// Zet de kop van een SSE-stroom (met de CORS van de agent-poort) op het antwoord.
fn stream_head<X: leanhttp::Conn>(
    ex: &mut Exchange<'_, X>,
    cors: Option<&Request>,
) -> leanhttp::Result {
    let mut head = Response::empty(200);
    if let Some(req) = cors {
        api::cors(req, &mut head);
    }
    ex.header_mut().set("Content-Type", "text/event-stream")?;
    ex.header_mut().set("Cache-Control", "no-cache")?;
    for (k, v) in &head.headers {
        if k.eq_ignore_ascii_case("Content-Length") {
            continue;
        }
        ex.header_mut().set(k, v)?;
    }
    ex.write_header(200)
}

/// Geeft een stroom door: `GET url`, en elke hap van de bron meteen naar de
/// lezer, tot een van beide kanten stopt.
///
/// Een bron die geen 200 geeft, gaat als gewoon antwoord terug (status en
/// het begin van de body), zoals Go's `proxyToAgent`.
async fn relay<X: leanhttp::Conn, C: Connect, R: Resolve>(
    ex: &mut Exchange<'_, X>,
    client: &mut Client<C, R>,
    url: &str,
    headers: &[(&str, &str)],
    cors: Option<&Request>,
) -> leanhttp::Result {
    let r = Req {
        method: "GET",
        url,
        headers,
        body: None,
        timeout: AGENT_TIMEOUT,
    };
    let mut src = match client.open(r).await {
        Ok(o) => o,
        Err(e) => {
            let mut resp = Response::error(502, &format!("{}: {e}", redact(url)));
            if let Some(req) = cors {
                api::cors(req, &mut resp);
            }
            return hop_http::write_reply(ex, &Reply::Plain(resp)).await;
        }
    };
    if src.status != 200 {
        let body = src
            .read_to_end(api::PROXY_MAX_BODY)
            .await
            .unwrap_or_default();
        let mut resp = plain(src.status, src.header.get("Content-Type"), body);
        if let Some(req) = cors {
            api::cors(req, &mut resp);
        }
        return hop_http::write_reply(ex, &Reply::Plain(resp)).await;
    }
    stream_head(ex, cors)?;
    ex.flush().await?;
    let mut buf = alloc::vec![0u8; RELAY_CHUNK];
    loop {
        // Een bron die dichtgaat of zwijgt tot zijn termijn, is het einde.
        let n = match src.read(&mut buf).await {
            Ok(0) | Err(_) => return Ok(()),
            Ok(n) => n,
        };
        ex.write(buf.get(..n).unwrap_or_default()).await?;
        ex.flush().await?;
    }
}

/// Voert een [`Forward`] uit op de verbinding; `cors` is het verzoek als het
/// op de agent-poort binnenkwam (de koppen van het dashboard).
pub async fn execute<X: leanhttp::Conn, C: Connect, R: Resolve>(
    ex: &mut Exchange<'_, X>,
    fwd: Forward,
    client: &mut Client<C, R>,
    cors: Option<&Request>,
) -> leanhttp::Result {
    let mut resp = match fwd {
        Forward::Leader {
            addr,
            req,
            stream: false,
        } => to_leader(client, &addr, &req).await,
        Forward::Leader {
            addr,
            req,
            stream: true,
        } => {
            let url = with_query(&format!("http://{addr}{}", req.path), &req.query);
            let headers = passed(&req);
            return relay(ex, client, &url, &headers, cors).await;
        }
        Forward::Agent {
            endpoint,
            path,
            auth,
            stream: false,
        } => to_agent(client, &endpoint, &path, auth.as_deref()).await,
        Forward::Agent {
            endpoint,
            path,
            auth,
            stream: true,
        } => {
            let url = format!("{}{path}", endpoint.trim_end_matches('/'));
            let mut headers: Vec<(&str, &str)> = Vec::new();
            if let Some(a) = &auth {
                headers.push((auth::AUTH_HEADER, a));
            }
            return relay(ex, client, &url, &headers, cors).await;
        }
        Forward::Tasks {
            agents,
            auth,
            scope,
        } => tasks(client, agents, auth.as_deref(), &scope).await,
    };
    if let Some(req) = cors {
        api::cors(req, &mut resp);
    }
    hop_http::write_reply(ex, &Reply::Plain(resp)).await
}

/// Het verzoek zonder body: genoeg voor de CORS-koppen van een doorgifte.
fn head_of(req: &Request) -> Request {
    Request {
        method: req.method,
        path: req.path.clone(),
        query: String::new(),
        headers: req.headers.clone(),
        body: Vec::new(),
    }
}

/// Bedient één verbinding zoals [`hop_http::serve`], met de doorgiftes erbij.
///
/// Een [`Routed::Reply`] gaat zoals altijd (een stroom van de eigenaar via
/// `streams`); een [`Routed::Forward`] voert deze taak zelf uit met `client`.
/// Na elke stroom, op elk pad, meldt `streams.done()` hem af.
pub async fn serve<X, H, S, C, R>(
    conn: X,
    mut handler: H,
    streams: &mut S,
    client: &mut Client<C, R>,
    port: Port,
) -> Result<(), leanhttp::Error>
where
    X: AsyncRead + AsyncWrite + Close,
    H: AsyncFnMut(Request) -> Routed,
    S: Streams,
    C: Connect,
    R: Resolve,
{
    let out = leanhttp::serve(conn, async |ex: &mut Exchange<'_, X>| {
        let req = hop_http::read_request(ex).await?;
        let cors = (port == Port::Agent).then(|| head_of(&req));
        match handler(req).await {
            Routed::Reply(reply) => {
                let Reply::Stream { ref ask, .. } = reply else {
                    return hop_http::write_reply(ex, &reply).await;
                };
                let r = match hop_http::write_reply(ex, &reply).await {
                    Ok(()) => hop_http::pump(ex, ask, streams).await,
                    Err(e) => Err(e),
                };
                streams.done();
                r
            }
            Routed::Forward(fwd) => {
                let stream = fwd.is_stream();
                let r = execute(ex, fwd, client, cors.as_ref()).await;
                if stream {
                    streams.done();
                }
                r
            }
        }
    })
    .await?;
    // Geen enkele route neemt de verbinding over; komt er toch een terug,
    // dan valt hij hier (Drop sluit hem).
    drop(out);
    Ok(())
}
