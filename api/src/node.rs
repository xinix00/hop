//! De agent-API van een node (poort P): taken aannemen, stoppen, tonen.
//!
//! Bezit de routes en hun antwoorden over een geleende [`agent::Agent`]; de
//! staat blijft van de agent. De clusterroutes (`/v1/...`) gaan als
//! [`Effect::Proxy`] naar de leader die de lus bevestigde.

use alloc::string::String;
use alloc::vec::Vec;

use agent::{Agent, Error as AgentError};
use types::json::{self, Value};
use types::{Job, Nanos};

use crate::{Method, Request, Response, check_auth, reply, s};

/// Hoe groot een body is die de proxy naar de leader doorgeeft.
///
/// De proxy moet de body lezen voordat hij hem doorgeeft, en een onbegrensde
/// lees op een route die nog niet geauthenticeerd is, is een geheugen-DoS
/// vóór de toets. Zelfde maat als in Go (`proxyMaxBody`, 8 MiB).
pub const PROXY_MAX_BODY: usize = 8 << 20;

/// Welke logstroom.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogStream {
    /// Standaard uitvoer.
    Stdout,
    /// Standaard fout.
    Stderr,
}

/// Wat de adapter na het antwoord nog moet doen.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Effect {
    /// Niets: het antwoord is compleet.
    None,
    /// Geef het verzoek door aan de leader en stuur zijn antwoord terug.
    ///
    /// Met `stream` per brok doorspoelen (SSE, log-tails); anders gebufferd
    /// met een timeout. De `X-Hop-Auth` van de aanroeper gaat mee: de proxy
    /// geeft dezelfde methode, pad en body door, dus de handtekening blijft
    /// geldig (het hele cluster deelt één sleutel).
    Proxy {
        /// Het leader-adres (`ip:poort`).
        leader: String,
        /// Of het antwoord een stroom is.
        stream: bool,
    },
    /// Stroom de logs van een taak als SSE (de adapter vraagt de runner; 404 als die hem niet kent).
    Logs {
        /// Het taak-id.
        task_id: String,
        /// De stroom.
        stream: LogStream,
    },
    /// Vervang de kern van de node (HopOS-flip): haal, toets, spring.
    Flip {
        /// De absolute URL van de bundel.
        url: String,
        /// De sha256 in hex.
        sha256: String,
        /// De KOUDE flip (`"cold": true`): de taken op deze node stoppen,
        /// de kern springt zonder ze over te dragen en start Hop koud. De
        /// weg voor een kern met een andere switch-code, die de warme flip
        /// weigert (HopOS `docs/flip.md`).
        cold: bool,
    },
}

/// De agent-API.
#[derive(Clone, Debug, Default)]
pub struct NodeApi {
    key: Vec<u8>,
    flip_enabled: bool,
}

impl NodeApi {
    /// Een API met HMAC-sleutel `key` (leeg = geen authenticatie).
    pub fn new(key: &[u8], flip_enabled: bool) -> Self {
        Self {
            key: key.to_vec(),
            flip_enabled,
        }
    }

    /// Behandelt één verzoek.
    ///
    /// `pool_largest` is de grootste partitie die de HopOS-runner nog kan
    /// plaatsen (voor de toelating van `/run`).
    pub fn handle(
        &self,
        agent: &mut Agent,
        now: Nanos,
        pool_largest: Option<u64>,
        req: &Request,
    ) -> (Response, Effect) {
        let (mut resp, effect) = self.route(agent, now, pool_largest, req);
        cors(req, &mut resp);
        (resp, effect)
    }

    fn route(
        &self,
        agent: &mut Agent,
        now: Nanos,
        pool_largest: Option<u64>,
        req: &Request,
    ) -> (Response, Effect) {
        let none = |r| (r, Effect::None);
        if req.method == Method::Options {
            return none(Response::empty(200));
        }
        let path = req.path.as_str();
        match path {
            "/health" => return none(reply(200, [("status", s("ok"))])),
            "/leader" => return none(leader(agent)),
            _ => {}
        }
        if let Some(reject) = check_auth(&self.key, req) {
            return none(reject);
        }
        if is_proxy_route(path) {
            return proxy(agent, req);
        }
        if let Some(rest) = path.strip_prefix("/logs/") {
            return logs(rest);
        }
        let r = match path {
            "/capacity" => capacity(agent),
            "/tasks" => tasks(agent),
            "/run" => run(agent, now, pool_largest, req),
            "/flip" => return self.flip(req),
            _ => {
                if let Some(name) = path.strip_prefix("/delete/") {
                    delete(agent, now, req, name)
                } else if let Some(name) = path.strip_prefix("/stop/") {
                    stop(agent, req, name)
                } else if let Some(id) = path.strip_prefix("/stop-task/") {
                    stop_task(agent, req, id)
                } else {
                    Response::error(404, "not found")
                }
            }
        };
        none(r)
    }

    /// `/flip`: de node vervangt zijn eigen kern terwijl de taken doordraaien.
    ///
    /// Achter dezelfde HMAC als dispatch, want wie een kern mag aanleveren mag
    /// alles. Het antwoord is een 202 en niet de uitkomst: een geslaagde flip
    /// keert per definitie nooit terug.
    ///
    /// Met `"cold": true` draaien de taken NIET door: ze stoppen, de kern
    /// springt koud en Hop start opnieuw, en de jobs komen daarna terug
    /// (29-09). Dezelfde ene route, een vlag in dezelfde body.
    fn flip(&self, req: &Request) -> (Response, Effect) {
        let none = |r| (r, Effect::None);
        if req.method != Method::Post {
            return none(Response::error(405, "method not allowed"));
        }
        if !self.flip_enabled {
            return none(Response::error(
                501,
                "this node cannot flip its kernel (not a HopOS node, or flipping is disabled)",
            ));
        }
        let Ok(v) = json::parse(&req.body) else {
            return none(Response::error(400, "invalid json"));
        };
        let url = v
            .as_object()
            .and_then(|o| o.get("url"))
            .and_then(Value::as_str)
            .unwrap_or("");
        let sum = v
            .as_object()
            .and_then(|o| o.get("sha256"))
            .and_then(Value::as_str)
            .unwrap_or("");
        if !url.contains("://") {
            return none(Response::error(400, "url must be absolute"));
        }
        if sum.len() != 64 || !sum.bytes().all(|c| c.is_ascii_hexdigit()) {
            return none(Response::error(400, "sha256 must be 64 hex characters"));
        }
        // Afwezig is warm; iets anders dan een bool is een typefout, geen
        // stille warme flip.
        let cold = match v.as_object().and_then(|o| o.get("cold")) {
            None => false,
            Some(c) => match c.as_bool() {
                Some(b) => b,
                None => return none(Response::error(400, "cold must be true or false")),
            },
        };
        let effect = Effect::Flip {
            url: String::from(url),
            sha256: String::from(sum),
            cold,
        };
        let msg = if cold {
            "cold flip requested: the tasks on this node stop, the node fetches, verifies and replaces its kernel, and Hop starts again; watch its console and its re-registration"
        } else {
            "flip requested: the node fetches, verifies and replaces its kernel; watch its console and its re-registration"
        };
        (reply(202, [("status", s(msg))]), effect)
    }
}

/// De CORS-koppen voor browsertoegang (de gehoste GUI praat direct met de agent).
fn cors(req: &Request, resp: &mut Response) {
    resp.set_header("Access-Control-Allow-Origin", "*");
    resp.set_header(
        "Access-Control-Allow-Methods",
        "GET, POST, DELETE, PATCH, OPTIONS",
    );
    resp.set_header("Access-Control-Allow-Headers", "Content-Type, X-Hop-Auth");
    // Chrome Private Network Access: een publieke origin die een LAN-adres
    // haalt, krijgt de preflight alleen door als de server het toestaat.
    if req.header("Access-Control-Request-Private-Network") == Some("true") {
        resp.set_header("Access-Control-Allow-Private-Network", "true");
    }
}

fn is_proxy_route(path: &str) -> bool {
    matches!(
        path,
        "/v1/agents" | "/v1/jobs" | "/v1/status" | "/v1/tasks" | "/v1/events"
    ) || path.starts_with("/v1/agents/")
        || path.starts_with("/v1/jobs/")
}

fn proxy(agent: &Agent, req: &Request) -> (Response, Effect) {
    let leader = agent.leader_addr();
    if leader.is_empty() {
        return (Response::error(503, "no leader available"), Effect::None);
    }
    if req.body.len() > PROXY_MAX_BODY {
        return (Response::error(413, "request body too large"), Effect::None);
    }
    let stream = req.path == "/v1/events"
        || (req.path.starts_with("/v1/agents/") && req.path.contains("/logs/"));
    (
        Response::empty(200),
        Effect::Proxy {
            leader: String::from(leader),
            stream,
        },
    )
}

fn leader(agent: &Agent) -> Response {
    let mut pairs = alloc::vec![("leader", s(agent.leader_addr()))];
    let exp = agent.lease_expires_at();
    if !exp.is_zero() {
        // Alleen de leader zelf kent zijn lease; een volger meldt alleen het adres.
        let mut t = String::new();
        if exp.write_rfc3339(&mut t).is_ok() {
            pairs.push(("lease_expires_at", s(&t)));
        }
    }
    reply(200, pairs)
}

fn capacity(agent: &Agent) -> Response {
    let c = agent.capacity();
    let mut attrs = types::de::ObjectBuilder::new();
    for (k, v) in agent.attributes() {
        if attrs.str(k, v).is_err() {
            return Response::empty(500);
        }
    }
    let mut pairs = alloc::vec![
        ("cpu_cores", Value::uint(u64::from(c.cpu_cores))),
        ("memory_bytes", Value::uint(c.memory_bytes)),
        ("cpu_used_shares", Value::int(c.cpu_used_shares)),
        ("memory_used_bytes", Value::uint(c.memory_used_bytes)),
        (
            "tasks_running",
            Value::uint(u64::try_from(c.tasks_running).unwrap_or(u64::MAX))
        ),
    ];
    if !agent.attributes().is_empty() {
        pairs.push(("attributes", attrs.build()));
    }
    reply(200, pairs)
}

fn tasks(agent: &Agent) -> Response {
    let mut list = Vec::new();
    for t in agent.tasks() {
        match t.to_value() {
            Ok(v) if list.try_reserve(1).is_ok() => list.push(v),
            _ => return Response::empty(500),
        }
    }
    Response::json(200, &Value::Array(list))
}

fn run(agent: &mut Agent, now: Nanos, pool_largest: Option<u64>, req: &Request) -> Response {
    if req.method != Method::Post {
        return Response::error(405, "method not allowed");
    }
    let replace = req.query_param("replace") == Some("1");
    let Ok(job) = Job::from_json(&req.body) else {
        return Response::error(400, "invalid json");
    };
    let name = job.name.clone();
    match agent.run(now, job, replace, pool_largest) {
        Ok(_) => reply(
            202,
            [
                ("status", s("accepted")),
                ("job", s(&name)),
                ("message", s("job accepted, starting in background")),
            ],
        ),
        Err(AgentError::AffinityMismatch) => Response::error(406, "affinity mismatch"),
        Err(AgentError::NoCapacity | AgentError::TooManyTasks | AgentError::TooManyJobs) => {
            Response::error(503, "insufficient capacity")
        }
        Err(_) => Response::error(500, "internal error"),
    }
}

fn count(key: &'static str, n: usize) -> Response {
    reply(
        200,
        [(key, Value::uint(u64::try_from(n).unwrap_or(u64::MAX)))],
    )
}

fn delete(agent: &mut Agent, now: Nanos, req: &Request, name: &str) -> Response {
    if req.method != Method::Delete {
        return Response::error(405, "method not allowed");
    }
    if name.is_empty() {
        return Response::error(400, "job name required");
    }
    count("deleted", agent.delete_job(now, name))
}

/// `/stop/{job}`: stopt de taken maar houdt de job (preemptie).
fn stop(agent: &mut Agent, req: &Request, name: &str) -> Response {
    if req.method != Method::Post {
        return Response::error(405, "method not allowed");
    }
    if name.is_empty() {
        return Response::error(400, "job name required");
    }
    count("stopped", agent.stop_job_tasks(name))
}

fn stop_task(agent: &mut Agent, req: &Request, id: &str) -> Response {
    if req.method != Method::Post {
        return Response::error(405, "method not allowed");
    }
    if id.is_empty() {
        return Response::error(400, "task id required");
    }
    if !agent.stop_task(id) {
        return Response::error(404, "task not found");
    }
    reply(200, [("stopped", s(id))])
}

fn logs(rest: &str) -> (Response, Effect) {
    let mut parts = rest.split('/');
    let (Some(id), Some(stream), None) = (parts.next(), parts.next(), parts.next()) else {
        return (
            Response::error(400, "usage: /logs/{taskID}/stdout or /logs/{taskID}/stderr"),
            Effect::None,
        );
    };
    let stream = match stream {
        "stdout" => LogStream::Stdout,
        "stderr" => LogStream::Stderr,
        _ => {
            return (
                Response::error(400, "stream must be stdout or stderr"),
                Effect::None,
            );
        }
    };
    let mut r = Response::empty(200);
    r.set_header("Content-Type", "text/event-stream");
    (
        r,
        Effect::Logs {
            task_id: String::from(id),
            stream,
        },
    )
}

#[cfg(test)]
mod flip_tests {
    //! De vlag van de koude flip op `POST /flip`.
    use super::*;
    use crate::Method;

    fn api_agent() -> Agent {
        Agent::new(agent::Settings {
            id: "flip-node".into(),
            ..agent::Settings::default()
        })
    }

    #[test]
    fn the_cold_flag_rides_on_the_same_route() {
        let mut a = api_agent();
        let on = NodeApi::new(b"", true);
        let sum = "b".repeat(64);
        let body = |extra: &str| {
            alloc::format!(r#"{{"url":"http://10.0.2.2/k.flip","sha256":"{sum}"{extra}}}"#)
        };
        let ask = |a: &mut Agent, b: String| {
            on.handle(
                a,
                0,
                None,
                &Request::new(Method::Post, "/flip", b.as_bytes()),
            )
        };
        let (r, e) = ask(&mut a, body(""));
        assert_eq!(r.status, 202);
        assert!(
            matches!(e, Effect::Flip { cold: false, .. }),
            "absent is warm"
        );
        let (r, e) = ask(&mut a, body(r#","cold":true"#));
        assert_eq!(r.status, 202);
        assert!(matches!(e, Effect::Flip { cold: true, .. }));
        let (r, e) = ask(&mut a, body(r#","cold":"yes""#));
        assert_eq!(r.status, 400, "a string is not a flag");
        assert_eq!(e, Effect::None);
    }
}
