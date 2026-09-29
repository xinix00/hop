//! De leader-API (poort P + 1000): agents, jobs, status, meldingen.
//!
//! Bezit de routes en de JSON-vorm; de clusterstaat is van de leader, die
//! [`Cluster`] implementeert. De trait is het deel van de leader dat de API
//! nodig heeft, zodat de handlers zonder de hele leader te testen zijn.

use alloc::string::String;
use alloc::vec::Vec;
use core::fmt;

use types::json::{self, Value};
use types::{Agent as AgentRecord, Job, Nanos, Time};

use crate::{Method, Request, Response, check_auth, reply, s};

/// Een weigering van de leader bij een update.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClusterError {
    /// Er loopt al een update van deze job (409).
    Locked,
    /// Iets anders (500), met de reden.
    Failed(String),
}

impl fmt::Display for ClusterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClusterError::Locked => f.write_str("job is locked by another operation"),
            ClusterError::Failed(why) => f.write_str(why),
        }
    }
}

/// Wat de leader-API van de leader vraagt.
pub trait Cluster {
    /// De geregistreerde agents.
    fn agents(&self) -> Vec<AgentRecord>;
    /// Registreert een agent met zijn placed-tellers; `false` als het id al met een ander endpoint bestaat.
    fn register_agent(
        &mut self,
        now: Nanos,
        id: &str,
        endpoint: &str,
        version: &str,
        placed: &[(String, i64)],
    ) -> bool;
    /// Een levensteken; `false` als de agent onbekend is (hij herregistreert dan).
    fn heartbeat(&mut self, now: Nanos, id: &str, version: &str, temp_milli_c: i64) -> bool;
    /// Meldt een agent af.
    fn unregister_agent(&mut self, id: &str);
    /// Alle jobs.
    fn jobs(&self) -> Vec<Job>;
    /// Of de job bestaat.
    fn has_job(&self, name: &str) -> bool;
    /// Rolt een update van een bestaande job uit.
    fn update_job(&mut self, now: Nanos, job: Job) -> Result<(), ClusterError>;
    /// Plaatst een nieuwe job; een fout betekent "opgeslagen, nog niet geplaatst".
    fn dispatch_job(&mut self, now: Nanos, job: Job) -> Result<(), String>;
    /// De prioriteit voor een nieuwe job zonder eigen prioriteit (achteraan).
    fn next_priority(&self) -> i64;
    /// Zet de prioriteit van een job; `false` als hij er niet is.
    fn patch_priority(&mut self, now: Nanos, name: &str, priority: i64) -> bool;
    /// Verwijdert een job en ruimt zijn taken op.
    fn delete_job(&mut self, now: Nanos, name: &str);
    /// Geplaatste instanties per job, over alle agents.
    fn placed_counts(&self) -> Vec<(String, i64)>;
    /// Of de settle-periode voorbij is.
    fn is_settled(&self) -> bool;
    /// Wanneer de job-store het laatst veranderde.
    fn state_time(&self) -> Time;
    /// Een agent gaf een taak terug (onplaatsbaar): boek de plaatsing af.
    fn mark_unplaced(&mut self, agent: &str, job: &str);
    /// Een gebeurtenis voor de SSE-abonnees (`job:<naam>[:<event>]`, of leeg).
    fn notify(&mut self, topic: &str);
}

/// De leader-API.
#[derive(Clone, Debug, Default)]
pub struct LeaderApi {
    key: Vec<u8>,
    cluster_name: String,
}

impl LeaderApi {
    /// Een API met HMAC-sleutel `key` voor cluster `cluster_name`.
    pub fn new(key: &[u8], cluster_name: &str) -> Self {
        Self {
            key: key.to_vec(),
            cluster_name: String::from(cluster_name),
        }
    }

    /// Behandelt één verzoek.
    pub fn handle<C: Cluster>(&self, cluster: &mut C, now: Nanos, req: &Request) -> Response {
        let path = req.path.as_str();
        if path == "/health" {
            return reply(200, [("status", s("ok"))]);
        }
        if let Some(reject) = check_auth(&self.key, req) {
            return reject;
        }
        match (req.method, path) {
            (Method::Get, "/v1/agents") => agents(cluster),
            (Method::Post, "/v1/agents") => register(cluster, now, req),
            (Method::Post, "/v1/heartbeat") => heartbeat(cluster, now, req),
            (Method::Get, "/v1/jobs") => jobs(cluster),
            (Method::Post, "/v1/jobs") => apply(cluster, now, req),
            (Method::Get, "/v1/status") => self.status(cluster),
            (Method::Post, "/v1/notify") => notify(cluster, req),
            (Method::Delete, p) if p.starts_with("/v1/agents/") => {
                let id = p.trim_start_matches("/v1/agents/");
                if id.is_empty() {
                    return Response::error(400, "agent id required");
                }
                cluster.unregister_agent(id);
                Response::empty(204)
            }
            (Method::Delete, p) if p.starts_with("/v1/jobs/") => {
                let name = p.trim_start_matches("/v1/jobs/");
                if name.is_empty() {
                    return Response::error(400, "job name required");
                }
                cluster.delete_job(now, name);
                Response::empty(204)
            }
            (Method::Patch, p) if p.starts_with("/v1/jobs/") && p.ends_with("/priority") => {
                patch_priority(cluster, now, req, p)
            }
            _ => Response::error(404, "not found"),
        }
    }

    /// Het clusteroverzicht uit de placed-tellers, zonder één aanroep naar een agent.
    fn status<C: Cluster>(&self, cluster: &C) -> Response {
        let jobs = cluster.jobs();
        let placed = cluster.placed_counts();
        let total: i64 = placed.iter().map(|(_, n)| *n).sum();
        let mut pm = types::de::ObjectBuilder::new();
        for (name, n) in &placed {
            if pm.field(name, Value::int(*n)).is_err() {
                return Response::empty(500);
            }
        }
        // Jobs waarvan de laatste uitrol niet afliep: eerlijk "nog niet gezond".
        let deploying: Vec<Value> = jobs
            .iter()
            .filter(|j| j.deploying)
            .map(|j| s(&j.name))
            .collect();
        reply(
            200,
            [
                ("cluster_name", s(&self.cluster_name)),
                ("agents", uint(cluster.agents().len())),
                ("jobs", uint(jobs.len())),
                ("total_placed", Value::int(total)),
                ("settling", Value::Bool(!cluster.is_settled())),
                ("placed", pm.build()),
                ("deploying", Value::Array(deploying)),
            ],
        )
    }
}

fn uint(n: usize) -> Value {
    Value::uint(u64::try_from(n).unwrap_or(u64::MAX))
}

fn field<'a>(v: &'a Value, key: &str) -> Option<&'a Value> {
    v.as_object().and_then(|o| o.get(key))
}

fn str_field<'a>(v: &'a Value, key: &str) -> &'a str {
    field(v, key).and_then(Value::as_str).unwrap_or("")
}

fn agents<C: Cluster>(cluster: &C) -> Response {
    let mut list = Vec::new();
    for a in cluster.agents() {
        match a.to_value() {
            Ok(v) if list.try_reserve(1).is_ok() => list.push(v),
            _ => return Response::empty(500),
        }
    }
    Response::json(200, &Value::Array(list))
}

fn jobs_value<C: Cluster>(cluster: &C) -> Option<Value> {
    let mut list = Vec::new();
    for j in cluster.jobs() {
        let v = j.to_value().ok()?;
        list.try_reserve(1).ok()?;
        list.push(v);
    }
    Some(Value::Array(list))
}

fn jobs<C: Cluster>(cluster: &C) -> Response {
    match jobs_value(cluster) {
        Some(v) => Response::json(200, &v),
        None => Response::empty(500),
    }
}

fn register<C: Cluster>(cluster: &mut C, now: Nanos, req: &Request) -> Response {
    let Ok(v) = json::parse(&req.body) else {
        return Response::error(400, "invalid json");
    };
    let (id, endpoint) = (str_field(&v, "id"), str_field(&v, "endpoint"));
    if id.is_empty() || endpoint.is_empty() {
        return Response::error(400, "id and endpoint required");
    }
    let mut placed = Vec::new();
    if let Some(Value::Object(o)) = field(&v, "placed") {
        for (name, n) in o.iter() {
            if placed.try_reserve(1).is_err() {
                return Response::empty(500);
            }
            placed.push((String::from(name), n.as_i64().unwrap_or(0)));
        }
    }
    if !cluster.register_agent(now, id, endpoint, str_field(&v, "version"), &placed) {
        let msg = alloc::format!("agent {id} already registered with different endpoint");
        return Response::error(409, &msg);
    }
    let mut t = String::new();
    let _ = cluster.state_time().write_rfc3339(&mut t);
    let Some(jobs) = jobs_value(cluster) else {
        return Response::empty(500);
    };
    reply(
        200,
        [
            ("status", s("registered")),
            ("jobs", jobs),
            ("state_time", s(&t)),
        ],
    )
}

/// Puur levensteken: id/endpoint/version in, "ok" uit. De job-lijsten die hier
/// vroeger heen en weer reisden zijn weg (16-07): gewenste staat heeft één
/// auteur, de leader.
fn heartbeat<C: Cluster>(cluster: &mut C, now: Nanos, req: &Request) -> Response {
    let Ok(v) = json::parse(&req.body) else {
        return Response::error(400, "invalid json");
    };
    let (id, endpoint) = (str_field(&v, "id"), str_field(&v, "endpoint"));
    if id.is_empty() || endpoint.is_empty() {
        return Response::error(400, "id and endpoint required");
    }
    let temp = field(&v, "temp_milli_c")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    if !cluster.heartbeat(now, id, str_field(&v, "version"), temp) {
        return Response::error(404, "not registered");
    }
    reply(200, [("status", s("ok"))])
}

/// Maakt of werkt een job bij op naam (upsert).
fn apply<C: Cluster>(cluster: &mut C, now: Nanos, req: &Request) -> Response {
    let Ok(mut job) = Job::from_json(&req.body) else {
        return Response::error(400, "invalid json");
    };
    if job.name.is_empty() {
        return Response::error(400, "name required");
    }
    // Eén artifact zonder command en zonder image kan alleen de hop-driver zijn:
    // het artifact IS het programma. Zelfde afkorting als de init-jobs.
    if job.driver.is_none()
        && job.command.is_empty()
        && job.image.is_empty()
        && job.artifacts.len() == 1
    {
        job.driver = Some(types::Driver::Hop);
    }
    let hop_image = job.driver == Some(types::Driver::Hop) && !job.artifacts.is_empty();
    if job.command.is_empty() && job.image.is_empty() && !hop_image {
        return Response::error(
            400,
            "command or image required (or driver \"hop\" with at least one artifact)",
        );
    }
    let name = job.name.clone();
    if cluster.has_job(&name) {
        let policy = job.policy().as_str();
        return match cluster.update_job(now, job) {
            Ok(()) => reply(
                200,
                [
                    ("name", s(&name)),
                    ("status", s("updated")),
                    ("policy", s(policy)),
                ],
            ),
            Err(ClusterError::Locked) => Response::error(409, "job is locked by another operation"),
            Err(ClusterError::Failed(why)) => Response::error(500, &why),
        };
    }
    let explicit = job.priority;
    if job.priority.is_none() {
        job.priority = Some(cluster.next_priority());
    }
    if let Err(why) = cluster.dispatch_job(now, job) {
        return reply(
            201,
            [
                ("name", s(&name)),
                ("status", s("pending")),
                ("error", s(&why)),
            ],
        );
    }
    if let Some(p) = explicit {
        cluster.patch_priority(now, &name, p);
    }
    reply(201, [("name", s(&name)), ("status", s("dispatched"))])
}

/// Een melding van een agent; een hand-back boekt ook de plaatsing af.
///
/// Zonder die afboeking bleef placed op 1 staan en zag reconcile een gezonde
/// job waar niets draaide (gemeten 01-08: cloudflared voor eeuwig pending).
fn notify<C: Cluster>(cluster: &mut C, req: &Request) -> Response {
    let v = json::parse(&req.body).unwrap_or(Value::Null);
    let (job, event, agent) = (
        str_field(&v, "job"),
        str_field(&v, "event"),
        str_field(&v, "agent"),
    );
    if event == "unplaceable" && !job.is_empty() && !agent.is_empty() {
        cluster.mark_unplaced(agent, job);
    }
    if job.is_empty() {
        cluster.notify("");
    } else {
        let mut topic = alloc::format!("job:{job}");
        if !event.is_empty() {
            topic.push(':');
            topic.push_str(event);
        }
        cluster.notify(&topic);
    }
    Response::empty(204)
}

fn patch_priority<C: Cluster>(cluster: &mut C, now: Nanos, req: &Request, path: &str) -> Response {
    let name = path
        .trim_start_matches("/v1/jobs/")
        .trim_end_matches("/priority");
    if name.is_empty() || name.contains('/') {
        return Response::error(400, "job name required");
    }
    let Ok(v) = json::parse(&req.body) else {
        return Response::error(400, "invalid json");
    };
    let p = field(&v, "priority").and_then(Value::as_i64).unwrap_or(0);
    if !cluster.patch_priority(now, name, p) {
        let msg = alloc::format!("job {name} not found");
        return Response::error(404, &msg);
    }
    Response::empty(204)
}
