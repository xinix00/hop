//! De leader-API (poort P + 1000): agents, jobs, status, meldingen.
//!
//! Bezit de routes en de JSON-vorm; de clusterstaat is van de leader, die
//! [`Cluster`] implementeert. De trait is het deel van de leader dat de API
//! nodig heeft, zodat de handlers zonder de hele leader te testen zijn.
//!
//! Vier routes gaan verder dan de staat van de leader, en krijgen daarom
//! een [`LeaderEffect`] dat de adapter uitvoert (de handler heeft geen
//! sockets): `/v1/tasks` (de taken van elke agent, Go's `GetClusterStatus`),
//! `/v1/jobs/{naam}/status` (de taken van één job bij de agents waar hij
//! staat, Go's `GetJobStatus`: de takentabel van het dashboard),
//! `/v1/agents/{id}/logs/...` en `/v1/agents/{id}/capacity` (een doorgifte
//! naar die agent, zodat `hop logs`, `hop agents <id>` en het dashboard
//! alleen de leader hoeven te bereiken), en `/v1/events` (de SSE-stroom).

use alloc::string::String;
use alloc::vec::Vec;
use core::fmt;

use types::json::{self, Value};
use types::{Agent as AgentRecord, Job, Nanos, Telemetry, Time};

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
    fn heartbeat(&mut self, now: Nanos, id: &str, version: &str, telemetry: Telemetry) -> bool;
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
    /// De agents waar job `name` minstens één keer staat (Go's
    /// `GetJobStatus`: alleen die worden gevraagd).
    fn placed_agents(&self, name: &str) -> Vec<AgentRecord>;
    /// Of de settle-periode voorbij is.
    fn is_settled(&self) -> bool;
    /// Wanneer de job-store het laatst veranderde.
    fn state_time(&self) -> Time;
    /// Een agent gaf een taak terug (onplaatsbaar): boek de plaatsing af.
    fn mark_unplaced(&mut self, agent: &str, job: &str);
    /// Een gebeurtenis voor de SSE-abonnees (`job:<naam>[:<event>]`, of leeg).
    fn notify(&mut self, topic: &str);
}

/// Wat de adapter na een antwoord van de leader-API nog moet doen.
#[derive(Clone, Debug, PartialEq)]
pub enum LeaderEffect {
    /// Niets: het antwoord is compleet.
    None,
    /// Een rondgang: vraag elke agent in `agents` zijn taken (`GET /tasks`,
    /// ondertekend, met één totale termijn) en antwoord met
    /// [`TasksScope::reply`]. Een agent die niet antwoordt, ontbreekt,
    /// zoals in Go.
    Tasks {
        /// `(id, endpoint)` van elke agent om te vragen.
        agents: Vec<(String, String)>,
        /// Welke taken het antwoord draagt, en in welke vorm.
        scope: TasksScope,
    },
    /// Geef het verzoek door aan één agent: `GET {endpoint}{path}`,
    /// ondertekend met de clustersleutel, en zijn antwoord terug.
    Agent {
        /// Het endpoint van de agent (`http://ip:poort`).
        endpoint: String,
        /// Het pad op de agent, met de query van de aanroeper.
        path: String,
        /// Of het antwoord een stroom is (per brok doorspoelen).
        stream: bool,
    },
    /// `GET /v1/events`: open een SSE-stroom met [`crate::PING`] en daarna
    /// de meldingen uit de [`crate::EventLog`] van de node.
    Events,
}

/// Welke taken een rondgang ([`LeaderEffect::Tasks`]) terugmeldt, en in welke vorm.
#[derive(Clone, Debug, PartialEq)]
pub enum TasksScope {
    /// `GET /v1/tasks`: alle taken van elke agent, plus de systeemtaken uit
    /// zijn heartbeat ([`tasks_reply`]).
    All {
        /// De agents zoals de leader ze kent (hun laatste heartbeat).
        agents: Vec<AgentRecord>,
    },
    /// `GET /v1/jobs/{naam}/status`: alleen de taken van deze job, van de
    /// agents waar hij staat ([`job_status_reply`]).
    Job {
        /// De job.
        name: String,
        /// De agents waar hij staat, zoals Go ze in `agents` teruggeeft.
        agents: Vec<AgentRecord>,
    },
}

impl TasksScope {
    /// Het antwoord uit de taken per agent (`None`: die agent antwoordde niet).
    pub fn reply(&self, results: &[(String, Option<Vec<types::Task>>)]) -> Response {
        match self {
            Self::All { agents } => tasks_reply(agents, results),
            Self::Job { name, agents } => job_status_reply(name, agents, results),
        }
    }
}

/// Het antwoord op `/v1/jobs/{naam}/status`, zoals Go's `handleJobStatus`:
/// `{"agents": [agent, ...], "tasks_by_agent": {id: [taak, ...]}}`, met
/// alleen de taken van job `name`.
///
/// Een agent die niet antwoordde, of die geen taak van deze job meer heeft,
/// ontbreekt in `tasks_by_agent` (Go stuurde er dan `nil` in en liet hem
/// weg); in `agents` staat hij wel, want daar staat waar de job geplaatst is.
pub fn job_status_reply(
    name: &str,
    agents: &[AgentRecord],
    results: &[(String, Option<Vec<types::Task>>)],
) -> Response {
    let mut list = Vec::new();
    for a in agents {
        match a.to_value() {
            Ok(v) if list.try_reserve(1).is_ok() => list.push(v),
            _ => return Response::empty(500),
        }
    }
    let mut by_agent = types::de::ObjectBuilder::new();
    for (id, tasks) in results {
        let mut mine = Vec::new();
        for t in tasks.iter().flatten().filter(|t| t.job_name == name) {
            match t.to_value() {
                Ok(v) if mine.try_reserve(1).is_ok() => mine.push(v),
                _ => return Response::empty(500),
            }
        }
        if mine.is_empty() {
            continue;
        }
        if by_agent.field(id, Value::Array(mine)).is_err() {
            return Response::empty(500);
        }
    }
    reply(
        200,
        [
            ("agents", Value::Array(list)),
            ("tasks_by_agent", by_agent.build()),
        ],
    )
}

/// Het antwoord op `/v1/tasks` uit de taken per agent: `{"tasks_by_agent":
/// {id: [taak, ...]}}`, en de agents die niet antwoordden onder
/// `"unreachable"` (een uitbreiding op Go, die ze stil wegliet: zo kan de
/// CLI zeggen wélke agent ontbreekt).
///
/// Achter de taken van een agent die antwoordde staan zijn systeemtaken
/// (`kern` en `hop`, [`AgentRecord::system_tasks`]) uit zijn laatste
/// heartbeat in `agents`. Alleen hier: de takenlijst van de agent zelf
/// (`GET /tasks`) en de boeken van de leader kennen ze niet.
pub fn tasks_reply(
    agents: &[AgentRecord],
    results: &[(String, Option<Vec<types::Task>>)],
) -> Response {
    let mut by_agent = types::de::ObjectBuilder::new();
    let mut unreachable = Vec::new();
    for (id, tasks) in results {
        let Some(tasks) = tasks else {
            if unreachable.try_reserve(1).is_err() {
                return Response::empty(500);
            }
            unreachable.push(s(id));
            continue;
        };
        let system = match agents.iter().find(|a| a.id == *id) {
            Some(a) => match a.system_tasks() {
                Ok(s) => s,
                Err(_) => return Response::empty(500),
            },
            None => Vec::new(),
        };
        let mut list = Vec::new();
        for t in tasks.iter().chain(&system) {
            match t.to_value() {
                Ok(v) if list.try_reserve(1).is_ok() => list.push(v),
                _ => return Response::empty(500),
            }
        }
        if by_agent.field(id, Value::Array(list)).is_err() {
            return Response::empty(500);
        }
    }
    reply(
        200,
        [
            ("tasks_by_agent", by_agent.build()),
            ("unreachable", Value::Array(unreachable)),
        ],
    )
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

    /// Behandelt één verzoek; het [`LeaderEffect`] zegt wat de adapter daarna nog doet.
    pub fn handle<C: Cluster>(
        &self,
        cluster: &mut C,
        now: Nanos,
        req: &Request,
    ) -> (Response, LeaderEffect) {
        let path = req.path.as_str();
        if path == "/health" {
            return (reply(200, [("status", s("ok"))]), LeaderEffect::None);
        }
        if let Some(reject) = check_auth(&self.key, req) {
            return (reject, LeaderEffect::None);
        }
        match (req.method, path) {
            (Method::Get, "/v1/tasks") => return tasks(cluster),
            (Method::Get, p) if p.starts_with("/v1/jobs/") && p.ends_with("/status") => {
                return job_status(cluster, p);
            }
            (Method::Get, "/v1/events") => {
                let mut r = Response::empty(200);
                r.set_header("Content-Type", "text/event-stream");
                return (r, LeaderEffect::Events);
            }
            (Method::Get, p) if p.starts_with("/v1/agents/") => {
                if let Some(out) = agent_route(cluster, req, p) {
                    return out;
                }
            }
            _ => {}
        }
        (self.state(cluster, now, req), LeaderEffect::None)
    }

    /// De routes over de staat van de leader zelf.
    fn state<C: Cluster>(&self, cluster: &mut C, now: Nanos, req: &Request) -> Response {
        let path = req.path.as_str();
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

/// `GET /v1/tasks`: de agents om te vragen.
fn tasks<C: Cluster>(cluster: &C) -> (Response, LeaderEffect) {
    let records = cluster.agents();
    let mut agents = Vec::new();
    if agents.try_reserve_exact(records.len()).is_err() {
        return (Response::empty(500), LeaderEffect::None);
    }
    for a in &records {
        agents.push((a.id.clone(), a.endpoint.clone()));
    }
    (
        Response::empty(200),
        LeaderEffect::Tasks {
            agents,
            scope: TasksScope::All { agents: records },
        },
    )
}

/// `GET /v1/jobs/{naam}/status`: de taken van één job, van de agents waar
/// hij staat (Go's `GetJobStatus`). Het dashboard leest er zijn
/// takentabel uit (`tasks_by_agent`).
///
/// Zonder de job, of zonder plaatsing, is er niets te vragen en antwoordt
/// de handler zelf, met Go's vorm op de byte: een onbekende job is
/// `{"agents":null,"tasks_by_agent":null}`, een job die nergens staat
/// `{"agents":null,"tasks_by_agent":{}}`. Beide 200, zoals in Go.
fn job_status<C: Cluster>(cluster: &C, path: &str) -> (Response, LeaderEffect) {
    let name = path
        .strip_prefix("/v1/jobs/")
        .and_then(|p| p.strip_suffix("/status"))
        .unwrap_or("");
    if name.is_empty() || name.contains('/') {
        return (
            Response::error(400, "job name required"),
            LeaderEffect::None,
        );
    }
    if !cluster.has_job(name) {
        let r = reply(
            200,
            [("agents", Value::Null), ("tasks_by_agent", Value::Null)],
        );
        return (r, LeaderEffect::None);
    }
    let placed = cluster.placed_agents(name);
    if placed.is_empty() {
        let empty = types::de::ObjectBuilder::new().build();
        let r = reply(200, [("agents", Value::Null), ("tasks_by_agent", empty)]);
        return (r, LeaderEffect::None);
    }
    let mut agents = Vec::new();
    if agents.try_reserve_exact(placed.len()).is_err() {
        return (Response::empty(500), LeaderEffect::None);
    }
    for a in &placed {
        agents.push((a.id.clone(), a.endpoint.clone()));
    }
    let scope = TasksScope::Job {
        name: String::from(name),
        agents: placed,
    };
    (Response::empty(200), LeaderEffect::Tasks { agents, scope })
}

/// `GET /v1/agents/{id}/capacity` en `GET /v1/agents/{id}/logs/{taak}/{stroom}`:
/// een doorgifte naar die agent. `None` voor elk ander pad onder `/v1/agents/`.
fn agent_route<C: Cluster>(
    cluster: &C,
    req: &Request,
    path: &str,
) -> Option<(Response, LeaderEffect)> {
    let rest = path.strip_prefix("/v1/agents/")?;
    let (id, sub) = rest.split_once('/')?;
    let (target, stream) = if sub == "capacity" {
        (String::from("/capacity"), false)
    } else {
        let logs = sub.strip_prefix("logs/")?;
        let mut parts = logs.split('/');
        let (Some(task), Some(which), None) = (parts.next(), parts.next(), parts.next()) else {
            return Some(bad_request());
        };
        if id.is_empty() || task.is_empty() || !matches!(which, "stdout" | "stderr") {
            return Some(bad_request());
        }
        let mut t = alloc::format!("/logs/{task}/{which}");
        if !req.query.is_empty() {
            t.push('?');
            t.push_str(&req.query);
        }
        // Go's contract: deze route IS de levende tail (het dashboard en de
        // Go-CLI vragen hem zonder query en verwachten "Live output"). De
        // agent-route `/logs/...` geeft zonder `follow` een momentopname;
        // hier volgt de doorgifte dus tenzij de aanroeper `follow=0` zegt
        // (`hop logs` zonder `--follow`).
        if req.query_param("follow").is_none() {
            t.push(if req.query.is_empty() { '?' } else { '&' });
            t.push_str("follow=1");
        }
        (t, true)
    };
    let Some(agent) = cluster.agents().into_iter().find(|a| a.id == id) else {
        return Some((Response::error(404, "agent not found"), LeaderEffect::None));
    };
    Some((
        Response::empty(200),
        LeaderEffect::Agent {
            endpoint: agent.endpoint,
            path: target,
            stream,
        },
    ))
}

/// Go's antwoord op een kapot log-pad onder `/v1/agents/`.
fn bad_request() -> (Response, LeaderEffect) {
    (
        Response::error(400, "invalid request parameters"),
        LeaderEffect::None,
    )
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
    // Telemetrie die niet te lezen is, kost de heartbeat niet: dan niets gemeten.
    let telemetry = Telemetry::from_value(&v).unwrap_or_default();
    if !cluster.heartbeat(now, id, str_field(&v, "version"), telemetry) {
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
