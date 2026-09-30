//! De antwoorden van de agent-API als typen, en hun parsers.
//!
//! Bezit de vorm van de antwoorden die niet in hop's `types` staan
//! (`/v1/status`, `/capacity`, `/leader`, het antwoord op een apply, de
//! taken per agent). De JSON-vorm is die van hop's `api`-crate, veld voor
//! veld; onbekende velden tellen niet, zodat een nieuwere agent een oudere
//! plugin niet breekt.

use alloc::string::String;
use alloc::vec::Vec;

use types::json::{self, Object, Value};
use types::{Job, Map, Task, Time, de, try_push, try_string};

use crate::{AgentInfo, Error, Reason, Response, Result};

/// Het clusteroverzicht van `GET /v1/status`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Status {
    /// De naam van het cluster (hopdns bouwt er zijn zone mee).
    pub cluster_name: String,
    /// Hoeveel agents er geregistreerd zijn.
    pub agents: u64,
    /// Hoeveel jobs er zijn.
    pub jobs: u64,
    /// Alle geplaatste instanties samen.
    pub total_placed: i64,
    /// Of de leader nog aan het bijtrekken is (net gekozen, agents melden zich).
    pub settling: bool,
    /// Geplaatste instanties per job.
    pub placed: Map<i64>,
    /// Jobs waarvan de laatste uitrol niet afliep.
    pub deploying: Vec<String>,
}

/// De capaciteit van een agent (`GET /capacity`).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Capacity {
    /// CPU-cores van de node.
    pub cpu_cores: u64,
    /// Geheugen van de node in bytes.
    pub memory_bytes: u64,
    /// De cpu-shares die de taken samen vragen.
    pub cpu_used_shares: i64,
    /// Het geheugen dat de taken samen vragen.
    pub memory_used_bytes: u64,
    /// Hoeveel taken er draaien.
    pub tasks_running: u64,
    /// De attributen van de node (`node.arch`, ...), als die er zijn.
    pub attributes: Map<String>,
}

/// Wie er leidt, volgens een agent (`GET /leader`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Leader {
    /// Het adres van de leader (`ip:poort`), of leeg als er geen is.
    pub leader: String,
    /// Wanneer de lease verloopt; alleen de leader zelf kent die, anders nul.
    pub lease_expires_at: Time,
}

/// Het antwoord op `POST /v1/jobs`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Applied {
    /// De naam van de job.
    pub name: String,
    /// `dispatched`, `pending` (opgeslagen, nog niet geplaatst) of `updated`.
    pub status: String,
    /// Bij `updated`: de uitrol (`rolling`, ...).
    pub policy: String,
    /// Bij `pending`: waarom de plaatsing nog niet lukte.
    pub error: String,
}

/// De taken van één agent uit `GET /v1/tasks`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct AgentTasks {
    /// Het id van de agent.
    pub agent: String,
    /// Zijn taken; `None` als hij de leader niet op tijd antwoordde.
    pub tasks: Option<Vec<Task>>,
}

/// De taken van één job (`GET /v1/jobs/{naam}/status`, Go's `GetJobStatus`).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct JobStatus {
    /// De agents waar de job staat; leeg voor een onbekende of ongeplaatste job.
    pub agents: Vec<AgentInfo>,
    /// De taken van deze job per agent; een agent zonder taak ervan ontbreekt.
    pub tasks: Vec<AgentTasks>,
}

/// De body van een geslaagd antwoord, of de fout met de melding van de server.
pub(crate) fn check(r: &Response) -> Result<&[u8]> {
    if r.status < 400 {
        return Ok(&r.body);
    }
    Err(status_error(r.status, &r.body))
}

/// De fout bij status `code`, met de melding uit een body `{"error": ...}`.
pub(crate) fn status_error(code: u16, body: &[u8]) -> Error {
    let reason = json::parse(body)
        .ok()
        .and_then(|v| {
            v.as_object()
                .and_then(|o| o.get("error"))
                .and_then(Value::as_str)
                .map(Reason::new)
        })
        .unwrap_or_else(|| Reason::new(""));
    Error::Status { code, reason }
}

/// De body van een geslaagd antwoord als JSON-object.
fn object(r: &Response) -> Result<Value> {
    let v = json::parse(check(r)?)?;
    de::object(&v, "reply")?;
    Ok(v)
}

/// De body van een geslaagd antwoord als JSON-lijst (`null` is leeg, zoals Go).
fn array(r: &Response) -> Result<Value> {
    let v = json::parse(check(r)?)?;
    if !v.is_null() {
        de::array(&v, "reply")?;
    }
    Ok(v)
}

/// De items van een lijst, elk door `one`.
fn items<T>(v: &Value, one: impl Fn(&Value) -> types::Result<T>) -> Result<Vec<T>> {
    let mut out = Vec::new();
    let Some(list) = v.as_array() else {
        return Ok(out);
    };
    out.try_reserve_exact(list.len())
        .map_err(|_| Error::OutOfMemory)?;
    for i in list {
        out.push(one(i)?);
    }
    Ok(out)
}

/// Het veld `k` van een object als string; leeg als het ontbreekt of `null` is.
fn string(o: &Object, k: &str) -> Result<String> {
    match o.get(k) {
        None | Some(Value::Null) => Ok(String::new()),
        Some(v) => Ok(de::string(v, k)?),
    }
}

fn int(o: &Object, k: &str) -> Result<i64> {
    match o.get(k) {
        None | Some(Value::Null) => Ok(0),
        Some(v) => Ok(de::int(v, k)?),
    }
}

fn uint(o: &Object, k: &str) -> Result<u64> {
    match o.get(k) {
        None | Some(Value::Null) => Ok(0),
        Some(v) => Ok(de::uint(v, k)?),
    }
}

/// Het object in `v` (al getoetst door [`object`]).
fn obj(v: &Value) -> Result<&Object> {
    Ok(de::object(v, "reply")?)
}

pub(crate) fn parse_status(r: &Response) -> Result<Status> {
    let v = object(r)?;
    let o = obj(&v)?;
    let mut s = Status {
        cluster_name: string(o, "cluster_name")?,
        agents: uint(o, "agents")?,
        jobs: uint(o, "jobs")?,
        total_placed: int(o, "total_placed")?,
        settling: match o.get("settling") {
            None | Some(Value::Null) => false,
            Some(b) => de::boolean(b, "settling")?,
        },
        ..Status::default()
    };
    if let Some(Value::Object(p)) = o.get("placed") {
        for (name, n) in p.iter() {
            s.placed.insert(try_string(name)?, de::int(n, "placed")?)?;
        }
    }
    if let Some(list) = o.get("deploying").and_then(Value::as_array) {
        for name in list {
            try_push(&mut s.deploying, de::string(name, "deploying")?)?;
        }
    }
    Ok(s)
}

pub(crate) fn parse_agents(r: &Response) -> Result<Vec<AgentInfo>> {
    items(&array(r)?, AgentInfo::from_value)
}

pub(crate) fn parse_jobs(r: &Response) -> Result<Vec<Job>> {
    items(&array(r)?, |v| Job::from_value(v, false))
}

pub(crate) fn parse_job(r: &Response, name: &str) -> Result<Option<Job>> {
    Ok(parse_jobs(r)?.into_iter().find(|j| j.name == name))
}

pub(crate) fn parse_task_list(r: &Response) -> Result<Vec<Task>> {
    items(&array(r)?, Task::from_value)
}

/// `{"tasks_by_agent": {id: [taak]}, "unreachable": [id]}`; `None` bij een 404.
pub(crate) fn parse_tasks_by_agent(r: &Response) -> Result<Option<Vec<AgentTasks>>> {
    if r.status == 404 {
        return Ok(None);
    }
    let v = object(r)?;
    let o = obj(&v)?;
    let mut out = Vec::new();
    if let Some(Value::Object(by)) = o.get("tasks_by_agent") {
        for (id, ts) in by.iter() {
            let tasks = items(ts, Task::from_value)?;
            let agent = try_string(id)?;
            try_push(
                &mut out,
                AgentTasks {
                    agent,
                    tasks: Some(tasks),
                },
            )?;
        }
    }
    if let Some(silent) = o.get("unreachable").and_then(Value::as_array) {
        for id in silent {
            let agent = de::string(id, "unreachable")?;
            try_push(&mut out, AgentTasks { agent, tasks: None })?;
        }
    }
    Ok(Some(out))
}

/// `{"agents": [...], "tasks_by_agent": {...}}` (beide mogen `null`); `None` bij een 404.
pub(crate) fn parse_job_status(r: &Response) -> Result<Option<JobStatus>> {
    if r.status == 404 {
        return Ok(None);
    }
    let v = object(r)?;
    let o = obj(&v)?;
    let mut st = JobStatus::default();
    if let Some(list) = o.get("agents").filter(|v| !v.is_null()) {
        st.agents = items(list, AgentInfo::from_value)?;
    }
    if let Some(Value::Object(by)) = o.get("tasks_by_agent") {
        for (id, ts) in by.iter() {
            let tasks = items(ts, Task::from_value)?;
            let agent = try_string(id)?;
            try_push(
                &mut st.tasks,
                AgentTasks {
                    agent,
                    tasks: Some(tasks),
                },
            )?;
        }
    }
    Ok(Some(st))
}

impl JobStatus {
    /// De terugval uit alle taken ([`crate::Client::tasks`]) en agents: alleen
    /// de taken van job `name`, en alleen de agents waar hij staat (een agent
    /// zonder antwoord valt weg, zoals bij de leader).
    pub fn from_tasks(name: &str, agents: Vec<AgentInfo>, all: Vec<AgentTasks>) -> Self {
        let mut tasks = Vec::new();
        for mut at in all {
            let mine: Vec<Task> = at
                .tasks
                .take()
                .unwrap_or_default()
                .into_iter()
                .filter(|t| t.job_name == name)
                .collect();
            if !mine.is_empty() && tasks.try_reserve(1).is_ok() {
                tasks.push(AgentTasks {
                    agent: at.agent,
                    tasks: Some(mine),
                });
            }
        }
        let agents = agents
            .into_iter()
            .filter(|a| tasks.iter().any(|t: &AgentTasks| t.agent == a.id))
            .collect();
        JobStatus { agents, tasks }
    }
}

pub(crate) fn parse_capacity(r: &Response) -> Result<Capacity> {
    let v = object(r)?;
    let o = obj(&v)?;
    let mut c = Capacity {
        cpu_cores: uint(o, "cpu_cores")?,
        memory_bytes: uint(o, "memory_bytes")?,
        cpu_used_shares: int(o, "cpu_used_shares")?,
        memory_used_bytes: uint(o, "memory_used_bytes")?,
        tasks_running: uint(o, "tasks_running")?,
        ..Capacity::default()
    };
    if let Some(Value::Object(a)) = o.get("attributes") {
        for (k, val) in a.iter() {
            c.attributes
                .insert(try_string(k)?, de::string(val, "attributes")?)?;
        }
    }
    Ok(c)
}

/// De capaciteit via de doorgifte van de leader; `None` als de leader die
/// route niet kent (404 zonder "agent not found", de toets van de CLI).
pub(crate) fn parse_capacity_via_leader(r: &Response) -> Result<Option<Capacity>> {
    if r.status == 404 && !contains(&r.body, b"agent not found") {
        return Ok(None);
    }
    parse_capacity(r).map(Some)
}

pub(crate) fn parse_leader(r: &Response) -> Result<Leader> {
    let v = object(r)?;
    let o = obj(&v)?;
    let exp = string(o, "lease_expires_at")?;
    let lease_expires_at = if exp.is_empty() {
        Time::default()
    } else {
        Time::parse_rfc3339(&exp)?
    };
    Ok(Leader {
        leader: string(o, "leader")?,
        lease_expires_at,
    })
}

pub(crate) fn parse_applied(r: &Response) -> Result<Applied> {
    let v = object(r)?;
    let o = obj(&v)?;
    Ok(Applied {
        name: string(o, "name")?,
        status: string(o, "status")?,
        policy: string(o, "policy")?,
        error: string(o, "error")?,
    })
}

fn contains(hay: &[u8], needle: &[u8]) -> bool {
    hay.windows(needle.len()).any(|w| w == needle)
}
