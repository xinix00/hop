//! `hop`: het commando voor een Hop-cluster, zoals `OLD/cmd/cli` in Go.
//!
//! Commando's: `apply` (een jobspec-bestand of vlaggen), `jobs`, `status`,
//! `agents [id]`, `logs <job|taak>` (met `--follow` een levende tail),
//! `events`, `delete <job>`, `flip <url> <sha256> [--cold]`. Alles gaat naar de
//! leader (`--leader`, standaard `localhost:9080`, of `HOP_LEADER`): de
//! taken via `/v1/tasks`, logs en de capaciteit van een agent via de
//! doorgifte `/v1/agents/{id}/...`. Alleen de flip gaat naar een agent zelf
//! (`--agent`). Alles is ondertekend met `--api-key` (of `HOP_API_KEY`); de
//! sleutel komt nooit in een melding.
//!
//! Waarom via de leader: een agent staat vaak op een adres dat de CLI niet
//! ziet (het slot-LAN van HopOS, een privénet achter de leader), en de
//! leader bereikt ze allemaal. Een oudere leader zonder die routes (404)
//! krijgt de oude weg: de agents direct, op hun adres uit `/v1/agents`.

#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )
)]
#![forbid(unsafe_code)]

mod client;
mod jobspec;
mod sse;
mod table;
#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::process::ExitCode;

use types::json::{self, Value};
use types::{Agent, Job, Task, TaskState};

use crate::client::Client;
use crate::jobspec::{ApplyFlags, build_job};
use crate::table::Table;

const USAGE: &str =
    "Usage: hop [--leader host:port] [--agent host:port] [--api-key KEY] <command> [args]

Commands:
  apply <jobspec.json>   Create or update a job from a JSON file (upsert by name)
  apply --name N ...     Or build the job from flags (hop apply --help)
  jobs                   List jobs and their tasks
  status                 Show cluster status
  agents [id]            List agents, or show one agent's capacity
  logs <job|task>        Show the latest log lines (--stream stderr)
  logs -f <job|task>     Follow the log live until the task stops (--follow)
  events                 Follow the cluster events (SSE /v1/events)
  delete <job>           Delete a job and all its tasks
  flip <url> <sha256>    Replace the kernel of a HopOS node (--agent; --cold stops the apps first)

Environment: HOP_LEADER, HOP_AGENT, HOP_API_KEY";

const APPLY_USAGE: &str = "Usage: hop apply <jobspec.json>
       hop apply --name N (--command CMD | --image IMG | --driver hop --artifact URL) [flags]

Flags:
  --driver exec|docker|hop   --count N (-1 = every agent)   --cpu SHARES
  --memory 512M              --priority N (0 = first)        --update-policy rolling|recreate|blue-green
  --env K=V (repeatable)     --artifact [k=v,...::]URL (repeatable)
  --affinity k=v[,k=v]       --tag k=v[,k=v]
  --check-type http|tcp|file --check-path P  --check-port NAME  --check-failures N";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match run(args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("Error: {e}");
            ExitCode::FAILURE
        }
    }
}

/// Leest de globale vlaggen (overal toegestaan) en geeft de rest terug.
fn globals(args: Vec<String>) -> Result<(Client, Vec<String>), String> {
    let env = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| String::from(d));
    let mut leader = env("HOP_LEADER", "localhost:9080");
    let mut agent = env("HOP_AGENT", "localhost:8080");
    let mut key = env("HOP_API_KEY", "");
    let mut rest = Vec::new();
    let mut it = args.into_iter();
    while let Some(a) = it.next() {
        let (flag, inline) = match a.split_once('=') {
            Some((f, v)) if f.starts_with("--") => (String::from(f), Some(String::from(v))),
            _ => (a.clone(), None),
        };
        let slot = match flag.as_str() {
            "--leader" | "-leader" => &mut leader,
            "--agent" | "-agent" => &mut agent,
            "--api-key" | "-api-key" => &mut key,
            _ => {
                rest.push(a);
                continue;
            }
        };
        *slot = match inline {
            Some(v) => v,
            None => it.next().ok_or_else(|| format!("{flag} needs a value"))?,
        };
    }
    Ok((Client::new(leader, agent, key), rest))
}

fn run(args: Vec<String>) -> Result<(), String> {
    let (client, rest) = globals(args)?;
    let Some((cmd, args)) = rest.split_first() else {
        eprintln!("{USAGE}");
        return Err(String::from("no command"));
    };
    match cmd.as_str() {
        "apply" => apply(&client, args),
        "jobs" => jobs(&client),
        "status" => status(&client),
        "agents" => agents(&client, args),
        "logs" => logs(&client, args),
        "events" => events(&client),
        "delete" => delete(&client, args),
        "flip" => flip(&client, args),
        "help" | "--help" | "-h" => {
            println!("{USAGE}");
            Ok(())
        }
        other => {
            eprintln!("{USAGE}");
            Err(format!("unknown command: {other}"))
        }
    }
}

/// Leest de vlaggen van `apply` (herhaalbare vlaggen verzamelen, zoals Go's `stringList`).
pub(crate) fn parse_apply(args: &[String]) -> Result<(ApplyFlags, Option<String>), String> {
    let mut f = ApplyFlags::new();
    let mut file = None;
    let mut it = args.iter();
    while let Some(a) = it.next() {
        let Some(name) = a.strip_prefix("--").or_else(|| a.strip_prefix('-')) else {
            if file.replace(a.clone()).is_some() {
                return Err(String::from("apply takes one jobspec file"));
            }
            continue;
        };
        if name == "help" || name == "h" {
            return Err(String::from(APPLY_USAGE));
        }
        let (name, inline) = match name.split_once('=') {
            Some((n, v)) => (n, Some(v.to_string())),
            None => (name, None),
        };
        let mut value = || -> Result<String, String> {
            match &inline {
                Some(v) => Ok(v.clone()),
                None => it
                    .next()
                    .cloned()
                    .ok_or_else(|| format!("--{name} needs a value")),
            }
        };
        let int = |s: String| -> Result<i64, String> {
            s.parse()
                .map_err(|_| format!("--{name}: {s:?} is not a number"))
        };
        match name {
            "name" => f.name = value()?,
            "command" => f.command = value()?,
            "image" => f.image = value()?,
            "driver" => f.driver = value()?,
            "count" => f.count = int(value()?)?,
            "cpu" => f.cpu = int(value()?)?,
            "memory" => f.memory = value()?,
            "priority" => f.priority = int(value()?)?,
            "update-policy" => f.update_policy = value()?,
            "check-type" => f.check_type = value()?,
            "check-path" => f.check_path = value()?,
            "check-port" => f.check_port = value()?,
            "check-failures" => f.check_failures = int(value()?)?,
            "env" => f.env.push(value()?),
            "artifact" => f.artifacts.push(value()?),
            "affinity" => f.affinity.push(value()?),
            "tag" => f.tags.push(value()?),
            other => return Err(format!("apply: unknown flag --{other}\n{APPLY_USAGE}")),
        }
    }
    Ok((f, file))
}

/// De body voor `POST /v1/jobs` en de jobnaam: het bestand ongewijzigd, of de job uit de vlaggen.
fn apply_body(args: &[String]) -> Result<(Vec<u8>, String), String> {
    let (flags, file) = parse_apply(args)?;
    if let Some(path) = file {
        let data = std::fs::read(&path).map_err(|e| format!("{path}: {e}"))?;
        // Eerst zelf lezen: een typefout hoort hier luid, niet als "invalid json" van de leader.
        let job = Job::from_json(&data).map_err(|e| format!("{path}: {e}"))?;
        if job.name.is_empty() {
            return Err(format!("{path}: the job has no name"));
        }
        return Ok((data, job.name));
    }
    if flags.name.is_empty() {
        return Err(format!(
            "--name is required (or give a jobspec file)\n{APPLY_USAGE}"
        ));
    }
    if flags.driver == "hop" {
        if flags.artifacts.is_empty() {
            return Err(String::from("--driver hop requires --artifact <url>"));
        }
    } else if flags.command.is_empty() && flags.image.is_empty() {
        return Err(String::from("either --command or --image is required"));
    }
    let job = build_job(&flags)?;
    let body = job.to_json().map_err(|e| format!("job: {e}"))?;
    Ok((body.into_bytes(), job.name))
}

fn apply(c: &Client, args: &[String]) -> Result<(), String> {
    let (body, name) = apply_body(args)?;
    let r = c.leader("POST", "/v1/jobs", Some(&body))?;
    let v = json::parse(&r.body).map_err(|e| format!("leader reply: {e}"))?;
    match field_str(&v, "status") {
        "updated" => println!("Job '{name}' updated (policy={})", field_str(&v, "policy")),
        "pending" => println!(
            "Job '{name}' stored, pending dispatch: {}",
            field_str(&v, "error")
        ),
        _ => println!("Job '{name}' dispatched"),
    }
    Ok(())
}

fn field<'a>(v: &'a Value, k: &str) -> Option<&'a Value> {
    v.as_object().and_then(|o| o.get(k))
}

fn field_str<'a>(v: &'a Value, k: &str) -> &'a str {
    field(v, k).and_then(Value::as_str).unwrap_or("")
}

fn list<T>(
    body: &[u8],
    what: &str,
    one: impl Fn(&Value) -> types::Result<T>,
) -> Result<Vec<T>, String> {
    let v = json::parse(body).map_err(|e| format!("{what}: {e}"))?;
    let Some(items) = v.as_array() else {
        return Ok(Vec::new());
    };
    items
        .iter()
        .map(|i| one(i).map_err(|e| format!("{what}: {e}")))
        .collect()
}

fn get_jobs(c: &Client) -> Result<Vec<Job>, String> {
    let r = c.leader("GET", "/v1/jobs", None)?;
    list(&r.body, "jobs", |v| Job::from_value(v, false))
}

fn get_agents(c: &Client) -> Result<Vec<Agent>, String> {
    let r = c.leader("GET", "/v1/agents", None)?;
    list(&r.body, "agents", Agent::from_value)
}

/// De taken per agent-id; een agent die niet antwoordt, staat er met zijn fout in.
type AgentTasks = Vec<(String, Result<Vec<Task>, String>)>;

/// De taken van het cluster in één aanroep bij de leader (`GET /v1/tasks`,
/// Go's `clusterTasks`); een leader zonder die route (404) krijgt de oude
/// weg langs de agents zelf.
fn cluster_tasks(c: &Client) -> Result<AgentTasks, String> {
    let url = format!("{}/v1/tasks", client::base(&c.leader));
    let r = c.call("GET", &url, None)?;
    if r.status == 404 {
        let agents = get_agents(c)?;
        return Ok(direct_tasks(c, &agents));
    }
    let r = client::check(r)?;
    parse_tasks_by_agent(&r.body)
}

/// Het antwoord van `/v1/tasks`: `tasks_by_agent`, en `unreachable` als fout per agent.
pub(crate) fn parse_tasks_by_agent(body: &[u8]) -> Result<AgentTasks, String> {
    let v = json::parse(body).map_err(|e| format!("tasks: {e}"))?;
    let mut out = Vec::new();
    if let Some(o) = field(&v, "tasks_by_agent").and_then(Value::as_object) {
        for (id, ts) in o.iter() {
            let tasks = ts
                .as_array()
                .map(|a| {
                    a.iter()
                        .map(|t| Task::from_value(t).map_err(|e| format!("tasks: {e}")))
                        .collect()
                })
                .unwrap_or_else(|| Ok(Vec::new()));
            out.push((String::from(id), tasks));
        }
    }
    if let Some(silent) = field(&v, "unreachable").and_then(Value::as_array) {
        for id in silent.iter().filter_map(Value::as_str) {
            out.push((
                String::from(id),
                Err(String::from("did not answer the leader in time")),
            ));
        }
    }
    Ok(out)
}

/// De oude weg: elke agent zelf om zijn taken vragen.
fn direct_tasks(c: &Client, agents: &[Agent]) -> AgentTasks {
    agents
        .iter()
        .map(|a| {
            let tasks = c
                .agent_at(&a.endpoint, "GET", "/tasks", None)
                .and_then(|r| list(&r.body, "tasks", Task::from_value));
            (a.id.clone(), tasks)
        })
        .collect()
}

/// De placed-tellers uit `/v1/status`.
fn placed(v: &Value) -> BTreeMap<String, i64> {
    let mut m = BTreeMap::new();
    if let Some(o) = field(v, "placed").and_then(Value::as_object) {
        for (k, n) in o.iter() {
            m.insert(String::from(k), n.as_i64().unwrap_or(0));
        }
    }
    m
}

/// Hoeveel instanties een job wil: -1 is "op elke agent", 0 is 1.
fn expected(job: &Job, agents: usize) -> (i64, String) {
    match job.count {
        -1 => (
            i64::try_from(agents).unwrap_or(i64::MAX),
            format!("all({agents})"),
        ),
        n if n <= 0 => (1, String::from("1")),
        n => (n, n.to_string()),
    }
}

/// De startfase van een taak als leesbare tekst, zoals Go's `startPhases`.
fn phase(t: &Task) -> String {
    match t.state {
        TaskState::Downloading if t.image_size > 0 => format!(
            "downloading {}% ({:.1}/{:.1} MB)",
            t.downloaded.saturating_mul(100) / t.image_size,
            t.downloaded as f64 / f64::from(1u32 << 20),
            t.image_size as f64 / f64::from(1u32 << 20)
        ),
        s => String::from(s.as_str()),
    }
}

fn jobs(c: &Client) -> Result<(), String> {
    let jobs = get_jobs(c)?;
    let st = c.leader("GET", "/v1/status", None)?;
    let st = json::parse(&st.body).map_err(|e| format!("status: {e}"))?;
    let placed = placed(&st);
    let agents = get_agents(c)?;
    let tasks = cluster_tasks(c)?;
    let mut t = Table::new(&["NAME", "DRIVER", "PLACED", "EXPECTED", "TASKS"]);
    for j in &jobs {
        let (_, want) = expected(j, agents.len());
        let mut states: BTreeMap<&str, usize> = BTreeMap::new();
        for (_, ts) in &tasks {
            for task in ts.iter().flatten().filter(|x| x.job_name == j.name) {
                *states.entry(task.state.as_str()).or_default() += 1;
            }
        }
        let summary = if states.is_empty() {
            String::from("-")
        } else {
            states
                .iter()
                .map(|(s, n)| format!("{n} {s}"))
                .collect::<Vec<_>>()
                .join(", ")
        };
        t.row(vec![
            j.name.clone(),
            String::from(j.driver().as_str()),
            placed.get(&j.name).copied().unwrap_or(0).to_string(),
            want,
            summary,
        ]);
    }
    print!("{}", t.render());
    let mut tt = Table::new(&["TASK", "JOB", "AGENT", "STATE", "PID", "PORTS", "RESTARTS"]);
    let mut any = false;
    for (a, ts) in &tasks {
        match ts {
            Ok(ts) => {
                for task in ts {
                    any = true;
                    let ports = task
                        .ports
                        .iter()
                        .map(|(k, p)| format!("{k}={p}"))
                        .collect::<Vec<_>>()
                        .join(",");
                    tt.row(vec![
                        task.id.clone(),
                        task.job_name.clone(),
                        a.clone(),
                        phase(task),
                        task.pid.to_string(),
                        if ports.is_empty() {
                            String::from("-")
                        } else {
                            ports
                        },
                        task.restart_count.to_string(),
                    ]);
                }
            }
            Err(e) => eprintln!("agent {a}: {e}"),
        }
    }
    if any {
        println!();
        print!("{}", tt.render());
    }
    Ok(())
}

fn status(c: &Client) -> Result<(), String> {
    let st = c.leader("GET", "/v1/status", None)?;
    let v = json::parse(&st.body).map_err(|e| format!("status: {e}"))?;
    let agents = usize::try_from(field(&v, "agents").and_then(Value::as_u64).unwrap_or(0))
        .unwrap_or(usize::MAX);
    let total = field(&v, "total_placed")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let settling = field(&v, "settling")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let placed = placed(&v);
    let jobs = get_jobs(c)?;
    let name = field_str(&v, "cluster_name");
    if !name.is_empty() {
        println!("Cluster: {name}");
    }
    println!("Leader:  {}", c.leader);
    println!("Agents:  {agents}");
    println!("Placed:  {total} total");
    if settling {
        println!("Status:  settling...");
    }
    println!();
    if jobs.is_empty() {
        return Ok(());
    }
    // De startfase uit de taken van de agents; onbereikbaar is een lege
    // kolom, de tabel blijft heel (Go: best effort).
    let all = cluster_tasks(c).unwrap_or_default();
    let mut t = Table::new(&["NAME", "PLACED", "EXPECTED", "STATUS"]);
    for j in &jobs {
        let (want, want_s) = expected(j, agents);
        let got = placed.get(&j.name).copied().unwrap_or(0);
        let mut st = if got < want { "DEGRADED" } else { "OK" }.to_string();
        if j.deploying {
            st = String::from("DEPLOYING");
        }
        for (_, ts) in &all {
            for task in ts.iter().flatten().filter(|x| x.job_name == j.name) {
                if matches!(task.state, TaskState::Queued | TaskState::Downloading) {
                    st = phase(task).to_uppercase();
                }
            }
        }
        t.row(vec![j.name.clone(), got.to_string(), want_s, st]);
    }
    print!("{}", t.render());
    Ok(())
}

/// Een temperatuur in milligraden; 0 is geen sensor en wordt een streepje.
pub(crate) fn fmt_temp(milli_c: i64) -> String {
    if milli_c == 0 {
        return String::from("-");
    }
    format!("{:.1}\u{b0}C", milli_c as f64 / 1000.0)
}

/// De kloktijd (UTC) van een tijdstip, `-` als het niet gezet is.
fn clock(t: types::Time) -> String {
    if t.is_zero() {
        return String::from("-");
    }
    let mut s = String::new();
    if t.write_rfc3339(&mut s).is_err() {
        return String::from("-");
    }
    s.get(11..19).map_or_else(|| s.clone(), String::from)
}

fn agents(c: &Client, args: &[String]) -> Result<(), String> {
    let agents = get_agents(c)?;
    if let Some(id) = args.first() {
        let a = agents
            .iter()
            .find(|a| &a.id == id)
            .ok_or_else(|| format!("agent {id} not found"))?;
        return agent_details(c, a);
    }
    let mut t = Table::new(&["ID", "ENDPOINT", "VERSION", "TEMP", "LAST SEEN"]);
    for a in &agents {
        t.row(vec![
            a.id.clone(),
            a.endpoint.clone(),
            a.version.clone(),
            fmt_temp(a.temp_milli_c),
            clock(a.last_seen),
        ]);
    }
    print!("{}", t.render());
    Ok(())
}

fn agent_details(c: &Client, a: &Agent) -> Result<(), String> {
    println!("Agent:    {}", a.id);
    println!("Endpoint: {}", a.endpoint);
    println!("Version:  {}", a.version);
    if a.temp_milli_c != 0 {
        println!("CPU temp: {}", fmt_temp(a.temp_milli_c));
    }
    println!("LastSeen: {}", clock(a.last_seen));
    println!();
    let r = match capacity(c, a) {
        Ok(r) => r,
        Err(e) => {
            println!("Capacity: (unavailable - {e})");
            return Ok(());
        }
    };
    let v = json::parse(&r.body).map_err(|e| format!("capacity: {e}"))?;
    let num = |k| field(&v, k).and_then(Value::as_i64).unwrap_or(0);
    let cores = num("cpu_cores");
    let used = num("cpu_used_shares");
    let gib = |n: i64| n as f64 / f64::from(1u32 << 30);
    println!("Tasks:    {} running", num("tasks_running"));
    println!(
        "CPU:      {:.1} / {cores} cores ({used} / {} shares)",
        used as f64 / 1024.0,
        cores.saturating_mul(1024)
    );
    println!(
        "Memory:   {:.1} / {:.0} GB",
        gib(num("memory_used_bytes")),
        gib(num("memory_bytes"))
    );
    if let Some(o) = field(&v, "attributes").and_then(Value::as_object) {
        println!();
        println!("Attributes:");
        let mut kv: Vec<(&str, &str)> = o
            .iter()
            .map(|(k, x)| (k, x.as_str().unwrap_or("")))
            .collect();
        kv.sort_unstable();
        for (k, x) in kv {
            println!("  {k} = {x}");
        }
    }
    Ok(())
}

/// De capaciteit van een agent: via de doorgifte van de leader, en bij een
/// leader zonder die route (404) bij de agent zelf.
fn capacity(c: &Client, a: &Agent) -> Result<hostnet::Reply, String> {
    let url = format!("{}/v1/agents/{}/capacity", client::base(&c.leader), a.id);
    let r = c.call("GET", &url, None)?;
    if r.status == 404 && !String::from_utf8_lossy(&r.body).contains("agent not found") {
        return c.agent_at(&a.endpoint, "GET", "/capacity", None);
    }
    client::check(r)
}

/// De vlaggen van `hop logs`.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct LogsFlags {
    /// `stdout` of `stderr` (`--stream`, zoals Go).
    pub(crate) stream: String,
    /// Levend volgen (`--follow`, `-f`).
    pub(crate) follow: bool,
    /// De job of de taak.
    pub(crate) target: String,
}

/// Leest de vlaggen van `hop logs`.
///
/// `--stream` kiest de stroom (`stdout`, `stderr`), zoals in Go; `--follow`
/// (of `-f`) houdt de stroom open en toont nieuwe regels tot de taak stopt.
pub(crate) fn parse_logs(args: &[String]) -> Result<LogsFlags, String> {
    let mut stream = String::from("stdout");
    let mut follow = false;
    let mut target = None;
    let mut it = args.iter();
    while let Some(a) = it.next() {
        match a.as_str() {
            "--stream" | "-stream" => {
                stream = it.next().cloned().ok_or("--stream needs a value")?;
            }
            s if s.starts_with("--stream=") => stream = String::from(&s[9..]),
            "--follow" | "-follow" | "-f" => follow = true,
            s if s.starts_with('-') => return Err(format!("logs: unknown flag {s}")),
            s => {
                if target.replace(String::from(s)).is_some() {
                    return Err(String::from("logs takes one job name or task id"));
                }
            }
        }
    }
    let target = target.ok_or("job name or task id required")?;
    if stream != "stdout" && stream != "stderr" {
        return Err(String::from("stream must be stdout or stderr"));
    }
    Ok(LogsFlags {
        stream,
        follow,
        target,
    })
}

fn logs(c: &Client, args: &[String]) -> Result<(), String> {
    let f = parse_logs(args)?;
    let mut found = Vec::new();
    for (a, ts) in cluster_tasks(c)? {
        for t in ts.unwrap_or_default() {
            if t.id == f.target || t.job_name == f.target {
                found.push((a.clone(), t));
            }
        }
    }
    if found.is_empty() {
        return Err(format!(
            "no task of job or with id {} on any agent",
            f.target
        ));
    }
    if f.follow {
        let [(a, t)] = found.as_slice() else {
            let ids: Vec<&str> = found.iter().map(|(_, t)| t.id.as_str()).collect();
            return Err(format!(
                "{} has {} tasks; follow one: {}",
                f.target,
                found.len(),
                ids.join(" ")
            ));
        };
        return follow(c, a, &t.id, &f.stream);
    }
    let many = found.len() > 1;
    for (a, t) in found {
        if many {
            println!("== {} on {a} ==", t.id);
        }
        // `follow=0`: de momentopname. Zonder query volgt de leader-route
        // de log (Go's contract, voor het dashboard).
        let path = format!("/v1/agents/{a}/logs/{}/{}?follow=0", t.id, f.stream);
        match c.leader("GET", &path, None) {
            Ok(r) => {
                let mut rd = sse::Reader::default();
                for e in rd.feed(&r.body) {
                    println!("{}", e.data);
                }
            }
            Err(e) => eprintln!("{}: {e}", t.id),
        }
    }
    Ok(())
}

/// Volgt de log van één taak via de leader tot de taak stopt of de stroom breekt.
fn follow(c: &Client, agent: &str, task: &str, stream: &str) -> Result<(), String> {
    let path = format!("/v1/agents/{agent}/logs/{task}/{stream}?follow=1");
    let mut o = c.leader_stream(&path)?;
    print_stream(&mut o, |e| println!("{}", e.data))
}

/// Leest een SSE-stroom tot zijn einde en geeft elke gebeurtenis aan `each`.
fn print_stream(o: &mut hostnet::Open, mut each: impl FnMut(&sse::Event)) -> Result<(), String> {
    let mut rd = sse::Reader::default();
    let mut buf = vec![0u8; 16 << 10];
    loop {
        let n = o.read(&mut buf).map_err(|e| format!("stream: {e}"))?;
        if n == 0 {
            return Ok(());
        }
        for e in rd.feed(buf.get(..n).unwrap_or_default()) {
            each(&e);
        }
    }
}

/// `hop events`: de meldingen van de leader, één regel per gebeurtenis, tot
/// de leader de stroom sluit (hij leidt niet meer) of Ctrl-C.
fn events(c: &Client) -> Result<(), String> {
    let mut o = c.leader_stream("/v1/events")?;
    print_stream(&mut o, |e| println!("{} {}", e.kind, e.data))
}

fn delete(c: &Client, args: &[String]) -> Result<(), String> {
    let name = args.first().ok_or("job name required")?;
    c.leader("DELETE", &format!("/v1/jobs/{name}"), None)?;
    println!("Job deleted ({name})");
    Ok(())
}

fn flip(c: &Client, args: &[String]) -> Result<(), String> {
    // `--cold`: de koude weg (docs/flip.md in hop-os). Hop stopt eerst zijn
    // taken op de node, de kern zet de app-cores uit en springt zonder
    // adoptie; dat is de weg voor een bundel met een andere switch-code,
    // die warm vóór de sprong geweigerd wordt.
    let cold = args.iter().any(|a| a == "--cold");
    let rest: Vec<&String> = args.iter().filter(|a| *a != "--cold").collect();
    let (Some(url), Some(sum)) = (rest.first(), rest.get(1)) else {
        return Err(String::from(
            "usage: hop flip <url> <sha256> [--cold] [--agent host:port]",
        ));
    };
    if sum.len() != 64 || !sum.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(String::from("sha256 must be 64 hex characters"));
    }
    let mut body = String::new();
    body.push_str("{\"url\":");
    json::write_string(url, &mut body).map_err(|e| e.to_string())?;
    body.push_str(",\"sha256\":");
    json::write_string(sum, &mut body).map_err(|e| e.to_string())?;
    if cold {
        body.push_str(",\"cold\":true");
    }
    body.push('}');
    let r = c.agent_at(&c.agent, "POST", "/flip", Some(body.as_bytes()))?;
    let v = json::parse(&r.body).unwrap_or(Value::Null);
    println!("{}: {}", c.agent, field_str(&v, "status"));
    Ok(())
}
