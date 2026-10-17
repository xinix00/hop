//! De exporter: [`Metrics`] als Prometheus-tekstformaat (versie 0.0.4).
//!
//! Bezit de namen, de HELP-teksten, de volgorde en de escaping; geen
//! socket. Namen, teksten en volgorde zijn die van de Go-generatie
//! (`OLD/internal/exporter/exporter.go`), met twee verschillen die de
//! uitvoer beter maken zonder een naam te veranderen:
//!
//! - Elke reeks is gesorteerd op label (Go liep een deel van zijn maps in
//!   willekeurige volgorde af); twee scrapes van dezelfde staat zijn
//!   byte voor byte gelijk.
//! - Labelwaarden zijn ge-escapet zoals het formaat het zegt: `\` wordt
//!   `\\`, `"` wordt `\"` en een regeleinde `\n`. Go's `%q` escapete ook
//!   niet-ASCII als `\u...`, wat Prometheus letterlijk leest.
//!
//! Nieuw is `hop_agent_temperature_celsius`, uit de heartbeat van Hop v3;
//! alleen voor agents die een temperatuur melden. En `hop_job_deploying`
//! heeft nu een eigen HELP en TYPE (Go zette hem onder de kop van
//! `hop_jobs_deploying`).

use alloc::string::String;
use core::fmt::{self, Write};

use types::time::SECOND;
use types::{Map, Time};

use crate::{Error, Metrics, Result};

/// De `Content-Type` van `/metrics`.
pub const CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

/// Schrijft `m` als Prometheus-tekst achter `out`.
///
/// `now` is de wandklok voor `hop_leader_lease_seconds`; `None` meldt 0.
pub fn render(m: &Metrics, now: Option<Time>, out: &mut String) -> Result {
    let mut w = Out(out);
    write(&mut w, m, now).map_err(|_| Error::OutOfMemory)
}

/// Een `String` als faalbare schrijver: eerst ruimte, dan de tekst.
struct Out<'a>(&'a mut String);

impl Write for Out<'_> {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        self.0.try_reserve(s.len()).map_err(|_| fmt::Error)?;
        self.0.push_str(s);
        Ok(())
    }
}

/// Het hele document.
fn write(w: &mut Out<'_>, m: &Metrics, now: Option<Time>) -> fmt::Result {
    agents(w, m)?;
    tasks(w, m)?;
    jobs(w, m)?;
    counter(
        w,
        "hop_task_starts_total",
        "Total task starts per job",
        &m.starts_total,
    )?;
    counter(
        w,
        "hop_task_failures_total",
        "Total task failures per job",
        &m.failures_total,
    )?;
    counter(
        w,
        "hop_task_restarts_total",
        "Total task restarts per job",
        &m.restarts_total,
    )?;
    leader(w, m, now)
}

/// `# HELP` en `# TYPE` van één metric.
fn head(w: &mut Out<'_>, name: &str, help: &str, kind: &str) -> fmt::Result {
    write!(w, "# HELP {name} {help}\n# TYPE {name} {kind}\n")
}

/// `name{label="value"} ` met de waarde ge-escapet; de aanroeper schrijft het getal.
fn series(w: &mut Out<'_>, name: &str, label: &str, value: &str) -> fmt::Result {
    write!(w, "{name}{{{label}=\"")?;
    escape(w, value)?;
    w.write_str("\"} ")
}

/// Escapet een labelwaarde: `\`, `"` en het regeleinde.
fn escape(w: &mut Out<'_>, value: &str) -> fmt::Result {
    let mut rest = value;
    while let Some(i) = rest.find(['\\', '"', '\n']) {
        w.write_str(rest.get(..i).unwrap_or_default())?;
        match rest.as_bytes().get(i) {
            Some(b'\\') => w.write_str("\\\\")?,
            Some(b'"') => w.write_str("\\\"")?,
            _ => w.write_str("\\n")?,
        }
        rest = rest.get(i + 1..).unwrap_or_default();
    }
    w.write_str(rest)
}

/// Een float met `prec` decimalen, en de niet-eindige waarden zoals
/// Prometheus ze leest (`NaN`, `+Inf`, `-Inf`).
fn float(w: &mut Out<'_>, v: f64, prec: usize) -> fmt::Result {
    if v.is_nan() {
        w.write_str("NaN")
    } else if v.is_infinite() {
        w.write_str(if v > 0.0 { "+Inf" } else { "-Inf" })
    } else {
        write!(w, "{v:.prec$}")
    }
}

/// Agents: totaal, gezond, en per agent capaciteit en temperatuur.
fn agents(w: &mut Out<'_>, m: &Metrics) -> fmt::Result {
    head(
        w,
        "hop_agents_total",
        "Total number of registered agents",
        "gauge",
    )?;
    writeln!(w, "hop_agents_total {}\n", m.agents_total)?;
    head(
        w,
        "hop_agents_healthy",
        "Number of healthy agents (seen in last 60s)",
        "gauge",
    )?;
    writeln!(w, "hop_agents_healthy {}\n", m.agents_healthy)?;

    let caps = || {
        m.agents
            .iter()
            .filter_map(|(id, a)| a.capacity.map(|c| (id, c)))
    };
    if caps().next().is_some() {
        head(
            w,
            "hop_agent_cpu_cores",
            "Total CPU cores per agent",
            "gauge",
        )?;
        for (id, c) in caps() {
            series(w, "hop_agent_cpu_cores", "agent", id)?;
            writeln!(w, "{}", c.cpu_cores)?;
        }
        w.write_str("\n")?;
        head(
            w,
            "hop_agent_cpu_used_cores",
            "Used CPU cores per agent",
            "gauge",
        )?;
        for (id, c) in caps() {
            series(w, "hop_agent_cpu_used_cores", "agent", id)?;
            // Go: CPUUsedShares / 1024, als float met twee decimalen.
            float(w, c.cpu_used_shares as f64 / 1024.0, 2)?;
            w.write_str("\n")?;
        }
        w.write_str("\n")?;
        head(
            w,
            "hop_agent_memory_bytes",
            "Total memory bytes per agent",
            "gauge",
        )?;
        for (id, c) in caps() {
            series(w, "hop_agent_memory_bytes", "agent", id)?;
            writeln!(w, "{}", c.memory_bytes)?;
        }
        w.write_str("\n")?;
        head(
            w,
            "hop_agent_memory_used_bytes",
            "Used memory bytes per agent",
            "gauge",
        )?;
        for (id, c) in caps() {
            series(w, "hop_agent_memory_used_bytes", "agent", id)?;
            writeln!(w, "{}", c.memory_used_bytes)?;
        }
        w.write_str("\n")?;
    }

    let temps = || m.agents.iter().filter(|(_, a)| a.temp_milli_c != 0);
    if temps().next().is_some() {
        head(
            w,
            "hop_agent_temperature_celsius",
            "CPU temperature per agent from its heartbeat (the hottest sensor)",
            "gauge",
        )?;
        for (id, a) in temps() {
            series(w, "hop_agent_temperature_celsius", "agent", id)?;
            float(w, a.temp_milli_c as f64 / 1000.0, 1)?;
            w.write_str("\n")?;
        }
        w.write_str("\n")?;
    }
    Ok(())
}

/// Taken: per staat (altijd de kop), geplaatst per job, herstarts per job.
fn tasks(w: &mut Out<'_>, m: &Metrics) -> fmt::Result {
    head(w, "hop_tasks_total", "Number of tasks by state", "gauge")?;
    for (state, n) in m.tasks_by_state.iter() {
        series(w, "hop_tasks_total", "state", state)?;
        writeln!(w, "{n}")?;
    }
    w.write_str("\n")?;
    // "placed", niet "running": het cluster weet dat een taak geplaatst is
    // en de node meldt dat hij startte; echte levendigheid is van de node
    // (en de health check), niet iets dat het cluster beweert.
    gauge_map(
        w,
        "hop_tasks_placed",
        "Placed tasks per job",
        "job",
        &m.tasks_placed,
    )?;
    gauge_map(
        w,
        "hop_task_restarts",
        "Current restart count per job",
        "job",
        &m.task_restarts,
    )
}

/// Een gauge per label, alleen als er iets is.
fn gauge_map(w: &mut Out<'_>, name: &str, help: &str, label: &str, map: &Map<u64>) -> fmt::Result {
    if map.is_empty() {
        return Ok(());
    }
    head(w, name, help, "gauge")?;
    for (k, n) in map.iter() {
        series(w, name, label, k)?;
        writeln!(w, "{n}")?;
    }
    w.write_str("\n")
}

/// Jobs: het totaal, en per job geplaatst, verwacht, gezond, cpu en geheugen.
fn jobs(w: &mut Out<'_>, m: &Metrics) -> fmt::Result {
    head(w, "hop_jobs_total", "Total number of jobs", "gauge")?;
    writeln!(w, "hop_jobs_total {}\n", m.jobs_total)?;
    if m.jobs.is_empty() {
        return Ok(());
    }
    head(
        w,
        "hop_job_instances_placed",
        "Placed instances per job",
        "gauge",
    )?;
    for (job, j) in m.jobs.iter() {
        series(w, "hop_job_instances_placed", "job", job)?;
        writeln!(w, "{}", j.placed)?;
    }
    w.write_str("\n")?;
    head(
        w,
        "hop_job_instances_expected",
        "Expected instances per job",
        "gauge",
    )?;
    for (job, j) in m.jobs.iter() {
        series(w, "hop_job_instances_expected", "job", job)?;
        writeln!(w, "{}", j.expected)?;
    }
    w.write_str("\n")?;
    head(
        w,
        "hop_job_healthy",
        "Job is healthy (placed >= expected)",
        "gauge",
    )?;
    for (job, j) in m.jobs.iter() {
        series(w, "hop_job_healthy", "job", job)?;
        writeln!(w, "{}", u8::from(j.healthy))?;
    }
    w.write_str("\n")?;
    head(
        w,
        "hop_job_cpu_percent",
        "Average CPU usage percent per job (relative to allocation)",
        "gauge",
    )?;
    for (job, j) in m.jobs.iter() {
        series(w, "hop_job_cpu_percent", "job", job)?;
        float(w, j.cpu_percent, 1)?;
        w.write_str("\n")?;
    }
    w.write_str("\n")?;
    head(
        w,
        "hop_job_mem_percent",
        "Average memory usage percent per job (relative to allocation)",
        "gauge",
    )?;
    for (job, j) in m.jobs.iter() {
        series(w, "hop_job_mem_percent", "job", job)?;
        float(w, j.mem_percent, 1)?;
        w.write_str("\n")?;
    }
    w.write_str("\n")
}

/// Een teller per job, alleen als er iets is.
fn counter(w: &mut Out<'_>, name: &str, help: &str, map: &Map<u64>) -> fmt::Result {
    if map.is_empty() {
        return Ok(());
    }
    head(w, name, help, "counter")?;
    for (job, n) in map.iter() {
        series(w, name, "job", job)?;
        writeln!(w, "{n}")?;
    }
    w.write_str("\n")
}

/// Leader en lease: de hartslag van het cluster, gezien vanaf deze node.
fn leader(w: &mut Out<'_>, m: &Metrics, now: Option<Time>) -> fmt::Result {
    head(
        w,
        "hop_leader_info",
        "Current leader address as a label (1 = a leader is known)",
        "gauge",
    )?;
    series(w, "hop_leader_info", "leader", &m.leader)?;
    writeln!(w, "{}\n", u8::from(!m.leader.is_empty()))?;

    head(
        w,
        "hop_leader_lease_seconds",
        "Seconds until the local leader's lease lapses unless renewed (0 when this node does not lead)",
        "gauge",
    )?;
    let lease = match now {
        Some(now) if !m.lease_expires_at.is_zero() => {
            m.lease_expires_at.since(now) as f64 / SECOND as f64
        }
        _ => 0.0,
    };
    w.write_str("hop_leader_lease_seconds ")?;
    float(w, lease, 1)?;
    w.write_str("\n\n")?;

    head(
        w,
        "hop_leader_changes_total",
        "Leader switches observed since hopprom started",
        "counter",
    )?;
    writeln!(w, "hop_leader_changes_total {}\n", m.leader_changes)?;
    head(
        w,
        "hop_cluster_settling",
        "1 while a new leader waits before its first reconcile",
        "gauge",
    )?;
    writeln!(w, "hop_cluster_settling {}\n", u8::from(m.settling))?;
    head(
        w,
        "hop_jobs_deploying",
        "Jobs whose last rollout did not finish",
        "gauge",
    )?;
    writeln!(w, "hop_jobs_deploying {}\n", m.deploying.len())?;
    if m.deploying.is_empty() {
        return Ok(());
    }
    // Go zette deze reeks zonder eigen kop onder `hop_jobs_deploying`; een
    // andere naam is een eigen familie, dus hier met HELP en TYPE.
    head(
        w,
        "hop_job_deploying",
        "1 for each job whose last rollout did not finish",
        "gauge",
    )?;
    for job in &m.deploying {
        series(w, "hop_job_deploying", "job", job)?;
        w.write_str("1\n")?;
    }
    w.write_str("\n")
}
