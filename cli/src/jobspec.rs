//! Een job uit de vlaggen van `hop apply`, zoals `buildJob` in Go.
//!
//! Bezit alleen de vertaling van vlaggen naar een [`types::Job`]; het
//! versturen doet `main`. Een jobspec als bestand gaat er niet doorheen: die
//! gaat ongewijzigd naar de leader, zodat de CLI nooit een veld weglaat dat
//! hij niet kent.

use types::{Artifact, CheckType, Driver, HealthCheck, Job, Map, UpdatePolicy};

/// De vlaggen van `hop apply`, al gelezen.
#[derive(Debug, Default)]
pub(crate) struct ApplyFlags {
    pub(crate) name: String,
    pub(crate) command: String,
    pub(crate) image: String,
    pub(crate) driver: String,
    pub(crate) count: i64,
    pub(crate) cpu: i64,
    pub(crate) memory: String,
    /// -1 is "niet gezet": achteraan plaatsen.
    pub(crate) priority: i64,
    pub(crate) update_policy: String,
    pub(crate) check_type: String,
    pub(crate) check_path: String,
    pub(crate) check_port: String,
    pub(crate) check_failures: i64,
    /// Herhaalbaar: één paar per `--env`, waarden houden hun komma's.
    pub(crate) env: Vec<String>,
    /// Herhaalbaar: `k=v[,k=v]::URL` of een kale URL.
    pub(crate) artifacts: Vec<String>,
    /// Herhaalbaar: `k=v[,k=v]`.
    pub(crate) affinity: Vec<String>,
    /// Herhaalbaar: `k=v[,k=v]`.
    pub(crate) tags: Vec<String>,
}

impl ApplyFlags {
    /// De standaarden van Go: prioriteit -1 (niet gezet), policy rolling.
    pub(crate) fn new() -> Self {
        Self {
            priority: -1,
            update_policy: String::from("rolling"),
            ..Self::default()
        }
    }
}

/// Maakt van `k=v,k2=v2` een map; alleen voor waarden die gewone tokens zijn
/// (tags, affinity). Een paar zonder `=` valt af.
pub(crate) fn parse_kv(s: &str) -> Map<String> {
    let mut m = Map::new();
    for pair in s.split(',') {
        if let Some((k, v)) = pair.split_once('=') {
            // Meer dan de grens van de map is een fout van de gebruiker; de
            // leader weigert de job dan toch, dus hier stil afkappen is eerlijk.
            let _ = m.insert(String::from(k), String::from(v));
        }
    }
    m
}

/// Leest elk element als ÉÉN paar, gesplitst op de eerste `=`: waarden
/// houden hun komma's en `=`-tekens (de oude komma-join brak `FOO=a,b`).
pub(crate) fn parse_pairs(pairs: &[String]) -> Map<String> {
    let mut m = Map::new();
    for pair in pairs {
        if let Some((k, v)) = pair.split_once('=') {
            let _ = m.insert(String::from(k), String::from(v));
        }
    }
    m
}

/// Een geheugengrootte als `512M`, `1G`, `64k` of een kaal getal bytes.
pub(crate) fn parse_memory(s: &str) -> Result<u64, String> {
    let bad = || format!("invalid memory value: {s}");
    let Some(unit) = s.chars().last() else {
        return Ok(0);
    };
    let (num, mul) = match unit {
        'K' | 'k' => (&s[..s.len() - 1], 1u64 << 10),
        'M' | 'm' => (&s[..s.len() - 1], 1u64 << 20),
        'G' | 'g' => (&s[..s.len() - 1], 1u64 << 30),
        _ => (s, 1),
    };
    let n: u64 = num.parse().map_err(|_| bad())?;
    n.checked_mul(mul).ok_or_else(bad)
}

/// Bouwt de job uit de vlaggen, zoals Go's `buildJob`.
pub(crate) fn build_job(f: &ApplyFlags) -> Result<Job, String> {
    let mut job = Job {
        name: f.name.clone(),
        command: f.command.clone(),
        image: f.image.clone(),
        count: f.count,
        cpu_shares: f.cpu,
        ..Job::default()
    };
    if !f.driver.is_empty() {
        job.driver =
            Some(Driver::parse(&f.driver).ok_or_else(|| format!("unknown driver {:?}", f.driver))?);
    }
    if !f.update_policy.is_empty() {
        job.update_policy = Some(
            UpdatePolicy::parse(&f.update_policy)
                .ok_or_else(|| format!("unknown update policy {:?}", f.update_policy))?,
        );
    }
    if !f.tags.is_empty() {
        job.tags = parse_kv(&f.tags.join(","));
    }
    if f.priority >= 0 {
        job.priority = Some(f.priority);
    }
    for art in &f.artifacts {
        let mut a = Artifact::default();
        match art.find("::") {
            Some(i) if i > 0 => {
                a.url = String::from(&art[i + 2..]);
                a.matches = parse_kv(&art[..i]);
            }
            _ => a.url = art.clone(),
        }
        job.artifacts.push(a);
    }
    if !f.memory.is_empty() {
        job.memory_limit = parse_memory(&f.memory)?;
    }
    if !f.env.is_empty() {
        job.env = parse_pairs(&f.env);
    }
    if !f.affinity.is_empty() {
        job.affinity = parse_kv(&f.affinity.join(","));
    }
    if !f.check_type.is_empty() || !f.check_path.is_empty() {
        let kind = if f.check_type.is_empty() {
            None
        } else {
            Some(
                CheckType::parse(&f.check_type)
                    .ok_or_else(|| format!("unknown check type {:?}", f.check_type))?,
            )
        };
        job.health_check = Some(HealthCheck {
            kind,
            path: f.check_path.clone(),
            port: f.check_port.clone(),
            failure_threshold: f.check_failures,
            ..HealthCheck::default()
        });
    }
    Ok(job)
}
