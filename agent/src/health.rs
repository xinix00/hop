//! Gezondheidscontroles en backoff als pure functies over waarden.
//!
//! Bezit de teller per taak (opeenvolgende missers, tijd van de vorige
//! controle, of "started" al gemeld is). De probe zelf doet de executor; hier
//! komt alleen de uitkomst binnen.

use types::{CheckType, HealthCheck, Map, Nanos, Task, Time};

use crate::action::{Outcome, Probe};
use crate::{DEFAULT_FAILURE_THRESHOLD, MAX_RESTART_DELAY};

/// De gezondheidstoestand van één taak.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Check {
    /// Opeenvolgende mislukte controles.
    pub(crate) fail_count: u32,
    /// Wanneer de vorige controle was (voor de mtime-vergelijking van een file-check).
    pub(crate) last_check: Time,
    /// Of "started" al gemeld is na de eerste geslaagde controle.
    pub(crate) notified_healthy: bool,
}

/// Wat een controle na de drempel betekent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Verdict {
    /// Gezond (of nog onder de drempel).
    Healthy {
        /// Of de controle echt slaagde (en niet alleen onder de drempel bleef).
        passed: bool,
    },
    /// De drempel is bereikt: doden en herstarten.
    Unhealthy,
}

/// Bouwt de probe voor een taak, of `Err(())` als de genoemde poort er niet is.
///
/// Een ontbrekende poort is een mislukte controle, geen fout van de agent.
pub(crate) fn probe_for(
    task: &Task,
    hc: &HealthCheck,
    default_timeout: Nanos,
) -> Result<Probe, ()> {
    let timeout = if hc.timeout > 0 {
        hc.timeout
    } else {
        default_timeout
    };
    let kind = hc.kind.unwrap_or(CheckType::Http);
    if kind == CheckType::File {
        return types::try_string(&hc.path)
            .map(|path| Probe::File { path })
            .map_err(|_| ());
    }
    let port = named_port(&task.ports, &hc.port).ok_or(())?;
    match kind {
        CheckType::Tcp => Ok(Probe::Tcp { port, timeout }),
        _ => types::try_string(&hc.path)
            .map(|path| Probe::Http {
                port,
                path,
                timeout,
            })
            .map_err(|_| ()),
    }
}

/// De poort met naam `name` (leeg = "http").
fn named_port(ports: &Map<u16>, name: &str) -> Option<u16> {
    let name = if name.is_empty() { "http" } else { name };
    ports.get(name).copied()
}

/// Of een uitkomst gezond is, gegeven de vorige controle.
pub(crate) fn is_healthy(outcome: Outcome, check: &Check) -> bool {
    match outcome {
        Outcome::Http(Some(status)) => (200..400).contains(&status),
        Outcome::Http(None) => false,
        Outcome::Tcp(connected) => connected,
        Outcome::File(None) => false,
        // De eerste controle: bestaan is gezond. Daarna moet hij veranderd zijn.
        Outcome::File(Some(mtime)) => check.last_check.is_zero() || mtime > check.last_check,
    }
}

/// Past de drempel toe: `threshold` opeenvolgende missers voordat de taak ongezond is.
pub(crate) fn apply(check: &mut Check, healthy: bool, now: Nanos, threshold: i64) -> Verdict {
    check.last_check = Time(now);
    if healthy {
        check.fail_count = 0;
        return Verdict::Healthy { passed: true };
    }
    check.fail_count = check.fail_count.saturating_add(1);
    let threshold = u32::try_from(threshold)
        .ok()
        .filter(|t| *t > 0)
        .unwrap_or(DEFAULT_FAILURE_THRESHOLD);
    if check.fail_count < threshold {
        Verdict::Healthy { passed: false }
    } else {
        Verdict::Unhealthy
    }
}

/// De wachttijd vóór herstart nummer `count`, met jitter uit `rand`.
///
/// Verzadigt vóór het schuiven, zodat een groot of onbeperkt aantal herstarts
/// nooit overloopt naar een wachttijd van nul of minder (een strakke lus).
/// De basis is 1 s, 2 s, 4 s, 8 s, 16 s, daarna 30 s; de jitter trekt uit de
/// bovenste helft, zodat een kudde herstarts uiteenvalt maar nooit sneller
/// wordt dan de helft.
pub(crate) fn restart_delay(count: i64, rand: u64) -> Nanos {
    if count <= 0 {
        return 0;
    }
    let base = if count <= 5 {
        let shift = u32::try_from(count - 1).unwrap_or(0);
        types::time::SECOND << shift
    } else {
        MAX_RESTART_DELAY
    };
    let half = base / 2;
    let delay = half.saturating_add(rand % half.saturating_add(1));
    delay.clamp(1, MAX_RESTART_DELAY)
}
