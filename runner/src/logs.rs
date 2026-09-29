//! De laatste logregels van een taak, ook nog even nadat hij stopte.
//!
//! Bezit per taak twee ringen (stdout, stderr) en de gepensioneerde ringen
//! van taken die net afliepen. Bezit geen lezers: wie volgt, onthoudt zijn
//! eigen volgnummer en vraagt [`LogRing::since`]. Geen kanalen, geen slot.

use alloc::collections::{BTreeMap, VecDeque};
use alloc::string::String;

/// Hoeveel een runner van de uitvoer bewaart.
///
/// De standaard is bewust klein: Hop draait ook op boards met een paar
/// honderd MB.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LogPolicy {
    /// Regels per stroom.
    pub tail_lines: usize,
    /// Hoe lang de regels na het einde van de taak blijven, in milliseconden.
    pub keep_ms: u64,
}

impl LogPolicy {
    /// 50 regels, 5 minuten.
    pub const DEFAULT: LogPolicy = LogPolicy {
        tail_lines: 50,
        keep_ms: 5 * 60 * 1000,
    };

    /// Vult nulvelden aan uit [`LogPolicy::DEFAULT`].
    pub fn or_default(self) -> Self {
        Self {
            tail_lines: if self.tail_lines == 0 {
                Self::DEFAULT.tail_lines
            } else {
                self.tail_lines
            },
            keep_ms: if self.keep_ms == 0 {
                Self::DEFAULT.keep_ms
            } else {
                self.keep_ms
            },
        }
    }
}

impl Default for LogPolicy {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// Een ring van de laatste N regels van één stroom.
///
/// # Invariants
///
/// `lines.len() <= cap`, en `seq` is het aantal regels dat ooit geschreven is.
#[derive(Clone, Debug)]
pub struct LogRing {
    lines: VecDeque<String>,
    cap: usize,
    seq: u64,
    closed: bool,
}

impl LogRing {
    /// Een lege ring van `cap` regels (0 wordt de standaard).
    pub fn new(cap: usize) -> Self {
        let cap = if cap == 0 {
            LogPolicy::DEFAULT.tail_lines
        } else {
            cap
        };
        Self {
            lines: VecDeque::new(),
            cap,
            seq: 0,
            closed: false,
        }
    }

    /// Voegt een regel toe; de oudste valt eruit als de ring vol is.
    ///
    /// Na [`LogRing::close`] of zonder geheugen wordt de regel stil gedropt:
    /// een log mag de node nooit omleggen.
    pub fn write(&mut self, line: &str) {
        if self.closed {
            return;
        }
        if self.lines.len() >= self.cap {
            self.lines.pop_front();
        } else if self.lines.try_reserve(1).is_err() {
            return;
        }
        let mut s = String::new();
        if s.try_reserve(line.len()).is_err() {
            return;
        }
        s.push_str(line);
        self.lines.push_back(s);
        self.seq = self.seq.wrapping_add(1);
    }

    /// De bewaarde regels, oudste eerst.
    pub fn tail(&self) -> impl Iterator<Item = &str> {
        self.lines.iter().map(String::as_str)
    }

    /// Het aantal regels in de ring.
    pub fn len(&self) -> usize {
        self.lines.len()
    }

    /// Of de ring leeg is.
    pub fn is_empty(&self) -> bool {
        self.lines.is_empty()
    }

    /// Het volgnummer na de laatste regel.
    pub fn seq(&self) -> u64 {
        self.seq
    }

    /// De regels na volgnummer `seq` die nog in de ring staan.
    pub fn since(&self, seq: u64) -> impl Iterator<Item = &str> {
        let newer = self.seq.saturating_sub(seq);
        let skip = self
            .lines
            .len()
            .saturating_sub(usize::try_from(newer).unwrap_or(usize::MAX));
        self.lines.iter().skip(skip).map(String::as_str)
    }

    /// Sluit de ring: er komt niets meer bij, en een lezer weet dat hij klaar is.
    pub fn close(&mut self) {
        self.closed = true;
    }

    /// Of de ring gesloten is.
    pub fn is_closed(&self) -> bool {
        self.closed
    }
}

/// De ringen van lopende en net afgelopen taken.
#[derive(Debug)]
pub struct LogStore {
    policy: LogPolicy,
    live: BTreeMap<String, (LogRing, LogRing)>,
    retired: BTreeMap<String, (LogRing, LogRing, u64)>,
}

impl LogStore {
    /// Een lege store met `policy`.
    pub fn new(policy: LogPolicy) -> Self {
        Self {
            policy: policy.or_default(),
            live: BTreeMap::new(),
            retired: BTreeMap::new(),
        }
    }

    /// Het beleid van deze store.
    pub fn policy(&self) -> LogPolicy {
        self.policy
    }

    /// Zet verse ringen klaar voor `task_id`; een hergebruikt id krijgt nooit de oude regels.
    pub fn open(&mut self, task_id: &str) {
        self.retired.remove(task_id);
        let n = self.policy.tail_lines;
        self.live
            .insert(String::from(task_id), (LogRing::new(n), LogRing::new(n)));
    }

    /// De ring van een lopende taak om in te schrijven.
    pub fn live_mut(&mut self, task_id: &str, stream: crate::Stream) -> Option<&mut LogRing> {
        let pair = self.live.get_mut(task_id)?;
        Some(match stream {
            crate::Stream::Stdout => &mut pair.0,
            crate::Stream::Stderr => &mut pair.1,
        })
    }

    /// De ring van een taak, lopend of gepensioneerd en niet verlopen.
    pub fn get(&self, now: u64, task_id: &str, stream: crate::Stream) -> Option<&LogRing> {
        let (out, err) = match self.live.get(task_id) {
            Some((o, e)) => (o, e),
            None => match self.retired.get(task_id) {
                Some((o, e, until)) if now < *until => (o, e),
                _ => return None,
            },
        };
        Some(match stream {
            crate::Stream::Stdout => out,
            crate::Stream::Stderr => err,
        })
    }

    /// Pensioneert de ringen van `task_id`: gesloten, nog `keep_ms` opvraagbaar.
    ///
    /// Onbekend of al gepensioneerd is geen fout.
    pub fn retire(&mut self, now: u64, task_id: &str) {
        self.sweep(now);
        if let Some((mut out, mut err)) = self.live.remove(task_id) {
            out.close();
            err.close();
            let until = now.saturating_add(self.policy.keep_ms);
            self.retired
                .insert(String::from(task_id), (out, err, until));
        }
    }

    /// Ruimt verlopen gepensioneerde ringen op.
    pub fn sweep(&mut self, now: u64) {
        self.retired.retain(|_, (_, _, until)| now < *until);
    }

    /// Het aantal lopende en gepensioneerde entries (voor tests en de meetlat).
    pub fn counts(&self) -> (usize, usize) {
        (self.live.len(), self.retired.len())
    }
}
