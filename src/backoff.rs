//! Hoe lang een stroom wacht voor hij opnieuw verbindt.
//!
//! Bezit alleen de huidige wachttijd. De Go-watchers van hoplb en hopdns
//! wachtten elk een vaste `interval` en schreven die lus zelf; hier staat
//! hij één keer, en verdubbelt hij, zodat een agent die herstart niet door
//! elke plugin tegelijk elke seconde bestookt wordt.

use core::time::Duration;

/// De eerste wachttijd, en de wachttijd na een stroom die gezond was.
pub const BACKOFF_MIN: Duration = Duration::from_secs(1);

/// De langste wachttijd: een halve minuut. Langer maakt een plugin na een
/// storing traag wakker; korter bestookt een agent die nog opstart.
pub const BACKOFF_MAX: Duration = Duration::from_secs(30);

/// Een verdubbelende wachttijd tussen [`Backoff::min`](Backoff::new) en een plafond.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Backoff {
    min: Duration,
    max: Duration,
    cur: Duration,
}

impl Default for Backoff {
    fn default() -> Self {
        Self::new(BACKOFF_MIN, BACKOFF_MAX)
    }
}

impl Backoff {
    /// Een backoff van `min` tot `max` (een `max` onder `min` is `min`).
    pub fn new(min: Duration, max: Duration) -> Self {
        Self {
            min,
            max: max.max(min),
            cur: min,
        }
    }

    /// De volgende wachttijd; de keer daarna is hij twee keer zo lang, tot het plafond.
    pub fn next_delay(&mut self) -> Duration {
        let d = self.cur;
        self.cur = self.cur.saturating_mul(2).min(self.max);
        d
    }

    /// De wachttijd die [`Backoff::next_delay`] nu zou geven, zonder op te schuiven.
    pub fn peek(&self) -> Duration {
        self.cur
    }

    /// Terug naar het begin: de stroom was gezond.
    pub fn reset(&mut self) {
        self.cur = self.min;
    }
}
