//! De willekeur van een TLS-handshake, uit timer-jitter.
//!
//! Een handshake verbruikt 96 bytes willekeur (`leantls::Entropy`): de
//! X25519-sleutel en twee willekeurige velden. Wie die bytes kan
//! voorspellen, kan een verbinding meelezen en een artifact onderweg
//! vervangen, ondanks de ketenverificatie. De app heeft geen eigen bron:
//! de slot-kooi geeft geen RNG door, de cores van QEMU virt en de Pi's
//! hebben geen `RNDR`, en de kern biedt nog geen random-op.
//!
//! Dus verzamelen we wat er is: de onderste bits van de teller rond werk
//! waarvan de duur schommelt (cache, DRAM-refresh, de emulator), en de
//! tijd van gebeurtenissen van buiten (een download, een tik). Alles gaat
//! door SHA-256 in een staat van 32 bytes; elke trekking hasht staat en
//! teller naar 96 bytes en ratelt de staat daarna door, zodat een
//! uitgelekte trekking geen eerdere of latere verraadt.
//!
//! Dit is zwakker dan een hardware-RNG en dat staat op de console
//! (`HOP_TLS_ENTROPY_WEAK`). De echte bron is een random-op van de kern
//! over de board-RNG; tot die er is, is dit de best beschikbare.

use auth::Sha256;

/// Hoeveel jitter-metingen [`Pool::harvest`] bij de start doet. Elke meting
/// geeft hooguit een paar bits; 512 houdt de start onder een milliseconde
/// op QEMU en de Pi (gemeten 29-09 op QEMU: ongeveer 0,3 ms).
pub const HARVEST_ROUNDS: usize = 512;

/// De staat van de willekeur: één eigenaar (de downloader).
pub struct Pool {
    state: [u8; 32],
    draws: u64,
}

impl Pool {
    /// Een pool met `seed` als eerste inbreng (het slot, de wandklok).
    pub fn new(seed: &[u8]) -> Self {
        let mut h = Sha256::new();
        h.update(b"hop-entropy-v1");
        h.update(seed);
        Self {
            state: h.finish(),
            draws: 0,
        }
    }

    /// Mengt `sample` in de staat (een tellerstand, een tijd, bytes van
    /// buiten).
    pub fn stir(&mut self, sample: &[u8]) {
        let mut h = Sha256::new();
        h.update(&self.state);
        h.update(sample);
        self.state = h.finish();
    }

    /// Meet `rounds` keer de duur van een hash met `clock` (monotone ns) en
    /// mengt de tijden in: de jitter is de willekeur.
    pub fn harvest(&mut self, clock: impl Fn() -> u64, rounds: usize) {
        let mut h = Sha256::new();
        h.update(&self.state);
        let mut prev = clock();
        for i in 0..rounds {
            // Werk met een schommelende duur: een blok hashen.
            let mut w = Sha256::new();
            w.update(&prev.to_le_bytes());
            w.update(&(i as u64).to_le_bytes());
            let d = w.finish();
            let now = clock();
            h.update(&now.wrapping_sub(prev).to_le_bytes());
            h.update(d.get(..1).unwrap_or_default());
            prev = now;
        }
        self.state = h.finish();
    }

    /// 96 bytes voor één handshake; daarna ratelt de staat door.
    pub fn draw(&mut self) -> [u8; 96] {
        self.draws = self.draws.wrapping_add(1);
        let mut out = [0u8; 96];
        for (i, chunk) in out.chunks_mut(32).enumerate() {
            let mut h = Sha256::new();
            h.update(&self.state);
            h.update(&self.draws.to_le_bytes());
            h.update(&[b'o', i as u8]);
            chunk.copy_from_slice(&h.finish());
        }
        let mut h = Sha256::new();
        h.update(&self.state);
        h.update(b"ratchet");
        self.state = h.finish();
        out
    }

    /// Een `leantls::Entropy` voor één handshake.
    pub fn entropy(&mut self) -> leantls::Entropy {
        leantls::Entropy::new(self.draw())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::cell::Cell;

    #[test]
    fn draws_differ_and_depend_on_every_input() {
        let mut a = Pool::new(b"slot1");
        let mut b = Pool::new(b"slot1");
        let (a1, b1) = (a.draw(), b.draw());
        assert_eq!(
            a1, b1,
            "zelfde invoer, zelfde uitkomst: geen verborgen bron"
        );
        assert_ne!(a.draw(), a1, "elke trekking is nieuw");
        let mut c = Pool::new(b"slot1");
        c.stir(&42u64.to_le_bytes());
        assert_ne!(c.draw(), a1, "een tijd van buiten verandert alles");
        assert_ne!(Pool::new(b"slot2").draw(), a1);
    }

    #[test]
    fn harvest_takes_the_jitter() {
        let t = Cell::new(0u64);
        let steady = || {
            t.set(t.get() + 100);
            t.get()
        };
        let mut a = Pool::new(b"x");
        a.harvest(steady, 8);
        let u = Cell::new(0u64);
        let jitter = || {
            u.set(u.get() + 100 + (u.get() / 100) % 3);
            u.get()
        };
        let mut b = Pool::new(b"x");
        b.harvest(jitter, 8);
        assert_ne!(a.draw(), b.draw());
    }
}
