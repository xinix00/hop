//! De willekeur van een TLS-handshake en van de spreiding van herhalingen.
//!
//! Bezit een pool van 32 bytes staat die de tunnel voedt met wat er is, en
//! een SHA-256 om dat te mengen. Een handshake verbruikt 96 bytes
//! (`leantls::Entropy`: de X25519-sleutel en twee willekeurige velden); wie
//! die kan voorspellen, kan de verbinding met de edge meelezen, ondanks de
//! ketenverificatie. Een slot heeft geen eigen bron: de kooi geeft geen RNG
//! door, de cores van QEMU virt en de Pi's hebben geen `RNDR`, en de kern
//! biedt nog geen random-op.
//!
//! Dus verzamelen we wat er is, zoals `agentd-hopos` van Hop het doet
//! (`entropy.rs` daar, dezelfde vorm): het slot, de wandklok, de onderste
//! bits van de teller rond werk waarvan de duur schommelt, en de tijd van
//! gebeurtenissen van buiten (elke handshake mengt de tijd van zijn TCP-
//! verbinding in). Elke trekking hasht staat en teller naar 96 bytes en
//! ratelt de staat daarna door, zodat een uitgelekte trekking geen eerdere
//! of latere verraadt.
//!
//! Dit is zwakker dan een hardware-RNG en dat staat op de console
//! (`HOPOS_CFTUNNEL_ENTROPY_WEAK`). De SHA-256 staat hier omdat lean hem
//! niet exporteert (leantls heeft er een, intern); zie de README.

#![forbid(unsafe_code)]

/// Hoeveel jitter-metingen [`Pool::harvest`] bij de start doet; dezelfde
/// maat als in Hop (gemeten 29-09 op QEMU: ongeveer 0,3 ms).
pub(crate) const HARVEST_ROUNDS: usize = 512;

/// De rondeconstanten van SHA-256 (FIPS 180-4 §4.2.2).
const K: [u32; 64] = [
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
];

/// De beginwaarde (FIPS 180-4 §5.3.3).
const H0: [u32; 8] = [
    0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
];

/// SHA-256 in stukken.
#[derive(Clone)]
pub(crate) struct Sha256 {
    /// De toestand.
    h: [u32; 8],
    /// Een onvol blok.
    buf: [u8; 64],
    /// Bytes in `buf`.
    fill: usize,
    /// Alle bytes tot nu toe.
    total: u64,
}

impl Sha256 {
    /// Een lege hash.
    pub(crate) fn new() -> Self {
        Self {
            h: H0,
            buf: [0; 64],
            fill: 0,
            total: 0,
        }
    }

    /// Eén blok van 64 bytes.
    fn block(h: &mut [u32; 8], b: &[u8; 64]) {
        let mut w = [0u32; 64];
        for (i, c) in b.chunks_exact(4).enumerate() {
            if let (Some(slot), &[a, b2, c2, d]) = (w.get_mut(i), c) {
                *slot = u32::from_be_bytes([a, b2, c2, d]);
            }
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }
        let [mut a, mut bb, mut c, mut d, mut e, mut f, mut g, mut hh] = *h;
        for (k, wi) in K.iter().zip(w.iter()) {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ (!e & g);
            let t1 = hh
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(*k)
                .wrapping_add(*wi);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & bb) ^ (a & c) ^ (bb & c);
            let t2 = s0.wrapping_add(maj);
            hh = g;
            g = f;
            f = e;
            e = d.wrapping_add(t1);
            d = c;
            c = bb;
            bb = a;
            a = t1.wrapping_add(t2);
        }
        for (x, v) in h.iter_mut().zip([a, bb, c, d, e, f, g, hh]) {
            *x = x.wrapping_add(v);
        }
    }

    /// Voegt `data` toe.
    pub(crate) fn update(&mut self, data: &[u8]) {
        self.total = self.total.wrapping_add(data.len() as u64);
        for &byte in data {
            if let Some(slot) = self.buf.get_mut(self.fill) {
                *slot = byte;
            }
            self.fill += 1;
            if self.fill == 64 {
                Self::block(&mut self.h, &self.buf);
                self.fill = 0;
            }
        }
    }

    /// De hash.
    pub(crate) fn finish(mut self) -> [u8; 32] {
        let bits = self.total.wrapping_mul(8);
        self.update(&[0x80]);
        while self.fill != 56 {
            self.update(&[0]);
        }
        self.update(&bits.to_be_bytes());
        let mut out = [0u8; 32];
        for (o, v) in out.chunks_exact_mut(4).zip(self.h.iter()) {
            o.copy_from_slice(&v.to_be_bytes());
        }
        out
    }
}

/// De staat van de willekeur, met één eigenaar (een verbindingstaak).
pub(crate) struct Pool {
    /// De staat.
    state: [u8; 32],
    /// Het aantal trekkingen.
    draws: u64,
}

impl Pool {
    /// Een pool met `seed` als eerste inbreng.
    pub(crate) fn new(seed: &[u8]) -> Self {
        let mut h = Sha256::new();
        h.update(b"cloudflared-lean-entropy-v1");
        h.update(seed);
        Self {
            state: h.finish(),
            draws: 0,
        }
    }

    /// Mengt `sample` in de staat (een tijd, een tellerstand).
    pub(crate) fn stir(&mut self, sample: &[u8]) {
        let mut h = Sha256::new();
        h.update(&self.state);
        h.update(sample);
        self.state = h.finish();
    }

    /// Meet `rounds` keer de duur van een hash met `clock` en mengt de tijden
    /// in: de jitter is de willekeur.
    pub(crate) fn harvest(&mut self, clock: impl Fn() -> u64, rounds: usize) {
        let mut h = Sha256::new();
        h.update(&self.state);
        let mut prev = clock();
        for i in 0..rounds {
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

    /// Vult `out` en ratelt de staat door.
    pub(crate) fn fill(&mut self, out: &mut [u8]) {
        self.draws = self.draws.wrapping_add(1);
        for (i, chunk) in out.chunks_mut(32).enumerate() {
            let mut h = Sha256::new();
            h.update(&self.state);
            h.update(&self.draws.to_le_bytes());
            h.update(&(i as u64).to_le_bytes());
            let d = h.finish();
            chunk.copy_from_slice(d.get(..chunk.len()).unwrap_or_default());
        }
        let mut h = Sha256::new();
        h.update(&self.state);
        h.update(b"ratchet");
        self.state = h.finish();
    }

    /// Een `leantls::Entropy` voor één handshake.
    pub(crate) fn entropy(&mut self) -> leantls::Entropy {
        let mut b = [0u8; leantls::Entropy::LEN];
        self.fill(&mut b);
        leantls::Entropy::new(b)
    }
}

impl leanrand::Source for Pool {
    fn fill(&mut self, buf: &mut [u8]) {
        Pool::fill(self, buf);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::cell::Cell;

    fn hex(b: &[u8]) -> alloc::string::String {
        b.iter().map(|x| alloc::format!("{x:02x}")).collect()
    }

    // FIPS 180-2 bijlage B, en het lege bericht.
    #[test]
    fn sha256_vectors() {
        let one = |s: &[u8]| {
            let mut h = Sha256::new();
            h.update(s);
            hex(&h.finish())
        };
        assert_eq!(
            one(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            one(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(
            one(b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"),
            "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"
        );
        // In stukken is hetzelfde als in één keer.
        let mut h = Sha256::new();
        for _ in 0..1000 {
            h.update(b"a");
        }
        let mut g = Sha256::new();
        g.update(&[b'a'; 1000]);
        assert_eq!(h.finish(), g.finish());
    }

    #[test]
    fn draws_differ_and_depend_on_every_input() {
        let mut a = Pool::new(b"slot1");
        let mut b = Pool::new(b"slot1");
        let (mut a1, mut b1) = ([0u8; 96], [0u8; 96]);
        a.fill(&mut a1);
        b.fill(&mut b1);
        assert_eq!(
            a1, b1,
            "zelfde invoer, zelfde uitkomst: geen verborgen bron"
        );
        let mut a2 = [0u8; 96];
        a.fill(&mut a2);
        assert_ne!(a2, a1, "elke trekking is nieuw");
        let mut c = Pool::new(b"slot1");
        c.stir(&42u64.to_le_bytes());
        let mut c1 = [0u8; 96];
        c.fill(&mut c1);
        assert_ne!(c1, a1, "een tijd van buiten verandert alles");
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
        let (mut x, mut y) = ([0u8; 32], [0u8; 32]);
        a.fill(&mut x);
        b.fill(&mut y);
        assert_ne!(x, y);
    }
}
