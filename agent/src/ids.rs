//! Taak-id's en jitter uit één deterministische bron.
//!
//! Bezit een splitmix64-toestand. Geen OS-entropie in `no_std`: de executor
//! geeft een zaad mee ([`crate::Settings::seed`]), de tests een vast getal.

use alloc::string::String;

/// Een splitmix64-generator.
#[derive(Clone, Debug)]
pub(crate) struct Ids {
    state: u64,
}

impl Ids {
    /// Een generator op `seed`.
    pub(crate) fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    /// Het volgende getal.
    pub(crate) fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// Een id van 16 hex-tekens, zoals Go's `leanrand.ID(16)`.
    pub(crate) fn id(&mut self) -> types::Result<String> {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut s = String::new();
        s.try_reserve_exact(16)
            .map_err(|_| types::Error::OutOfMemory)?;
        let mut v = self.next_u64();
        for _ in 0..16 {
            let nibble = usize::try_from(v & 0xf).unwrap_or(0);
            s.push(char::from(HEX.get(nibble).copied().unwrap_or(b'0')));
            v >>= 4;
        }
        Ok(s)
    }
}
