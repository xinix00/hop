//! De staat van Hop bij de kern: [`agent::Store`] over `STATE_PUT` en `STATE_GET`.
//!
//! De kern bewaart één blob voor Hop in zijn eigen geheugen. Het overleeft
//! een herstart van Hop (`HOP_RESTART`, of een crash die de kern opvangt)
//! en sterft met de boot, zodat een staat van een vorige boot nooit een
//! bewoner van deze boot voor de zijne kan houden. De kern leest het niet.
//!
//! De opnummers, de maat en de jobnaam van een nieuw image staan hier als
//! getal en tekst tot de getagde HopOS ze in `abi` draagt (de werkboom heeft
//! ze als `PrivOp::StatePut`, `StateGet`, `HopRestart`, `HOP_STATE_MAX` en
//! `HOP_IMAGE_JOB`); Hop pint `abi` op een tag zonder. Een oudere kern kent
//! de ops niet en antwoordt met een foutstatus: dat is
//! [`agent::StoreError::Refused`], en Hop draait dan zoals vóór de kern-staat.

use alloc::vec::Vec;

use applib::sys;

use crate::{Call, KernSys};

/// `PrivOp::StatePut`: `data` het hele blob, vervangend en atomair; nul bytes wist.
pub const OP_STATE_PUT: u8 = 0x4B;

/// `PrivOp::StateGet`: `n` de offset; het antwoord `size` de maat van het
/// hele blob (0: er ligt niets), `data` de bytes vanaf `n`.
pub const OP_STATE_GET: u8 = 0x4C;

/// `PrivOp::HopRestart`: `off` 0 of het slot met een nieuw image, `path` de
/// SHA-256 als hex, `n` de stoptermijn in ms (0: de standaard van de kern).
pub const OP_HOP_RESTART: u8 = 0x4D;

/// De grootste staat die de kern bewaart (`HOP_STATE_MAX`): één I/O-hap.
pub const HOP_STATE_MAX: usize = sys::MAX_CHUNK;

/// De jobnaam waarmee Hop een slot reserveert voor een nieuw image van
/// zichzelf (`HOP_IMAGE_JOB`): de kern neemt de bytes rauw aan, zonder
/// plaatsing en zonder start, tot `HOP_RESTART` ze opeist.
pub const HOP_IMAGE_JOB: &str = "hopos.hop-image";

/// Een fout van de client als fout van de staat-opslag.
///
/// Een foutstatus van de kern is een weigering van de op zelf: een oudere
/// kern zegt "wrong kind" op een op die hij niet kent. Al het andere is de
/// weg ernaartoe, en die kan later weer werken.
fn store_err(e: &sys::Error) -> agent::StoreError {
    match e {
        sys::Error::Call { status, .. } => agent::StoreError::Refused { status: *status },
        sys::Error::TooLarge { len, max } => agent::StoreError::TooBig {
            len: *len,
            max: *max,
        },
        _ => agent::StoreError::Io,
    }
}

impl<C: Call> agent::Store for KernSys<C> {
    async fn save(&mut self, blob: &[u8]) -> Result<(), agent::StoreError> {
        if blob.len() > HOP_STATE_MAX {
            return Err(agent::StoreError::TooBig {
                len: blob.len(),
                max: HOP_STATE_MAX,
            });
        }
        let req = sys::Req {
            op: OP_STATE_PUT,
            seq: 0,
            off: 0,
            n: 0,
            path: "",
            data: blob,
        };
        match self.exchange(req, &mut []).await {
            Ok(_) => Ok(()),
            // Een kern die "niet gevonden" zegt, kent de op ook niet.
            Err(sys::Error::NotFound { .. }) => Err(agent::StoreError::Refused {
                status: abi::hopabi::STATUS_NO_ENT,
            }),
            Err(e) => Err(store_err(&e)),
        }
    }

    /// Leest het blob in één call, en in meer als de kern het knipt.
    ///
    /// De client weigert een antwoord dat langer is dan zijn buffer, en
    /// STATE_GET draagt geen leesmaat, dus de eerste call krijgt een buffer
    /// van [`HOP_STATE_MAX`]: eenmalig bij de start, daarna terug op maat.
    async fn load(&mut self) -> Result<Option<Vec<u8>>, agent::StoreError> {
        let mut out = Vec::new();
        out.try_reserve_exact(HOP_STATE_MAX)
            .map_err(|_| agent::StoreError::OutOfMemory)?;
        out.resize(HOP_STATE_MAX, 0);
        let mut at = 0usize;
        let mut size = None;
        loop {
            let off = u64::try_from(at).unwrap_or(u64::MAX);
            let req = sys::Req {
                op: OP_STATE_GET,
                seq: 0,
                off: 0,
                n: off,
                path: "",
                data: &[],
            };
            let end = size.unwrap_or(HOP_STATE_MAX);
            let dst = out.get_mut(at..end).unwrap_or_default();
            let (resp, n) = match self.exchange(req, dst).await {
                Ok(v) => v,
                Err(sys::Error::NotFound { .. }) => return Ok(None),
                Err(e) => return Err(store_err(&e)),
            };
            let total = usize::try_from(resp.size).unwrap_or(usize::MAX);
            if total == 0 {
                return Ok(None);
            }
            if total > HOP_STATE_MAX {
                return Err(agent::StoreError::TooBig {
                    len: total,
                    max: HOP_STATE_MAX,
                });
            }
            size = Some(total);
            at = at.saturating_add(n).min(total);
            if at >= total {
                break;
            }
            if n == 0 {
                // Geen voortgang: het blob veranderde onder de lezer, of de
                // kern geeft niets meer. Liever geen staat dan een halve.
                return Err(agent::StoreError::Io);
            }
        }
        out.truncate(at);
        out.shrink_to_fit();
        Ok(Some(out))
    }
}
