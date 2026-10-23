//! De staat van Hop bij de kern: [`agent::Store`] over `STATE_PUT` en `STATE_GET`.
//!
//! De kern bewaart één blob voor Hop in zijn eigen geheugen. Het overleeft
//! een herstart van Hop (`HOP_RESTART`) en sterft met de boot, zodat een
//! staat van een vorige boot nooit een bewoner van deze boot voor de zijne
//! kan houden. De kern leest het niet. Een crash van Hop herstart de kern
//! niet: een falende Hop is een falende node, de watchdog reset hem, en de
//! koude boot begint zonder blob.
//!
//! De opnummers en de maat komen uit `abi` ([`PrivOp::StatePut`],
//! [`PrivOp::StateGet`], [`HOP_STATE_MAX`], HopOS v3.0.16). Een oudere kern
//! kent de ops niet en antwoordt met een foutstatus: dat is
//! [`agent::StoreError::Refused`], en Hop draait dan zoals vóór de kern-staat.

use alloc::vec::Vec;

use abi::systemapi::{HOP_STATE_MAX, PrivOp};
use applib::sys;

use crate::{Call, KernSys};

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
            op: PrivOp::StatePut.op(),
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
                op: PrivOp::StateGet.op(),
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
