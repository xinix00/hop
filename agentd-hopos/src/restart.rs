//! Hop herstart zichzelf onder zijn apps: `POST /restart` (achter dezelfde
//! HMAC als `/flip`, `api::NodeApi`) wordt de staat bij de kern, en
//! `HOP_RESTART`.
//!
//! "Als we binnen 30 s herstarten en onze staat terugzetten, is er niets
//! gebeurd": de lease van de cluster leeft 30 s en dezelfde node pakt zijn
//! eigen lease meteen terug (`discovery::Discovery::plan_claim`), de leader
//! verklaart een agent pas na 30 s dood, en de apps merken niets: hun
//! slots, hun poorten en hun verbindingen blijven van hen. Wat een herstarte
//! Hop zonder hulp mist, is welke taak in welk slot draait; die staat legt
//! hij bij de kern neer ([`hopos_runner::state`], `agent::Handoff`) en de
//! volgende neemt de kooien over die de kern nog als draaiend meldt
//! (`Node::restore`).
//!
//! De weg (de kernkant staat in de werkboom van HopOS, `PrivOp::HopRestart`):
//!
//! 1. met een `url`: het image rauw in een slot, precies de weg van de
//!    flip-bundel ([`crate::flip::fetch_raw`] met [`HOP_IMAGE_JOB`]);
//! 2. de staat nog één keer naar de kern (`STATE_PUT`): wat de nieuwe Hop
//!    terugleest, is de staat van nu, niet die van de laatste tik;
//! 3. `HOP_RESTART` met het slot (0: hetzelfde image) en de som. `OK` is
//!    "getoetst en overgenomen": de kern wacht nu tot deze Hop ZELF
//!    vertrekt (terug uit main, hoogstens de stoptermijn, 3 s), en luidt
//!    pas daarna de stop-bel als vangnet;
//! 4. Hop laat eerst het 202-antwoord helemaal wegschrijven: de werker die
//!    het verzoek droeg sluit de verbinding na het antwoord (`Connection:
//!    close`) en meldt zich vrij ([`crate::Handoff::freed`]), met
//!    [`REPLY_GRACE`] als grens ([`depart`]). Tot 22-10 stopte de kern Hop
//!    een vaste 500 ms na zijn OK, en het antwoord kwam nooit aan (curl
//!    HTTP 000 in `qemu-test-hop-restart.sh`);
//! 5. dan de staat een laatste keer (`Node::leave`) en terug uit main: de
//!    main-schil van applib doet het net-afscheid en de exit, en de kern zet
//!    de nieuwe Hop in hetzelfde slot.
//!
//! Luidt de bel toch eerst (een kern van vóór deze afspraak, of een
//! antwoord dat te lang duurt), dan is dat de weg van elke stop: de staat
//! een laatste keer, en weg.
//!
//! Een kern die de staat niet houdt (een oudere, zonder de ops) of de
//! herstart weigert: 502 met de reden, en deze Hop draait door. Zonder
//! staat zou de nieuwe Hop elke app als bewoner van niemand stoppen.

use alloc::format;
use alloc::string::String;
use core::future::Future;

use hopos_runner::state::OP_HOP_RESTART;
use hopos_runner::{Call, KernSys};
use runner::{Slot, SystemApi};

use crate::node::Images;

pub use hopos_runner::state::HOP_IMAGE_JOB;

/// Hoe lang Hop na het OK van de kern hoogstens wacht tot zijn antwoord
/// de deur uit is: ruim onder de stoptermijn van de kern (3 s), zodat de
/// staat en het net-afscheid er nog in passen.
pub const REPLY_GRACE: core::time::Duration = core::time::Duration::from_secs(1);

/// Waarom Hop na een aangenomen herstart vertrekt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Departure {
    /// Het antwoord is geschreven en de verbinding dicht.
    Replied,
    /// [`REPLY_GRACE`] verstreek eerst.
    GraceOver,
    /// De kern luidde de stop-bel eerst.
    Bell,
}

impl Departure {
    /// Het woord in de logregel (`why=`).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Replied => "replied",
            Self::GraceOver => "grace",
            Self::Bell => "bell",
        }
    }
}

/// Wacht na een aangenomen herstart op wat het eerst komt: het antwoord de
/// deur uit (`replied`), de grens (`grace`, [`REPLY_GRACE`]) of de stop-bel
/// (`bell`). De bel gaat voor: wie belt, wacht niet meer.
pub async fn depart(
    replied: impl Future<Output = ()>,
    grace: impl Future<Output = ()>,
    bell: impl Future<Output = ()>,
) -> Departure {
    let mut replied = core::pin::pin!(replied);
    let mut grace = core::pin::pin!(grace);
    let mut bell = core::pin::pin!(bell);
    core::future::poll_fn(|cx| {
        if bell.as_mut().poll(cx).is_ready() {
            return core::task::Poll::Ready(Departure::Bell);
        }
        if replied.as_mut().poll(cx).is_ready() {
            return core::task::Poll::Ready(Departure::Replied);
        }
        if grace.as_mut().poll(cx).is_ready() {
            return core::task::Poll::Ready(Departure::GraceOver);
        }
        core::task::Poll::Pending
    })
    .await
}

/// Hoe lang de HOP_RESTART-call mag duren: de kern hasht een nieuw image
/// voor hij antwoordt (een image van Hop is een paar MB, zoals een bundel).
const RESTART_TIMEOUT: core::time::Duration = core::time::Duration::from_secs(20);

/// De herstart-call van de kern, naast de gewone [`SystemApi`].
pub trait KernRestart {
    /// `HOP_RESTART` met het image in `slot` (`None`: hetzelfde image), de
    /// verwachte som als 64 hex-tekens (leeg zonder slot) en de stoptermijn
    /// in ms (0: de standaard van de kern).
    fn hop_restart(
        &mut self,
        slot: Option<Slot>,
        sha256_hex: &str,
        grace_ms: u64,
    ) -> impl Future<Output = Result<(), String>>;
}

impl<C: Call> KernRestart for KernSys<C> {
    async fn hop_restart(
        &mut self,
        slot: Option<Slot>,
        sha256_hex: &str,
        grace_ms: u64,
    ) -> Result<(), String> {
        let req = applib::sys::Req {
            op: OP_HOP_RESTART,
            seq: 0,
            off: slot.map_or(0, |s| u64::from(s.0)),
            n: grace_ms,
            path: sha256_hex,
            data: &[],
        };
        let mut reason = [0u8; 256];
        self.conn_mut()
            .call(req, &mut reason, RESTART_TIMEOUT)
            .await
            .map(|_| ())
            .map_err(|e| format!("kernel refused the restart: {e}"))
    }
}

/// Haalt het nieuwe image van Hop op en stroomt het rauw de kern in.
pub async fn fetch<S, I>(sys: &mut S, images: &mut I, url: &str) -> Result<Slot, String>
where
    S: SystemApi,
    I: Images,
{
    crate::flip::fetch_raw(sys, images, url, HOP_IMAGE_JOB, "image").await
}

/// Vraagt de herstart; `Ok` is "de kern nam hem aan en stopt deze Hop zo".
/// Een weigering ruimt het slot van een nieuw image op.
pub async fn ask<S>(sys: &mut S, slot: Option<Slot>, sha256: &str) -> Result<(), String>
where
    S: SystemApi + KernRestart,
{
    let r = sys.hop_restart(slot, sha256, 0).await;
    if r.is_err()
        && let Some(slot) = slot
    {
        // Een stop op een leeg slot (de kern gaf het al terug) is geen fout.
        let _ = sys.stop_slot(slot, 0).await;
    }
    r
}
