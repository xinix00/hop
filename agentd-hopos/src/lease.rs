//! De lease-taak en de staat-taak: de eigenaars van de verbinding met de lock-opslag.
//!
//! De lease-taak ([`lease_task`]) bezit de [`Discovery`] (de handle en
//! generatie van onze lease) en de [`LeaseBackend`]. Hij voert één opdracht
//! van de elector tegelijk uit en meldt de uitkomst als [`Mail::Lease`]; de
//! elector in de eigenaar antwoordt de verkiezing intussen uit het laatste
//! antwoord (`agent::Elector`). Dat is de lease-thread van de daemon
//! (`agentd/src/elector.rs`), als taak op de app-executor.
//!
//! De staat-taak ([`state_task`]) bezit de [`StateBackend`]: een lees bij het
//! leider worden ([`Mail::Loaded`]) en elke snapshot van de leader. Een eigen
//! taak, zodat een trage snapshot (een object-store doet seconden over een
//! PUT) nooit een lease-renew ophoudt.
//!
//! De stappen van een claim, renew en lees zijn die van `discovery`
//! (sans-I/O: [`Discovery::plan_claim`], [`Discovery::renew_op`],
//! [`Discovery::on_write`], [`discovery::leader_state`]); hier staan ze
//! async tegen een backend die op het net wacht.

use alloc::format;
use alloc::string::String;

use agent::{LeaseOp, LeaseReply};
use discovery::{Claim, Discovery, RenewOp};

use crate::lock::{LeaseBackend, StateBackend};
use crate::mail::{Inbox, LeaseQueue, Mail, Nap, StateOp, StateQueue, deliver};

/// Probeert de lease te claimen: aanmaken, vernieuwen of overnemen.
pub async fn claim<B: LeaseBackend>(
    disc: &mut Discovery,
    b: &mut B,
    now: u64,
) -> discovery::Result {
    let read = b.read().await;
    match disc.plan_claim(read, now) {
        Claim::Write { prev, state } => {
            let written = b.write(&prev, &state).await;
            disc.on_write(&state, written)
        }
        Claim::Fail(e) => Err(e),
    }
}

/// Vernieuwt de lease: `(renewed, displaced)`, zoals `Discovery::renew_lease`.
///
/// Met een handle is het één voorwaardelijke PUT zonder lees; zonder handle
/// (vers proces) het volle claimpad.
pub async fn renew<B: LeaseBackend>(disc: &mut Discovery, b: &mut B, now: u64) -> (bool, bool) {
    let result = match disc.renew_op(now) {
        RenewOp::Write { prev, state } => {
            let written = b.write(&prev, &state).await;
            disc.on_write(&state, written)
        }
        RenewOp::Claim => claim(disc, b, now).await,
    };
    match result {
        Ok(()) => (true, false),
        Err(e) => (false, e == discovery::Error::LeaseHeld),
    }
}

/// De leader volgens de opslag, en of de opslag antwoordde.
pub async fn leader_state<B: LeaseBackend>(b: &mut B, now: u64) -> (Option<String>, bool) {
    let read = b.read().await;
    let (leader, ok) = discovery::leader_state(&read, now);
    (leader.map(String::from), ok)
}

/// Verwijdert de lease naar beste kunnen, zodat een ander meteen kan overnemen.
pub async fn release<B: LeaseBackend>(disc: &mut Discovery, b: &mut B) {
    let handle = disc.take_handle();
    if !handle.is_empty() {
        // Naar beste kunnen: lukt het niet, dan loopt de lease vanzelf af.
        let _ = b.delete(&handle).await;
    }
}

/// De lease-taak: bezit `disc` en `backend`, voert de opdrachten uit `ops` uit.
///
/// Een fout van de opslag (401, DNS, een termijn) komt één keer op de log,
/// en opnieuw pas als hij verandert: een storing van een uur is één regel.
pub async fn lease_task<B: LeaseBackend, N: Nap>(
    mut disc: Discovery,
    mut backend: B,
    ops: &'static LeaseQueue,
    inbox: &'static Inbox,
    wall_ms: fn() -> u64,
    nap: N,
) {
    let mut said = String::new();
    loop {
        let op = ops.recv().await;
        let now = wall_ms();
        let reply = match op {
            LeaseOp::Read => {
                let (leader, ok) = leader_state(&mut backend, now).await;
                LeaseReply::Read { leader, ok }
            }
            LeaseOp::Claim => {
                LeaseReply::Claimed(claim(&mut disc, &mut backend, now).await.is_ok())
            }
            LeaseOp::Renew => {
                let (renewed, displaced) = renew(&mut disc, &mut backend, now).await;
                LeaseReply::Renewed(renewed, displaced)
            }
            LeaseOp::Release => {
                release(&mut disc, &mut backend).await;
                continue;
            }
        };
        match backend.last_error() {
            Some(e) if e != said => {
                said = String::from(e);
                let line = format!("hop: lease store: {e} HOP_LEASE_STORE");
                deliver(inbox, Mail::Note(line), &nap).await;
            }
            Some(_) => {}
            None => said.clear(),
        }
        deliver(inbox, Mail::Lease(reply), &nap).await;
    }
}

/// De staat-taak: bezit `backend`, leest bij het leider worden en schrijft de snapshots.
///
/// Van de snapshots telt alleen de laatste: liggen er meer in de rij, dan
/// gaan de oudere niet meer over de draad (de staat is één blob die de
/// leaseholder onvoorwaardelijk overschrijft).
pub async fn state_task<B: StateBackend, N: Nap>(
    mut backend: B,
    ops: &'static StateQueue,
    inbox: &'static Inbox,
    nap: N,
) {
    loop {
        let mut op = ops.recv().await;
        if let StateOp::Save(_) = op {
            while let Some(next) = ops.try_recv() {
                if let StateOp::Load = next {
                    // Een lees gaat voor: die wacht op antwoord.
                    let got = backend.load().await;
                    deliver(inbox, Mail::Loaded(got), &nap).await;
                    continue;
                }
                op = next;
            }
        }
        match op {
            StateOp::Load => {
                let got = backend.load().await;
                deliver(inbox, Mail::Loaded(got), &nap).await;
            }
            StateOp::Save(snapshot) => {
                if let Err(e) = backend.save(&snapshot).await {
                    deliver(inbox, Mail::SaveFailed(e), &nap).await;
                }
            }
        }
    }
}
