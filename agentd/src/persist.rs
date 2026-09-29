//! De opslag van de gecommitte clusterstaat op een eigen thread.
//!
//! Bezit de [`store::StateStore`] (S3, hoplockserver of een bestand). De
//! leader schrijft elke gedebouncede snapshot weg, en op een trage opslag
//! kost één PUT seconden (Bunny: 4 tot 20 s, 08-09-2026); de eigenaar
//! stuurt de bytes en werkt door. Alleen het laden bij het leider worden
//! wacht de eigenaar af: zonder de staat weet een verse leider niet wat er
//! moet draaien.

use std::sync::mpsc::{self, Sender, SyncSender};
use std::time::Duration;

use store::StateStore;

use crate::msg::Msg;

/// Een opdracht aan de opslag-thread.
#[derive(Debug)]
pub(crate) enum PersistOp {
    /// Lees de snapshot; het antwoord gaat over het meegegeven kanaal.
    Load(SyncSender<Result<Option<Vec<u8>>, String>>),
    /// Schrijf deze snapshot.
    Save(Vec<u8>),
}

/// Start de opslag-thread; hij bezit `st` tot het kanaal dichtgaat.
pub(crate) fn spawn(
    mut st: Box<dyn StateStore + Send>,
    owner: Sender<Msg>,
) -> std::io::Result<Sender<PersistOp>> {
    let (tx, rx) = mpsc::channel::<PersistOp>();
    std::thread::Builder::new()
        .name(String::from("persist"))
        .spawn(move || {
            for op in rx {
                match op {
                    PersistOp::Load(reply) => {
                        let _ = reply.send(st.load().map_err(|e| e.to_string()));
                    }
                    PersistOp::Save(bytes) => {
                        if let Err(e) = st.save(&bytes) {
                            let msg = Msg::SnapshotFailed(format!("{}: {e}", st.describe()));
                            if owner.send(msg).is_err() {
                                return;
                            }
                        }
                    }
                }
            }
        })?;
    Ok(tx)
}

/// Leest de snapshot via de opslag-thread en wacht hoogstens `wait`.
pub(crate) fn load(ops: &Sender<PersistOp>, wait: Duration) -> Result<Option<Vec<u8>>, String> {
    let (tx, rx) = mpsc::sync_channel(1);
    ops.send(PersistOp::Load(tx))
        .map_err(|_| String::from("state store thread is gone"))?;
    rx.recv_timeout(wait)
        .map_err(|_| format!("state store did not answer within {} s", wait.as_secs()))?
}
