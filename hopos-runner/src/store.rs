//! De store-ops van de system-API als frames: `NEXT_STORE`, `STORE_READ`,
//! `STORE_WRITE` en `STORE_DONE` (abi `PrivOp` 0x47 tot en met 0x4A, de
//! payloads in `abi::systemapi::store`).
//!
//! De kern zet de store-calls van de apps in een rij; Hop haalt ze hier op
//! en verplaatst de bytes. Het pad in een lees of schrijf is letterlijk het
//! pad van de opdracht: de kern vergelijkt het en resolvet het met de
//! mount-tabel van het slot van de app, niet van Hop.

use alloc::string::String;
use alloc::vec::Vec;

use abi::hopabi;
use abi::systemapi::store::{self as wire, DoneHead};
use abi::systemapi::{self, PrivOp};
use applib::sys;
use runner::{Slot, StoreOp, StoreStatus, StoreTask, SysError};

use crate::{Call, KernSys, sys_error, to_sys};

/// De grootste opdracht op de draad: kop, jobnaam, naam en pad.
const TASK_MAX: usize = wire::TASK_HEAD_LEN + wire::MAX_STORE_JOB + 2 * wire::MAX_STORE_PATH;

/// De op van een app als [`StoreOp`].
fn op_of(op: u8) -> Option<StoreOp> {
    match op {
        hopabi::OP_STORE_PULL => Some(StoreOp::Pull),
        hopabi::OP_STORE_PUSH => Some(StoreOp::Push),
        hopabi::OP_STORE_LIST => Some(StoreOp::List),
        hopabi::OP_STORE_DROP => Some(StoreOp::Drop),
        _ => None,
    }
}

/// De status op de draad.
fn status_of(s: StoreStatus) -> u16 {
    match s {
        StoreStatus::Ok => hopabi::STATUS_OK,
        StoreStatus::Error => hopabi::STATUS_ERROR,
        StoreStatus::NotFound => hopabi::STATUS_NO_ENT,
        StoreStatus::Denied => hopabi::STATUS_DENIED,
    }
}

impl<C: Call> KernSys<C> {
    /// `NEXT_STORE` over de draad.
    pub(crate) async fn wire_next_store(
        &mut self,
        wait_ms: u64,
    ) -> Result<Option<StoreTask>, SysError> {
        let r = systemapi::plain_req(PrivOp::NextStore, 0, 0, wait_ms);
        let mut buf = Vec::new();
        buf.try_reserve_exact(TASK_MAX)
            .map_err(|_| SysError::Refused(String::from("store task buffer: out of memory")))?;
        buf.resize(TASK_MAX, 0);
        let (resp, n) = self
            .exchange(to_sys(&r, ""), &mut buf)
            .await
            .map_err(|e| sys_error(&e))?;
        if resp.size == 0 {
            return Ok(None);
        }
        let t = wire::StoreTask::decode(buf.get(..n).unwrap_or_default())
            .map_err(|e| SysError::Refused(alloc::format!("malformed store task: {e:?}")))?;
        let text = |b: &[u8]| core::str::from_utf8(b).map(String::from);
        let (Some(op), Ok(job), Ok(key), Ok(path)) =
            (op_of(t.op), text(t.job), text(t.key), text(t.path))
        else {
            // Een naam die geen UTF-8 is, kan de client van de kern niet
            // terugsturen (zijn pad is tekst): meteen luid afmelden.
            let why = b"store: object name or path is not UTF-8";
            let _ = self
                .wire_store_done(t.ticket, StoreStatus::Denied, 0, why)
                .await;
            return Ok(None);
        };
        Ok(Some(StoreTask {
            ticket: t.ticket,
            slot: Slot(t.slot),
            op,
            job,
            key,
            path,
        }))
    }

    /// `STORE_READ` over de draad: (maat van het bestand, gelezen bytes).
    pub(crate) async fn wire_store_read(
        &mut self,
        task: &StoreTask,
        off: u64,
        dst: &mut [u8],
    ) -> Result<(u64, usize), SysError> {
        let max = wire::read_len(u64::try_from(dst.len()).unwrap_or(u64::MAX));
        let req = sys::Req {
            op: PrivOp::StoreRead.op(),
            seq: 0,
            off: task.ticket,
            n: off,
            path: &task.path,
            data: &max,
        };
        let (resp, n) = self.exchange(req, dst).await.map_err(|e| sys_error(&e))?;
        Ok((resp.size, n))
    }

    /// `STORE_WRITE` over de draad.
    pub(crate) async fn wire_store_write(
        &mut self,
        task: &StoreTask,
        off: u64,
        data: &[u8],
    ) -> Result<(), SysError> {
        let req = sys::Req {
            op: PrivOp::StoreWrite.op(),
            seq: 0,
            off: task.ticket,
            n: off,
            path: &task.path,
            data,
        };
        self.exchange(req, &mut [])
            .await
            .map(|_| ())
            .map_err(|e| sys_error(&e))
    }

    /// `STORE_DONE` over de draad.
    pub(crate) async fn wire_store_done(
        &mut self,
        ticket: u64,
        status: StoreStatus,
        size: u64,
        payload: &[u8],
    ) -> Result<(), SysError> {
        let head = DoneHead {
            status: status_of(status),
            reserved: [0; 6],
        }
        .encode();
        let mut data = Vec::new();
        data.try_reserve_exact(head.len() + payload.len())
            .map_err(|_| SysError::Refused(String::from("store done: out of memory")))?;
        data.extend_from_slice(&head);
        data.extend_from_slice(payload);
        let req = sys::Req {
            op: PrivOp::StoreDone.op(),
            seq: 0,
            off: ticket,
            n: size,
            path: "",
            data: &data,
        };
        self.exchange(req, &mut [])
            .await
            .map(|_| ())
            .map_err(|e| sys_error(&e))
    }
}
