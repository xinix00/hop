//! De `runner::SystemApi` van Hop over de system-client van applib: elke bevoegde op als frame naar de kern.
//!
//! Op HopOS is Hop de eerste bewoner (PORT.md §6, beslissing 1). De runner
//! ([`runner::HopRunner`]) is sans-I/O en roept de kern via de trait
//! [`runner::SystemApi`]; deze crate is de rand die daar bytes van maakt: een
//! `Call`-frame met een `hopabi::Req` en de juiste opcode uit
//! [`abi::systemapi::PrivOp`] (0x40 tot en met 0x45), en het antwoord terug
//! naar de typen van de runner ([`abi::systemapi::StreamResp`],
//! [`abi::systemapi::SlotInfo`], de logregel).
//!
//! Deze crate bezit [`KernSys`]: de verbinding met de kern (een [`Call`],
//! in de bewoner `applib::sys::Client`), de offset van elke lopende
//! image-stroom en de laatst bekende stand van elk slot. Hij bezit niet de
//! beslissing wélke kooi wat draait: dat is de runner.
//!
//! # Asynchroon, zonder geneste rondes
//!
//! `SystemApi` is asynchroon, net als de verbinding (TCP over leannet, met
//! een pomp-taak op dezelfde executor): elke op wacht met `.await` op
//! [`Call::call`], en de eigenaar-taak geeft zo bij elke call de core terug.
//! Er is geen naad meer die een future synchroon afdwingt: dat was een
//! geneste executor-ronde binnen de poll van een taak, en de executor roept
//! zichzelf nooit aan (handboek §4). Een host-test pollt de nep-verbinding,
//! die altijd klaar is.
//!
//! Ook hier: [`KernSys`] is een [`agent::Store`] over hopfs, zodat
//! `Action::SaveState` de agent-staat in het eigen bestand van Hop zet.

#![cfg_attr(not(test), no_std)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )
)]
#![forbid(unsafe_code)]

extern crate alloc;

pub mod fake;

use alloc::collections::BTreeMap;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::future::Future;
use core::time::Duration;

use abi::hopabi;
use abi::systemapi::{self, CoreClass, PrivOp, SlotInfo, StartReq, StreamResp, StreamState};
use applib::sys;

mod start_mounts;

use runner::{Slot, SlotApp, SlotState, SlotStatus, StartSpec, Streamed, SysError, SystemApi};

/// Het pad van de agent-staat in de eigen map van Hop op hopfs.
pub const STATE_PATH: &str = "/hop/agent-state.json";

/// Hoe groot een foutreden van de kern bij een stroom mag zijn; langer
/// wordt een protocolfout (de client weigert een te lang antwoord).
const REASON_MAX: usize = 512;

/// Eén call naar de kern: `applib::sys::Client::call`, of een nep in een test.
pub trait Call {
    /// Stuurt `req` en leest de data van het antwoord in `dst`.
    fn call(
        &mut self,
        req: sys::Req<'_>,
        dst: &mut [u8],
        timeout: Duration,
    ) -> impl Future<Output = sys::Result<(sys::Resp, usize)>>;
}

impl<D: sys::Dial, T: sys::Timer> Call for sys::Client<D, T> {
    fn call(
        &mut self,
        req: sys::Req<'_>,
        dst: &mut [u8],
        timeout: Duration,
    ) -> impl Future<Output = sys::Result<(sys::Resp, usize)>> {
        sys::Client::call(self, req, dst, timeout)
    }
}

/// De bevoegde system-API van de kern, over een [`Call`].
///
/// # Invariants
///
/// `offsets` heeft een sleutel precies voor de slots waarvan deze kant een
/// stroom opende en die nog niet geplaatst, mislukt of gestopt zijn; de
/// waarde is het aantal bytes dat de kern al bevestigde.
#[derive(Debug)]
pub struct KernSys<C> {
    call: C,
    cores: u32,
    offsets: BTreeMap<Slot, u64>,
    last: BTreeMap<Slot, SlotStatus>,
}

impl<C: Call> KernSys<C> {
    /// Een system-API over `call`; `cores` zijn de app-cores die de kern Hop biedt.
    pub fn new(call: C, cores: u32) -> Self {
        Self {
            call,
            cores,
            offsets: BTreeMap::new(),
            last: BTreeMap::new(),
        }
    }

    /// De verbinding (tests en diagnose).
    pub fn conn(&self) -> &C {
        &self.call
    }

    /// De verbinding, muteerbaar (tests).
    pub fn conn_mut(&mut self) -> &mut C {
        &mut self.call
    }

    /// Eén call; wie wacht, geeft de core terug.
    async fn exchange(
        &mut self,
        req: sys::Req<'_>,
        dst: &mut [u8],
    ) -> sys::Result<(sys::Resp, usize)> {
        self.call.call(req, dst, sys::RPC_TIMEOUT).await
    }

    /// Een request met alleen getallen (stop, status, log, klok).
    async fn plain(
        &mut self,
        op: PrivOp,
        slot: u64,
        n: u64,
        dst: &mut [u8],
    ) -> sys::Result<(sys::Resp, usize)> {
        let r = systemapi::plain_req(op, 0, slot, n);
        self.exchange(to_sys(&r, ""), dst).await
    }
}

/// Zet een `hopabi::Req` om naar de vorm van de client (pad als tekst).
fn to_sys<'a>(r: &hopabi::Req<'a>, path: &'a str) -> sys::Req<'a> {
    sys::Req {
        op: r.op,
        seq: 0,
        off: r.off,
        n: r.n,
        path,
        data: r.data,
    }
}

/// De env van een app als blob: `key=val\n`, zoals de control-page hem draagt.
pub fn env_blob(env: &BTreeMap<String, String>) -> Vec<u8> {
    let mut out = Vec::new();
    for (k, v) in env {
        out.extend_from_slice(k.as_bytes());
        out.push(b'=');
        out.extend_from_slice(v.as_bytes());
        out.push(b'\n');
    }
    out
}

/// De poorten van een jobspec, elk één keer, in de volgorde van de namen
/// (twee namen op één nummer zijn één publicatie). De draadvorm is die van
/// `abi::systemapi::port_blob`: de kern zet elke poort van de uplink door
/// naar dezelfde poort in het slot (tcp en udp) en trekt ze bij de stop
/// weer in.
pub fn unique_ports<'a>(ports: impl IntoIterator<Item = &'a u16>) -> Vec<u16> {
    let mut out: Vec<u16> = Vec::new();
    for &p in ports {
        if !out.contains(&p) {
            out.push(p);
        }
    }
    out
}

/// De core-klasse van een jobtag.
fn core_class(s: &str) -> Result<CoreClass, SysError> {
    match s {
        "" => Ok(CoreClass::Any),
        "small" => Ok(CoreClass::Small),
        "mid" => Ok(CoreClass::Mid),
        "big" => Ok(CoreClass::Big),
        other => Err(SysError::Refused(alloc::format!(
            "unknown core class {other:?} (small, mid, big)"
        ))),
    }
}

/// Een fout van de client als fout van de kern.
///
/// De kern zegt "vol" alleen in tekst: `no free run of N app core(s)` (geen
/// core), `... full` (geen stroomplek) of een partitie die niet past. Dat is
/// een plaatsingsfout (de taak hoort op een andere node), geen crash; alles
/// anders is een weigering.
pub fn sys_error(e: &sys::Error) -> SysError {
    match e {
        sys::Error::Call { msg, .. } => {
            let text = msg.as_str();
            let full = [
                "no free",
                "full",
                "no region",
                "does not fit",
                "out of memory",
            ];
            if full.iter().any(|w| text.contains(w)) {
                SysError::NoCapacity(text.to_string())
            } else {
                SysError::Refused(text.to_string())
            }
        }
        other => SysError::Refused(alloc::format!("{other}")),
    }
}

/// De app-toestand van de control-page als [`SlotApp`].
fn slot_app(raw: u64) -> SlotApp {
    match hopabi::AppStatus::from_raw(raw) {
        Some(hopabi::AppStatus::Ready) => SlotApp::Ready,
        Some(hopabi::AppStatus::Exited) => SlotApp::Exited,
        Some(hopabi::AppStatus::Booting | hopabi::AppStatus::Staged) => SlotApp::Booting,
        Some(hopabi::AppStatus::Empty) | None => SlotApp::Empty,
    }
}

/// Een [`SlotInfo`] van de draad als [`SlotStatus`] van de runner.
pub fn slot_status_of(info: &SlotInfo) -> SlotStatus {
    let state = match info.slot_state() {
        Some(systemapi::SlotState::Streaming) => SlotState::Streaming,
        Some(systemapi::SlotState::Running) => SlotState::Running,
        Some(systemapi::SlotState::Quarantined) => SlotState::Quarantined,
        Some(systemapi::SlotState::Empty) | None => SlotState::Empty,
    };
    SlotStatus {
        state,
        core_on: info.core_on != 0,
        app: slot_app(info.app),
        exit_code: info.exit_code,
        heartbeat: info.heartbeat,
        mem_sys: 0,
        cpu_pct: None,
        fault_vec: info.fault_vec,
        fault_esr: info.fault_esr,
        fault_far: info.fault_far,
        cage: String::new(),
    }
}

impl<C: Call> SystemApi for KernSys<C> {
    fn num_cores(&self) -> u32 {
        self.cores
    }

    async fn start_slot(&mut self, spec: &StartSpec) -> Result<Slot, SysError> {
        let mounts = start_mounts::blob(spec)?;
        let env = env_blob(&spec.env);
        let too_big =
            |what: &str| SysError::Refused(alloc::format!("{what} does not fit the start request"));
        let ports = unique_ports(spec.ports.values());
        let mut port_bytes = [0u8; systemapi::MAX_START_PORTS * systemapi::PORT_LEN];
        let port_len = systemapi::port_blob(&ports, &mut port_bytes).map_err(|e| {
            SysError::Refused(alloc::format!(
                "ports {ports:?}: {e:?} (at most {} per job, none of them 0)",
                systemapi::MAX_START_PORTS
            ))
        })?;
        let mut req = StartReq {
            memory_limit: spec.mem_limit,
            image_size: spec.image_size,
            cores: u16::try_from(spec.cores).map_err(|_| too_big("cores"))?,
            pool_cores: u16::try_from(spec.pool_cores).map_err(|_| too_big("pool cores"))?,
            core_class: core_class(&spec.core_class)?,
            group: spec.sharegroup.as_bytes(),
            env: &env,
            ports: port_bytes.get(..port_len).unwrap_or_default(),
            job: spec.job.as_bytes(),
            ..Default::default()
        };
        start_mounts::attach(&mut req, &mounts);
        // De helper van abi schrijft de hele payload (kop, jobnaam, StartHead,
        // groep, env, poorten); de client schrijft kop en pad zelf, dus hier
        // alleen het deel erachter. Zo is er één plek die de StartHead-bytes
        // kent.
        let skip = hopabi::HDR_LEN + spec.job.len();
        let mut buf = alloc::vec![
            0u8;
            skip + systemapi::START_HEAD_LEN + req.group.len() + env.len() + port_len + mounts.len()
        ];
        let n = req
            .encode(&mut buf, 0)
            .map_err(|e| SysError::Refused(alloc::format!("start request: {e:?}")))?;
        let data = buf.get(skip..n).unwrap_or_default().to_vec();
        let call = sys::Req {
            op: PrivOp::StartSlot.op(),
            seq: 0,
            off: 0,
            n: 0,
            path: &spec.job,
            data: &data,
        };
        let (resp, _) = self
            .exchange(call, &mut [])
            .await
            .map_err(|e| sys_error(&e))?;
        let slot = u32::try_from(resp.size)
            .ok()
            .filter(|&s| s >= 1)
            .ok_or_else(|| {
                SysError::Refused(alloc::format!("kernel answered slot {}", resp.size))
            })?;
        self.offsets.insert(Slot(slot), 0);
        self.last.remove(&Slot(slot));
        Ok(Slot(slot))
    }

    async fn stream_image(&mut self, slot: Slot, chunk: &[u8]) -> Result<Streamed, SysError> {
        let Some(&start) = self.offsets.get(&slot) else {
            return Err(SysError::Refused(alloc::format!(
                "no open stream on slot {}",
                slot.0
            )));
        };
        let mut off = start;
        let mut last = Streamed::More;
        // De kern neemt hoogstens één I/O-hap per call; een grotere brok van
        // de download gaat in happen, en alleen de laatste mag Placed zeggen.
        for piece in chunk.chunks(sys::MAX_CHUNK) {
            if last != Streamed::More {
                self.offsets.remove(&slot);
                return Err(SysError::Refused(String::from(
                    "kernel placed before the last byte",
                )));
            }
            let mut reason = [0u8; REASON_MAX];
            let r = systemapi::stream_req(0, u64::from(slot.0), off, piece);
            let (resp, n) = match self.exchange(to_sys(&r, ""), &mut reason).await {
                Ok(v) => v,
                Err(e) => {
                    // Een geweigerde brok: de kern brak de stroom af.
                    self.offsets.remove(&slot);
                    return Err(sys_error(&e));
                }
            };
            let wire = hopabi::Resp {
                op: resp.op,
                status: resp.status,
                seq: resp.seq,
                size: resp.size,
                data: reason.get(..n).unwrap_or_default(),
            };
            let Ok(sr) = StreamResp::decode(&wire) else {
                self.offsets.remove(&slot);
                return Err(SysError::Refused(String::from("malformed stream answer")));
            };
            off = sr.received;
            last = match sr.state {
                StreamState::More => Streamed::More,
                StreamState::Placed => Streamed::Placed,
                StreamState::Failed => {
                    let why = String::from_utf8_lossy(sr.why).into_owned();
                    Streamed::Failed(SysError::Refused(why))
                }
            };
        }
        if last == Streamed::More {
            self.offsets.insert(slot, off);
        } else {
            self.offsets.remove(&slot);
        }
        Ok(last)
    }

    async fn stop_slot(&mut self, slot: Slot, timeout_ms: u64) -> Result<(), SysError> {
        self.offsets.remove(&slot);
        match self
            .plain(PrivOp::StopSlot, u64::from(slot.0), timeout_ms, &mut [])
            .await
        {
            Ok(_) => {
                self.last.remove(&slot);
                Ok(())
            }
            Err(_) => Err(SysError::NotConfirmed),
        }
    }

    async fn slot_status(&mut self, slot: Slot) -> SlotStatus {
        let mut info = [0u8; systemapi::SLOT_INFO_LEN];
        let got = self
            .plain(PrivOp::SlotStatus, u64::from(slot.0), 0, &mut info)
            .await
            .ok()
            .and_then(|(_, n)| SlotInfo::decode(info.get(..n).unwrap_or_default()).ok());
        match got {
            Some(i) => {
                let s = slot_status_of(&i);
                self.last.insert(slot, s.clone());
                s
            }
            // Zonder antwoord de laatst bekende stand: een verbinding die even
            // wegvalt (een kern-flip) is geen crash van elke app. Wie nooit een
            // stand zag, krijgt een leeg slot.
            None => self
                .last
                .get(&slot)
                .cloned()
                .unwrap_or_else(SlotStatus::empty),
        }
    }

    async fn next_log_line(&mut self, slot: Slot, buf: &mut [u8]) -> Option<usize> {
        let max = u64::try_from(buf.len()).unwrap_or(u64::MAX);
        match self
            .plain(PrivOp::NextLog, u64::from(slot.0), max, buf)
            .await
        {
            Ok((resp, n)) if resp.size == 1 => Some(n),
            _ => None,
        }
    }

    async fn set_clock(&mut self, unix_ns: u64) -> Result<(), SysError> {
        self.plain(PrivOp::SetClock, 0, unix_ns, &mut [])
            .await
            .map(|_| ())
            .map_err(|e| sys_error(&e))
    }
}

impl<C: Call> agent::Store for KernSys<C> {
    async fn save(&mut self, blob: &[u8]) -> Result<(), agent::StoreError> {
        // Eerst op nul, dan de happen: een halve schrijf is dan een kort
        // bestand, geen mengsel van oud en nieuw (zoals `write_file` van applib).
        let trunc = sys::Req {
            n: 0,
            ..sys::Req::path(hopabi::OP_TRUNCATE, STATE_PATH)
        };
        self.exchange(trunc, &mut [])
            .await
            .map_err(|_| agent::StoreError::Io)?;
        let mut off = 0u64;
        for piece in blob.chunks(sys::MAX_CHUNK) {
            let w = sys::Req {
                off,
                data: piece,
                ..sys::Req::path(hopabi::OP_WRITE, STATE_PATH)
            };
            self.exchange(w, &mut [])
                .await
                .map_err(|_| agent::StoreError::Io)?;
            off = off.saturating_add(u64::try_from(piece.len()).unwrap_or(u64::MAX));
        }
        Ok(())
    }

    async fn load(&mut self) -> Result<Option<Vec<u8>>, agent::StoreError> {
        let size = match self
            .exchange(sys::Req::path(hopabi::OP_STAT, STATE_PATH), &mut [])
            .await
        {
            Ok((r, _)) => r.size,
            Err(sys::Error::NotFound { .. }) => return Ok(None),
            Err(_) => return Err(agent::StoreError::Io),
        };
        let len = usize::try_from(size).map_err(|_| agent::StoreError::Io)?;
        let mut out = Vec::new();
        out.try_reserve_exact(len)
            .map_err(|_| agent::StoreError::Io)?;
        out.resize(len, 0);
        let mut at = 0usize;
        while at < len {
            let end = len.min(at.saturating_add(sys::MAX_CHUNK));
            let dst = out.get_mut(at..end).unwrap_or_default();
            let r = sys::Req {
                off: u64::try_from(at).unwrap_or(u64::MAX),
                n: u64::try_from(dst.len()).unwrap_or(u64::MAX),
                ..sys::Req::path(hopabi::OP_READ, STATE_PATH)
            };
            let (_, n) = self
                .exchange(r, dst)
                .await
                .map_err(|_| agent::StoreError::Io)?;
            if n == 0 {
                break;
            }
            at = at.saturating_add(n);
        }
        out.truncate(at);
        Ok(Some(out))
    }
}

#[cfg(test)]
mod tests;
