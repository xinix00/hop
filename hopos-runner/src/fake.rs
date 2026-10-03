//! Een nep-kern die de echte frames spreekt, voor host-tests van Hop als bewoner.
//!
//! [`FakeDial`] is een `applib::sys::Dial`: de verbinding die hij geeft leest
//! elk `Call`-frame met de decoders van `abi` (framekop, `hopabi::Req`,
//! [`StartReq`]) en antwoordt met een `Result`-frame zoals `kern::system`
//! doet. Zo loopt een test door de échte `sys::Client` (framing, volgnummers,
//! foutstatus met tekst) en toetst hij de bytes, niet een nagebootste API.
//!
//! Dit is testgereedschap: de staat is gedeeld tussen de test en de
//! verbinding (`Rc<RefCell<..>>`), wat in productiecode een eigenaar zonder
//! naam zou zijn (handboek §1.2). Daarom alleen met de feature `fake` of in
//! de eigen tests van deze crate.

#![cfg(any(test, feature = "fake"))]

use alloc::collections::{BTreeMap, VecDeque};
use alloc::rc::Rc;
use alloc::string::String;
use alloc::vec::Vec;
use core::cell::RefCell;
use core::future::Future;
use core::future::poll_fn;
use core::task::Poll;
use core::time::Duration;

use abi::hopabi::{self, Req, Resp};
use abi::systemapi::{
    self, HEADER_LEN, Kind, PrivOp, SlotInfo, StartReq, StreamResp, StreamState, store,
};
use applib::sys::{self, ConnError};

/// De foutstatus van de kern.
const STATUS_ERROR: u16 = hopabi::STATUS_ERROR;

/// Eén slot in de nep-kern.
#[derive(Clone, Debug, Default)]
pub struct FakeSlot {
    /// De jobnaam uit de start.
    pub job: String,
    /// De env-blob uit de start.
    pub env: Vec<u8>,
    /// De gepubliceerde poorten uit de start.
    pub ports: Vec<u16>,
    /// De volumes uit de start, `(lokaal, gedeeld)` zoals de kern ze leest.
    pub mounts: Vec<(String, String)>,
    /// De partitiemaat.
    pub memory_limit: u64,
    /// De aangekondigde image-maat.
    pub size: u64,
    /// Wat er binnenkwam.
    pub image: Vec<u8>,
    /// Na de laatste byte geplaatst en gestart.
    pub placed: bool,
    /// Logregels die de app "schreef".
    pub logs: VecDeque<Vec<u8>>,
    /// De app viel om (voor status-tests).
    pub crashed: bool,
}

/// De staat van de nep-kern.
#[derive(Debug, Default)]
pub struct KernState {
    /// De bezette slots, op nummer.
    pub slots: BTreeMap<u64, FakeSlot>,
    /// Slots die de kern zelf beantwoordt, met een vaste stand: de kern in
    /// slot 0 en Hop in zijn eigen slot. Een start slaat ze over.
    pub system: BTreeMap<u64, SlotInfo>,
    /// Hoeveel slots er tegelijk passen; daarboven "no free run".
    pub max_slots: usize,
    /// Elke op die binnenkwam, in volgorde.
    pub ops: Vec<u8>,
    /// De laatst gezette klok.
    pub clock: u64,
    /// Bestanden op de nep-hopfs.
    pub files: BTreeMap<String, Vec<u8>>,
    /// Hoeveel verbindingen er geopend zijn.
    pub dials: u32,
    /// Elk binnengekomen frame, rauw (kop plus payload).
    pub frames: Vec<Vec<u8>>,
    /// Elke lees geeft eerst één keer `Pending` (en wekt zichzelf), zoals
    /// een echte verbinding waar het antwoord nog onderweg is. Zo toetst een
    /// test dat een wachtende call de core teruggeeft.
    pub yield_reads: bool,
    /// Hoe vaak een lees de core teruggaf.
    pub yields: u64,
    /// Store-calls van apps die op Hop wachten (`NEXT_STORE`).
    pub store_queue: VecDeque<FakeStore>,
    /// Store-calls die Hop heeft, op ticket.
    pub store_taken: BTreeMap<u64, FakeStore>,
    /// Afgemelde store-calls: (ticket, status, maat, namen of tekst).
    pub store_done: Vec<(u64, u16, u64, Vec<u8>)>,
    /// Het volgende ticket.
    pub next_ticket: u64,
    /// Geen schijf: elke gewone bestandscall krijgt `STATUS_ERROR` met "no
    /// storage layer on board", zoals `kern::system` zonder hopfs (de Pi's
    /// zonder NVMe).
    pub no_storage: bool,
    /// De FLIP lukt: de kern neemt de bundel aan en geeft het slot terug,
    /// zoals `kern::system` vóór de sprong. Zonder: een weigering.
    pub flip_ok: bool,
}

/// Eén store-call van een app in de nep-kern. De bestanden van de app zijn
/// de bestanden van de nep-hopfs ([`KernState::files`]), op pad.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FakeStore {
    /// Het slot van de app.
    pub slot: u32,
    /// De op van de app (`OP_STORE_*`).
    pub op: u8,
    /// De jobnaam van het slot.
    pub job: String,
    /// De genormaliseerde objectnaam (`/a/b`).
    pub key: String,
    /// Het lokale pad.
    pub path: String,
}

/// De nep-kern: deelbaar tussen de test en de verbindingen.
#[derive(Clone, Debug)]
pub struct FakeKern(pub Rc<RefCell<KernState>>);

impl FakeKern {
    /// Een kern met plaats voor `max_slots` slots.
    pub fn new(max_slots: usize) -> Self {
        Self(Rc::new(RefCell::new(KernState {
            max_slots,
            ..KernState::default()
        })))
    }

    /// Zet een store-call van een app in de rij; geeft zijn ticket.
    pub fn queue_store(&self, call: FakeStore) -> u64 {
        let mut k = self.0.borrow_mut();
        k.next_ticket += 1;
        let t = k.next_ticket;
        k.store_queue.push_back(call);
        t
    }

    /// Een dialer naar deze kern.
    pub fn dial(&self) -> FakeDial {
        FakeDial(self.clone())
    }

    /// Een `sys::Client` over deze kern, met een timer die nooit afgaat.
    pub fn client(&self) -> sys::Client<FakeDial, NeverTimer> {
        sys::Client::new(self.dial(), NeverTimer)
    }
}

/// Opent verbindingen naar een [`FakeKern`].
#[derive(Debug)]
pub struct FakeDial(FakeKern);

impl sys::Dial for FakeDial {
    type Conn = FakeConn;

    async fn dial(&mut self) -> Result<FakeConn, ConnError> {
        self.0.0.borrow_mut().dials += 1;
        Ok(FakeConn {
            kern: self.0.clone(),
            tx: Vec::new(),
            rx: VecDeque::new(),
        })
    }
}

/// Een timer die nooit afgaat: de nep-kern antwoordt altijd meteen.
#[derive(Debug)]
pub struct NeverTimer;

impl sys::Timer for NeverTimer {
    fn sleep(&self, _: Duration) -> impl Future<Output = ()> {
        core::future::pending()
    }
}

/// Eén verbinding met de nep-kern.
#[derive(Debug)]
pub struct FakeConn {
    kern: FakeKern,
    tx: Vec<u8>,
    rx: VecDeque<u8>,
}

impl sys::Conn for FakeConn {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, ConnError> {
        let wait = self.kern.0.borrow().yield_reads;
        if wait {
            self.kern.0.borrow_mut().yields += 1;
            let mut once = false;
            poll_fn(|cx| {
                if once {
                    return Poll::Ready(());
                }
                once = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            })
            .await;
        }
        // Leeg is EOF: in deze nep komt een antwoord altijd tegelijk met de
        // laatste byte van de call, dus wachten zou een test laten hangen.
        let n = buf.len().min(self.rx.len());
        for (slot, b) in buf.iter_mut().zip(self.rx.drain(..n)) {
            *slot = b;
        }
        Ok(n)
    }

    async fn write(&mut self, buf: &[u8]) -> Result<usize, ConnError> {
        self.tx.extend_from_slice(buf);
        self.pump();
        Ok(buf.len())
    }
}

impl FakeConn {
    /// Verwerkt elk compleet frame in `tx`.
    fn pump(&mut self) {
        loop {
            let Some(head) = self
                .tx
                .get(..HEADER_LEN)
                .and_then(|h| <[u8; HEADER_LEN]>::try_from(h).ok())
            else {
                return;
            };
            let Ok(h) = systemapi::decode_header(&head) else {
                self.tx.clear();
                return;
            };
            let end = HEADER_LEN + h.len;
            if self.tx.len() < end {
                return;
            }
            let frame: Vec<u8> = self.tx.drain(..end).collect();
            self.kern.0.borrow_mut().frames.push(frame.clone());
            if h.kind != Kind::Call {
                continue;
            }
            let payload = frame.get(HEADER_LEN..).unwrap_or_default();
            let out = match hopabi::decode_req(payload) {
                Ok(req) => self.kern.handle(&req),
                Err(_) => answer(0, 0, STATUS_ERROR, 0, b"bad request"),
            };
            self.rx.extend(out);
        }
    }
}

/// Een `Result`-frame.
fn answer(op: u8, seq: u32, status: u16, size: u64, data: &[u8]) -> Vec<u8> {
    let mut p = alloc::vec![0u8; hopabi::HDR_LEN + data.len()];
    let resp = Resp {
        op,
        status,
        seq,
        size,
        data,
    };
    let n = hopabi::encode_resp(&mut p, &resp).unwrap_or(0);
    p.truncate(n);
    let mut f = systemapi::encode_header(Kind::Result, p.len())
        .map(|h| h.to_vec())
        .unwrap_or_default();
    f.extend_from_slice(&p);
    f
}

/// De volumes van een start, gelezen met de decoder van abi.
fn fake_mounts(s: &StartReq<'_>) -> Result<Vec<(String, String)>, abi::Error> {
    systemapi::Mounts::new(s.mounts)
        .map(|m| {
            m.map(|m| {
                (
                    String::from_utf8_lossy(m.local).into_owned(),
                    String::from_utf8_lossy(m.shared).into_owned(),
                )
            })
        })
        .collect()
}

impl FakeKern {
    /// Beantwoordt één call zoals `kern::system` hem zou beantwoorden.
    fn handle(&self, req: &Req<'_>) -> Vec<u8> {
        let mut k = self.0.borrow_mut();
        k.ops.push(req.op);
        let ok = |size: u64, data: &[u8]| answer(req.op, req.seq, hopabi::STATUS_OK, size, data);
        let err = |msg: &str| answer(req.op, req.seq, STATUS_ERROR, 0, msg.as_bytes());
        let path = core::str::from_utf8(req.path).unwrap_or("");
        match PrivOp::from_op(req.op) {
            Some(PrivOp::StartSlot) => {
                let Ok(s) = StartReq::decode(req) else {
                    return err("bad start request");
                };
                if s.image_size == 0 {
                    return err("image size 0");
                }
                if k.slots.len() >= k.max_slots {
                    return err("no free run of 1 app core(s)");
                }
                let mut slot = 1;
                while k.slots.contains_key(&slot) || k.system.contains_key(&slot) {
                    slot += 1;
                }
                let Ok(mounts) = fake_mounts(&s) else {
                    return err("bad start mounts");
                };
                let fresh = FakeSlot {
                    job: String::from_utf8_lossy(s.job).into_owned(),
                    env: s.env.to_vec(),
                    ports: s.ports().collect(),
                    mounts,
                    memory_limit: s.memory_limit,
                    size: s.image_size,
                    ..FakeSlot::default()
                };
                k.slots.insert(slot, fresh);
                ok(slot, b"")
            }
            Some(PrivOp::StreamImage) => {
                let Some(s) = k.slots.get_mut(&req.off).filter(|s| !s.placed) else {
                    return err("no open stream on that slot");
                };
                if req.n != s.image.len() as u64 {
                    return err("offset mismatch");
                }
                if s.image.len() as u64 + req.data.len() as u64 > s.size {
                    k.slots.remove(&req.off);
                    return err("more bytes than announced");
                }
                s.image.extend_from_slice(req.data);
                let received = s.image.len() as u64;
                let (state, why): (StreamState, &[u8]) = if received < s.size {
                    (StreamState::More, b"")
                } else if s.image.starts_with(b"\x7fELF") {
                    s.placed = true;
                    (StreamState::Placed, b"")
                } else {
                    (StreamState::Failed, b"not an ELF image")
                };
                if state == StreamState::Failed {
                    k.slots.remove(&req.off);
                }
                let mut d = [0u8; 64];
                let r = StreamResp {
                    received,
                    state,
                    why,
                };
                let n = r.encode_data(&mut d).unwrap_or(0);
                ok(received, d.get(..n).unwrap_or_default())
            }
            Some(PrivOp::StopSlot) => match k.slots.remove(&req.off) {
                Some(_) => ok(0, b""),
                None => err("no such slot"),
            },
            Some(PrivOp::SlotStatus) => {
                if let Some(info) = k.system.get(&req.off) {
                    return ok(0, &info.encode());
                }
                let info = match k.slots.get(&req.off) {
                    Some(s) => SlotInfo {
                        state: if s.placed { 2 } else { 1 },
                        core_on: u8::from(s.placed && !s.crashed),
                        app: if s.crashed {
                            3
                        } else if s.placed {
                            2
                        } else {
                            0
                        },
                        exit_code: u64::from(s.crashed),
                        heartbeat: 7,
                        partition: s.memory_limit,
                        received: s.image.len() as u64,
                        image_size: s.size,
                        ..SlotInfo::default()
                    },
                    None => SlotInfo::default(),
                };
                ok(0, &info.encode())
            }
            Some(PrivOp::NextLog) => {
                let line = k.slots.get_mut(&req.off).and_then(|s| s.logs.pop_front());
                match line {
                    Some(mut l) => {
                        l.truncate(usize::try_from(req.n).unwrap_or(usize::MAX));
                        ok(1, &l)
                    }
                    None => ok(0, b""),
                }
            }
            Some(PrivOp::SetClock) => {
                k.clock = req.n;
                ok(0, b"")
            }
            Some(PrivOp::Flip) if k.flip_ok && k.slots.remove(&req.off).is_some() => ok(0, b""),
            Some(PrivOp::Flip) => err("flip not in the fake kernel"),
            Some(PrivOp::NextStore) => {
                let Some(c) = k.store_queue.pop_front() else {
                    return ok(0, b"");
                };
                let ticket = k.next_ticket - k.store_queue.len() as u64;
                let t = store::StoreTask {
                    ticket,
                    slot: c.slot,
                    op: c.op,
                    job: c.job.as_bytes(),
                    key: c.key.as_bytes(),
                    path: c.path.as_bytes(),
                };
                let mut d = alloc::vec![0u8; t.len()];
                let n = t.encode(&mut d).unwrap_or(0);
                k.store_taken.insert(ticket, c);
                ok(1, d.get(..n).unwrap_or_default())
            }
            Some(PrivOp::StoreRead | PrivOp::StoreWrite) => {
                let Some(c) = k.store_taken.get(&req.off).cloned() else {
                    return answer(
                        req.op,
                        req.seq,
                        hopabi::STATUS_NO_ENT,
                        0,
                        b"store call gone",
                    );
                };
                if c.path != path {
                    return answer(req.op, req.seq, hopabi::STATUS_DENIED, 0, b"not the path");
                }
                let at = usize::try_from(req.n).unwrap_or(usize::MAX);
                if req.op == PrivOp::StoreRead.op() {
                    let max = store::decode_read_len(req.data).unwrap_or(0) as usize;
                    let Some(f) = k.files.get(path) else {
                        return answer(req.op, req.seq, hopabi::STATUS_NO_ENT, 0, b"no such file");
                    };
                    let start = at.min(f.len());
                    let end = start.saturating_add(max).min(f.len());
                    return ok(f.len() as u64, f.get(start..end).unwrap_or_default());
                }
                let f = k.files.entry(String::from(path)).or_default();
                f.resize(at.min(f.len()), 0);
                if f.len() < at {
                    f.resize(at, 0);
                }
                f.extend_from_slice(req.data);
                ok(req.data.len() as u64, b"")
            }
            Some(PrivOp::StoreDone) => {
                if k.store_taken.remove(&req.off).is_none() {
                    return answer(
                        req.op,
                        req.seq,
                        hopabi::STATUS_NO_ENT,
                        0,
                        b"store call gone",
                    );
                }
                let Ok((h, rest)) = store::DoneHead::decode(req.data) else {
                    return err("bad done head");
                };
                k.store_done.push((req.off, h.status, req.n, rest.to_vec()));
                ok(0, b"")
            }
            None if k.no_storage => err("no storage layer on board"),
            None => match req.op {
                hopabi::OP_TRUNCATE => {
                    let f = k.files.entry(String::from(path)).or_default();
                    f.truncate(usize::try_from(req.n).unwrap_or(usize::MAX));
                    ok(0, b"")
                }
                hopabi::OP_WRITE => {
                    let f = k.files.entry(String::from(path)).or_default();
                    let at = usize::try_from(req.off).unwrap_or(usize::MAX);
                    if f.len() < at {
                        f.resize(at, 0);
                    }
                    f.truncate(at);
                    f.extend_from_slice(req.data);
                    ok(req.data.len() as u64, b"")
                }
                hopabi::OP_STAT => match k.files.get(path) {
                    Some(f) => ok(f.len() as u64, b""),
                    None => answer(req.op, req.seq, hopabi::STATUS_NO_ENT, 0, b"no such file"),
                },
                hopabi::OP_READ => match k.files.get(path) {
                    Some(f) => {
                        let at = usize::try_from(req.off).unwrap_or(usize::MAX).min(f.len());
                        let n = usize::try_from(req.n).unwrap_or(usize::MAX);
                        let end = at.saturating_add(n).min(f.len());
                        ok(0, f.get(at..end).unwrap_or_default())
                    }
                    None => answer(req.op, req.seq, hopabi::STATUS_NO_ENT, 0, b"no such file"),
                },
                _ => err("unknown op"),
            },
        }
    }
}
