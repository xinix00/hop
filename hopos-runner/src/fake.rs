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
use core::pin::pin;
use core::task::{Context, Poll, Waker};
use core::time::Duration;

use abi::hopabi::{self, Req, Resp};
use abi::systemapi::{self, HEADER_LEN, Kind, PrivOp, SlotInfo, StartReq, StreamResp, StreamState};
use applib::sys::{self, ConnError};

use crate::Block;

/// De foutstatus van de kern.
const STATUS_ERROR: u16 = hopabi::STATUS_ERROR;

/// Eén slot in de nep-kern.
#[derive(Clone, Debug, Default)]
pub struct FakeSlot {
    /// De jobnaam uit de start.
    pub job: String,
    /// De env-blob uit de start.
    pub env: Vec<u8>,
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
                while k.slots.contains_key(&slot) {
                    slot += 1;
                }
                let fresh = FakeSlot {
                    job: String::from_utf8_lossy(s.job).into_owned(),
                    env: s.env.to_vec(),
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
            Some(PrivOp::Flip) => err("flip not in the fake kernel"),
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

/// Wacht door te pollen: voor verbindingen die altijd meteen klaar zijn.
///
/// Zonder grens: een future die nooit klaar komt, laat de test hangen in
/// plaats van hem met een paniek te beëindigen (bibliotheekcode panikeert
/// niet). De nep-kern hierboven antwoordt altijd binnen één poll.
#[derive(Clone, Copy, Debug, Default)]
pub struct Spin;

impl Block for Spin {
    fn block_on<F: Future>(&mut self, f: F) -> F::Output {
        let mut f = pin!(f);
        let mut cx = Context::from_waker(Waker::noop());
        loop {
            if let Poll::Ready(v) = f.as_mut().poll(&mut cx) {
                return v;
            }
        }
    }
}
