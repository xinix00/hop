//! De kern-flip vanuit Hop: `POST /flip` met een URL en een sha256 (achter
//! dezelfde HMAC als een jobspec, `api::NodeApi`) wordt bundel ophalen, hem
//! rauw de kern in stromen, en `PrivOp::FLIP`.
//!
//! De weg (HopOS `hopos/src/flip.rs` is de kernkant):
//!
//! 1. START_SLOT met de jobnaam [`FLIP_BUNDLE_JOB`]: de kern reserveert een
//!    partitie en neemt de bytes daarna RAUW aan, zonder ELF-plaatsing en
//!    zonder start;
//! 2. de download (dezelfde [`Images`] als een artifact) stroomt erin via
//!    STREAM_IMAGE; de kern zegt "placed" als de laatste byte binnen is;
//! 3. FLIP (0x46) met het slot en de som als hex: de kern toetst de som en
//!    de bundel, legt de nieuwe kern klaar, antwoordt, en springt een halve
//!    seconde later. Hop draait door: zijn kooi, zijn geheugen en zijn
//!    luisterende poorten overleven de wissel; de verbinding met de kern
//!    niet, en die bouwt de system-client zelf opnieuw op.
//!
//! Het antwoord op `POST /flip` is de 202 van de api pas als de kern de
//! bundel aannam: een weigering (verkeerde som, geen bundel, geen plek) komt
//! als 502 met de reden terug, want daarna is er niets meer om naar te
//! kijken.
//!
//! De KOUDE flip (`"cold": true`, 29-09) is dezelfde weg met één vlag in
//! `n` van de FLIP ([`FLIP_COLD`]) en één stap ertussen: na de download en
//! vóór de FLIP stopt Hop zijn eigen taken op deze node (`node.rs`), zodat
//! de kern alleen nog stopt wat Hop niet kende. De kern springt dan zonder
//! bewoners over te dragen en start Hop koud; de jobs staan in de
//! agent-staat op hopfs en worden daarna opnieuw geplaatst. Weigert de kern
//! (een bundel die ook koud niet kan), dan plaatst de agent ze gewoon weer.

use alloc::format;
use alloc::string::String;
use core::future::Future;

use hopos_runner::{Call, KernSys};
use runner::{Slot, StartSpec, Streamed, SystemApi};

use crate::node::{Images, Sink};

/// De jobnaam waarmee de kern een slot voor een flip-bundel reserveert
/// (`kern::system::FLIP_BUNDLE_JOB` in HopOS). Hier als tekst tot de
/// getagde HopOS hem in `abi` draagt.
pub const FLIP_BUNDLE_JOB: &str = "hopos.flip-bundle";

/// `abi::systemapi::PrivOp::Flip`.
const OP_FLIP: u8 = 0x46;

/// De koude vlag in `n` van de FLIP (`abi::systemapi::FLIP_COLD` in HopOS,
/// na alpha.9; hier als getal tot de getagde HopOS hem draagt). Een kern
/// van vóór de vlag weigert hem niet maar flipt warm, en dan weigert hij
/// een bundel met een andere switch-code alsnog vóór de sprong.
pub const FLIP_COLD: u64 = 1;

/// De ABI-staart van een partitie (`abi::layout::ABI_TAIL`, 2 MiB): de
/// bundel moet eronder passen.
const TAIL: u64 = 2 << 20;

/// Hoe lang de FLIP-call mag duren: de kern hasht en legt de nieuwe kern
/// neer voor hij antwoordt (op QEMU ruim onder een seconde).
const FLIP_TIMEOUT: core::time::Duration = core::time::Duration::from_secs(20);

/// De flip-call van de kern, naast de gewone [`SystemApi`].
pub trait KernFlip {
    /// `PrivOp::FLIP` op `slot` met de verwachte som als 64 hex-tekens;
    /// `cold` zet [`FLIP_COLD`].
    fn flip(
        &mut self,
        slot: Slot,
        sha256_hex: &str,
        cold: bool,
    ) -> impl Future<Output = Result<(), String>>;
}

impl<C: Call> KernFlip for KernSys<C> {
    async fn flip(&mut self, slot: Slot, sha256_hex: &str, cold: bool) -> Result<(), String> {
        let req = applib::sys::Req {
            op: OP_FLIP,
            seq: 0,
            off: u64::from(slot.0),
            n: if cold { FLIP_COLD } else { 0 },
            path: sha256_hex,
            data: &[],
        };
        let mut reason = [0u8; 256];
        self.conn_mut()
            .call(req, &mut reason, FLIP_TIMEOUT)
            .await
            .map(|_| ())
            .map_err(|e| format!("kernel refused the flip: {e}"))
    }
}

/// De download als stroom in het gereserveerde slot.
struct Bundle<'a, S> {
    sys: &'a mut S,
    slot: Option<Slot>,
    done: bool,
}

impl<S: SystemApi> Sink for Bundle<'_, S> {
    async fn begin(&mut self, size: u64) -> Result<(), String> {
        let mem = size.saturating_add(TAIL).next_multiple_of(2 << 20);
        let spec = StartSpec {
            image_size: size,
            mem_limit: mem,
            core_class: String::new(),
            cores: 1,
            sharegroup: String::new(),
            pool_cores: 0,
            env: Default::default(),
            mounts: Default::default(),
            ports: Default::default(),
            job: String::from(FLIP_BUNDLE_JOB),
        };
        let slot = self
            .sys
            .start_slot(&spec)
            .await
            .map_err(|e| format!("reserve a slot for the bundle: {e:?}"))?;
        self.slot = Some(slot);
        Ok(())
    }

    async fn chunk(&mut self, bytes: &[u8]) -> Result<(), String> {
        let slot = self.slot.ok_or_else(|| String::from("no bundle slot"))?;
        match self.sys.stream_image(slot, bytes).await {
            Ok(Streamed::More) => Ok(()),
            Ok(Streamed::Placed) => {
                self.done = true;
                Ok(())
            }
            Ok(Streamed::Failed(e)) | Err(e) => Err(format!("stream the bundle: {e:?}")),
        }
    }
}

/// Haalt de bundel op en stroomt hem de kern in: het gereserveerde slot
/// met de complete bundel. Een fout ruimt het slot op.
pub async fn fetch<S, I>(sys: &mut S, images: &mut I, url: &str) -> Result<Slot, String>
where
    S: SystemApi,
    I: Images,
{
    let mut sink = Bundle {
        sys: &mut *sys,
        slot: None,
        done: false,
    };
    let fetched = images.fetch(url, &mut sink).await;
    let (slot, done) = (sink.slot, sink.done);
    let r = match (fetched, slot, done) {
        (Ok(()), Some(slot), true) => return Ok(slot),
        (Ok(()), _, _) => Err(format!("download {url}: the bundle ended early")),
        (Err(e), _, _) => Err(e),
    };
    if let Some(slot) = slot {
        // Een afgebroken stroom ruimen we hier op; een stop op een leeg
        // slot is geen fout.
        let _ = sys.stop_slot(slot, 0).await;
    }
    r
}

/// Vraagt de flip van de bundel in `slot` ([`fetch`]). `Ok` is "de kern
/// nam hem aan en springt zo"; een fout ruimt het slot op.
pub async fn ask<S>(sys: &mut S, slot: Slot, sha256: &str, cold: bool) -> Result<(), String>
where
    S: SystemApi + KernFlip,
{
    let r = sys.flip(slot, sha256, cold).await;
    if r.is_err() {
        // De kern geeft een geweigerde bundel zelf terug; een FLIP die hem
        // nooit bereikte, laat het slot staan. Een stop op een leeg slot
        // is geen fout.
        let _ = sys.stop_slot(slot, 0).await;
    }
    r
}

/// Haalt de bundel op, stroomt hem de kern in en vraagt de flip: [`fetch`]
/// en dan [`ask`], zonder stap ertussen (de warme flip).
pub async fn flip<S, I>(
    sys: &mut S,
    images: &mut I,
    url: &str,
    sha256: &str,
    cold: bool,
) -> Result<(), String>
where
    S: SystemApi + KernFlip,
    I: Images,
{
    let slot = fetch(sys, images, url).await?;
    ask(sys, slot, sha256, cold).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::{Images, Sink};
    use alloc::vec::Vec;
    use core::pin::pin;
    use core::task::{Context, Poll, Waker};
    use hopos_runner::fake::FakeKern;

    fn block_on<F: Future>(f: F) -> F::Output {
        let mut f = pin!(f);
        let mut cx = Context::from_waker(Waker::noop());
        for _ in 0..100_000 {
            if let Poll::Ready(v) = f.as_mut().poll(&mut cx) {
                return v;
            }
        }
        panic!("future bleef hangen");
    }

    /// Een download van `bytes`, waarvan hoogstens `send` echt aankomen.
    struct OneFile {
        bytes: Vec<u8>,
        send: usize,
    }

    impl Images for OneFile {
        async fn fetch<K: Sink>(&mut self, _url: &str, sink: &mut K) -> Result<(), String> {
            sink.begin(self.bytes.len() as u64).await?;
            for c in self.bytes.get(..self.send).unwrap_or_default().chunks(7) {
                sink.chunk(c).await?;
            }
            Ok(())
        }
    }

    /// Een bundel begint met de kern-ELF.
    fn bundle() -> Vec<u8> {
        let mut b = b"\x7fELF".to_vec();
        b.resize(100, 0xAB);
        b
    }

    #[test]
    fn the_bundle_streams_into_a_reserved_slot_and_the_flip_is_asked() {
        let k = FakeKern::new(4);
        let mut sys = KernSys::new(k.client(), 4);
        let mut img = OneFile {
            bytes: bundle(),
            send: 100,
        };
        let sha = "ab".repeat(32);
        let r = block_on(flip(
            &mut sys,
            &mut img,
            "http://10.0.2.2/b.flip",
            &sha,
            false,
        ));
        // De nep-kern kent geen flip: de weigering komt terug als tekst, en
        // het gereserveerde slot is opgeruimd.
        let e = r.unwrap_err();
        assert!(e.contains("kernel refused the flip"), "{e}");
        let st = k.0.borrow();
        assert!(st.slots.is_empty(), "bundle slot left behind");
        let start = abi::systemapi::PrivOp::StartSlot.op();
        let stream = abi::systemapi::PrivOp::StreamImage.op();
        let flip = abi::systemapi::PrivOp::Flip.op();
        assert_eq!(OP_FLIP, flip);
        assert_eq!(st.ops.first(), Some(&start));
        assert!(st.ops.contains(&stream));
        assert!(st.ops.contains(&flip), "no FLIP op: {:?}", st.ops);
    }

    #[test]
    fn a_cold_flip_fetches_first_and_asks_with_the_same_slot() {
        let k = FakeKern::new(4);
        let mut sys = KernSys::new(k.client(), 4);
        let mut img = OneFile {
            bytes: bundle(),
            send: 100,
        };
        // Eerst de download (daar stopt node.rs de taken), dan de vraag.
        let slot = block_on(fetch(&mut sys, &mut img, "http://10.0.2.2/b.flip")).unwrap();
        assert!(
            k.0.borrow().slots.contains_key(&u64::from(slot.0)),
            "the bundle slot is kept"
        );
        let e = block_on(ask(&mut sys, slot, &"cd".repeat(32), true)).unwrap_err();
        assert!(e.contains("kernel refused the flip"), "{e}");
        let st = k.0.borrow();
        assert!(st.slots.is_empty(), "bundle slot left behind");
        assert!(st.ops.contains(&abi::systemapi::PrivOp::Flip.op()));
        assert_eq!(FLIP_COLD, 1);
    }

    #[test]
    fn a_short_download_never_asks_the_flip() {
        let k = FakeKern::new(4);
        let mut sys = KernSys::new(k.client(), 4);
        let mut img = OneFile {
            bytes: bundle(),
            send: 50,
        };
        let r = block_on(flip(
            &mut sys,
            &mut img,
            "http://10.0.2.2/b.flip",
            &"00".repeat(32),
            false,
        ));
        assert!(r.unwrap_err().contains("ended early"));
        let st = k.0.borrow();
        assert!(st.slots.is_empty(), "bundle slot left behind");
        assert!(!st.ops.contains(&abi::systemapi::PrivOp::Flip.op()));
    }
}
