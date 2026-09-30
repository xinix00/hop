//! De store-dienst tegen de nep-kern (de echte frames) en een bucket in RAM.

use super::*;
use alloc::collections::BTreeMap;
use hopos_runner::KernSys;
use hopos_runner::fake::{FakeKern, FakeStore};

fn bl<F: Future>(f: F) -> F::Output {
    let mut f = core::pin::pin!(f);
    let mut cx = core::task::Context::from_waker(core::task::Waker::noop());
    for _ in 0..1000 {
        if let core::task::Poll::Ready(v) = f.as_mut().poll(&mut cx) {
            return v;
        }
    }
    panic!("de nep-kern wacht nooit");
}

/// Een bucket in RAM die de bytes via de kern verplaatst, zoals de echte.
#[derive(Default)]
struct RamBucket {
    objects: BTreeMap<String, Vec<u8>>,
    /// Laat een list afgekapt lijken.
    truncate: bool,
}

impl Bucket for RamBucket {
    async fn pull<S: SystemApi>(
        &mut self,
        key: &str,
        sys: &mut S,
        task: &StoreTask,
    ) -> Result<Option<u64>, String> {
        let Some(b) = self.objects.get(key) else {
            return Ok(None);
        };
        sys.store_write(task, 0, b)
            .await
            .map_err(|e| format!("{e}"))?;
        Ok(Some(b.len() as u64))
    }

    async fn push<S: SystemApi>(
        &mut self,
        key: &str,
        sys: &mut S,
        task: &StoreTask,
        size: u64,
        sha256: &str,
    ) -> Result<(), String> {
        let mut b = alloc::vec![0u8; size as usize];
        let (_, n) = sys
            .store_read(task, 0, &mut b)
            .await
            .map_err(|e| format!("{e}"))?;
        b.truncate(n);
        let mut hex = String::new();
        for x in auth::sha256(&b) {
            let _ = write!(hex, "{x:02x}");
        }
        if hex != sha256 {
            return Err(String::from("BadDigest"));
        }
        self.objects.insert(String::from(key), b);
        Ok(())
    }

    async fn list(&mut self, prefix: &str, max: usize) -> Result<(Vec<String>, bool), String> {
        let keys: Vec<String> = self
            .objects
            .keys()
            .filter(|k| k.starts_with(prefix))
            .take(max)
            .cloned()
            .collect();
        Ok((keys, self.truncate))
    }

    async fn delete(&mut self, key: &str) -> Result<(), String> {
        self.objects.remove(key);
        Ok(())
    }
}

fn call(op: u8, key: &str, path: &str) -> FakeStore {
    FakeStore {
        slot: 2,
        op,
        job: String::from("demo"),
        key: String::from(key),
        path: String::from(path),
    }
}

/// De toets van `store_demo.go` van Hop's kant: push onder de prefix van de
/// cluster en de job van het slot, list relatief aan de eigen map, pull naar
/// een ander pad, een pull van niets, drop, en list leeg.
#[test]
fn the_service_runs_the_store_demo_under_the_cluster_prefix() {
    use abi::hopabi::{OP_STORE_DROP, OP_STORE_LIST, OP_STORE_PULL, OP_STORE_PUSH};
    let k = FakeKern::new(4);
    let mut sys = KernSys::new(k.client(), 4);
    let mut svc = Service::new(Some(RamBucket::default()), "hopos");
    assert_eq!(bl(svc.serve_one(&mut sys)).unwrap(), None);
    k.0.borrow_mut()
        .files
        .insert(String::from("/data/state.json"), b"leven 1".to_vec());
    k.queue_store(call(OP_STORE_PUSH, "/data/state.json", "/data/state.json"));
    let o = bl(svc.serve_one(&mut sys)).unwrap().unwrap();
    assert_eq!(
        (o.status, o.delivered),
        (StoreStatus::Ok, true),
        "{}",
        o.why
    );
    let obj = &svc.bucket.as_ref().unwrap().objects;
    assert_eq!(obj["apps/hopos/demo/data/state.json"], b"leven 1");
    k.queue_store(call(OP_STORE_LIST, "/", ""));
    bl(svc.serve_one(&mut sys)).unwrap().unwrap();
    k.queue_store(call(OP_STORE_PULL, "/data/state.json", "/copy.json"));
    bl(svc.serve_one(&mut sys)).unwrap().unwrap();
    k.queue_store(call(OP_STORE_PULL, "/never.json", "/never.json"));
    let o = bl(svc.serve_one(&mut sys)).unwrap().unwrap();
    assert_eq!(o.status, StoreStatus::NotFound);
    k.queue_store(call(OP_STORE_DROP, "/data/state.json", "/data/state.json"));
    bl(svc.serve_one(&mut sys)).unwrap().unwrap();
    k.queue_store(call(OP_STORE_LIST, "/", ""));
    bl(svc.serve_one(&mut sys)).unwrap().unwrap();
    let k = k.0.borrow();
    assert_eq!(k.files["/copy.json"], b"leven 1");
    let done: Vec<(u16, u64, &[u8])> = k
        .store_done
        .iter()
        .map(|(_, s, n, d)| (*s, *n, d.as_slice()))
        .collect();
    assert_eq!(
        done,
        [
            (abi::hopabi::STATUS_OK, 7, &b""[..]),
            (abi::hopabi::STATUS_OK, 1, b"data/state.json"),
            (abi::hopabi::STATUS_OK, 7, b""),
            (
                abi::hopabi::STATUS_NO_ENT,
                0,
                b"no such object: /never.json"
            ),
            (abi::hopabi::STATUS_OK, 0, b""),
            (abi::hopabi::STATUS_OK, 0, b""),
        ]
    );
}

/// Zonder bucket, met een afgekapte lijst en met een job die geen
/// naamruimte kan zijn: telkens een luide fout voor de app, nooit een stille.
#[test]
fn refusals_are_loud_for_the_app() {
    use abi::hopabi::{OP_STORE_LIST, OP_STORE_PUSH, STATUS_ERROR};
    let k = FakeKern::new(4);
    let mut sys = KernSys::new(k.client(), 4);
    let mut bare: Service<RamBucket> = Service::new(None, "hopos");
    assert!(!bare.has_bucket());
    k.queue_store(call(OP_STORE_PUSH, "/x", "/x"));
    let o = bl(bare.serve_one(&mut sys)).unwrap().unwrap();
    assert_eq!((o.status, o.why.as_str()), (StoreStatus::Error, NO_STORE));
    let mut svc = Service::new(
        Some(RamBucket {
            truncate: true,
            ..RamBucket::default()
        }),
        "hopos",
    );
    k.queue_store(call(OP_STORE_LIST, "/", ""));
    let o = bl(svc.serve_one(&mut sys)).unwrap().unwrap();
    assert!(o.why.contains("narrower prefix"), "{}", o.why);
    k.queue_store(FakeStore {
        job: String::from("a/b"),
        ..call(OP_STORE_PUSH, "/x", "/x")
    });
    let o = bl(svc.serve_one(&mut sys)).unwrap().unwrap();
    assert!(o.why.contains("namespace"), "{}", o.why);
    // Een push van een bestand dat er niet is: de reden van de kern.
    k.queue_store(call(OP_STORE_PUSH, "/nope", "/nope"));
    let o = bl(svc.serve_one(&mut sys)).unwrap().unwrap();
    assert_eq!(o.status, StoreStatus::Error);
    assert!(k.0.borrow().store_done.iter().all(|d| d.1 == STATUS_ERROR));
}

/// Een naam van meer dan 8 KiB aan lijst is een fout, geen afgekapte lijst.
#[test]
fn names_are_relative_and_bounded() {
    let own = "apps/c/j/";
    let keys = [String::from("apps/c/j/a"), String::from("apps/c/j/b/c")];
    assert_eq!(names(&keys, own, "/").unwrap(), (2, b"a\nb/c".to_vec()));
    let big: Vec<String> = (0..2000)
        .map(|i| alloc::format!("apps/c/j/{i:08}"))
        .collect();
    assert!(names(&big, own, "/").is_err());
}
