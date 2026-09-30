//! De object-store van de apps: Hop's kant van de store-ops (Go: `slots/storage.go` en `cmd/hopos/store.go`).
//!
//! Een app kopieert op afroep tussen zijn eigen map in de bucket en zijn
//! hopfs-zicht (`applib::store`). De kern heeft geen S3, geen sleutels en
//! geen TLS, dus zet hij de call in een rij; deze module haalt hem op
//! (`NEXT_STORE`), doet de S3-kant, verplaatst de bytes met `STORE_READ` en
//! `STORE_WRITE` (het bestand van het slot van de app, door de mount-tabel
//! van dát slot) en meldt af met `STORE_DONE`.
//!
//! De toegangsgrens op de bucket is de prefix: `apps/<cluster>/<job>/`,
//! naast de `leases/<cluster>` en `state/<cluster>` van de clusterstaat.
//! De jobnaam komt van de kern (het slot van de app, niet wat de app zegt),
//! de objectnaam heeft de kern al getoetst (geen `..`, niet leeg), en de
//! cluster kent alleen Hop. Een app kiest dus alleen een naam binnen zijn
//! eigen map.
//!
//! Wat hij bezit: de [`Service`] (de bucket, of geen) en per opdracht één
//! doorloop. De S3-kant zelf zit achter [`Bucket`]: in de bewoner leans3
//! over leanhttps ([`bucket::S3Bucket`]), in een test een map in RAM.

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use core::fmt::Write as _;
use core::future::Future;

use runner::{StoreOp, StoreStatus, StoreTask, SysError, SystemApi};

pub mod bucket;

/// Hoe lang `NEXT_STORE` op een opdracht wacht: net onder de grens van de
/// kern (`abi::systemapi::store::MAX_WAIT_MS`, 5 s), zodat een lange wacht
/// nooit de call-timeout van de client (10 s) raakt.
pub const POLL_MS: u64 = 4_000;

/// De happen waarin een bestand tussen de kern en S3 gaat: groot genoeg om
/// de calls te amortiseren, klein genoeg voor de heap van Hop (één buffer
/// per transfer).
pub const CHUNK: usize = 256 << 10;

/// Zoveel keys haalt een list hoogstens op (Go: `storeListMax`): het
/// antwoord aan de app draagt toch maar 8 KiB namen, en een onbegrensde
/// listing liet de heap van Hop groeien met wat een app in zijn map zet.
pub const LIST_CAP: usize = 4096;

/// De grootste lijst namen in één antwoord aan de app (8 KiB, de grens van
/// de kern).
pub const LIST_MAX: usize = 8 << 10;

/// De zin voor een app op een node zonder object-store.
pub const NO_STORE: &str = "no object store on this node (boot with hopos.s3.*)";

/// De S3-kant van één bucket, met VOLLEDIGE keys (de prefix bouwt de
/// [`Service`]). Pull en push stromen rechtstreeks tussen de bucket en het
/// bestand van de opdracht, via `sys`.
pub trait Bucket {
    /// Haalt object `key` naar het bestand van `task` (vervangend); de maat,
    /// of `None` als het object niet bestaat (dan is het bestand
    /// onaangeraakt).
    fn pull<S: SystemApi>(
        &mut self,
        key: &str,
        sys: &mut S,
        task: &StoreTask,
    ) -> impl Future<Output = Result<Option<u64>, String>>;

    /// Uploadt `size` bytes uit het bestand van `task` als object `key`;
    /// `sha256` is de hex-hash van precies die bytes (de handtekening dekt
    /// de payload, bewust geen streaming-handtekening).
    fn push<S: SystemApi>(
        &mut self,
        key: &str,
        sys: &mut S,
        task: &StoreTask,
        size: u64,
        sha256: &str,
    ) -> impl Future<Output = Result<(), String>>;

    /// De keys onder `prefix`, hoogstens `max`; `true` als er meer waren.
    fn list(
        &mut self,
        prefix: &str,
        max: usize,
    ) -> impl Future<Output = Result<(Vec<String>, bool), String>>;

    /// Verwijdert `key`; een object dat er niet is, is geen fout.
    fn delete(&mut self, key: &str) -> impl Future<Output = Result<(), String>>;
}

/// De store-dienst van één node: de bucket (of geen) en de naamruimte.
pub struct Service<B> {
    bucket: Option<B>,
    /// `apps/<cluster>`, zonder slash achteraan.
    base: String,
}

/// Wat een doorloop deed, voor de logregel van de bewoner.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Outcome {
    /// De opdracht.
    pub task: StoreTask,
    /// De uitkomst voor de app.
    pub status: StoreStatus,
    /// De tekst bij een fout (leeg bij succes).
    pub why: String,
    /// Of de kern de afmelding aannam (anders was de app al weg).
    pub delivered: bool,
}

impl<B: Bucket> Service<B> {
    /// Een dienst voor `cluster`; zonder bucket weigert hij elke opdracht.
    pub fn new(bucket: Option<B>, cluster: &str) -> Self {
        Self {
            bucket,
            base: format!("apps/{}", cluster.trim_end_matches('/')),
        }
    }

    /// Heeft deze node een bucket?
    pub fn has_bucket(&self) -> bool {
        self.bucket.is_some()
    }

    /// Haalt één opdracht op (met een lange wacht) en handelt hem af. `None`
    /// als er niets kwam; een fout van de kern (geen verbinding, een kern
    /// zonder rij) gaat naar de aanroeper, die even wacht.
    pub async fn serve_one<S: SystemApi>(
        &mut self,
        sys: &mut S,
    ) -> Result<Option<Outcome>, SysError> {
        let Some(task) = sys.next_store(POLL_MS).await? else {
            return Ok(None);
        };
        Ok(Some(self.handle(sys, task).await))
    }

    /// Eén opdracht: de S3-kant, dan de afmelding.
    pub async fn handle<S: SystemApi>(&mut self, sys: &mut S, task: StoreTask) -> Outcome {
        let (status, size, payload) = match self.run(sys, &task).await {
            Ok((size, names)) => (StoreStatus::Ok, size, names),
            Err((status, why)) => (status, 0, why.into_bytes()),
        };
        let delivered = sys
            .store_done(task.ticket, status, size, &payload)
            .await
            .is_ok();
        let why = if status == StoreStatus::Ok {
            String::new()
        } else {
            String::from_utf8_lossy(&payload).into_owned()
        };
        Outcome {
            task,
            status,
            why,
            delivered,
        }
    }

    /// De S3-kant van één opdracht: (maat, namen) of (status, reden).
    async fn run<S: SystemApi>(
        &mut self,
        sys: &mut S,
        task: &StoreTask,
    ) -> Result<(u64, Vec<u8>), (StoreStatus, String)> {
        let err = |why: String| (StoreStatus::Error, why);
        let Some(bucket) = self.bucket.as_mut() else {
            return Err(err(String::from(NO_STORE)));
        };
        // De kern toetste de job al; dit is de tweede lijn (Go: `storeGate`).
        if task.job.is_empty() || task.job.contains(['/', '\\']) || task.job == ".." {
            return Err(err(format!(
                "job name {:?} cannot form a store namespace",
                task.job
            )));
        }
        let own = format!("{}/{}/", self.base, task.job);
        let key = format!("{}/{}{}", self.base, task.job, task.key);
        match task.op {
            StoreOp::Pull => match bucket.pull(&key, sys, task).await.map_err(err)? {
                Some(n) => Ok((n, Vec::new())),
                None => Err((
                    StoreStatus::NotFound,
                    format!("no such object: {}", task.key),
                )),
            },
            StoreOp::Push => {
                let (size, sha) = hash_file(sys, task).await.map_err(err)?;
                bucket
                    .push(&key, sys, task, size, &sha)
                    .await
                    .map_err(err)?;
                Ok((size, Vec::new()))
            }
            StoreOp::List => {
                let prefix = if task.key == "/" { own.clone() } else { key };
                let (keys, truncated) = bucket.list(&prefix, LIST_CAP).await.map_err(err)?;
                if truncated {
                    // Stilletjes inkorten leest als "dit is alles" (Go).
                    return Err(err(format!(
                        "list {}: too many objects for one answer; use a narrower prefix",
                        task.key
                    )));
                }
                names(&keys, &own, &task.key).map_err(err)
            }
            StoreOp::Drop => {
                bucket.delete(&key).await.map_err(err)?;
                Ok((0, Vec::new()))
            }
        }
    }
}

/// De namen relatief aan de eigen map, `\n`-gescheiden, zodat een naam uit
/// de lijst rechtstreeks naar een pull kan; begrensd op [`LIST_MAX`].
fn names(keys: &[String], own: &str, prefix: &str) -> Result<(u64, Vec<u8>), String> {
    let mut out = String::new();
    for (i, k) in keys.iter().enumerate() {
        let name = k.strip_prefix(own).unwrap_or(k);
        if out.len() + name.len() + usize::from(i > 0) > LIST_MAX {
            return Err(format!(
                "list {prefix}: more than {LIST_MAX} bytes of names; use a narrower prefix"
            ));
        }
        if i > 0 {
            out.push('\n');
        }
        let _ = out.write_str(name);
    }
    Ok((keys.len() as u64, out.into_bytes()))
}

/// De eerste pass van een push (Go: `hashFile`): de maat en de sha256 over
/// precies die bytes, in happen van [`CHUNK`]. De bron is lokale opslag, dus
/// twee keer lezen is goedkoop, en de handtekening moet de hash vooraf
/// kennen. Een bestand dat onder de hash kromp is een fout; een bestand dat
/// daarna nog verandert, weigert S3 (de hash klopt dan niet).
async fn hash_file<S: SystemApi>(sys: &mut S, task: &StoreTask) -> Result<(u64, String), String> {
    let (size, _) = sys
        .store_read(task, 0, &mut [])
        .await
        .map_err(|e| format!("stat {}: {e}", task.path))?;
    let mut buf = Vec::new();
    let want = usize::try_from(size).unwrap_or(usize::MAX).min(CHUNK);
    buf.try_reserve_exact(want)
        .map_err(|_| String::from("hash buffer: out of memory"))?;
    buf.resize(want, 0);
    let mut h = auth::Sha256::new();
    let mut off = 0u64;
    while off < size {
        let left = usize::try_from(size - off)
            .unwrap_or(usize::MAX)
            .min(buf.len());
        let dst = buf.get_mut(..left).unwrap_or_default();
        let (_, n) = sys
            .store_read(task, off, dst)
            .await
            .map_err(|e| format!("read {}: {e}", task.path))?;
        if n == 0 {
            return Err(format!(
                "{} shrank during hashing ({off} of {size} bytes)",
                task.path
            ));
        }
        h.update(dst.get(..n).unwrap_or_default());
        off += n as u64;
    }
    let mut hex = String::new();
    for b in h.finish() {
        let _ = write!(hex, "{b:02x}");
    }
    Ok((size, hex))
}

#[cfg(test)]
mod tests;
