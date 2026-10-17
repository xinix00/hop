//! De store op een volume met alleen bestandscalls: hopfs op HopOS.
//!
//! Bezit de recordvorm en de sleutel-naar-pad-regel; de calls zelf zijn van
//! de [`Volume`] (op HopOS de system-client van applib, in de tests een map
//! in het geheugen). De kern kent stat, lezen, schrijven, truncaten, lijsten
//! en verwijderen, maar geen rename en geen fsync: een schrijf is "op nul,
//! dan de happen" (`applib::sys::Client::write_file`), en de duurzaamheid is
//! de commit van hopfs (elke 10 s en meteen na de stop van een slot, zie
//! `kern::rpc::COMMIT_EVERY`). Een stroomuitval midden in een schrijf kan
//! dus een half record achterlaten.
//!
//! Daarom draagt elk record een kop met de lengte en de SHA-256 van de body
//! ([`HEADER`]): een half of verminkt record wordt herkend en nooit als lease
//! uitgeleverd. Het telt als afwezig (een GET geeft 404, een aanmaak met
//! `If-None-Match: *` mag eroverheen, een `If-Match` faalt), en de eigenaar
//! krijgt het te zien in [`VolumeStore::corrupt`] voor zijn logregel. Een
//! verloren lease is voor hoplock een nieuwe verkiezing; een lease met een
//! verkeerde body zou dat niet zijn. De ETag is de opgeslagen SHA-256, dus
//! dezelfde als die van de host-store voor dezelfde body.
//!
//! De vorm op het volume is dus niet die van de host (een kaal bestand per
//! sleutel, zoals Go): een datamap van de host en een volume van HopOS zijn
//! geen uitwisselbare formaten.

use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::fmt;
use core::future::Future;

use crate::etag::Etag;
use crate::key::Key;
use crate::store::{Condition, Error, Object, Result, Store, check_delete, check_put};

/// De magie van een record, versie 1.
pub const MAGIC: [u8; 4] = *b"HLK1";

/// De kop van een record: magie (4), lengte van de body (u32 LE, 4) en de
/// SHA-256 van de body (32).
pub const HEADER: usize = 40;

/// De grootste lijst die [`VolumeStore::count_keys`] per map leest.
pub const LIST_BUF: usize = 64 << 10;

/// Hoe diep [`VolumeStore::count_keys`] afdaalt; dieper telt niet mee.
pub const MAX_DEPTH: usize = 32;

/// Een fout van het volume, als tekst van de onderliggende laag.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VolumeError(pub String);

impl fmt::Display for VolumeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// De bestandscalls die de store nodig heeft.
pub trait Volume {
    /// De grootste lees per call ([`Volume::read_at`]).
    const CHUNK: usize;

    /// De grootte van `path`; `None` als hij niet bestaat (een map is 0).
    fn size(
        &mut self,
        path: &str,
    ) -> impl Future<Output = core::result::Result<Option<u64>, VolumeError>>;

    /// Leest hooguit `dst.len()` (≤ [`Volume::CHUNK`]) bytes vanaf `off`; 0 is het einde.
    fn read_at(
        &mut self,
        path: &str,
        off: u64,
        dst: &mut [u8],
    ) -> impl Future<Output = core::result::Result<usize, VolumeError>>;

    /// Vervangt de inhoud van `path` door `data`; ouders worden gemaakt.
    fn replace(
        &mut self,
        path: &str,
        data: &[u8],
    ) -> impl Future<Output = core::result::Result<(), VolumeError>>;

    /// Verwijdert `path`; `false` als hij er niet was.
    fn remove(
        &mut self,
        path: &str,
    ) -> impl Future<Output = core::result::Result<bool, VolumeError>>;

    /// De namen in map `path`, `\n`-gescheiden in `dst` (een map eindigt op
    /// `/`); het aantal bytes, of `None` als de map er niet is.
    fn list(
        &mut self,
        path: &str,
        dst: &mut [u8],
    ) -> impl Future<Output = core::result::Result<Option<usize>, VolumeError>>;
}

/// Wat er onder een sleutel op het volume staat.
enum Stored {
    /// Niets.
    Missing,
    /// Een record dat niet klopt (kort, verkeerde magie, verkeerde som).
    Corrupt,
    /// Een geldig record.
    Valid(Object),
}

/// De store op een [`Volume`], onder de map `root`.
#[derive(Debug)]
pub struct VolumeStore<V> {
    vol: V,
    root: String,
    corrupt: u64,
    last_corrupt: Option<Key>,
}

impl<V: Volume> VolumeStore<V> {
    /// Een store op `vol` onder `root` (bijvoorbeeld `/data`).
    pub fn new(vol: V, root: &str) -> VolumeStore<V> {
        let root = root.trim_end_matches('/');
        VolumeStore {
            vol,
            root: root.to_string(),
            corrupt: 0,
            last_corrupt: None,
        }
    }

    /// De map van de store op het volume.
    #[must_use]
    pub fn root(&self) -> &str {
        if self.root.is_empty() {
            "/"
        } else {
            &self.root
        }
    }

    /// Hoeveel verminkte records er sinds de start gezien zijn.
    #[must_use]
    pub fn corrupt(&self) -> u64 {
        self.corrupt
    }

    /// De sleutel van het laatst geziene verminkte record, één keer.
    pub fn take_corrupt(&mut self) -> Option<Key> {
        self.last_corrupt.take()
    }

    /// Het volume, voor de eigenaar.
    pub fn volume_mut(&mut self) -> &mut V {
        &mut self.vol
    }

    /// Het pad van `key` op het volume.
    fn path(&self, key: &Key) -> String {
        format!("{}/{}", self.root, key.as_str())
    }

    /// Telt de sleutels onder de map: elk bestand in de boom is er één.
    pub async fn count_keys(&mut self) -> core::result::Result<u64, VolumeError> {
        let mut buf = Vec::new();
        buf.try_reserve_exact(LIST_BUF)
            .map_err(|_| VolumeError(String::from("out of memory for the listing")))?;
        buf.resize(LIST_BUF, 0);
        let mut dirs: Vec<(String, usize)> = Vec::new();
        dirs.push((String::from(self.root()), 0));
        let mut keys = 0u64;
        while let Some((dir, depth)) = dirs.pop() {
            let Some(n) = self.vol.list(&dir, &mut buf).await? else {
                continue;
            };
            let listing = buf.get(..n).unwrap_or_default();
            for name in names(listing) {
                match name.strip_suffix('/') {
                    Some(sub) if depth + 1 < MAX_DEPTH => {
                        let base = dir.trim_end_matches('/');
                        dirs.push((format!("{base}/{sub}"), depth + 1));
                    }
                    Some(_) => {}
                    None => keys = keys.saturating_add(1),
                }
            }
        }
        Ok(keys)
    }

    /// Leest en toetst het record op `path`.
    async fn load(&mut self, key: &Key, path: &str) -> Result<Stored> {
        let io = |op: &'static str, e: VolumeError| Error::Io {
            op,
            key: key.to_string(),
            why: e.0,
        };
        let Some(size) = self.vol.size(path).await.map_err(|e| io("stat", e))? else {
            return Ok(Stored::Missing);
        };
        let len = usize::try_from(size).unwrap_or(usize::MAX);
        if !(HEADER..=HEADER + crate::MAX_BODY).contains(&len) {
            return Ok(self.mark_corrupt(key));
        }
        let mut buf = Vec::new();
        buf.try_reserve_exact(len)
            .map_err(|_| Error::OutOfMemory { bytes: len })?;
        buf.resize(len, 0);
        let mut at = 0usize;
        while at < len {
            let end = len.min(at.saturating_add(V::CHUNK));
            let dst = buf.get_mut(at..end).unwrap_or_default();
            let off = u64::try_from(at).unwrap_or(u64::MAX);
            let n = self
                .vol
                .read_at(path, off, dst)
                .await
                .map_err(|e| io("read", e))?;
            if n == 0 {
                return Ok(self.mark_corrupt(key));
            }
            at = at.saturating_add(n);
        }
        match open(buf) {
            Some(obj) => Ok(Stored::Valid(obj)),
            None => Ok(self.mark_corrupt(key)),
        }
    }

    fn mark_corrupt(&mut self, key: &Key) -> Stored {
        self.corrupt = self.corrupt.saturating_add(1);
        self.last_corrupt = Some(key.clone());
        Stored::Corrupt
    }
}

/// Toetst een record en geeft het object, of `None` als het niet klopt.
fn open(mut rec: Vec<u8>) -> Option<Object> {
    let (head, body) = rec.split_at_checked(HEADER)?;
    if head.get(..4)? != MAGIC {
        return None;
    }
    let len = u32::from_le_bytes(head.get(4..8)?.try_into().ok()?);
    if usize::try_from(len).ok()? != body.len() {
        return None;
    }
    let sum: [u8; 32] = head.get(8..HEADER)?.try_into().ok()?;
    if auth::sha256(body) != sum {
        return None;
    }
    let etag = Etag::from_digest(&sum);
    rec.drain(..HEADER);
    Some(Object { body: rec, etag })
}

/// Bouwt het record van `body`: kop en body in één buffer.
fn seal(body: &[u8]) -> Result<(Vec<u8>, Etag)> {
    let len = u32::try_from(body.len()).map_err(|_| Error::OutOfMemory { bytes: body.len() })?;
    let sum = auth::sha256(body);
    let mut rec = Vec::new();
    rec.try_reserve_exact(HEADER + body.len())
        .map_err(|_| Error::OutOfMemory {
            bytes: HEADER + body.len(),
        })?;
    rec.extend_from_slice(&MAGIC);
    rec.extend_from_slice(&len.to_le_bytes());
    rec.extend_from_slice(&sum);
    rec.extend_from_slice(body);
    Ok((rec, Etag::from_digest(&sum)))
}

/// De namen van een lijst: `\n`-gescheiden, geen lege.
fn names(b: &[u8]) -> impl Iterator<Item = &str> {
    b.split(|&c| c == b'\n')
        .filter(|n| !n.is_empty())
        .filter_map(|n| core::str::from_utf8(n).ok())
}

impl<V: Volume> Store for VolumeStore<V> {
    async fn get(&mut self, key: &Key) -> Result<Object> {
        let path = self.path(key);
        match self.load(key, &path).await? {
            Stored::Valid(obj) => Ok(obj),
            Stored::Missing | Stored::Corrupt => Err(Error::NotFound),
        }
    }

    async fn put_if(&mut self, key: &Key, body: Vec<u8>, cond: &Condition) -> Result<Etag> {
        let path = self.path(key);
        let current = match self.load(key, &path).await? {
            Stored::Valid(obj) => Some(obj.etag),
            Stored::Missing | Stored::Corrupt => None,
        };
        check_put(current.as_ref(), cond)?;
        let (rec, etag) = seal(&body)?;
        drop(body);
        self.vol.replace(&path, &rec).await.map_err(|e| Error::Io {
            op: "write",
            key: key.to_string(),
            why: e.0,
        })?;
        Ok(etag)
    }

    async fn delete(&mut self, key: &Key, cond: &Condition) -> Result {
        let path = self.path(key);
        match self.load(key, &path).await? {
            Stored::Missing => return Err(Error::NotFound),
            // Een verminkt record heeft geen ETag die een If-Match kan raken;
            // zonder voorwaarde mag het weg.
            Stored::Corrupt if !cond.if_match.is_empty() => return Err(Error::Precondition),
            Stored::Corrupt => {}
            Stored::Valid(obj) => check_delete(&obj.etag, cond)?,
        }
        self.vol.remove(&path).await.map_err(|e| Error::Io {
            op: "remove",
            key: key.to_string(),
            why: e.0,
        })?;
        Ok(())
    }
}

/// Een volume in het geheugen, voor de tests: bestanden als pad naar bytes.
#[cfg(test)]
pub(crate) mod mem {
    use super::*;
    use alloc::collections::BTreeMap;

    /// Zoals hopfs: mappen bestaan impliciet, een map heeft grootte 0.
    #[derive(Debug, Default)]
    pub(crate) struct MemVolume {
        pub(crate) files: BTreeMap<String, Vec<u8>>,
    }

    impl MemVolume {
        fn is_dir(&self, path: &str) -> bool {
            let prefix = format!("{}/", path.trim_end_matches('/'));
            self.files.keys().any(|k| k.starts_with(&prefix))
        }
    }

    impl Volume for MemVolume {
        /// Klein, zodat elke lees in stukken gaat.
        const CHUNK: usize = 7;

        async fn size(&mut self, path: &str) -> core::result::Result<Option<u64>, VolumeError> {
            match self.files.get(path) {
                Some(f) => Ok(Some(f.len() as u64)),
                None if self.is_dir(path) => Ok(Some(0)),
                None => Ok(None),
            }
        }

        async fn read_at(
            &mut self,
            path: &str,
            off: u64,
            dst: &mut [u8],
        ) -> core::result::Result<usize, VolumeError> {
            assert!(dst.len() <= Self::CHUNK);
            let f = self
                .files
                .get(path)
                .ok_or_else(|| VolumeError(String::from("not found")))?;
            let tail = f.get(off as usize..).unwrap_or_default();
            let n = tail.len().min(dst.len());
            dst[..n].copy_from_slice(&tail[..n]);
            Ok(n)
        }

        async fn replace(
            &mut self,
            path: &str,
            data: &[u8],
        ) -> core::result::Result<(), VolumeError> {
            if self.is_dir(path) {
                return Err(VolumeError(String::from("is a directory")));
            }
            self.files.insert(path.to_string(), data.to_vec());
            Ok(())
        }

        async fn remove(&mut self, path: &str) -> core::result::Result<bool, VolumeError> {
            Ok(self.files.remove(path).is_some())
        }

        async fn list(
            &mut self,
            path: &str,
            dst: &mut [u8],
        ) -> core::result::Result<Option<usize>, VolumeError> {
            let prefix = format!("{}/", path.trim_end_matches('/'));
            let mut out: Vec<String> = Vec::new();
            for k in self.files.keys() {
                let Some(rest) = k.strip_prefix(&prefix) else {
                    continue;
                };
                let name = match rest.split_once('/') {
                    Some((d, _)) => format!("{d}/"),
                    None => rest.to_string(),
                };
                if !out.contains(&name) {
                    out.push(name);
                }
            }
            if out.is_empty() {
                return Ok(None);
            }
            let joined = out.join("\n");
            let n = joined.len().min(dst.len());
            dst[..n].copy_from_slice(&joined.as_bytes()[..n]);
            Ok(Some(n))
        }
    }
}
