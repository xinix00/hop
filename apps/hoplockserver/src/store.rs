//! De store: blobs onder sleutels, met ETags en compare-and-swap.
//!
//! Bezit het contract en de voorwaarden, niet de opslag. [`Store`] is de
//! trait met Go's drie operaties (`Get`, `Put`, `Delete`); een implementatie
//! bezit haar eigen opslag en wordt door precies één eigenaar gebruikt (de
//! store-thread, de store-taak), dus `&mut self` en geen slot. De
//! voorwaarden zelf ([`check_put`], [`check_delete`]) staan hier één keer,
//! zodat elke store ze gelijk toepast.
//!
//! De methodes geven een future: de store op HopOS praat met de kern over
//! het net, die op de host en in het geheugen zijn meteen klaar.

use alloc::collections::BTreeMap;
use alloc::string::String;
use alloc::vec::Vec;
use core::fmt;
use core::future::Future;

use crate::etag::Etag;
use crate::key::Key;

/// Waarom een store-operatie niet lukte; de `Display` is die van Go.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    /// De sleutel bestaat niet (Go: `ErrNotFound`).
    NotFound,
    /// Een voorwaardelijke schrijf faalde: `If-None-Match: *` op een
    /// bestaande sleutel, of `If-Match` op een oude ETag (Go: `ErrPrecondition`).
    Precondition,
    /// Een sleutel die buiten de datamap reikt of een NUL bevat (Go: `ErrBadKey`).
    BadKey,
    /// Geheugen voor een body of een pad was er niet.
    OutOfMemory {
        /// Hoeveel bytes er gevraagd werden.
        bytes: usize,
    },
    /// De opslag zelf faalde: een bestand, een call naar de kern.
    Io {
        /// De stap (`read`, `write`, `remove`, `mkdir`).
        op: &'static str,
        /// De sleutel.
        key: String,
        /// De zin van de opslag.
        why: String,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound => f.write_str("hoplockserver/store: not found"),
            Self::Precondition => f.write_str("hoplockserver/store: precondition failed"),
            Self::BadKey => f.write_str("hoplockserver/store: invalid key"),
            Self::OutOfMemory { bytes } => {
                write!(f, "hoplockserver/store: out of memory ({bytes} bytes)")
            }
            Self::Io { op, key, why } => write!(f, "hoplockserver/store: {op} {key}: {why}"),
        }
    }
}

/// Het resultaat-type van deze crate.
pub type Result<T = (), E = Error> = core::result::Result<T, E>;

/// Een voorwaarde op een schrijf of een verwijdering; leeg is onvoorwaardelijk.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Condition {
    /// `*` betekent "mag nog niet bestaan"; iets anders weigert de server al.
    pub if_none_match: String,
    /// De verwachte ETag, met aanhalingstekens; leeg is geen voorwaarde.
    pub if_match: String,
}

impl Condition {
    /// Alleen aanmaken (`If-None-Match: *`).
    #[must_use]
    pub fn create() -> Condition {
        Condition {
            if_none_match: String::from("*"),
            if_match: String::new(),
        }
    }

    /// Alleen als de huidige ETag `etag` is (`If-Match`).
    #[must_use]
    pub fn matching(etag: &str) -> Condition {
        Condition {
            if_none_match: String::new(),
            if_match: String::from(etag),
        }
    }
}

/// Een opgeslagen object: de body en zijn ETag.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Object {
    /// De body, ongewijzigd.
    pub body: Vec<u8>,
    /// De ETag van de body.
    pub etag: Etag,
}

/// Mag een schrijf door, gegeven de ETag van wat er nu staat?
///
/// Go's `Store.Put`: `If-None-Match: *` wint van `If-Match`; `If-Match` op
/// een sleutel die er niet is, faalt; zonder voorwaarde mag alles.
pub fn check_put(current: Option<&Etag>, cond: &Condition) -> Result {
    if cond.if_none_match == "*" {
        return match current {
            Some(_) => Err(Error::Precondition),
            None => Ok(()),
        };
    }
    if !cond.if_match.is_empty() {
        return match current {
            Some(e) if *e == *cond.if_match => Ok(()),
            _ => Err(Error::Precondition),
        };
    }
    Ok(())
}

/// Mag een verwijdering door, gegeven de ETag van wat er staat?
///
/// Go's `Store.Delete`: alleen `If-Match` telt, en alleen als hij gezet is.
pub fn check_delete(current: &Etag, cond: &Condition) -> Result {
    if !cond.if_match.is_empty() && *current != *cond.if_match {
        return Err(Error::Precondition);
    }
    Ok(())
}

/// De drie operaties van Go's `store.Store`.
///
/// Een implementatie is van één eigenaar; de futures lenen haar `&mut`.
pub trait Store {
    /// Het object onder `key`, of [`Error::NotFound`].
    fn get(&mut self, key: &Key) -> impl Future<Output = Result<Object>>;

    /// Schrijft `body` onder `key` als `cond` het toelaat ([`check_put`]);
    /// geeft de nieuwe ETag.
    fn put_if(
        &mut self,
        key: &Key,
        body: Vec<u8>,
        cond: &Condition,
    ) -> impl Future<Output = Result<Etag>>;

    /// Verwijdert `key` als `cond` het toelaat ([`check_delete`]); een
    /// sleutel die er niet is, is [`Error::NotFound`].
    fn delete(&mut self, key: &Key, cond: &Condition) -> impl Future<Output = Result>;
}

/// De store in het geheugen: voor de tests en voor wie niets hoeft te bewaren.
#[derive(Debug, Default)]
pub struct MemStore {
    data: BTreeMap<Key, Object>,
}

impl MemStore {
    /// Een lege store.
    #[must_use]
    pub fn new() -> MemStore {
        MemStore::default()
    }

    /// Hoeveel sleutels er staan.
    #[must_use]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Staat er niets?
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl Store for MemStore {
    async fn get(&mut self, key: &Key) -> Result<Object> {
        let obj = self.data.get(key).ok_or(Error::NotFound)?;
        let mut body = Vec::new();
        body.try_reserve_exact(obj.body.len())
            .map_err(|_| Error::OutOfMemory {
                bytes: obj.body.len(),
            })?;
        body.extend_from_slice(&obj.body);
        Ok(Object {
            body,
            etag: obj.etag,
        })
    }

    async fn put_if(&mut self, key: &Key, body: Vec<u8>, cond: &Condition) -> Result<Etag> {
        check_put(self.data.get(key).map(|o| &o.etag), cond)?;
        let etag = Etag::of(&body);
        self.data.insert(key.clone(), Object { body, etag });
        Ok(etag)
    }

    async fn delete(&mut self, key: &Key, cond: &Condition) -> Result {
        let obj = self.data.get(key).ok_or(Error::NotFound)?;
        check_delete(&obj.etag, cond)?;
        self.data.remove(key);
        Ok(())
    }
}
