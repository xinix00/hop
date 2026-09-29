//! Een kleine gesorteerde map van tekst naar waarde, met faalbare invoeging.
//!
//! De Go-code gebruikte `map[string]string` voor env, tags, affinity en
//! poorten. `BTreeMap` uit `alloc` kan een invoeging niet laten falen, dus
//! hier een gesorteerde `Vec` met `try_reserve`: deze maps hebben er een
//! handvol sleutels, en gesorteerd is ook wat Go bij het schrijven deed
//! (`encoding/json` sorteert map-sleutels), dus de JSON blijft gelijk.

use alloc::string::String;
use alloc::vec::Vec;

use crate::{Error, Result, TryClone};

/// Een map met `String`-sleutels, gesorteerd op sleutel.
///
/// # Invariants
///
/// `pairs` is strikt oplopend gesorteerd op sleutel: geen dubbelen.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Map<V> {
    pairs: Vec<(String, V)>,
}

impl<V> Map<V> {
    /// Een lege map, zonder allocatie.
    pub const fn new() -> Self {
        Self { pairs: Vec::new() }
    }

    /// Het aantal sleutels.
    pub fn len(&self) -> usize {
        self.pairs.len()
    }

    /// Of de map leeg is.
    pub fn is_empty(&self) -> bool {
        self.pairs.is_empty()
    }

    fn find(&self, key: &str) -> core::result::Result<usize, usize> {
        self.pairs.binary_search_by(|(k, _)| k.as_str().cmp(key))
    }

    /// De waarde bij `key`.
    pub fn get(&self, key: &str) -> Option<&V> {
        let i = self.find(key).ok()?;
        self.pairs.get(i).map(|(_, v)| v)
    }

    /// De waarde bij `key`, veranderbaar.
    pub fn get_mut(&mut self, key: &str) -> Option<&mut V> {
        let i = self.find(key).ok()?;
        self.pairs.get_mut(i).map(|(_, v)| v)
    }

    /// Of `key` erin staat.
    pub fn contains_key(&self, key: &str) -> bool {
        self.find(key).is_ok()
    }

    /// Zet `key` op `value` en geeft de vorige waarde terug.
    pub fn insert(&mut self, key: String, value: V) -> Result<Option<V>> {
        match self.find(&key) {
            Ok(i) => match self.pairs.get_mut(i) {
                Some(slot) => Ok(Some(core::mem::replace(&mut slot.1, value))),
                None => Ok(None),
            },
            Err(i) => {
                self.pairs.try_reserve(1).map_err(|_| Error::OutOfMemory)?;
                // INVARIANT: `i` is de sorteerplek die binary_search gaf.
                self.pairs.insert(i, (key, value));
                Ok(None)
            }
        }
    }

    /// Haalt `key` weg en geeft de waarde terug.
    pub fn remove(&mut self, key: &str) -> Option<V> {
        let i = self.find(key).ok()?;
        Some(self.pairs.remove(i).1)
    }

    /// Houdt alleen de paren waarvoor `keep` waar is.
    pub fn retain(&mut self, mut keep: impl FnMut(&str, &mut V) -> bool) {
        self.pairs.retain_mut(|(k, v)| keep(k, v));
    }

    /// Loopt de paren af in sleutelvolgorde.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &V)> {
        self.pairs.iter().map(|(k, v)| (k.as_str(), v))
    }

    /// Loopt de paren af in sleutelvolgorde, met veranderbare waarden.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&str, &mut V)> {
        self.pairs.iter_mut().map(|(k, v)| (k.as_str(), v))
    }

    /// De sleutels in volgorde.
    pub fn keys(&self) -> impl Iterator<Item = &str> {
        self.pairs.iter().map(|(k, _)| k.as_str())
    }
}

impl<V: TryClone> TryClone for Map<V> {
    fn try_clone(&self) -> Result<Self> {
        let mut pairs = Vec::new();
        pairs
            .try_reserve_exact(self.pairs.len())
            .map_err(|_| Error::OutOfMemory)?;
        for (k, v) in &self.pairs {
            pairs.push((k.try_clone()?, v.try_clone()?));
        }
        Ok(Self { pairs })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::string::ToString;

    #[test]
    fn map_keeps_keys_sorted_and_unique() {
        let mut m = Map::new();
        assert_eq!(m.insert("b".to_string(), 2).unwrap(), None);
        assert_eq!(m.insert("a".to_string(), 1).unwrap(), None);
        assert_eq!(m.insert("b".to_string(), 3).unwrap(), Some(2));
        assert_eq!(m.keys().collect::<Vec<_>>(), ["a", "b"]);
        assert_eq!(m.get("b"), Some(&3));
        assert_eq!(m.remove("a"), Some(1));
        assert_eq!(m.len(), 1);
    }
}
