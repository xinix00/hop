//! Een kleine gesorteerde tabel van tekst naar waarde, met faalbare invoeging.
//!
//! `BTreeMap` uit `alloc` kan een invoeging niet laten falen (handboek §6),
//! dus hier een gesorteerde `Vec` met `try_reserve`, zoals hop's
//! `types::Map`. De tabellen van hopdns houden clusters (een handvol) en
//! jobs (tientallen tot een paar honderd); binair zoeken over een
//! aaneengesloten rij is daar sneller dan een boom en alloceert niets bij
//! het lezen.

use alloc::string::String;
use alloc::vec::Vec;

use crate::Result;

/// Een gesorteerde tabel van sleutel naar waarde.
///
/// # Invariants
///
/// `rows` is strikt oplopend gesorteerd op sleutel: elke sleutel staat er
/// hoogstens één keer in.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Table<V> {
    rows: Vec<(String, V)>,
}

impl<V> Default for Table<V> {
    fn default() -> Self {
        Self { rows: Vec::new() }
    }
}

impl<V> Table<V> {
    /// Een lege tabel.
    #[must_use]
    pub const fn new() -> Self {
        Self { rows: Vec::new() }
    }

    /// Het aantal sleutels.
    #[must_use]
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Of de tabel leeg is.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    fn find(&self, key: &str) -> core::result::Result<usize, usize> {
        self.rows.binary_search_by(|(k, _)| k.as_str().cmp(key))
    }

    /// De waarde bij `key`.
    #[must_use]
    pub fn get(&self, key: &str) -> Option<&V> {
        let i = self.find(key).ok()?;
        self.rows.get(i).map(|(_, v)| v)
    }

    /// De waarde bij `key`, veranderbaar.
    pub fn get_mut(&mut self, key: &str) -> Option<&mut V> {
        let i = self.find(key).ok()?;
        self.rows.get_mut(i).map(|(_, v)| v)
    }

    /// Of `key` erin staat.
    #[must_use]
    pub fn contains_key(&self, key: &str) -> bool {
        self.find(key).is_ok()
    }

    /// Zet `value` bij `key`; de vorige waarde komt terug.
    pub fn insert(&mut self, key: &str, value: V) -> Result<Option<V>> {
        match self.find(key) {
            Ok(i) => Ok(self
                .rows
                .get_mut(i)
                .map(|(_, v)| core::mem::replace(v, value))),
            Err(i) => {
                let mut k = String::new();
                k.try_reserve_exact(key.len())?;
                k.push_str(key);
                self.rows.try_reserve(1)?;
                // INVARIANT: `i` is de plek die de sortering houdt.
                self.rows.insert(i, (k, value));
                Ok(None)
            }
        }
    }

    /// Haalt `key` eruit.
    pub fn remove(&mut self, key: &str) -> Option<V> {
        let i = self.find(key).ok()?;
        Some(self.rows.remove(i).1)
    }

    /// Elke rij, oplopend op sleutel.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &V)> {
        self.rows.iter().map(|(k, v)| (k.as_str(), v))
    }

    /// Elke waarde, oplopend op sleutel.
    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.rows.iter().map(|(_, v)| v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_keeps_order_and_replaces() {
        let mut t = Table::new();
        assert_eq!(t.insert("b", 2).unwrap(), None);
        assert_eq!(t.insert("a", 1).unwrap(), None);
        assert_eq!(t.insert("c", 3).unwrap(), None);
        assert_eq!(t.insert("b", 20).unwrap(), Some(2));
        let keys: Vec<&str> = t.iter().map(|(k, _)| k).collect();
        assert_eq!(keys, ["a", "b", "c"]);
        assert_eq!(t.get("b"), Some(&20));
        assert_eq!(t.remove("a"), Some(1));
        assert!(!t.contains_key("a"));
        assert_eq!(t.len(), 2);
    }
}
