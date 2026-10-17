//! De statische CNAME-tabel, met wildcards.
//!
//! Een naam in de tabel krijgt één CNAME-record, voor elk vraagtype, en
//! wint van een dienst met dezelfde naam; hopdns jaagt het doel niet na, de
//! resolver vraagt het doel zelf (Go: "no chasing"). Een sleutel `*.apps.hop.local`
//! past op elke naam met één of meer labels vóór `apps.hop.local`, niet op
//! `apps.hop.local` zelf; een exacte sleutel wint van een wildcard, en de
//! langste wildcard wint.
//!
//! Namen staan hier in kleine letters, zonder slotpunt; een doel wordt als
//! volledige naam (FQDN, met slotpunt) teruggegeven, zoals Go's `dns.Fqdn`.

use alloc::string::String;
use alloc::vec::Vec;

use crate::Result;
use crate::table::Table;

/// De CNAME-tabel.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CNAMEs {
    /// `mail.hop.local` naar `mailserver.example.com.`.
    exact: Table<String>,
    /// `apps.hop.local` (uit `*.apps.hop.local`) naar `ingress.hop.local.`.
    wildcard: Table<String>,
}

/// `name` in kleine letters, zonder spaties en zonder slotpunt.
fn key_of(name: &str) -> Result<String> {
    let t = name.trim();
    let t = t.strip_suffix('.').unwrap_or(t);
    let mut s = String::new();
    s.try_reserve_exact(t.len())?;
    s.extend(t.chars().map(|c| c.to_ascii_lowercase()));
    Ok(s)
}

/// `name` als volledige naam: met precies één slotpunt.
fn fqdn(name: &str) -> Result<String> {
    let t = name.trim();
    let mut s = String::new();
    s.try_reserve_exact(t.len() + 1)?;
    s.push_str(t);
    if !s.ends_with('.') {
        s.push('.');
    }
    Ok(s)
}

impl CNAMEs {
    /// Bouwt de tabel uit paren naam, doel. Een sleutel die met `*.`
    /// begint, wordt een wildcard; een lege naam of een leeg doel valt weg.
    pub fn new<'a>(pairs: impl IntoIterator<Item = (&'a str, &'a str)>) -> Result<Self> {
        let mut c = Self::default();
        for (k, v) in pairs {
            let key = key_of(k)?;
            if key.is_empty() || v.trim().is_empty() {
                continue;
            }
            let target = fqdn(v)?;
            match key.strip_prefix("*.") {
                Some(parent) if !parent.is_empty() => c.wildcard.insert(parent, target)?,
                _ => c.exact.insert(&key, target)?,
            };
        }
        Ok(c)
    }

    /// Het CNAME-doel voor `name`, als er een is.
    ///
    /// `name` mag hoofdletters en een slotpunt dragen; vergeleken wordt
    /// zonder. De kleine letters staan in een kladblok op de stack, zodat
    /// een vraag niets alloceert.
    #[must_use]
    pub fn lookup(&self, name: &str) -> Option<&str> {
        let name = name.strip_suffix('.').unwrap_or(name);
        let mut buf = [0u8; 256];
        let lower = lower_into(name, &mut buf)?;
        if let Some(v) = self.exact.get(lower) {
            return Some(v);
        }
        // Label voor label naar boven: "foo.bar.apps.hop.local" vraagt
        // "bar.apps.hop.local", dan "apps.hop.local" (raak), enzovoort. De
        // eerste treffer is de langste wildcard.
        let mut parent = lower;
        while let Some((_, rest)) = parent.split_once('.') {
            if rest.is_empty() {
                return None;
            }
            if let Some(v) = self.wildcard.get(rest) {
                return Some(v);
            }
            parent = rest;
        }
        None
    }

    /// Het aantal regels in de tabel.
    #[must_use]
    pub fn len(&self) -> usize {
        self.exact.len() + self.wildcard.len()
    }

    /// Of de tabel leeg is.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// De zones met minstens één regel: een exacte naam zonder zijn eerste
    /// label, een wildcard zijn ouder. Zonder slotpunt, zonder dubbelen.
    ///
    /// De server beantwoordt in die zones ook een miss gezaghebbend, zodat
    /// één hopdns CNAMEs over meer domeinen kan dienen.
    pub fn zones(&self) -> Result<Vec<&str>> {
        let mut out: Vec<&str> = Vec::new();
        let exact = self
            .exact
            .iter()
            .filter_map(|(k, _)| k.split_once('.').map(|(_, z)| z))
            .filter(|z| !z.is_empty());
        let wild = self.wildcard.iter().map(|(k, _)| k);
        for z in exact.chain(wild) {
            if !out.contains(&z) {
                out.try_reserve(1)?;
                out.push(z);
            }
        }
        Ok(out)
    }

    /// De zone uit [`CNAMEs::zones`] waar `name` (klein, zonder slotpunt) in
    /// valt, als die er is: de langste.
    #[must_use]
    pub fn zone_of<'a>(&'a self, name: &str) -> Option<&'a str> {
        let exact = self
            .exact
            .iter()
            .filter_map(|(k, _)| k.split_once('.').map(|(_, z)| z));
        let wild = self.wildcard.iter().map(|(k, _)| k);
        exact
            .chain(wild)
            .filter(|z| !z.is_empty() && crate::server::in_zone(name, z))
            .max_by_key(|z| z.len())
    }
}

/// `s` in kleine ASCII-letters in `buf`; `None` als het niet past.
fn lower_into<'b>(s: &str, buf: &'b mut [u8; 256]) -> Option<&'b str> {
    let out = buf.get_mut(..s.len())?;
    for (o, b) in out.iter_mut().zip(s.bytes()) {
        *o = b.to_ascii_lowercase();
    }
    core::str::from_utf8(out).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    // TestNewCNAMEsExact: exacte namen, hoofdletterongevoelig, met en zonder
    // slotpunt; het doel is altijd een FQDN.
    #[test]
    fn new_cnames_exact() {
        let c = CNAMEs::new([
            ("mail.hop.local", "mailserver.example.com"),
            ("GIT.hop.local", "gitea.prod-eu.hop.local."),
        ])
        .unwrap();
        let cases = [
            ("mail.hop.local", Some("mailserver.example.com.")),
            ("mail.hop.local.", Some("mailserver.example.com.")),
            ("MAIL.HOP.LOCAL", Some("mailserver.example.com.")),
            ("git.hop.local", Some("gitea.prod-eu.hop.local.")),
            ("unknown.hop.local", None),
        ];
        for (q, want) in cases {
            assert_eq!(c.lookup(q), want, "lookup({q:?})");
        }
    }

    // TestNewCNAMEsWildcard: een wildcard past op elke diepte eronder, niet
    // op de ouder zelf.
    #[test]
    fn new_cnames_wildcard() {
        let c = CNAMEs::new([("*.apps.hop.local", "ingress.prod-eu.hop.local")]).unwrap();
        let cases = [
            ("foo.apps.hop.local", Some("ingress.prod-eu.hop.local.")),
            ("bar.apps.hop.local.", Some("ingress.prod-eu.hop.local.")),
            (
                "deep.nested.apps.hop.local",
                Some("ingress.prod-eu.hop.local."),
            ),
            ("apps.hop.local", None),
            ("other.hop.local", None),
        ];
        for (q, want) in cases {
            assert_eq!(c.lookup(q), want, "lookup({q:?})");
        }
    }

    // TestCNAMEsExactBeatsWildcard: exact wint, de wildcard vangt de rest.
    #[test]
    fn cnames_exact_beats_wildcard() {
        let c = CNAMEs::new([
            ("*.apps.hop.local", "ingress.hop.local"),
            ("web.apps.hop.local", "special.hop.local"),
        ])
        .unwrap();
        assert_eq!(c.lookup("web.apps.hop.local"), Some("special.hop.local."));
        assert_eq!(c.lookup("other.apps.hop.local"), Some("ingress.hop.local."));
    }

    // TestCNAMEsNilSafe: een lege tabel (Go: nil) vindt niets en telt nul.
    #[test]
    fn cnames_nil_safe() {
        let c = CNAMEs::default();
        assert_eq!(c.lookup("anything.hop.local"), None);
        assert_eq!(c.len(), 0);
    }

    // TestCNAMEsIgnoresEmpty: lege namen en lege doelen vallen weg.
    #[test]
    fn cnames_ignores_empty() {
        let c = CNAMEs::new([
            ("", "target"),
            ("alias", ""),
            ("  ", "target"),
            ("keep.hop.local", "target.example.com"),
        ])
        .unwrap();
        assert_eq!(c.len(), 1);
    }

    // TestCNAMEsZones: de zones van exacte namen en wildcards, zonder dubbelen.
    #[test]
    fn cnames_zones() {
        let c = CNAMEs::new([
            ("mail.hop.local", "mail.example.com"),
            ("git.hop.local", "gitea.example.com"),
            (
                "database-cluster01-server00.production.svc.cluster.local",
                "bridge-cluster01-server00.ts.net",
            ),
            ("*.apps.hop.local", "ingress.hop.local"),
        ])
        .unwrap();
        let mut got = c.zones().unwrap();
        got.sort_unstable();
        assert_eq!(
            got,
            [
                "apps.hop.local",
                "hop.local",
                "production.svc.cluster.local"
            ]
        );
        assert_eq!(c.zone_of("x.apps.hop.local"), Some("apps.hop.local"));
        assert_eq!(c.zone_of("nope.hop.local"), Some("hop.local"));
        assert_eq!(c.zone_of("example.com"), None);
    }
}
