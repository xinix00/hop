//! De server-logica: één vraag in, één antwoord uit, zonder socket.
//!
//! Een vraag `<dienst>.<cluster>.<domein>` krijgt de adressen van die dienst
//! in dat cluster; een vraag `<dienst>.<domein>` de adressen uit elk cluster
//! samen, zonder dubbelen (de federatie-standaard). Elk record heeft TTL
//! [`crate::TTL`]. De volgorde van de beslissing:
//!
//! 1. Een statische CNAME wint, voor elk type, ook van een dienst met die
//!    naam; alleen het CNAME-record, de resolver jaagt het doel zelf na.
//! 2. In ons domein: A-records voor een bekende dienst met lopende taken.
//!    Een bekende naam zonder A-record (een dienst zonder lopende taak, een
//!    ander type, de zone zelf, of een clusternaam als tussenlabel) is
//!    NOERROR zonder antwoord; een onbekende naam NXDOMAIN. Beide met de
//!    SOA erbij (RFC 2308), zodat een resolver de lege uitkomst maar vijf
//!    seconden onthoudt.
//! 3. In een zone waar alleen CNAMEs staan: NXDOMAIN met de SOA van die
//!    zone.
//! 4. Al het andere: REFUSED, want daar zijn wij niet de baas.
//!
//! Een clusternaam als tussenlabel (`prod-eu.hop.local`) is NOERROR en
//! geen NXDOMAIN: een resolver met QNAME-minimalisatie vraagt eerst die
//! naam, en een NXDOMAIN daar zou volgens RFC 8020 alles eronder wissen.
//!
//! Past niet elk adres in het antwoord (512 bytes, of wat EDNS aanbiedt tot
//! 1232), dan gaan de eerste die passen mee, zonder TC-bit: hopdns heeft
//! geen TCP, en een resolver die op TC naar TCP gaat, zou hier niets
//! krijgen in plaats van een deel. Bij 512 bytes zijn dat er zo'n dertig,
//! met EDNS zo'n vierenzeventig, afhankelijk van de lengte van de naam.

use alloc::string::String;
use core::net::Ipv4Addr;

use crate::cache::Cache;
use crate::cnames::CNAMEs;
use crate::wire::{self, Bad, CLASS_ANY, CLASS_IN, NAME_MAX, Query, Rcode, Reply, rtype};
use crate::{DEFAULT_DOMAIN, Result, TTL};

/// Het grootste aantal A-records in één antwoord: wat in [`wire::EDNS_MAX`]
/// past met de kortste vraag. De vaste rij voor het ontdubbelen van een
/// samengevoegd antwoord is zo groot.
pub const MAX_ANSWERS: usize = (wire::EDNS_MAX - wire::HEADER_LEN - 5) / 16;

/// Of `name` in `zone` valt (of hem is); beide klein en zonder slotpunt.
#[must_use]
pub fn in_zone(name: &str, zone: &str) -> bool {
    name == zone
        || (name.len() > zone.len()
            && name.ends_with(zone)
            && name.as_bytes().get(name.len() - zone.len() - 1) == Some(&b'.'))
}

/// De server: het domein en de CNAME-tabel. De cache krijgt hij per vraag
/// te leen van zijn eigenaar.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Server {
    domain: String,
    cnames: CNAMEs,
}

/// Wat een naam in ons domein is.
enum Lookup<'c> {
    /// De adressen van één cluster (leeg: bekend, geen lopende taak).
    Cluster(Option<&'c [Ipv4Addr]>),
    /// Samengevoegd over alle clusters.
    Merged(&'c str),
    /// De zone zelf.
    Apex,
}

impl Server {
    /// Een server voor `domain` (hoofdletters en een slotpunt mogen; leeg is
    /// [`DEFAULT_DOMAIN`]).
    pub fn new(domain: &str) -> Result<Self> {
        let d = domain.trim();
        let d = d.strip_suffix('.').unwrap_or(d);
        let d = if d.is_empty() { DEFAULT_DOMAIN } else { d };
        let mut s = String::new();
        s.try_reserve_exact(d.len())?;
        s.extend(d.chars().map(|c| c.to_ascii_lowercase()));
        Ok(Self {
            domain: s,
            cnames: CNAMEs::default(),
        })
    }

    /// Zet de statische CNAMEs (die gaan vóór de dienstvraag).
    pub fn set_cnames(&mut self, c: CNAMEs) {
        self.cnames = c;
    }

    /// Het domein, klein en zonder slotpunt.
    #[must_use]
    pub fn domain(&self) -> &str {
        &self.domain
    }

    /// De CNAME-tabel.
    #[must_use]
    pub fn cnames(&self) -> &CNAMEs {
        &self.cnames
    }

    /// Beantwoordt één datagram `msg` in `out`; de lengte van het antwoord,
    /// of `None` als er niets terug moet (te kort, of zelf een antwoord).
    ///
    /// Alloceert niets.
    pub fn handle(&self, cache: &Cache, msg: &[u8], out: &mut [u8]) -> Option<usize> {
        let q = match wire::parse_query(msg) {
            Ok(q) => q,
            Err(Bad::Drop) => return None,
            Err(Bad::Reply { id, flags, rcode }) => {
                return Reply::bare(out, id, flags, rcode, wire::UDP_MAX)
                    .ok()
                    .map(Reply::finish);
            }
        };
        let limit = q.edns.map_or(wire::UDP_MAX, |size| {
            usize::from(size).clamp(wire::UDP_MAX, wire::EDNS_MAX)
        });
        let mut reply = Reply::start(out, &q, limit).ok()?;
        if self.answer(cache, &q, &mut reply).is_err() {
            reply.set_rcode(Rcode::ServFail);
        }
        Some(reply.finish())
    }

    /// Vult `reply` voor vraag `q`.
    fn answer(&self, cache: &Cache, q: &Query<'_>, reply: &mut Reply<'_>) -> Result {
        let mut buf = [0u8; NAME_MAX];
        let Some(name) = wire::name_text(q.qname, &mut buf) else {
            reply.set_rcode(Rcode::Refused);
            return Ok(());
        };
        if q.qclass != CLASS_IN && q.qclass != CLASS_ANY {
            reply.set_rcode(Rcode::Refused);
            return Ok(());
        }
        if let Some(target) = self.cnames.lookup(name) {
            return reply.answer_cname(target, TTL);
        }
        if in_zone(name, &self.domain) {
            return self.service(cache, name, q.qtype, reply);
        }
        if let Some(zone) = self.cnames.zone_of(name) {
            reply.set_rcode(Rcode::NxDomain);
            return reply.authority_soa(zone, TTL);
        }
        reply.set_rcode(Rcode::Refused);
        Ok(())
    }

    /// Een naam in ons domein.
    fn service(&self, cache: &Cache, name: &str, qtype: u16, reply: &mut Reply<'_>) -> Result {
        let lookup = match name
            .len()
            .checked_sub(self.domain.len() + 1)
            .and_then(|end| name.get(..end))
        {
            None => Lookup::Apex,
            Some(prefix) => match prefix.split_once('.') {
                Some((service, cluster)) => Lookup::Cluster(cache.get_cluster(cluster, service)),
                None => Lookup::Merged(prefix),
            },
        };
        let exists = match &lookup {
            Lookup::Apex => true,
            Lookup::Cluster(ips) => ips.is_some(),
            Lookup::Merged(job) => cache.has_job(job) || cache.has_cluster(job),
        };
        if matches!(qtype, rtype::A | rtype::ANY) {
            match lookup {
                Lookup::Apex => {}
                Lookup::Cluster(ips) => {
                    for ip in ips.unwrap_or_default() {
                        if reply.answer_a(*ip, TTL).is_err() {
                            break; // Vol: de eerste die pasten (zie de moduledoc).
                        }
                    }
                }
                Lookup::Merged(job) => write_merged(cache, job, reply),
            }
        }
        if reply.answers() == 0 {
            if !exists {
                reply.set_rcode(Rcode::NxDomain);
            }
            reply.authority_soa(&self.domain, TTL)?;
        }
        Ok(())
    }
}

/// De adressen van `job` uit elk cluster, elk één keer, tot het antwoord
/// vol is. De rij van geschreven adressen staat op de stack.
fn write_merged(cache: &Cache, job: &str, reply: &mut Reply<'_>) {
    let mut seen = [Ipv4Addr::UNSPECIFIED; MAX_ANSWERS];
    let mut n = 0;
    for ips in cache.merged(job) {
        for ip in ips {
            let written = seen.get(..n).unwrap_or_default();
            if written.contains(ip) {
                continue;
            }
            let Some(slot) = seen.get_mut(n) else {
                return;
            };
            if reply.answer_a(*ip, TTL).is_err() {
                return;
            }
            *slot = *ip;
            n += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::{Message, Rdata, decode, encode_query};
    use alloc::vec;
    use alloc::vec::Vec;

    fn ip(s: &str) -> Ipv4Addr {
        s.parse().unwrap()
    }

    /// Go's `handleQuery` met een nep-`ResponseWriter`: een vraag, het
    /// gelezen antwoord.
    fn ask(server: &Server, cache: &Cache, name: &str, qtype: u16) -> Message {
        let mut q = [0u8; 512];
        let n = encode_query(0x1234, name, qtype, None, &mut q).unwrap();
        let mut out = [0u8; 512];
        let len = server
            .handle(cache, &q[..n], &mut out)
            .expect("no response written");
        decode(&out[..len]).unwrap()
    }

    fn a_strings(m: &Message) -> Vec<String> {
        m.a_records().map(|a| alloc::format!("{a}")).collect()
    }

    // TestServerHandleQuery: dienst.cluster.domein geeft de twee adressen
    // van dat cluster als A-records.
    #[test]
    fn server_handle_query() {
        let mut cache = Cache::new();
        cache
            .set(
                "prod",
                "myapp",
                vec![ip("192.168.1.10"), ip("192.168.1.20")],
            )
            .unwrap();
        let server = Server::new("internal").unwrap();
        let m = ask(&server, &cache, "myapp.prod.internal.", rtype::A);
        assert_eq!(m.answers.len(), 2);
        for rec in &m.answers {
            let Rdata::A(a) = rec.data else {
                panic!("answer is not an A record");
            };
            assert!(a == ip("192.168.1.10") || a == ip("192.168.1.20"), "{a}");
            assert_eq!(rec.ttl, 5);
        }
        assert!(m.is_authoritative());
        assert_eq!(m.id, 0x1234);
    }

    // TestServerHandleQueryMergedAcrossClusters: de kale vraag voegt elke
    // peer samen.
    #[test]
    fn server_handle_query_merged_across_clusters() {
        let mut cache = Cache::new();
        cache
            .set("prod", "myapp", vec![ip("192.168.1.10")])
            .unwrap();
        cache
            .set("staging", "myapp", vec![ip("192.168.2.10")])
            .unwrap();
        let server = Server::new("internal").unwrap();
        let m = ask(&server, &cache, "myapp.internal.", rtype::A);
        assert_eq!(a_strings(&m), ["192.168.1.10", "192.168.2.10"]);
    }

    // TestServerHandleQueryWrongDomain: een ander domein krijgt niets (hier
    // REFUSED: daar zijn wij niet de baas).
    #[test]
    fn server_handle_query_wrong_domain() {
        let mut cache = Cache::new();
        cache
            .set("prod", "myapp", vec![ip("192.168.1.10")])
            .unwrap();
        let server = Server::new("internal").unwrap();
        let m = ask(&server, &cache, "myapp.other.example.", rtype::A);
        assert_eq!(m.answers.len(), 0);
        assert_eq!(m.rcode(), Rcode::Refused as u8);
    }

    // TestServerHandleQueryClusterSpecific: alleen het gevraagde cluster.
    #[test]
    fn server_handle_query_cluster_specific() {
        let mut cache = Cache::new();
        cache.set("prod-eu", "myapp", vec![ip("10.0.0.1")]).unwrap();
        cache.set("prod-us", "myapp", vec![ip("10.0.1.1")]).unwrap();
        let server = Server::new("internal").unwrap();
        let m = ask(&server, &cache, "myapp.prod-eu.internal.", rtype::A);
        assert_eq!(a_strings(&m), ["10.0.0.1"]);
    }

    // TestServerHandleQueryDifferentClusters: elk cluster zijn eigen adres.
    #[test]
    fn server_handle_query_different_clusters() {
        let mut cache = Cache::new();
        cache.set("prod-eu", "myapp", vec![ip("10.0.0.1")]).unwrap();
        cache.set("prod-us", "myapp", vec![ip("10.0.1.1")]).unwrap();
        let server = Server::new("internal").unwrap();
        let eu = ask(&server, &cache, "myapp.prod-eu.internal.", rtype::A);
        assert_eq!(a_strings(&eu), ["10.0.0.1"]);
        let us = ask(&server, &cache, "myapp.prod-us.internal.", rtype::A);
        assert_eq!(a_strings(&us), ["10.0.1.1"]);
    }

    // TestServerHandleQueryServiceDown: een dienst zonder lopende taak
    // geeft geen antwoord, wel de SOA met minimum 5.
    #[test]
    fn server_handle_query_service_down() {
        let mut cache = Cache::new();
        cache.set("prod", "myapp", Vec::new()).unwrap();
        let server = Server::new("internal").unwrap();
        let m = ask(&server, &cache, "myapp.prod.internal.", rtype::A);
        assert_eq!(m.answers.len(), 0);
        assert_eq!(m.rcode(), Rcode::NoError as u8, "known name: NODATA");
        assert_eq!(m.authority.len(), 1, "expected 1 SOA in Authority");
        let Rdata::Soa { minimum, .. } = m.authority[0].data else {
            panic!("authority is not a SOA");
        };
        assert_eq!(minimum, 5);
    }

    // TestServerHandleQuerySuccessNoSOA: een antwoord met adressen heeft
    // geen SOA.
    #[test]
    fn server_handle_query_success_no_soa() {
        let mut cache = Cache::new();
        cache
            .set("prod", "myapp", vec![ip("192.168.1.10")])
            .unwrap();
        let server = Server::new("internal").unwrap();
        let m = ask(&server, &cache, "myapp.prod.internal.", rtype::A);
        assert_eq!(m.answers.len(), 1);
        assert_eq!(m.authority.len(), 0);
    }

    fn with_cnames(pairs: &[(&str, &str)]) -> Server {
        let mut s = Server::new("hop.local").unwrap();
        s.set_cnames(CNAMEs::new(pairs.iter().copied()).unwrap());
        s
    }

    // TestServerHandleCNAMEExact: een exacte CNAME, met de gevraagde naam.
    #[test]
    fn server_handle_cname_exact() {
        let server = with_cnames(&[("mail.hop.local", "mailserver.example.com")]);
        let m = ask(&server, &Cache::new(), "mail.hop.local.", rtype::A);
        assert_eq!(m.answers.len(), 1);
        assert_eq!(m.answers[0].name, "mail.hop.local.");
        assert_eq!(
            m.answers[0].data,
            Rdata::Cname(String::from("mailserver.example.com."))
        );
    }

    // TestServerHandleCNAMEWildcard: een wildcard-CNAME.
    #[test]
    fn server_handle_cname_wildcard() {
        let server = with_cnames(&[("*.apps.hop.local", "ingress.prod-eu.hop.local")]);
        let m = ask(
            &server,
            &Cache::new(),
            "dashboard.apps.hop.local.",
            rtype::A,
        );
        assert_eq!(m.answers.len(), 1);
        assert_eq!(
            m.answers[0].data,
            Rdata::Cname(String::from("ingress.prod-eu.hop.local."))
        );
    }

    // TestServerCNAMEForExplicitCNAMEQuery: ook een CNAME-vraag krijgt de CNAME.
    #[test]
    fn server_cname_for_explicit_cname_query() {
        let server = with_cnames(&[("mail.hop.local", "mailserver.example.com")]);
        let m = ask(&server, &Cache::new(), "mail.hop.local.", rtype::CNAME);
        assert_eq!(m.answers.len(), 1);
        assert!(matches!(m.answers[0].data, Rdata::Cname(_)));
    }

    // TestServerCNAMEBeatsServiceLookup: een CNAME wint van een dienst met
    // dezelfde naam.
    #[test]
    fn server_cname_beats_service_lookup() {
        let mut cache = Cache::new();
        cache.set("prod", "mail", vec![ip("10.0.0.5")]).unwrap();
        let server = with_cnames(&[("mail.prod.hop.local", "external.example.com")]);
        let m = ask(&server, &cache, "mail.prod.hop.local.", rtype::A);
        assert_eq!(m.answers.len(), 1);
        assert!(matches!(m.answers[0].data, Rdata::Cname(_)));
    }

    #[test]
    fn nxdomain_for_an_unknown_name_and_nodata_for_known_names() {
        let mut cache = Cache::new();
        cache.set("prod-eu", "web", vec![ip("10.0.0.1")]).unwrap();
        let server = Server::new("Hop.Local.").unwrap();
        let unknown = ask(&server, &cache, "nope.hop.local", rtype::A);
        assert_eq!(unknown.rcode(), Rcode::NxDomain as u8);
        assert_eq!(unknown.authority.len(), 1);
        let unknown = ask(&server, &cache, "nope.prod-eu.hop.local", rtype::A);
        assert_eq!(unknown.rcode(), Rcode::NxDomain as u8);
        // Een ander type voor een bestaande dienst: NODATA.
        let aaaa = ask(&server, &cache, "web.hop.local", rtype::AAAA);
        assert_eq!(aaaa.rcode(), Rcode::NoError as u8);
        assert_eq!((aaaa.answers.len(), aaaa.authority.len()), (0, 1));
        // De clusternaam als tussenlabel (QNAME-minimalisatie) en de zone.
        for name in ["prod-eu.hop.local", "hop.local"] {
            let m = ask(&server, &cache, name, rtype::A);
            assert_eq!(m.rcode(), Rcode::NoError as u8, "{name}");
            assert_eq!(m.authority.len(), 1, "{name}");
        }
        // Hoofdletters in de vraag, de naam gaat terug zoals gevraagd.
        let m = ask(&server, &cache, "WEB.Hop.Local", rtype::A);
        assert_eq!(a_strings(&m), ["10.0.0.1"]);
        assert_eq!(m.answers[0].name, "WEB.Hop.Local.");
    }

    #[test]
    fn a_cname_only_zone_answers_its_own_misses() {
        let server = with_cnames(&[("db.svc.cluster.local", "x.ts.net")]);
        let m = ask(&server, &Cache::new(), "other.svc.cluster.local", rtype::A);
        assert_eq!(m.rcode(), Rcode::NxDomain as u8);
        let Rdata::Soa { mname, .. } = &m.authority[0].data else {
            panic!("no SOA");
        };
        assert_eq!(mname, "ns.svc.cluster.local.");
    }

    #[test]
    fn many_addresses_fill_the_answer_and_stop() {
        let mut cache = Cache::new();
        let ips: Vec<Ipv4Addr> = (1..=100).map(|i| Ipv4Addr::new(10, 1, 0, i)).collect();
        cache.set("a", "big", ips.clone()).unwrap();
        cache.set("b", "big", ips).unwrap();
        let server = Server::new("hop.local").unwrap();
        let m = ask(&server, &cache, "big.hop.local", rtype::A);
        assert_eq!(m.answers.len(), (512 - 12 - 17) / 16);
        assert!(!m.is_truncated());
        // Met EDNS 1232 past er meer in, en nog steeds zonder dubbelen.
        let mut q = [0u8; 512];
        let n = encode_query(1, "big.hop.local", rtype::A, Some(4096), &mut q).unwrap();
        let mut out = [0u8; 2048];
        let len = server.handle(&cache, &q[..n], &mut out).unwrap();
        assert!(len <= wire::EDNS_MAX);
        let m = decode(&out[..len]).unwrap();
        assert_eq!(m.answers.len(), (1232 - 12 - 17 - 11) / 16);
        let mut all: Vec<Ipv4Addr> = m.a_records().collect();
        all.dedup();
        assert_eq!(all.len(), m.answers.len());
    }

    #[test]
    fn malformed_and_foreign_datagrams() {
        let server = Server::new("hop.local").unwrap();
        let cache = Cache::new();
        let mut out = [0u8; 512];
        assert_eq!(server.handle(&cache, &[1, 2, 3], &mut out), None);
        let mut q = [0u8; 512];
        let n = encode_query(5, "x.hop.local", rtype::A, None, &mut q).unwrap();
        q[5] = 0; // QDCOUNT 0
        let len = server.handle(&cache, &q[..n], &mut out).unwrap();
        let m = decode(&out[..len]).unwrap();
        assert_eq!((m.id, m.rcode()), (5, Rcode::FormErr as u8));
    }
}
