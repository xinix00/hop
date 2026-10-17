//! De cache: per cluster, per jobnaam, de adressen van de lopende taken.
//!
//! Go hield hem achter een `sync.RWMutex` omdat elke watcher-goroutine en
//! elke DNS-goroutine hem aanraakten. Hier heeft hij één eigenaar: de taak
//! (of thread) die de vragen beantwoordt. Een watcher bouwt een compleet
//! clusterbeeld ([`Jobs`]) en stuurt het als bericht; de eigenaar legt het
//! met [`Cache::update`] in één zet op zijn plaats. Het leespad alloceert
//! niets en neemt geen slot: [`Cache::get_cluster`] geeft een lening.
//!
//! Bezit: de tabel. Bezit niet: wie welk cluster ververst (de watchers), en
//! het naamgebruik (de sleutels komen zoals de watcher ze gaf; de server
//! vraagt in kleine letters, de watcher bouwt in kleine letters).

use alloc::vec::Vec;
use core::net::Ipv4Addr;

use crate::Result;
use crate::table::Table;

/// Het beeld van één cluster: jobnaam naar adressen, elk adres één keer.
pub type Jobs = Table<Vec<Ipv4Addr>>;

/// Cluster naar [`Jobs`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Cache {
    clusters: Table<Jobs>,
}

impl Cache {
    /// Een lege cache.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            clusters: Table::new(),
        }
    }

    /// De adressen van `job` in `cluster`; `None` als de job daar onbekend
    /// is, een lege rij als hij bekend is zonder lopende taak.
    #[must_use]
    pub fn get_cluster(&self, cluster: &str, job: &str) -> Option<&[Ipv4Addr]> {
        self.clusters.get(cluster)?.get(job).map(Vec::as_slice)
    }

    /// De adressen van `job` in elk cluster, zonder dubbelen, in de volgorde
    /// van de clusters. Voor de clusterloze vraag `<dienst>.<domein>`, die
    /// elke peer samenvoegt (de federatie-standaard).
    ///
    /// De server gebruikt [`Cache::merged`] en ontdubbelt terwijl hij
    /// schrijft; deze vorm alloceert en is er voor wie een lijst wil.
    pub fn get_merged(&self, job: &str) -> Result<Vec<Ipv4Addr>> {
        let mut out: Vec<Ipv4Addr> = Vec::new();
        for ips in self.merged(job) {
            for ip in ips {
                if !out.contains(ip) {
                    out.try_reserve(1)?;
                    out.push(*ip);
                }
            }
        }
        Ok(out)
    }

    /// De adreslijst van `job` per cluster waar hij bekend is.
    pub fn merged<'a>(&'a self, job: &'a str) -> impl Iterator<Item = &'a [Ipv4Addr]> + 'a {
        self.clusters
            .values()
            .filter_map(move |jobs| jobs.get(job).map(Vec::as_slice))
    }

    /// Of `job` in enig cluster bekend is.
    #[must_use]
    pub fn has_job(&self, job: &str) -> bool {
        self.merged(job).next().is_some()
    }

    /// Of `cluster` in de cache staat.
    #[must_use]
    pub fn has_cluster(&self, cluster: &str) -> bool {
        self.clusters.contains_key(cluster)
    }

    /// Zet de adressen van één job in één cluster.
    pub fn set(&mut self, cluster: &str, job: &str, ips: Vec<Ipv4Addr>) -> Result {
        if let Some(jobs) = self.clusters.get_mut(cluster) {
            jobs.insert(job, ips)?;
            return Ok(());
        }
        let mut jobs = Jobs::new();
        jobs.insert(job, ips)?;
        self.clusters.insert(cluster, jobs)?;
        Ok(())
    }

    /// Vervangt het hele beeld van `cluster` (de herbouw na een verversing).
    pub fn update(&mut self, cluster: &str, jobs: Jobs) -> Result {
        self.replace(cluster, jobs).map(drop)
    }

    /// Als [`Cache::update`], en geeft het vorige beeld terug in plaats van
    /// het weg te gooien: voor een bekend cluster een verwisseling zonder
    /// allocatie (Go's "pointer swap").
    pub fn replace(&mut self, cluster: &str, jobs: Jobs) -> Result<Option<Jobs>> {
        self.clusters.insert(cluster, jobs)
    }

    /// Haalt `cluster` weg.
    pub fn clear(&mut self, cluster: &str) {
        self.clusters.remove(cluster);
    }

    /// Het aantal clusters.
    #[must_use]
    pub fn clusters(&self) -> usize {
        self.clusters.len()
    }

    /// Het aantal verschillende jobnamen over alle clusters samen.
    #[must_use]
    pub fn jobs(&self) -> usize {
        let mut n = 0;
        for (i, jobs) in self.clusters.values().enumerate() {
            // Een naam telt bij het eerste cluster waarin hij staat.
            n += jobs
                .iter()
                .filter(|(name, _)| {
                    !self
                        .clusters
                        .values()
                        .take(i)
                        .any(|earlier| earlier.contains_key(name))
                })
                .count();
        }
        n
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    pub(crate) fn ip(s: &str) -> Ipv4Addr {
        s.parse().unwrap()
    }

    // TestCacheSetGetCluster: wat erin gaat, komt er per cluster uit.
    #[test]
    fn cache_set_get_cluster() {
        let mut c = Cache::new();
        c.set(
            "prod",
            "myapp",
            vec![ip("192.168.1.10"), ip("192.168.1.20")],
        )
        .unwrap();
        assert_eq!(c.get_cluster("prod", "myapp").unwrap().len(), 2);
    }

    // TestCacheGetClusterEmpty: een onbekende job is nil (hier `None`).
    #[test]
    fn cache_get_cluster_empty() {
        let c = Cache::new();
        assert_eq!(c.get_cluster("prod", "nonexistent"), None);
    }

    // TestCacheGetClusterIsolation: dezelfde job in twee clusters blijft
    // gescheiden, en een onbekend cluster is nil.
    #[test]
    fn cache_get_cluster_isolation() {
        let mut c = Cache::new();
        c.set("prod-eu", "myapp", vec![ip("10.0.0.1")]).unwrap();
        c.set("prod-us", "myapp", vec![ip("10.0.1.1")]).unwrap();
        let got = c.get_cluster("prod-eu", "myapp").unwrap();
        assert_eq!(got, [ip("10.0.0.1")]);
        assert_eq!(c.get_cluster("nonexistent", "myapp"), None);
    }

    // TestCacheUpdate: een update vervangt het hele cluster.
    #[test]
    fn cache_update() {
        let mut c = Cache::new();
        c.set("prod", "app1", vec![ip("10.0.0.1")]).unwrap();
        let mut data = Jobs::new();
        data.insert("app2", vec![ip("10.0.0.2")]).unwrap();
        data.insert("app3", vec![ip("10.0.0.3")]).unwrap();
        c.update("prod", data).unwrap();
        assert_eq!(c.get_cluster("prod", "app1"), None, "app1 should be gone");
        assert_eq!(c.get_cluster("prod", "app2").unwrap().len(), 1);
        assert_eq!(c.get_cluster("prod", "app3").unwrap().len(), 1);
    }

    // TestCacheClear: clear raakt alleen dat cluster.
    #[test]
    fn cache_clear() {
        let mut c = Cache::new();
        c.set("prod", "myapp", vec![ip("10.0.0.1")]).unwrap();
        c.set("staging", "myapp", vec![ip("10.0.1.1")]).unwrap();
        c.clear("prod");
        assert_eq!(c.get_cluster("prod", "myapp"), None);
        assert_eq!(c.get_cluster("staging", "myapp").unwrap().len(), 1);
    }

    // TestCacheGetMergedDedupes: over clusters samengevoegd, zonder dubbelen,
    // en een andere job telt niet mee.
    #[test]
    fn cache_get_merged_dedupes() {
        let mut c = Cache::new();
        c.set("a", "web", vec![ip("10.0.0.1"), ip("10.0.0.2")])
            .unwrap();
        c.set("b", "web", vec![ip("10.0.0.2"), ip("10.0.0.3")])
            .unwrap();
        c.set("a", "other", vec![ip("10.9.9.9")]).unwrap();
        let merged = c.get_merged("web").unwrap();
        assert_eq!(merged.len(), 3, "{merged:?}");
    }

    #[test]
    fn job_count_is_distinct_names() {
        let mut c = Cache::new();
        c.set("a", "web", vec![]).unwrap();
        c.set("b", "web", vec![]).unwrap();
        c.set("b", "db", vec![]).unwrap();
        assert_eq!(c.jobs(), 2);
        assert_eq!(c.clusters(), 2);
        assert!(c.has_job("db") && !c.has_job("x"));
        assert!(c.has_cluster("b") && !c.has_cluster("c"));
    }
}
