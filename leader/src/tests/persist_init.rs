//! `persist_test.go` en `init_test.go`.
//!
//! Go had een persist-goroutine met een debounce-timer en een `fakePersister`;
//! hier geeft [`Leader::poll_snapshot`] de bytes en is de klok een getal.

use std::string::ToString;

use types::json::{self, Value};
use types::time::MILLISECOND;
use types::{Job, Time};

use crate::testkit::*;
use crate::{Error, Leader, MemStore, PERSIST_DEBOUNCE, decode_init_jobs as decode};

fn snapshot_names(s: &str) -> std::vec::Vec<std::string::String> {
    let v = json::parse_str(s).unwrap();
    v.as_object()
        .unwrap()
        .get("jobs")
        .unwrap()
        .as_array()
        .unwrap()
        .iter()
        .map(|j| {
            j.as_object()
                .unwrap()
                .get("name")
                .unwrap()
                .as_str()
                .unwrap()
                .to_string()
        })
        .collect()
}

// Elke mutatie maakt de staat vies; de snapshot komt na de debounce, één
// voor de hele golf; een delete is afwezigheid in de volgende.
#[test]
fn committed_state_mutaties_landen_in_snapshot() {
    let mut l = leader();
    let mut net = FakeNet::new();
    assert_eq!(l.poll_snapshot(NOW).unwrap(), None);
    l.dispatch_job(job("aap", 1), &mut net).unwrap_or(());
    l.dispatch_job(job("noot", 1), &mut net).unwrap_or(());
    // Eerste blik start het venster; binnen het venster nog niets.
    assert_eq!(l.poll_snapshot(NOW).unwrap(), None);
    assert_eq!(
        l.poll_snapshot(Time(NOW.0 + 500 * MILLISECOND)).unwrap(),
        None
    );
    let t1 = Time(NOW.0 + PERSIST_DEBOUNCE);
    let snap = l.poll_snapshot(t1).unwrap().unwrap();
    assert_eq!(snapshot_names(&snap), ["aap", "noot"]);
    // Schoon: geen tweede snapshot zonder mutatie.
    assert_eq!(l.poll_snapshot(at(10)).unwrap(), None);

    l.delete_job("aap", &mut net).unwrap();
    assert_eq!(l.poll_snapshot(at(20)).unwrap(), None);
    let snap = l.poll_snapshot(at(21)).unwrap().unwrap();
    assert_eq!(snapshot_names(&snap), ["noot"]);

    // Een mislukte schrijf maakt de staat weer vies.
    l.snapshot_failed();
    assert_eq!(l.poll_snapshot(at(30)).unwrap(), None);
    assert!(l.poll_snapshot(at(31)).unwrap().is_some());
}

// Een snapshot laadt de store, en alleen de snapshot: een lokale job die er
// niet in staat is elders verwijderd. Geen snapshot is een schone boot.
#[test]
fn committed_state_boot_load() {
    let mut old = leader();
    old.store_put(job("terug", 1)).unwrap();
    let snap = old.snapshot(NOW).unwrap();

    let mut l = leader();
    l.store_put(job("spook", 1)).unwrap();
    assert!(l.load_committed_state(Some(snap.as_bytes())).unwrap());
    assert!(l.job("terug").is_some());
    assert!(l.job("spook").is_none());
    assert_eq!(l.state_time(), NOW);

    let mut clean = leader();
    assert!(!clean.load_committed_state(None).unwrap());
    assert!(clean.jobs().is_empty());

    // Een kapotte snapshot is luid, niet half geladen.
    let mut broken = leader();
    assert!(broken.load_committed_state(Some(b"{\"jobs\": [")).is_err());
}

// ---- init_test.go ----

fn specs(src: &str) -> std::vec::Vec<Value> {
    json::parse_str(src).unwrap().as_array().unwrap().to_vec()
}

#[test]
fn decode_init_jobs() {
    let jobs = decode(&specs(
        r#"[
      {"name": "hopdns", "command": "/usr/local/bin/hopdns", "count": -1, "max_restarts": 0,
       "ports": {"dns": 5353}, "tags": {"lb": "none"}},
      {"name": "redis", "image": "redis:7", "count": 2}]"#,
    ))
    .unwrap();
    assert_eq!(jobs.len(), 2);
    let dns = &jobs[0];
    assert_eq!(dns.count, -1);
    assert_eq!(dns.ports.get("dns"), Some(&5353));
    assert_eq!(dns.tags.get("lb").unwrap(), "none");
    // 0 blijft expliciet 0 (geen restarts), niet "niet gezet".
    assert_eq!(dns.max_restarts, Some(0));
    assert_eq!(jobs[1].image, "redis:7");

    for (naam, spec) in [
        (
            "onbekend veld",
            r#"[{"name": "x", "command": "y", "comand": "typo"}]"#,
        ),
        ("naam ontbreekt", r#"[{"command": "y"}]"#),
        ("niets om te draaien", r#"[{"name": "x"}]"#),
    ] {
        let err = decode(&specs(spec)).expect_err(naam);
        assert!(
            matches!(err, Error::InitJob { index: 0, .. }),
            "{naam}: {err}"
        );
    }
}

#[test]
fn decode_init_jobs_from_config() {
    let src = r#"{
	  "cluster": {
	    "name": "dev",
	    "init_jobs": [
	      {"name": "hopdns", "command": "/usr/local/bin/hopdns", "count": -1,
	       "ports": {"dns": 5353}, "env": {"HOP_MODE": "init"}},
	      {"name": "my-app", "image": "myapp:v1", "count": 2,
	       "tags": {"hoplb-urlprefix": "*.app.local"}}
	    ]
	  }
	}"#;
    let cfg = config::Config::from_json(src.as_bytes()).unwrap();
    let jobs = decode(&cfg.cluster.init_jobs).unwrap();
    assert_eq!(jobs.len(), 2);
    assert_eq!(jobs[0].ports.get("dns"), Some(&5353));
    assert_eq!(jobs[0].env.get("HOP_MODE").unwrap(), "init");
    assert_eq!(jobs[1].tags.get("hoplb-urlprefix").unwrap(), "*.app.local");
}

#[test]
fn seed_init_jobs() {
    let mut l: Leader<MemStore> = leader();
    let mut bestaand = job("bestaand", 1);
    bestaand.command = "operator-versie".to_string();
    l.store_put(bestaand).unwrap();
    l.enable_settle(NOW);
    let mut net = FakeNet::new();
    let mut init_versie = job("bestaand", 1);
    init_versie.command = "init-versie".to_string();
    let app = Job {
        name: "app".to_string(),
        image: "myapp:v1".to_string(),
        count: 2,
        ..Job::default()
    };
    l.seed_init_jobs(std::vec![job("hopdns", -1), init_versie, app], &mut net)
        .unwrap();
    assert_eq!(l.job("hopdns").unwrap().count, -1);
    assert_eq!(l.job("app").unwrap().image, "myapp:v1");
    assert_eq!(l.job("bestaand").unwrap().command, "operator-versie");
    let dns = l.job("hopdns").unwrap().priority.unwrap();
    let app = l.job("app").unwrap().priority.unwrap();
    assert!(dns < app, "de configvolgorde: dns={dns} app={app}");
    // Tijdens settle: opgeslagen, niet gedispatcht.
    assert_eq!(net.total_tasks(), 0);
}

// Eén init-job met meerdere artifacts overspant meerdere architecturen;
// zonder driver geldt de afkorting alleen voor precies één artifact.
#[test]
fn decode_init_jobs_meerdere_artifacts_per_architectuur() {
    let jobs = decode(&specs(
        r#"[{"name": "welcome", "driver": "hop",
          "artifacts": [
            {"url": "https://example.com/welcome-arm64.elf", "match": {"node.arch": "arm64"}},
            {"url": "https://example.com/welcome-riscv64.elf", "match": {"node.arch": "riscv64"}}],
          "memory_limit": 67108864, "ports": {"http": 80}}]"#,
    ))
    .unwrap();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].artifacts.len(), 2);
    assert_eq!(
        jobs[0].artifacts[1].matches.get("node.arch").unwrap(),
        "riscv64"
    );

    assert!(
        decode(&specs(
            r#"[{"name": "welcome", "artifacts": [
              {"url": "https://example.com/a.elf"}, {"url": "https://example.com/b.elf"}]}]"#
        ))
        .is_err()
    );
    // Met precies één artifact vult de afkorting de hop-driver in.
    let one = decode(&specs(
        r#"[{"name": "welcome", "artifacts": [{"url": "https://example.com/a.elf"}]}]"#,
    ))
    .unwrap();
    assert_eq!(one[0].driver, Some(types::Driver::Hop));
}
