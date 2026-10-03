//! `placed_reporting_test.go`: `placed_counts` voor jobs uit de store, net
//! geplaatste jobs, daemons, updates en de settle-periode.

use std::string::ToString;

use types::time::MILLISECOND;
use types::{Job, Map};

use crate::testkit::*;
use crate::{JobStore as _, Leader, MemStore};

fn counts(pairs: &[(&str, u32)]) -> Map<u32> {
    let mut m = Map::new();
    for (k, v) in pairs {
        m.insert(k.to_string(), *v).unwrap();
    }
    m
}

/// Een leider over een store die al jobs heeft (van schijf of snapshot).
fn with_store(jobs: &[Job]) -> Leader<MemStore> {
    let mut store = MemStore::new();
    for j in jobs {
        store.put(j.clone()).unwrap();
    }
    store.set_state_time(NOW);
    Leader::new("leader".to_string(), store)
}

/// Registreert een agent zonder nep: niemand antwoordt, alleen de telling telt.
fn register_bare(l: &mut Leader<MemStore>, net: &mut FakeNet, id: &str, placed: Map<u32>) {
    let ep = std::format!("http://{id}:8080");
    assert!(l.register_agent(agent(id, &ep), placed, NOW, net).unwrap());
}

fn placed_of(l: &Leader<MemStore>, name: &str) -> u32 {
    l.placed_counts().unwrap().get(name).copied().unwrap_or(0)
}

#[test]
fn get_placed_counts_pre_existing_jobs() {
    let mut l = with_store(&[job("webapp", 3), job("api", 2), job("worker", 1)]);
    let mut net = FakeNet::new();
    register_bare(
        &mut l,
        &mut net,
        "agent-1",
        counts(&[("webapp", 2), ("api", 1), ("worker", 1)]),
    );
    register_bare(
        &mut l,
        &mut net,
        "agent-2",
        counts(&[("webapp", 1), ("api", 1)]),
    );
    assert!(l.heartbeat("agent-1", "", Default::default(), NOW));
    assert!(l.heartbeat("agent-2", "", Default::default(), NOW));

    assert_eq!(placed_of(&l, "webapp"), 3);
    assert_eq!(placed_of(&l, "api"), 2);
    assert_eq!(placed_of(&l, "worker"), 1);
    let total: u32 = l.placed_counts().unwrap().iter().map(|(_, n)| *n).sum();
    assert_eq!(total, 6);
}

#[test]
fn get_placed_counts_after_dispatch() {
    let mut l = with_store(&[]);
    let mut net = FakeNet::new();
    join(&mut l, &mut net, "agent-1", NOW);
    l.dispatch_job(job("myapp", 3), &mut net).unwrap();
    assert_eq!(placed_of(&l, "myapp"), 3);
}

#[test]
fn get_placed_counts_mixed_pre_existing_and_new() {
    let mut l = with_store(&[job("existing", 2)]);
    let mut net = FakeNet::new();
    let ep = net.add("agent-1");
    l.register_agent(
        agent("agent-1", &ep),
        counts(&[("existing", 2)]),
        NOW,
        &mut net,
    )
    .unwrap();
    l.dispatch_job(job("new-service", 1), &mut net).unwrap();
    assert_eq!(placed_of(&l, "existing"), 2);
    assert_eq!(placed_of(&l, "new-service"), 1);
}

#[test]
fn get_placed_counts_daemon_job() {
    let mut l = with_store(&[job("hopdns", -1)]);
    let mut net = FakeNet::new();
    for id in ["agent-1", "agent-2", "agent-3"] {
        register_bare(&mut l, &mut net, id, counts(&[("hopdns", 1)]));
    }
    assert_eq!(placed_of(&l, "hopdns"), 3);
}

#[test]
fn get_placed_counts_updated_job() {
    let mut l = with_store(&[]);
    let mut net = FakeNet::new();
    join(&mut l, &mut net, "agent-1", NOW);
    join(&mut l, &mut net, "agent-2", NOW);
    l.dispatch_job(job("myapp", 2), &mut net).unwrap();
    assert_eq!(placed_of(&l, "myapp"), 2);

    let v2 = Job {
        command: "./myapp-v2".to_string(),
        ..job("myapp", 2)
    };
    l.update_job(v2, &mut net).unwrap();
    assert_eq!(placed_of(&l, "myapp"), 2);
}

#[test]
fn get_placed_counts_leader_with_settle_period() {
    let mut l = with_store(&[job("job-a", 5), job("job-b", 3)]);
    l.settle(200 * MILLISECOND, NOW);
    let mut net = FakeNet::new();
    register_bare(
        &mut l,
        &mut net,
        "agent-1",
        counts(&[("job-a", 3), ("job-b", 2)]),
    );
    register_bare(
        &mut l,
        &mut net,
        "agent-2",
        counts(&[("job-a", 2), ("job-b", 1)]),
    );

    // Ook tijdens settle kloppen de tellingen.
    assert!(!l.is_settled());
    assert_eq!(placed_of(&l, "job-a"), 5);
    assert_eq!(placed_of(&l, "job-b"), 3);

    l.tick(at_ms(300), &mut net).unwrap();
    assert!(l.is_settled());
    assert_eq!(placed_of(&l, "job-a"), 5);
    assert_eq!(placed_of(&l, "job-b"), 3);
}

#[test]
fn job_store_seeded_at_init() {
    let jobs = [job("svc-a", 1), job("svc-b", 1), job("svc-c", 1)];
    let l = with_store(&jobs);
    for j in &jobs {
        assert_eq!(l.job(&j.name).unwrap().name, j.name);
        assert_eq!(l.store().get(&j.name).unwrap().name, j.name);
    }
}
