//! `priority_test.go`, `normalize_priorities_test.go`, `trim_surplus_test.go`,
//! `delete_test.go` en `delete_by_name_test.go`.

use std::format;
use std::string::ToString;

use types::Map;
use types::time::HOUR;

use crate::dispatch::effective_priority;
use crate::testkit::*;
use crate::{JobStore as _, MemStore};

#[test]
fn effective_priority_orders_unset_last() {
    // Go: TestEffectivePriority.
    for (p, want) in [
        (Some(0), 0),
        (Some(1), 1),
        (Some(5), 5),
        (Some(100), 100),
        (None, i64::MAX),
    ] {
        assert_eq!(effective_priority(p), want);
    }
}

fn one_slot(l: &mut crate::Leader<MemStore>, net: &mut FakeNet, cap: usize) {
    join(l, net, "agent-1", NOW);
    net.get("agent-1").max_capacity = cap;
}

#[test]
fn preemption_high_priority_evicts_low() {
    let mut l = leader();
    let mut net = FakeNet::new();
    one_slot(&mut l, &mut net, 1);
    l.dispatch_job(job_prio("batch", 1, 10), &mut net).unwrap();
    assert_eq!(net.get("agent-1").task_count(), 1);
    l.dispatch_job(job_prio("critical", 1, 1), &mut net)
        .unwrap();
    assert_eq!(net.get("agent-1").task_count(), 1);
    assert_eq!(net.get("agent-1").tasks_for_job("critical"), 1);
}

#[test]
fn preemption_low_priority_cannot_evict_high() {
    let mut l = leader();
    let mut net = FakeNet::new();
    one_slot(&mut l, &mut net, 1);
    l.dispatch_job(job_prio("critical", 1, 1), &mut net)
        .unwrap();
    assert!(l.dispatch_job(job_prio("batch", 1, 10), &mut net).is_err());
    assert_eq!(net.get("agent-1").tasks_for_job("batch"), 0);
}

#[test]
fn preemption_prio0_is_unevictable() {
    let mut l = leader();
    let mut net = FakeNet::new();
    one_slot(&mut l, &mut net, 1);
    l.dispatch_job(job_prio("first", 1, 0), &mut net).unwrap();
    assert!(l.dispatch_job(job_prio("later", 1, 5), &mut net).is_err());
    assert_eq!(net.get("agent-1").tasks_for_job("later"), 0);
}

#[test]
fn preemption_nil_priority_is_lowest() {
    let mut l = leader();
    let mut net = FakeNet::new();
    one_slot(&mut l, &mut net, 1);
    l.dispatch_job(job("background", 1), &mut net).unwrap();
    l.dispatch_job(job_prio("foreground", 1, 99), &mut net)
        .unwrap();
    assert_eq!(net.get("agent-1").tasks_for_job("foreground"), 1);
}

#[test]
fn preemption_affinity_agent_never_evicts() {
    let mut l = leader();
    let mut net = FakeNet::new();
    join(&mut l, &mut net, "agent-a", NOW);
    join(&mut l, &mut net, "agent-b", NOW);
    net.get("agent-a").reject_affinity = true;
    net.get("agent-b").fail_runs = true;
    assert!(
        l.dispatch_job(job_prio("critical", 1, 1), &mut net)
            .is_err()
    );
    assert!(net.get("agent-a").stops.is_empty());
}

#[test]
fn preemption_chooses_lowest_priority() {
    let mut l = leader();
    let mut net = FakeNet::new();
    one_slot(&mut l, &mut net, 2);
    l.dispatch_job(job_prio("medium", 1, 5), &mut net).unwrap();
    l.dispatch_job(job_prio("batch", 1, 20), &mut net).unwrap();
    assert_eq!(net.get("agent-1").task_count(), 2);
    l.dispatch_job(job_prio("critical", 1, 1), &mut net)
        .unwrap();
    assert_eq!(net.get("agent-1").tasks_for_job("batch"), 0);
    assert_eq!(net.get("agent-1").tasks_for_job("medium"), 1);
    assert_eq!(net.get("agent-1").tasks_for_job("critical"), 1);
}

#[test]
fn patch_priority_triggers_preemption() {
    let mut l = leader();
    let mut net = FakeNet::new();
    one_slot(&mut l, &mut net, 1);
    l.dispatch_job(job_prio("counter", 1, 10), &mut net)
        .unwrap();
    let _ = l.dispatch_job(job_prio("counter2", 1, 20), &mut net);
    l.patch_job_priority("counter2", 0, &mut net).unwrap();
    assert_eq!(net.get("agent-1").tasks_for_job("counter2"), 1);
    assert_eq!(net.get("agent-1").tasks_for_job("counter"), 0);
    // Dicht genummerd: counter2 bovenaan, counter erna.
    assert_eq!(l.job("counter2").unwrap().priority, Some(0));
    assert_eq!(l.job("counter").unwrap().priority, Some(1));
}

fn fleet(l: &mut crate::Leader<MemStore>, net: &mut FakeNet, n: usize, cap: usize) {
    for i in 0..n {
        let id = format!("agent-{i}");
        join(l, net, &id, NOW);
        net.get(&id).max_capacity = cap;
    }
}

#[test]
fn multi_agent_preemption_fills_all_agents() {
    let (n, cap) = (4, 4);
    let mut l = leader();
    let mut net = FakeNet::new();
    fleet(&mut l, &mut net, n, cap);
    l.dispatch_job(job_prio("low", (n * cap) as i64, 10), &mut net)
        .unwrap();
    assert_eq!(net.total_tasks(), n * cap);
    let _ = l.dispatch_job(job_prio("high", (n * cap) as i64, 1), &mut net);
    for i in 0..n {
        let a = net.get(&format!("agent-{i}"));
        assert_eq!(a.task_count(), cap, "agent-{i}");
        assert_eq!(a.tasks_for_job("low"), 0, "agent-{i}");
    }
}

#[test]
fn multi_agent_patch_preemption() {
    let (n, cap) = (4, 4);
    let mut l = leader();
    let mut net = FakeNet::new();
    fleet(&mut l, &mut net, n, cap);
    l.dispatch_job(job_prio("counter2", (n * cap) as i64, 0), &mut net)
        .unwrap();
    let _ = l.dispatch_job(job_prio("counter", (n * cap) as i64, 1), &mut net);
    l.patch_job_priority("counter", 0, &mut net).unwrap();
    for i in 0..n {
        let a = net.get(&format!("agent-{i}"));
        assert_eq!(a.task_count(), cap, "agent-{i}");
        assert_eq!(a.tasks_for_job("counter2"), 0, "agent-{i}");
    }
}

// 4 agents van 14 plekken = 56; beide jobs willen er 100. Wie bovenaan
// gesleept wordt krijgt alle 56.
#[test]
fn drag_above_oversized_count() {
    let (n, cores) = (4, 14);
    let mut l = leader();
    let mut net = FakeNet::new();
    fleet(&mut l, &mut net, n, cores);
    let _ = l.dispatch_job(job_prio("jobB", 100, 0), &mut net);
    assert_eq!(net.total_tasks(), n * cores);
    let _ = l.dispatch_job(job_prio("jobA", 100, 1), &mut net);
    l.patch_job_priority("jobA", 0, &mut net).unwrap();
    for i in 0..n {
        let a = net.get(&format!("agent-{i}"));
        assert_eq!(a.task_count(), cores, "agent-{i}");
        assert_eq!(a.tasks_for_job("jobB"), 0, "agent-{i}");
    }
}

// ---- normalize_priorities_test.go ----

// Hernummeren raakt alleen de prioriteit: een hele-job-schrijf overschreef
// het deploying = false van een net geslaagde update (traqqr, 2026-09-08).
#[test]
fn normalize_priorities_only_touches_priority() {
    let mut l = leader();
    let mut api = job_prio("api", 1, 9);
    api.command = "./api".to_string();
    api.deploying = true;
    l.store_put(api).unwrap();
    let mut db = job("db", 1);
    db.command = "./db".to_string();
    l.store_put(db).unwrap();
    // De update rondt af en wist de vlag ...
    assert!(l.store_set_deploying("api", false));
    // ... en dan pas nummert de reconcile.
    l.normalize_priorities().unwrap();
    let api = l.job("api").unwrap();
    assert!(!api.deploying);
    assert_eq!(api.priority, Some(0));
    let db = l.job("db").unwrap();
    assert_eq!(db.priority, Some(1));
    assert_eq!(db.command, "./db");
}

#[test]
fn normalize_priorities_does_not_resurrect_deleted_job() {
    let mut l = leader();
    l.store_put(job("gone", 1)).unwrap();
    l.store_put(job("kept", 1)).unwrap();
    l.store_remove("gone");
    l.normalize_priorities().unwrap();
    assert!(l.job("gone").is_none());
    assert!(l.job("kept").unwrap().priority.is_some());
}

#[test]
fn set_job_deploying_is_copy_on_write() {
    // In Rust deelt niemand een pointer met de store; de bewering die
    // overblijft: alleen de vlag verandert, en een onbekende job geeft false.
    let mut store = MemStore::new();
    let mut j = job("api", 1);
    j.command = "./api".to_string();
    j.deploying = true;
    store.put(j.clone()).unwrap();
    assert!(store.set_deploying("api", false));
    assert!(j.deploying);
    let cur = store.get("api").unwrap();
    assert!(!cur.deploying);
    assert_eq!(cur.command, "./api");
    assert!(!store.set_deploying("nope", false));
}

// ---- trim_surplus_test.go ----

fn trim_setup(count: i64) -> (crate::Leader<MemStore>, FakeNet) {
    let mut l = leader();
    // Nooit settled: registreren trimt en reconcilet dan niet vanzelf.
    l.settle(HOUR, NOW);
    let mut net = FakeNet::new();
    l.store_put(job("app", count)).unwrap();
    let mut one = Map::new();
    one.insert("app".to_string(), 1).unwrap();
    for id in ["b", "c"] {
        let a = agent(id, &format!("http://10.0.0.{id}:8080"));
        l.register_agent(a, one.clone(), NOW, &mut net).unwrap();
    }
    let ep = net.add("returning");
    net.get("returning").add_tasks("app", 1);
    l.register_agent(agent("returning", &ep), one, NOW, &mut net)
        .unwrap();
    (l, net)
}

#[test]
fn trim_returning_agent_surplus() {
    // Overschot wordt getrimd.
    let (mut l, mut net) = trim_setup(2);
    l.trim_returning_agent_surplus("returning", &mut net)
        .unwrap();
    assert_eq!(l.placed_counts().unwrap().get("app"), Some(&2));
    assert!(!l.placed("app").unwrap().contains_key("returning"));
    assert_eq!(net.get("returning").stops, ["app"]);

    // Een gat blijft gevuld.
    let (mut l, mut net) = trim_setup(3);
    l.trim_returning_agent_surplus("returning", &mut net)
        .unwrap();
    assert_eq!(l.placed_counts().unwrap().get("app"), Some(&3));

    // Een daemon wordt nooit getrimd.
    let (mut l, mut net) = trim_setup(-1);
    l.trim_returning_agent_surplus("returning", &mut net)
        .unwrap();
    assert_eq!(l.placed_counts().unwrap().get("app"), Some(&3));
}

// ---- delete_test.go en delete_by_name_test.go ----

#[test]
fn delete_job_removes_from_store() {
    let mut l = leader();
    let mut net = FakeNet::new();
    l.store_put(job("to-delete", 1)).unwrap();
    assert_eq!(l.jobs().len(), 1);
    l.delete_job("to-delete", &mut net).unwrap();
    assert!(l.jobs().is_empty());
    assert!(l.job("to-delete").is_none());
}

#[test]
fn delete_job_with_no_placement_still_removes_from_store() {
    let mut l = leader();
    let mut net = FakeNet::new();
    l.store_put(job("orphan", 1)).unwrap();
    l.delete_job("orphan", &mut net).unwrap();
    assert!(l.jobs().is_empty());
}

#[test]
fn delete_job_twice_does_not_error() {
    let mut l = leader();
    let mut net = FakeNet::new();
    l.store_put(job("test", 1)).unwrap();
    l.delete_job("test", &mut net).unwrap();
    l.delete_job("test", &mut net).unwrap();
    assert!(l.jobs().is_empty());
}

#[test]
fn delete_job_by_name() {
    let mut l = leader();
    let mut net = FakeNet::new();
    join(&mut l, &mut net, "agent-1", NOW);
    l.dispatch_job(job("test-app", 1), &mut net).unwrap();
    assert!(l.job("test-app").is_some());
    l.delete_job("test-app", &mut net).unwrap();
    assert!(l.job("test-app").is_none());
    assert_eq!(net.get("agent-1").task_count(), 0);
    assert_eq!(placed_total(&l, "test-app"), 0);
}

// Go: een her-submit tijdens een lopende delete mocht niet door de
// naveeg-sweep weggeveegd worden. Met één eigenaar kan een delete niet
// "lopen" terwijl een dispatch binnenkomt; de bewering die overblijft is
// dat een her-submit direct na de delete gewoon staat en draait.
#[test]
fn delete_sweep_spaart_hersubmit() {
    let mut l = leader();
    let mut net = FakeNet::new();
    join(&mut l, &mut net, "agent-1", NOW);
    let mut v1 = job("app", 1);
    v1.command = "./v1".to_string();
    l.dispatch_job(v1, &mut net).unwrap();
    l.delete_job("app", &mut net).unwrap();
    let mut v2 = job("app", 1);
    v2.command = "./v2".to_string();
    l.dispatch_job(v2, &mut net).unwrap();
    assert_eq!(l.job("app").unwrap().command, "./v2");
    assert_eq!(net.get("agent-1").tasks_for_job("app"), 1);
}

#[test]
fn get_job_by_name() {
    let mut l = leader();
    for n in ["app-1", "app-2", "app-3"] {
        l.store_put(job(n, 1)).unwrap();
    }
    assert_eq!(l.job("app-2").unwrap().name, "app-2");
    assert!(l.job("nonexistent").is_none());
}
