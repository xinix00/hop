//! `robustness_test.go`: placed-gedreven reconcile onder storingen.
//!
//! Go wachtte met `time.Sleep` tot de agent-timeout verliep; hier schuift
//! "nu" op en roept de test zelf `check_dead_agents`.

use std::string::ToString;

use types::time::{MILLISECOND, SECOND};
use types::{Map, Nanos};

use crate::testkit::*;
use crate::{Leader, MemStore};

fn cluster(ids: &[&str], timeout: Nanos) -> (Leader<MemStore>, FakeNet) {
    let mut l = Leader::new("leader".to_string(), MemStore::new());
    l.set_agent_timeout(timeout);
    let mut net = FakeNet::new();
    for id in ids {
        join(&mut l, &mut net, id, NOW);
    }
    (l, net)
}

#[test]
fn three_node_cluster_one_dies() {
    let (mut l, mut net) = cluster(&["agent-a", "agent-b", "agent-c"], 200 * MILLISECOND);
    l.dispatch_job(job("app", 30), &mut net).unwrap();
    let a = net.get("agent-a").task_count();
    let c = net.get("agent-c").task_count();
    assert_eq!(net.total_tasks(), 30);

    net.get("agent-b").down = true;
    l.heartbeat("agent-a", "", 0, at_ms(300));
    l.heartbeat("agent-c", "", 0, at_ms(300));
    l.check_dead_agents(at_ms(300), &mut net).unwrap();

    let a2 = net.get("agent-a").task_count();
    let c2 = net.get("agent-c").task_count();
    assert_eq!(a2 + c2, 30);
    // Round-robin: beide overlevers krijgen een deel van B.
    assert!(a2 > a && c2 > c, "A {a}->{a2}, C {c}->{c2}");
}

#[test]
fn daemon_stable_during_blip_new_node_joins() {
    let (mut l, mut net) = cluster(&["agent-a", "agent-b"], 2 * SECOND);
    l.dispatch_job(job("daemon", -1), &mut net).unwrap();
    assert_eq!(net.get("agent-a").task_count(), 1);
    assert_eq!(net.get("agent-b").task_count(), 1);
    let a_runs = net.get("agent-a").run_calls;
    let b_runs = net.get("agent-b").run_calls;

    // A zwijgt even (onder de timeout); C komt erbij.
    l.heartbeat("agent-b", "", 0, at_ms(100));
    join(&mut l, &mut net, "agent-c", at_ms(100));
    assert_eq!(net.get("agent-c").task_count(), 1);
    assert_eq!(net.get("agent-a").run_calls, a_runs);
    assert_eq!(net.get("agent-b").run_calls, b_runs);

    l.heartbeat("agent-a", "", 0, at_ms(150));
    assert_eq!(l.placed("daemon").unwrap().len(), 3);
}

#[test]
fn heartbeat_reduced_placed_triggers_reconcile() {
    let (mut l, mut net) = cluster(&["agent-a", "agent-b"], 30 * SECOND);
    l.dispatch_job(job("app", 20), &mut net).unwrap();
    let b = net.get("agent-b").task_count();

    // B verliest drie taken en meldt zich opnieuw met de echte telling.
    net.get("agent-b").tasks.truncate(b - 3);
    assert!(rejoin(&mut l, &mut net, "agent-b", at(1)));
    assert_eq!(net.total_tasks(), 20);
    assert_eq!(placed_total(&l, "app"), 20);
}

#[test]
fn mixed_jobs_agent_dies() {
    let (mut l, mut net) = cluster(&["agent-a", "agent-b"], 200 * MILLISECOND);
    l.dispatch_job(job("daemon", -1), &mut net).unwrap();
    l.dispatch_job(job("web", 4), &mut net).unwrap();
    assert_eq!(net.get("agent-b").task_count(), 3);
    let b_runs = net.get("agent-b").run_calls;

    net.get("agent-a").down = true;
    l.heartbeat("agent-b", "", 0, at_ms(300));
    l.check_dead_agents(at_ms(300), &mut net).unwrap();

    // Alleen de twee gewone taken verhuizen; B had zijn daemon al.
    assert_eq!(net.get("agent-b").run_calls - b_runs, 2);
    assert_eq!(l.placed("daemon").unwrap().len(), 1);
    assert_eq!(placed_total(&l, "web"), 4);
}

#[test]
fn graceful_leave_three_nodes() {
    let (mut l, mut net) = cluster(&["agent-a", "agent-b", "agent-c"], 30 * SECOND);
    l.dispatch_job(job("app", 30), &mut net).unwrap();

    l.unregister_agent("agent-b", &mut net).unwrap();
    let total = net.get("agent-a").task_count() + net.get("agent-c").task_count();
    assert_eq!(total, 30);
    assert!(l.agent("agent-b").is_none());
    assert!(l.placed("app").unwrap().get("agent-b").is_none());
}

#[test]
fn agent_dies_and_rejoins_gets_daemon() {
    let (mut l, mut net) = cluster(&["agent-a", "agent-b"], 200 * MILLISECOND);
    l.dispatch_job(job("daemon", -1), &mut net).unwrap();
    assert_eq!(net.get("agent-a").task_count(), 1);
    assert_eq!(net.get("agent-b").task_count(), 1);

    net.get("agent-b").down = true;
    l.heartbeat("agent-a", "", 0, at_ms(300));
    l.check_dead_agents(at_ms(300), &mut net).unwrap();
    // Een daemon verhuist niet: A had hem al.
    assert_eq!(net.get("agent-a").task_count(), 1);
    assert_eq!(l.placed("daemon").unwrap().len(), 1);

    // B komt terug als vers proces, zonder taken.
    net.agents
        .insert("http://agent-b".to_string(), MockAgent::default());
    l.register_agent(
        agent("agent-b", "http://agent-b"),
        Map::new(),
        at_ms(400),
        &mut net,
    )
    .unwrap();
    assert_eq!(net.get("agent-b").task_count(), 1);
    assert_eq!(l.placed("daemon").unwrap().len(), 2);
}

#[test]
fn zombie_agent_no_over_scheduling() {
    let (mut l, mut net) = cluster(&["agent-a", "agent-b"], 200 * MILLISECOND);
    l.dispatch_job(job("app", 20), &mut net).unwrap();
    let a = net.get("agent-a").task_count();

    net.get("agent-a").down = true;
    l.heartbeat("agent-b", "", 0, at_ms(300));
    l.check_dead_agents(at_ms(300), &mut net).unwrap();
    assert_eq!(net.get("agent-b").task_count(), 20);
    let b_runs = net.get("agent-b").run_calls;

    // A komt terug met zijn taken nog draaiend (de partitie heelde).
    let mut zombie = MockAgent::default();
    zombie.add_tasks("app", a);
    net.agents.insert("http://agent-a".to_string(), zombie);
    assert!(rejoin(&mut l, &mut net, "agent-a", at_ms(400)));
    assert!(l.heartbeat("agent-a", "", 0, at_ms(400)));

    // Geen enkele nieuwe /run.
    assert_eq!(net.get("agent-a").run_calls, 0);
    assert_eq!(net.get("agent-b").run_calls, b_runs);
    // Anders dan in Go (dat bleef op 30 staan): het overschot van de
    // terugkeerder wordt gestopt, want B draagt het aantal al.
    assert_eq!(net.get("agent-a").tasks_for_job("app"), 0);
    assert_eq!(placed_total(&l, "app"), 20);
}
