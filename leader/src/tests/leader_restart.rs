//! `leader_restart_test.go`: een nieuwe leider en een herstartende agent.

use std::format;
use std::string::ToString;
use std::vec::Vec;

use types::Map;
use types::time::{MILLISECOND, SECOND};

use crate::testkit::*;
use crate::{Leader, MemStore};

#[test]
fn leader_restart_reject_unknown_heartbeat() {
    // Fase 1: twintig taken over vier agents.
    let mut leader1 = Leader::new("agent-0".to_string(), MemStore::new());
    let mut net = FakeNet::new();
    for i in 0..4 {
        join(&mut leader1, &mut net, &format!("agent-{i}"), NOW);
    }
    leader1.dispatch_job(job("my-job", 20), &mut net).unwrap();
    let before: Vec<usize> = (0..4)
        .map(|i| net.get(&format!("agent-{i}")).task_count())
        .collect();
    assert_eq!(before.iter().sum::<usize>(), 20);

    // Fase 2 en 3: de leider valt weg; een nieuwe erft de store, met settle.
    let mut leader2 = Leader::new("agent-0".to_string(), leader1.into_store());
    leader2.settle(300 * MILLISECOND, NOW);
    assert!(rejoin(&mut leader2, &mut net, "agent-0", NOW));

    // Onbekende agents krijgen `false` op hun heartbeat (de adapter: 404).
    for i in 1..4 {
        let id = format!("agent-{i}");
        assert!(!leader2.heartbeat(&id, "", 0, NOW), "{id}");
    }
    assert_eq!(leader2.agents().len(), 1);

    // Na de 404 registreren ze zich opnieuw, met hun tellingen.
    for i in 1..4 {
        assert!(rejoin(&mut leader2, &mut net, &format!("agent-{i}"), NOW));
    }
    leader2.tick(at_ms(500), &mut net).unwrap();
    assert!(leader2.is_settled());

    // Geen overschot.
    assert_eq!(net.total_tasks(), 20);
    for (i, n) in before.iter().enumerate() {
        assert_eq!(net.get(&format!("agent-{i}")).task_count(), *n);
    }
}

#[test]
fn agent_restart_within_timeout_placed_stale() {
    let mut l = Leader::new("leader".to_string(), MemStore::new());
    // Lang genoeg dat de herstartende agent niet dood verklaard wordt.
    l.set_agent_timeout(30 * SECOND);
    let mut net = FakeNet::new();
    for i in 0..4 {
        join(&mut l, &mut net, &format!("agent-{i}"), NOW);
    }
    l.dispatch_job(job("my-job", 20), &mut net).unwrap();
    assert_eq!(net.total_tasks(), 20);

    // Agent-1 herstart schoon, binnen de timeout, en registreert zich (niet
    // alleen een heartbeat): de lege telling laat de reconcile het gat zien.
    net.get("agent-1").clear_tasks();
    let ep = "http://agent-1".to_string();
    assert!(
        l.register_agent(agent("agent-1", &ep), Map::new(), at(1), &mut net)
            .unwrap()
    );
    assert_eq!(net.total_tasks(), 20);
    assert_eq!(placed_total(&l, "my-job"), 20);
}
