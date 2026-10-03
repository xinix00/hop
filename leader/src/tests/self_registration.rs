//! `self_registration_test.go`: de leider is ook een agent in zijn eigen lijst.

use std::string::ToString;

use types::Map;

use crate::testkit::*;
use crate::{Leader, MemStore};

fn named(id: &str) -> Leader<MemStore> {
    Leader::new(id.to_string(), MemStore::new())
}

#[test]
fn leader_registers_itself() {
    let mut l = named("local-agent");
    let mut net = FakeNet::new();
    l.register_agent(
        agent("local-agent", "http://10.0.0.1:8080"),
        Map::new(),
        NOW,
        &mut net,
    )
    .unwrap();
    assert!(l.heartbeat("local-agent", "", Default::default(), NOW));
    assert_eq!(l.agents().len(), 1);
    assert_eq!(l.agents()[0].id, "local-agent");
    assert_eq!(l.agents()[0].endpoint, "http://10.0.0.1:8080");
}

#[test]
fn leader_plus_follower_agents() {
    let mut l = named("leader-node");
    let mut net = FakeNet::new();
    for (id, ep) in [
        ("leader-node", "http://10.0.0.1:8080"),
        ("follower-1", "http://10.0.0.2:8080"),
        ("follower-2", "http://10.0.0.3:8080"),
    ] {
        l.register_agent(agent(id, ep), Map::new(), NOW, &mut net)
            .unwrap();
        assert!(l.heartbeat(id, "", Default::default(), NOW));
    }
    assert_eq!(l.agents().len(), 3);
    for id in ["leader-node", "follower-1", "follower-2"] {
        assert!(l.agent(id).is_some(), "{id}");
    }
}

#[test]
fn leader_can_dispatch_to_itself() {
    // Go kon hier niet echt dispatchen (geen HTTP-server op 10.0.0.1); met
    // een nep-agent op het eigen id wel, en dat is de bedoeling van de test.
    let mut l = named("leader-node");
    let mut net = FakeNet::new();
    join(&mut l, &mut net, "leader-node", NOW);
    assert_eq!(l.agents().len(), 1);
    l.dispatch_job(job("test", 1), &mut net).unwrap();
    assert_eq!(l.placed("test").unwrap().get("leader-node"), Some(&1));
    assert_eq!(net.get("leader-node").task_count(), 1);
}

#[test]
fn single_node_cluster_leader_is_only_agent() {
    let mut l = named("solo-node");
    let mut net = FakeNet::new();
    join(&mut l, &mut net, "solo-node", NOW);
    assert!(l.heartbeat("solo-node", "", Default::default(), NOW));
    assert_eq!(l.agents().len(), 1);
    assert_eq!(l.agents()[0].id, "solo-node");
    assert_eq!(l.local_agent_id(), "solo-node");
    // Leider én agent: hij plaatst op zichzelf.
    l.dispatch_job(job("solo-job", 1), &mut net).unwrap();
    assert_eq!(net.get("solo-node").task_count(), 1);
}
