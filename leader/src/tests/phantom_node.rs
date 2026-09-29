//! `phantom_node_test.go`: een korte netwerkblip (onder de agent-timeout)
//! verplaatst niets; echt ontbrekende taken gaan wel meteen naar wie ruimte heeft.
//!
//! De les van deze tests (en de reden dat de leider op `placed` plant en
//! niet op wat `/tasks` toevallig teruggeeft): een agent die even niet
//! antwoordt maar nog niet dood is, draait zijn taken gewoon door.

use std::string::ToString;

use types::time::SECOND;
use types::{Map, Nanos};

use crate::testkit::*;
use crate::{Leader, MemStore};

fn counter() -> types::Job {
    types::Job {
        command: "sh -c 'i=0; while true; do echo counter: $i; i=$((i+1)); sleep 1; done'"
            .to_string(),
        ..job("counter", 20)
    }
}

/// Twintig taken over agent-a en agent-b.
fn twenty(timeout: Nanos) -> (Leader<MemStore>, FakeNet) {
    let mut l = Leader::new("leader".to_string(), MemStore::new());
    l.set_agent_timeout(timeout);
    let mut net = FakeNet::new();
    join(&mut l, &mut net, "agent-a", NOW);
    join(&mut l, &mut net, "agent-b", NOW);
    l.dispatch_job(counter(), &mut net).unwrap();
    assert_eq!(net.total_tasks(), 20);
    (l, net)
}

#[test]
fn network_blip_new_node_joins() {
    let (mut l, mut net) = twenty(2 * SECOND);
    let a = net.get("agent-a").task_count();
    let b = net.get("agent-b").task_count();
    let a_runs = net.get("agent-a").run_calls;
    let b_runs = net.get("agent-b").run_calls;

    // Stap 2: A mist heartbeats, maar blijft onder de timeout.
    l.heartbeat("agent-b", "", 0, at_ms(100));
    l.heartbeat("agent-b", "", 0, at_ms(200));
    l.check_dead_agents(at_ms(300), &mut net).unwrap();
    assert!(l.agent("agent-a").is_some());

    // Stap 3: C komt erbij en neemt niets over van A.
    join(&mut l, &mut net, "agent-c", at_ms(300));
    assert_eq!(net.get("agent-c").task_count(), 0);
    assert_eq!(net.get("agent-a").run_calls, a_runs);
    assert_eq!(net.get("agent-b").run_calls, b_runs);

    // Stap 5: A is terug, alles intact.
    assert!(l.heartbeat("agent-a", "", 0, at_ms(350)));
    assert_eq!(net.total_tasks(), 20);
    assert_eq!(net.get("agent-a").task_count(), a);
    assert_eq!(net.get("agent-b").task_count(), b);
}

#[test]
fn network_blip_agent_unreachable() {
    let (mut l, mut net) = twenty(2 * SECOND);
    let b = net.get("agent-b").task_count();

    // A's endpoint is weg, maar A is nog niet dood.
    net.get("agent-a").down = true;
    l.heartbeat("agent-b", "", 0, at_ms(50));
    l.check_dead_agents(at_ms(100), &mut net).unwrap();
    assert!(l.agent("agent-a").is_some());

    // C komt erbij terwijl A onbereikbaar is: de leider ziet via `/tasks`
    // maar tien taken, maar plant op `placed` en dispatcht dus niets.
    join(&mut l, &mut net, "agent-c", at_ms(100));
    let new = (net.get("agent-b").task_count() - b) + net.get("agent-c").task_count();
    assert_eq!(new, 0, "dubbele taken voor een agent die niet dood is");

    // A is terug; zijn taken draaiden gewoon door.
    net.get("agent-a").down = false;
    assert!(l.heartbeat("agent-a", "", 0, at_ms(200)));
    assert_eq!(net.total_tasks(), 20);
}

#[test]
fn network_blip_no_redistribution() {
    let (mut l, mut net) = twenty(SECOND);
    let a = net.get("agent-a").task_count();
    let b = net.get("agent-b").task_count();
    let a_runs = net.get("agent-a").run_calls;
    let b_runs = net.get("agent-b").run_calls;

    l.heartbeat("agent-b", "", 0, at_ms(100));
    l.heartbeat("agent-b", "", 0, at_ms(200));
    l.check_dead_agents(at_ms(300), &mut net).unwrap();
    l.heartbeat("agent-a", "", 0, at_ms(320));
    l.heartbeat("agent-b", "", 0, at_ms(320));

    assert_eq!(net.get("agent-a").run_calls, a_runs);
    assert_eq!(net.get("agent-b").run_calls, b_runs);
    assert_eq!(net.get("agent-a").task_count(), a);
    assert_eq!(net.get("agent-b").task_count(), b);
}

#[test]
fn pending_tasks_scheduled_on_new_node() {
    let (mut l, mut net) = twenty(2 * SECOND);

    // B verliest vijf van zijn tien taken; beide melden zich opnieuw.
    net.get("agent-b").tasks.truncate(5);
    assert!(rejoin(&mut l, &mut net, "agent-a", at_ms(100)));
    assert!(rejoin(&mut l, &mut net, "agent-b", at_ms(100)));
    assert_eq!(net.total_tasks(), 20);
    assert_eq!(placed_total(&l, "counter"), 20);

    // C komt erbij: alles staat al, dus niets voor C.
    let ep = net.add("agent-c");
    l.register_agent(agent("agent-c", &ep), Map::new(), at_ms(200), &mut net)
        .unwrap();
    assert_eq!(net.get("agent-c").task_count(), 0);
}
