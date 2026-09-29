//! `edge_cases_test.go`: dode agents, falende agents, lege clusters.
//!
//! Go bouwde hier eigen `httptest`-handlers; de nep-agents uit de testkit
//! doen hetzelfde met een vlag: `down` is een agent die niet (of te laat)
//! antwoordt, `run_status` een vaste status op `/run`.

use std::string::ToString;

use types::time::{MILLISECOND, SECOND};
use types::{Map, Task, TaskState};

use crate::testkit::*;
use crate::{DEFAULT_AGENT_TIMEOUT, JobStore as _};

#[test]
fn leader_check_dead_agents() {
    let mut l = leader();
    let mut net = FakeNet::new();
    l.set_agent_timeout(50 * MILLISECOND);
    l.register_agent(
        agent("dying-agent", "http://192.168.1.10:8080"),
        Map::new(),
        NOW,
        &mut net,
    )
    .unwrap();
    assert!(l.heartbeat("dying-agent", "", 0, NOW));
    assert_eq!(l.agents().len(), 1);

    l.check_dead_agents(at_ms(100), &mut net).unwrap();
    assert!(l.agents().is_empty());
}

#[test]
fn leader_redispatch_jobs_from_dead_agent() {
    let mut l = leader();
    let mut net = FakeNet::new();
    l.set_agent_timeout(50 * MILLISECOND);
    // De stervende agent heeft geen nep: niemand antwoordt op zijn adres.
    l.register_agent(
        agent("dying-agent", "http://dead.host:8080"),
        Map::new(),
        NOW,
        &mut net,
    )
    .unwrap();
    join(&mut l, &mut net, "healthy-agent", NOW);
    // De job pas na de registratie, met de plaatsing met de hand op de
    // stervende: anders plaatst de registratie hem al op de gezonde.
    l.store_put(job("test-job", 1)).unwrap();
    force_placed(&mut l, "dying-agent", "test-job", 1);
    assert_eq!(net.get("healthy-agent").run_calls, 0);

    l.heartbeat("healthy-agent", "", 0, at_ms(100));
    l.check_dead_agents(at_ms(100), &mut net).unwrap();
    assert_eq!(net.get("healthy-agent").run_calls, 1);
    assert_eq!(placed_total(&l, "test-job"), 1);
}

#[test]
fn leader_agent_timeout_configurable() {
    let mut l = leader();
    assert_eq!(l.agent_timeout, DEFAULT_AGENT_TIMEOUT);
    assert_eq!(DEFAULT_AGENT_TIMEOUT, 30 * SECOND);
    l.set_agent_timeout(5 * SECOND);
    assert_eq!(l.agent_timeout, 5 * SECOND);
}

#[test]
fn leader_get_cluster_status_with_failing_agent() {
    let mut l = leader();
    let mut net = FakeNet::new();
    join(&mut l, &mut net, "working-agent", NOW);
    join(&mut l, &mut net, "failing-agent", NOW);
    net.get("working-agent").tasks.push(Task {
        id: "task-1".to_string(),
        state: TaskState::Running,
        ..Task::default()
    });
    // Go antwoordde met een 500; voor de leider is dat "geen antwoord".
    net.get("failing-agent").down = true;

    let status = l.cluster_status(&mut net).unwrap();
    assert!(status.iter().any(|(id, _)| id == "working-agent"));
    assert!(
        !status
            .iter()
            .any(|(id, tasks)| id == "failing-agent" && !tasks.is_empty())
    );
}

#[test]
fn leader_dispatch_with_http_timeout() {
    let mut l = leader();
    let mut net = FakeNet::new();
    join(&mut l, &mut net, "slow-agent", NOW);
    // Een timeout ziet de leider als onbereikbaar.
    net.get("slow-agent").down = true;
    assert!(l.dispatch_job(job("timeout-job", 1), &mut net).is_err());
}

#[test]
fn leader_multiple_agents_partial_failure() {
    let mut l = leader();
    let mut net = FakeNet::new();
    join(&mut l, &mut net, "success-agent", NOW);
    join(&mut l, &mut net, "fail-agent", NOW);
    net.get("fail-agent").run_status = Some(503);

    // Eén instantie: de weigering van de ene agent gaat door naar de andere.
    l.dispatch_job(job("test", 1), &mut net).unwrap();
    assert_eq!(net.get("success-agent").run_calls, 1);
}

#[test]
fn leader_delete_job_not_found() {
    let mut l = leader();
    let mut net = FakeNet::new();
    l.delete_job("nonexistent-job", &mut net).unwrap();
}

#[test]
fn leader_empty_cluster_status() {
    let l = leader();
    let mut net = FakeNet::new();
    assert!(l.cluster_status(&mut net).unwrap().is_empty());
    assert!(l.store().jobs().is_empty());
}
