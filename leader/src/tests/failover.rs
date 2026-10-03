//! `failover_test.go`: wat een nieuwe leider doet met wat er al draait.

use std::format;
use std::string::ToString;
use std::vec::Vec;

use types::time::MILLISECOND;
use types::{Job, Map, TaskState, Time};

use crate::testkit::*;
use crate::{Leader, MemStore};

fn counts(pairs: &[(&str, u32)]) -> Map<u32> {
    let mut m = Map::new();
    for (k, v) in pairs {
        m.insert(k.to_string(), *v).unwrap();
    }
    m
}

fn with_store(jobs: Vec<Job>) -> Leader<MemStore> {
    let mut l = leader();
    for j in jobs {
        l.store_put(j).unwrap();
    }
    l
}

fn register(
    l: &mut Leader<MemStore>,
    net: &mut FakeNet,
    id: &str,
    placed: Map<u32>,
    now: Time,
) -> bool {
    let ep = format!("http://{id}");
    if !net.agents.contains_key(&ep) {
        net.add(id);
    }
    l.register_agent(agent(id, &ep), placed, now, net).unwrap()
}

#[test]
fn failover_count_minus_one_dispatches_to_new_agents() {
    let mut l = with_store(std::vec![job("daemon", -1)]);
    let mut net = FakeNet::new();
    net.add("agent-1");
    net.get("agent-1").add_tasks("daemon", 1);
    register(&mut l, &mut net, "agent-1", counts(&[("daemon", 1)]), NOW);
    assert_eq!(net.get("agent-1").run_calls, 0);
    register(&mut l, &mut net, "agent-2", Map::new(), NOW);
    assert_eq!(net.get("agent-2").run_calls, 1);
    assert!(net.get("agent-2").jobs.contains_key("daemon"));
    assert_eq!(l.placed("daemon").unwrap().len(), 2);
}

#[test]
fn failover_multiple_agents_with_daemon_job() {
    let mut l = with_store(std::vec![job("monitoring", -1)]);
    let mut net = FakeNet::new();
    for id in ["agent-a", "agent-b", "agent-c"] {
        net.add(id);
        net.get(id).add_tasks("monitoring", 1);
        register(&mut l, &mut net, id, counts(&[("monitoring", 1)]), NOW);
    }
    assert_eq!(l.placed("monitoring").unwrap().len(), 3);
    for m in net.agents.values() {
        assert_eq!(m.run_calls, 0);
    }
}

#[test]
fn failover_new_agent_gets_all_daemon_jobs() {
    let mut l = with_store(std::vec![
        job("hopdns", -1),
        job("hoplb", -1),
        job("monitoring", -1)
    ]);
    let mut net = FakeNet::new();
    let all = counts(&[("hopdns", 1), ("hoplb", 1), ("monitoring", 1)]);
    register(&mut l, &mut net, "existing", all, NOW);
    // "existing" meldde alles al; alleen de nieuwe agent krijgt /run.
    assert_eq!(net.get("existing").run_calls, 0);
    register(&mut l, &mut net, "new-agent", Map::new(), NOW);
    assert_eq!(net.get("new-agent").run_calls, 3);
    assert_eq!(net.get("new-agent").jobs.len(), 3);
}

#[test]
fn failover_preserves_job_metadata() {
    // De nieuwe leider leest de jobs uit de gecommitte snapshot.
    let mut original = job("complex", 3);
    original.cpu_shares = 100;
    original.memory_limit = 512 * 1024 * 1024;
    original.ports.insert("http".to_string(), 8080).unwrap();
    original.ports.insert("grpc".to_string(), 9090).unwrap();
    original
        .env
        .insert("ENV".to_string(), "prod".to_string())
        .unwrap();
    original
        .env
        .insert("DEBUG".to_string(), "false".to_string())
        .unwrap();
    original
        .tags
        .insert("urlprefix".to_string(), "api.example.com".to_string())
        .unwrap();
    let old = with_store(std::vec![original.clone()]);
    let snapshot = old.snapshot(NOW).unwrap();

    let mut l = leader();
    assert!(l.load_committed_state(Some(snapshot.as_bytes())).unwrap());
    let mut net = FakeNet::new();
    register(&mut l, &mut net, "agent-1", Map::new(), NOW);
    let got = l.job("complex").unwrap();
    // Alles gelijk, op de prioriteit na: die nummert de reconcile dicht.
    assert_eq!(got.priority, Some(0));
    assert_eq!(
        got,
        &types::Job {
            priority: Some(0),
            ..original
        }
    );
    assert_eq!(got.ports.get("grpc"), Some(&9090));
    assert_eq!(got.env.get("ENV").unwrap(), "prod");
}

#[test]
fn failover_agent_with_empty_jobs_still_registers() {
    let mut l = leader();
    let mut net = FakeNet::new();
    register(&mut l, &mut net, "fresh-agent", Map::new(), NOW);
    assert_eq!(l.agents().len(), 1);
    assert_eq!(l.agents()[0].id, "fresh-agent");
}

#[test]
fn failover_dispatch_failure_does_not_break_heartbeat() {
    let mut l = with_store(std::vec![job("daemon", -1)]);
    let mut net = FakeNet::new();
    register(
        &mut l,
        &mut net,
        "good-agent",
        counts(&[("daemon", 1)]),
        NOW,
    );
    net.add("rejecting-agent");
    net.get("rejecting-agent").run_status = Some(503);
    register(&mut l, &mut net, "rejecting-agent", Map::new(), NOW);
    assert_eq!(l.agents().len(), 2);
    assert!(l.heartbeat("rejecting-agent", "", Default::default(), NOW));
    assert_eq!(l.placed("daemon").unwrap().len(), 1);
}

#[test]
fn failover_new_leader_does_not_duplicate_own_tasks() {
    let mut l = with_store(std::vec![job("my-api", 2)]);
    let mut net = FakeNet::new();
    net.add("local-agent-id");
    net.get("local-agent-id").add_tasks("my-api", 1);
    register(
        &mut l,
        &mut net,
        "local-agent-id",
        counts(&[("my-api", 1)]),
        NOW,
    );
    // Eén erbij voor het tweede exemplaar, niet twee.
    assert!(net.get("local-agent-id").run_calls <= 1);
    assert!(l.placed("my-api").unwrap().contains_key("local-agent-id"));
    assert_eq!(placed_total(&l, "my-api"), 2);
}

#[test]
fn failover_new_leader_learns_from_local_agent() {
    let mut l = with_store(std::vec![job("job-a", 1), job("job-b", 1)]);
    let mut net = FakeNet::new();
    net.add("local-agent-id");
    net.get("local-agent-id").add_tasks("job-a", 1);
    net.get("local-agent-id").add_tasks("job-b", 1);
    let placed = net.get("local-agent-id").placed_counts();
    register(&mut l, &mut net, "local-agent-id", placed, NOW);
    assert_eq!(l.jobs().len(), 2);
    assert_eq!(net.get("local-agent-id").run_calls, 0);
}

fn six_regular() -> Vec<Job> {
    (1..=6).map(|i| job(&format!("job-{i}"), 1)).collect()
}

#[test]
fn failover_new_leader_reschedules_orphaned_jobs() {
    let mut jobs = std::vec![job("daemon", -1)];
    jobs.extend(six_regular());
    let mut l = with_store(jobs);
    let mut net = FakeNet::new();
    register(&mut l, &mut net, "new-leader-id", Map::new(), NOW);
    assert_eq!(net.get("new-leader-id").run_calls, 7);
    assert_eq!(l.placed("daemon").unwrap().len(), 1);
}

#[test]
fn failover_new_leader_with_existing_daemon() {
    let mut jobs = std::vec![job("daemon", -1)];
    jobs.extend(six_regular());
    let mut l = with_store(jobs);
    l.settle(200 * MILLISECOND, NOW);
    let mut net = FakeNet::new();
    net.add("new-leader-id");
    net.get("new-leader-id").add_tasks("daemon", 1);
    register(
        &mut l,
        &mut net,
        "new-leader-id",
        counts(&[("daemon", 1)]),
        NOW,
    );
    // Tijdens settle niets.
    assert_eq!(net.get("new-leader-id").run_calls, 0);
    l.tick(Time(NOW.0 + 300 * MILLISECOND), &mut net).unwrap();
    assert!(l.is_settled());
    assert_eq!(net.get("new-leader-id").run_calls, 6);
    assert_eq!(l.placed("daemon").unwrap().len(), 1);
}

#[test]
fn failover_jobs_with_affinity() {
    // De leider dispatcht alles; affinity toetst de agent (hier: accepteert).
    let mut jobs = std::vec![job("daemon", -1)];
    for i in 1..=3 {
        let mut j = job(&format!("job-{i}"), 1);
        j.affinity
            .insert("node.id".to_string(), "old-leader-id".to_string())
            .unwrap();
        jobs.push(j);
    }
    for i in 4..=6 {
        jobs.push(job(&format!("job-{i}"), 1));
    }
    let mut l = with_store(jobs);
    let mut net = FakeNet::new();
    register(&mut l, &mut net, "new-leader-id", Map::new(), NOW);
    assert_eq!(net.get("new-leader-id").run_calls, 7);
}

#[test]
fn failover_second_heartbeat_does_not_reschedule() {
    // Heartbeats zijn puur liveness: geen reconcile, dus geen /run.
    let mut l = with_store(std::vec![job("daemon", -1), job("job-1", 1)]);
    let mut net = FakeNet::new();
    net.add("new-leader-id");
    net.get("new-leader-id").fail_runs = true;
    register(&mut l, &mut net, "new-leader-id", Map::new(), NOW);
    assert!(l.heartbeat("new-leader-id", "", Default::default(), NOW));
    net.get("new-leader-id").fail_runs = false;
    assert!(l.heartbeat("new-leader-id", "", Default::default(), at(1)));
    assert_eq!(net.get("new-leader-id").run_calls, 0);
}

fn two_agents_twenty(timeout_ms: u64) -> (Leader<MemStore>, FakeNet) {
    let mut l = leader();
    l.set_agent_timeout(timeout_ms * MILLISECOND);
    let mut net = FakeNet::new();
    join(&mut l, &mut net, "agent-a", NOW);
    join(&mut l, &mut net, "agent-b", NOW);
    l.dispatch_job(job("ticker", 20), &mut net).unwrap();
    assert_eq!(net.total_tasks(), 20);
    (l, net)
}

#[test]
fn failover_agent_dies_tasks_rescheduled() {
    let (mut l, mut net) = two_agents_twenty(200);
    net.get("agent-a").down = true;
    let later = Time(NOW.0 + 300 * MILLISECOND);
    l.heartbeat("agent-b", "", Default::default(), later);
    l.check_dead_agents(later, &mut net).unwrap();
    assert_eq!(net.get("agent-b").task_count(), 20);
    assert!(l.agent("agent-a").is_none());
}

#[test]
fn failover_heartbeat_learns_wrong_placement_count() {
    let mut l = with_store(std::vec![job("ticker", 20)]);
    let mut net = FakeNet::new();
    join(&mut l, &mut net, "agent-a", NOW);
    join(&mut l, &mut net, "agent-b", NOW);
    assert!(l.heartbeat("agent-a", "", Default::default(), NOW));
    assert!(l.heartbeat("agent-b", "", Default::default(), NOW));
    assert_eq!(l.agents().len(), 2);
    assert_eq!(placed_total(&l, "ticker"), 20);
}

#[test]
fn redispatch_does_not_corrupt_job_count() {
    let mut l = with_store(std::vec![job("ticker", 20), job("caddy", -1)]);
    let mut net = FakeNet::new();
    join(&mut l, &mut net, "agent-a", NOW);
    join(&mut l, &mut net, "agent-b", NOW);
    l.unregister_agent("agent-a", &mut net).unwrap();
    assert_eq!(l.job("ticker").unwrap().count, 20);
    assert_eq!(l.job("caddy").unwrap().count, -1);
}

#[test]
fn count_minus_one_not_redispatched_on_agent_death() {
    let mut l = with_store(std::vec![job("daemon", -1)]);
    let mut net = FakeNet::new();
    register(&mut l, &mut net, "agent-a", counts(&[("daemon", 1)]), NOW);
    register(&mut l, &mut net, "agent-b", counts(&[("daemon", 1)]), NOW);
    net.get("agent-b").run_calls = 0;
    l.unregister_agent("agent-a", &mut net).unwrap();
    assert_eq!(net.get("agent-b").run_calls, 0);
    assert_eq!(l.placed("daemon").unwrap().len(), 1);
}

#[test]
fn redispatch_correct_instance_count() {
    let (mut l, mut net) = two_agents_twenty(100);
    let b_runs = net.get("agent-b").run_calls;
    net.get("agent-b").run_calls = 0;
    l.unregister_agent("agent-a", &mut net).unwrap();
    assert_eq!(b_runs + net.get("agent-b").run_calls, 20);
}

#[test]
fn redispatch_with_stale_placement() {
    let (mut l, mut net) = two_agents_twenty(100);
    net.get("agent-a").down = true;
    l.unregister_agent("agent-a", &mut net).unwrap();
    assert_eq!(net.get("agent-b").task_count(), 20);
}

#[test]
fn failover_agent_dies_realistic_heartbeat() {
    let (mut l, mut net) = two_agents_twenty(200);
    net.get("agent-a").down = true;
    // agent-b blijft kloppen, agent-a zwijgt.
    for ms in [100, 200, 300] {
        l.heartbeat(
            "agent-b",
            "",
            Default::default(),
            Time(NOW.0 + ms * MILLISECOND),
        );
    }
    l.check_dead_agents(Time(NOW.0 + 300 * MILLISECOND), &mut net)
        .unwrap();
    assert_eq!(net.get("agent-b").task_count(), 20);
}

#[test]
fn failover_failed_tasks_not_redispatched() {
    // Een failed taak telt mee: aanwezigheid is de maat, niet de staat.
    let mut l = with_store(std::vec![job("my-api", 3)]);
    l.settle(100 * MILLISECOND, NOW);
    let mut net = FakeNet::new();
    net.add("agent-a");
    net.get("agent-a").add_tasks("my-api", 3);
    let failed_id = net.get("agent-a").task_ids("my-api")[2].clone();
    for t in &mut net.get("agent-a").tasks {
        if t.id == failed_id {
            t.state = TaskState::Failed;
        }
    }
    register(&mut l, &mut net, "agent-a", counts(&[("my-api", 3)]), NOW);
    register(&mut l, &mut net, "agent-b", Map::new(), NOW);
    l.tick(at(1), &mut net).unwrap();
    let runs: usize = net.agents.values().map(|a| a.run_calls).sum();
    assert_eq!(runs, 0);
}

fn preempt_setup(fail_stops: bool) -> (Leader<MemStore>, FakeNet) {
    let mut l = with_store(std::vec![job_prio("high", 6, 0), job_prio("low", 6, 1)]);
    l.settle(50 * MILLISECOND, NOW);
    let mut net = FakeNet::new();
    for id in ["agent-a", "agent-b"] {
        net.add(id);
        net.get(id).max_capacity = 3;
        net.get(id).add_tasks("low", 3);
    }
    net.get("agent-a").fail_stops = fail_stops;
    for id in ["agent-a", "agent-b"] {
        let placed = net.get(id).placed_counts();
        register(&mut l, &mut net, id, placed, NOW);
    }
    l.tick(at(1), &mut net).unwrap();
    (l, net)
}

fn placed_sum(l: &Leader<MemStore>) -> u32 {
    l.placed_counts().unwrap().iter().map(|(_, n)| *n).sum()
}

#[test]
fn failover_preemption_stop_failure_no_ghosts() {
    let (l, mut net) = preempt_setup(true);
    let actual = net.total_tasks() as u32;
    assert_eq!(placed_sum(&l), actual, "spookplaatsingen");
    assert!(l.placed_counts().unwrap().get("low").copied().unwrap_or(0) > 0);
    assert_eq!(net.get("agent-a").tasks_for_job("low"), 3);
}

#[test]
fn failover_preemption_stop_success_replaces_ghosts() {
    let (l, net) = preempt_setup(false);
    assert_eq!(l.placed_counts().unwrap().get("high"), Some(&6));
    assert_eq!(net.total_for_job("low"), 0);
    assert_eq!(placed_sum(&l), net.total_tasks() as u32);
}

#[test]
fn register_agent_duplicate_id_rejected() {
    let mut l = leader();
    let mut net = FakeNet::new();
    let a1 = |v: &str| types::Agent {
        version: v.to_string(),
        ..agent("node-1", "http://10.0.0.1:8080")
    };
    assert!(
        l.register_agent(a1("v1"), Map::new(), NOW, &mut net)
            .unwrap()
    );
    assert!(
        l.register_agent(a1("v2"), Map::new(), NOW, &mut net)
            .unwrap()
    );
    let other = agent("node-1", "http://10.0.0.2:8080");
    assert!(!l.register_agent(other, Map::new(), NOW, &mut net).unwrap());
    assert_eq!(l.agents().len(), 1);
    assert_eq!(l.agents()[0].endpoint, "http://10.0.0.1:8080");
}

#[test]
fn register_agent_duplicate_id_allowed_after_timeout() {
    let mut l = leader();
    l.set_agent_timeout(50 * MILLISECOND);
    let mut net = FakeNet::new();
    l.register_agent(
        agent("node-1", "http://10.0.0.1:8080"),
        Map::new(),
        NOW,
        &mut net,
    )
    .unwrap();
    let later = Time(NOW.0 + 100 * MILLISECOND);
    assert!(
        l.register_agent(
            agent("node-1", "http://10.0.0.2:8080"),
            Map::new(),
            later,
            &mut net
        )
        .unwrap()
    );
    assert_eq!(l.agents().len(), 1);
    assert_eq!(l.agents()[0].endpoint, "http://10.0.0.2:8080");
}
