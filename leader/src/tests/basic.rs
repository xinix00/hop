//! `leader_test.go`, `heartbeat_test.go`, `dispatch_test.go`,
//! `dispatch_debug_test.go`, `count_minus_one_test.go`,
//! `affinity_dispatch_test.go` en `reconcile_test.go`.

use std::string::ToString;

use types::{Job, Map, Time};

use crate::testkit::*;
use crate::{Error, JobStore as _, Leader, MemStore, Refusal, Transport as _};

// ---- leader_test.go ----

#[test]
fn leader_new() {
    let l = leader();
    assert_eq!(l.local_agent_id(), "local-agent");
    assert!(l.is_settled());
}

#[test]
fn leader_get_jobs() {
    let mut store = MemStore::new();
    store.put(job("job1", 1)).unwrap();
    store.put(job("job2", 1)).unwrap();
    let l = Leader::new("local-agent".to_string(), store);
    assert_eq!(l.jobs().len(), 2);
}

// ---- heartbeat_test.go ----

#[test]
fn leader_heartbeat_registers_agent() {
    let mut l = leader();
    let mut net = FakeNet::new();
    let a = agent("remote-agent", "http://192.168.1.10:8080");
    assert!(l.register_agent(a, Map::new(), NOW, &mut net).unwrap());
    assert!(l.heartbeat("remote-agent", "", Default::default(), NOW));
    assert_eq!(l.agents().len(), 1);
    assert_eq!(l.agents()[0].id, "remote-agent");
    assert_eq!(l.agents()[0].endpoint, "http://192.168.1.10:8080");
}

#[test]
fn leader_heartbeat_updates_last_seen() {
    let mut l = leader();
    let mut net = FakeNet::new();
    join(&mut l, &mut net, "remote-agent", NOW);
    let first = l.agents()[0].last_seen;
    let beat = types::Telemetry {
        temp_milli_c: 42_000,
        hop: types::SysUsage {
            cpu_percent: Some(7.0),
            mem_bytes: 1 << 20,
            ram_bytes: 32 << 20,
            core: Some(1),
        },
        ..types::Telemetry::default()
    };
    assert!(l.heartbeat("remote-agent", "v2", beat, at(5)));
    let a = &l.agents()[0];
    assert!(a.last_seen > first);
    assert_eq!(a.version, "v2");
    assert_eq!(a.telemetry, beat);
    // Een onbekende agent krijgt `false` (de adapter maakt er 404 van).
    assert!(!l.heartbeat("stranger", "", beat, at(5)));
}

#[test]
fn leader_get_agents() {
    let mut l = leader();
    let mut net = FakeNet::new();
    for c in ['e', 'a', 'c', 'b', 'd'] {
        let id = std::format!("agent-{c}");
        l.register_agent(agent(&id, "http://host:8080"), Map::new(), NOW, &mut net)
            .unwrap();
    }
    let ids: std::vec::Vec<_> = l.agents().iter().map(|a| a.id.as_str()).collect();
    // Gesorteerd op id: de deterministische round-robin-volgorde.
    assert_eq!(ids, ["agent-a", "agent-b", "agent-c", "agent-d", "agent-e"]);
}

#[test]
fn leader_unregister_agent() {
    let mut l = leader();
    let mut net = FakeNet::new();
    join(&mut l, &mut net, "remote-agent", NOW);
    assert_eq!(l.agents().len(), 1);
    l.unregister_agent("remote-agent", &mut net).unwrap();
    assert!(l.agents().is_empty());
}

#[test]
fn leader_concurrent_heartbeats() {
    // Go testte races tussen goroutines; met één eigenaar is dit een lus.
    let mut l = leader();
    let mut net = FakeNet::new();
    for i in 0..10u8 {
        let id = std::format!("agent-{}", char::from(b'a' + i));
        l.register_agent(agent(&id, "http://host:8080"), Map::new(), NOW, &mut net)
            .unwrap();
    }
    for n in 0..200u8 {
        let id = std::format!("agent-{}", char::from(b'a' + n % 10));
        assert!(l.heartbeat(&id, "", Default::default(), NOW));
    }
    assert_eq!(l.agents().len(), 10);
}

// ---- dispatch_test.go ----

#[test]
fn leader_dispatch_job_to_agent() {
    let mut l = leader();
    let mut net = FakeNet::new();
    join(&mut l, &mut net, "mock-agent", NOW);
    l.dispatch_job(job("test-job", 1), &mut net).unwrap();
    assert!(l.job("test-job").is_some());
    assert_eq!(net.get("mock-agent").task_count(), 1);
}

#[test]
fn leader_dispatch_job_no_agents() {
    let mut l = leader();
    let mut net = FakeNet::new();
    let err = l.dispatch_job(job("test-job", 1), &mut net).unwrap_err();
    assert!(matches!(
        err,
        Error::Dispatch {
            why: Refusal::NoAgents,
            ..
        }
    ));
    // Toch opgeslagen: de reconcile pakt hem later op.
    assert!(l.job("test-job").is_some());
}

#[test]
fn leader_dispatch_all_agents_reject() {
    let mut l = leader();
    let mut net = FakeNet::new();
    join(&mut l, &mut net, "rejecting-agent", NOW);
    net.get("rejecting-agent").run_status = Some(503);
    assert!(l.dispatch_job(job("test-job", 1), &mut net).is_err());
}

#[test]
fn leader_dispatch_multiple_instances() {
    let mut l = leader();
    let mut net = FakeNet::new();
    for id in ["agent-a", "agent-b", "agent-c"] {
        join(&mut l, &mut net, id, NOW);
    }
    l.dispatch_job(job("multi-job", 6), &mut net).unwrap();
    assert_eq!(net.total_tasks(), 6);
    // Round-robin spreidt gelijk: twee per agent.
    for id in ["agent-a", "agent-b", "agent-c"] {
        assert_eq!(net.get(id).task_count(), 2, "{id}");
    }
}

#[test]
fn leader_delete_job_on_multiple_agents() {
    let mut l = leader();
    let mut net = FakeNet::new();
    join(&mut l, &mut net, "agent-a", NOW);
    join(&mut l, &mut net, "agent-b", NOW);
    // De job pas na de registratie, met de plaatsing met de hand.
    let mut store_job = job("test-job", 1);
    store_job.command = "echo".to_string();
    l.store_put(store_job).unwrap();
    force_placed(&mut l, "agent-a", "test-job", 1);
    force_placed(&mut l, "agent-b", "test-job", 1);
    l.delete_job("test-job", &mut net).unwrap();
    let deletes: usize = net.agents.values().map(|a| a.deletes.len()).sum();
    assert_eq!(deletes, 2);
    assert!(l.job("test-job").is_none());
}

#[test]
fn leader_dispatch_job_with_zero_count() {
    let mut l = leader();
    let mut net = FakeNet::new();
    join(&mut l, &mut net, "agent-1", NOW);
    l.dispatch_job(job("zero-count-job", 0), &mut net).unwrap();
    assert_eq!(net.get("agent-1").task_count(), 1);
}

#[test]
fn leader_dispatch_count_minus_one() {
    let mut l = leader();
    let mut net = FakeNet::new();
    for id in ["agent-a", "agent-b", "agent-c"] {
        join(&mut l, &mut net, id, NOW);
    }
    l.dispatch_job(job("hopdns", -1), &mut net).unwrap();
    assert_eq!(net.total_tasks(), 3);
}

#[test]
fn leader_count_minus_one_new_agent() {
    let mut l = leader();
    let mut net = FakeNet::new();
    join(&mut l, &mut net, "agent-a", NOW);
    l.dispatch_job(job("hopdns", -1), &mut net).unwrap();
    assert_eq!(net.get("agent-a").task_count(), 1);
    // Een nieuwe agent krijgt de daemon bij registratie.
    join(&mut l, &mut net, "agent-b", NOW);
    assert_eq!(net.get("agent-b").task_count(), 1);
    assert_eq!(net.get("agent-a").task_count(), 1);
}

#[test]
fn leader_concurrent_dispatch_and_delete() {
    // Go: tien dispatches en tien deletes door elkaar. Met één eigenaar is
    // de bewering sterker: na alles staat er niets meer, nergens.
    let mut l = leader();
    let mut net = FakeNet::new();
    join(&mut l, &mut net, "agent-1", NOW);
    for n in 0..10 {
        l.dispatch_job(job(&std::format!("job-{n}"), 1), &mut net)
            .unwrap();
    }
    for n in 0..10 {
        l.delete_job(&std::format!("job-{n}"), &mut net).unwrap();
    }
    assert!(l.jobs().is_empty());
    assert_eq!(net.total_tasks(), 0);
}

#[test]
fn leader_dispatch_accepts202() {
    let mut l = leader();
    let mut net = FakeNet::new();
    join(&mut l, &mut net, "async-agent", NOW);
    net.get("async-agent").run_status = Some(202);
    l.dispatch_job(job("async-job", 1), &mut net).unwrap();
    assert!(l.job("async-job").is_some());
}

#[test]
fn leader_dispatch_accepts201() {
    let mut l = leader();
    let mut net = FakeNet::new();
    join(&mut l, &mut net, "create-agent", NOW);
    net.get("create-agent").run_status = Some(201);
    l.dispatch_job(job("created-job", 1), &mut net).unwrap();
}

#[test]
fn leader_dispatch_rejects500() {
    let mut l = leader();
    let mut net = FakeNet::new();
    join(&mut l, &mut net, "error-agent", NOW);
    net.get("error-agent").run_status = Some(500);
    assert!(l.dispatch_job(job("error-job", 1), &mut net).is_err());
}

#[test]
fn leader_dispatch_rejects400() {
    let mut l = leader();
    let mut net = FakeNet::new();
    join(&mut l, &mut net, "bad-agent", NOW);
    net.get("bad-agent").run_status = Some(400);
    assert!(l.dispatch_job(job("bad-job", 1), &mut net).is_err());
}

// ---- dispatch_debug_test.go ----

#[test]
fn dispatch_simple_job() {
    let mut l = leader();
    let mut net = FakeNet::new();
    join(&mut l, &mut net, "agent-a", NOW);
    assert_eq!(l.agents().len(), 1);
    l.dispatch_job(job("simple", 1), &mut net).unwrap();
    assert_eq!(placed_total(&l, "simple"), 1);
}

#[test]
fn job_without_name_is_refused() {
    let mut l = leader();
    let mut net = FakeNet::new();
    assert_eq!(
        l.dispatch_job(Job::default(), &mut net),
        Err(Error::NameRequired)
    );
}

// ---- count_minus_one_test.go ----
// De Go-tests hier logden alleen ("full integration test would use mock
// agents"); met de nep-transport kunnen ze echt beweren.

#[test]
fn count_minus_one_dispatches_once_per_agent() {
    let mut l = leader();
    let mut net = FakeNet::new();
    for id in ["agent-a", "agent-b", "agent-c"] {
        join(&mut l, &mut net, id, NOW);
    }
    l.dispatch_job(job("daemon", -1), &mut net).unwrap();
    let placed = l.placed("daemon").unwrap();
    assert_eq!(placed.len(), 3);
    assert!(placed.iter().all(|(_, n)| *n == 1));
    // Een reconcile erna voegt niets toe: geen duplicaten.
    l.reconcile_jobs(&mut net).unwrap();
    assert_eq!(net.total_tasks(), 3);
}

#[test]
fn count_minus_one_with_two_agents() {
    let mut l = leader();
    let mut net = FakeNet::new();
    join(&mut l, &mut net, "agent-a", NOW);
    join(&mut l, &mut net, "agent-b", NOW);
    l.dispatch_job(job("daemon", -1), &mut net).unwrap();
    let runs: usize = net.agents.values().map(|a| a.run_calls).sum();
    assert_eq!(runs, 2);
}

#[test]
fn count_minus_one_no_agents() {
    // Geen agents: niets te doen, en geen fout (er mist niemand).
    let mut l = leader();
    let mut net = FakeNet::new();
    l.dispatch_job(job("daemon", -1), &mut net).unwrap();
    assert!(l.job("daemon").is_some());
}

#[test]
fn count_minus_one_does_not_use_round_robin() {
    let mut l = leader();
    let mut net = FakeNet::new();
    for id in ["agent-a", "agent-b", "agent-c"] {
        join(&mut l, &mut net, id, NOW);
    }
    l.dispatch_job(job("test1", -1), &mut net).unwrap();
    l.dispatch_job(job("test2", -1), &mut net).unwrap();
    for id in ["agent-a", "agent-b", "agent-c"] {
        assert_eq!(net.get(id).tasks_for_job("test1"), 1);
        assert_eq!(net.get(id).tasks_for_job("test2"), 1);
    }
}

// ---- affinity_dispatch_test.go ----

fn with_affinity(name: &str, count: i64, k: &str, v: &str) -> Job {
    let mut j = job(name, count);
    j.affinity.insert(k.to_string(), v.to_string()).unwrap();
    j
}

#[test]
fn dispatch406_skips_to_next_agent() {
    let mut l = leader();
    let mut net = FakeNet::new();
    join(&mut l, &mut net, "agent-a", NOW);
    join(&mut l, &mut net, "agent-b", NOW);
    net.get("agent-a").reject_affinity = true;
    l.dispatch_job(with_affinity("web", 1, "node.os", "linux"), &mut net)
        .unwrap();
    assert_eq!(net.get("agent-b").run_calls, 1);
}

#[test]
fn dispatch_all_agents_reject406() {
    let mut l = leader();
    let mut net = FakeNet::new();
    join(&mut l, &mut net, "agent-a", NOW);
    join(&mut l, &mut net, "agent-b", NOW);
    net.get("agent-a").reject_affinity = true;
    net.get("agent-b").reject_affinity = true;
    let err = l
        .dispatch_job(with_affinity("gpu-job", 1, "gpu", "true"), &mut net)
        .unwrap_err();
    assert!(matches!(
        err,
        Error::Dispatch {
            why: Refusal::NoneAccepted { tried: 2 },
            ..
        }
    ));
    assert!(l.job("gpu-job").is_some());
}

#[test]
fn daemon_with_affinity_mixed_agents() {
    let mut l = leader();
    let mut net = FakeNet::new();
    join(&mut l, &mut net, "darwin-node", NOW);
    join(&mut l, &mut net, "linux-node", NOW);
    net.get("linux-node").reject_affinity = true;
    l.dispatch_job(
        with_affinity("mac-monitor", -1, "node.os", "darwin"),
        &mut net,
    )
    .unwrap();
    assert_eq!(net.get("darwin-node").run_calls, 1);
    assert_eq!(placed_total(&l, "mac-monitor"), 1);
}

#[test]
fn daemon_all_agents_reject406() {
    let mut l = leader();
    let mut net = FakeNet::new();
    join(&mut l, &mut net, "agent-a", NOW);
    join(&mut l, &mut net, "agent-b", NOW);
    net.get("agent-a").reject_affinity = true;
    net.get("agent-b").reject_affinity = true;
    let err = l
        .dispatch_job(
            with_affinity("special-daemon", -1, "node.os", "windows"),
            &mut net,
        )
        .unwrap_err();
    assert!(matches!(err, Error::DaemonRejected { .. }));
    assert!(l.job("special-daemon").is_some());
}

#[test]
fn new_agent_joins_daemon_with_affinity() {
    let mut store = MemStore::new();
    store
        .put(with_affinity("darwin-only", -1, "node.os", "darwin"))
        .unwrap();
    let mut l = Leader::new("leader".to_string(), store);
    let mut net = FakeNet::new();
    let ep = net.add("linux-node");
    net.get("linux-node").reject_affinity = true;
    l.register_agent(agent("linux-node", &ep), Map::new(), NOW, &mut net)
        .unwrap();
    join(&mut l, &mut net, "darwin-node", NOW);
    assert!(net.get("darwin-node").run_calls >= 1);
    assert_eq!(net.get("linux-node").task_count(), 0);
}

#[test]
fn counted_job_with_affinity_mixed_agents() {
    let mut l = leader();
    let mut net = FakeNet::new();
    for id in ["match-a", "match-b", "no-match"] {
        join(&mut l, &mut net, id, NOW);
    }
    net.get("no-match").reject_affinity = true;
    l.dispatch_job(with_affinity("api", 2, "node.arch", "arm64"), &mut net)
        .unwrap();
    assert_eq!(
        net.get("match-a").run_calls + net.get("match-b").run_calls,
        2
    );
}

// ---- reconcile_test.go ----

#[test]
fn volume_affinity() {
    // Volume-afwijzing is agent-kant (406); de leider behandelt hem als affinity.
    let mut l = leader();
    let mut net = FakeNet::new();
    join(&mut l, &mut net, "agent-1", NOW);
    join(&mut l, &mut net, "agent-2", NOW);
    net.get("agent-1").reject_affinity = true;
    let mut j = job("postgres", 1);
    j.volumes
        .insert("/data/postgres".to_string(), "data".to_string())
        .unwrap();
    l.dispatch_job(j, &mut net).unwrap();
    assert_eq!(net.get("agent-2").task_count(), 1);
}

#[test]
fn reschedule_underscheduled() {
    let mut l = leader();
    let mut net = FakeNet::new();
    join(&mut l, &mut net, "agent-1", NOW);
    l.store_put(job("test-job", 3)).unwrap();
    force_placed(&mut l, "agent-1", "test-job", 1);
    assert_eq!(l.placed("test-job").unwrap().len(), 1);
    // Een nieuwe node triggert de reconcile, die de ontbrekende twee plaatst.
    join(&mut l, &mut net, "agent-2", NOW);
    assert_eq!(placed_total(&l, "test-job"), 3);
}

#[test]
fn node_recovery() {
    let mut l = leader();
    let mut net = FakeNet::new();
    let mut j = job("postgres", 1);
    j.volumes
        .insert(
            "/data/postgres".to_string(),
            "/var/lib/postgresql/data".to_string(),
        )
        .unwrap();
    l.store_put(j).unwrap();
    join(&mut l, &mut net, "node-a", NOW);
    assert_eq!(placed_total(&l, "postgres"), 1);
    // De node valt uit: agent en plaatsing weg.
    l.remove_agent("node-a");
    net.get("node-a").clear_tasks();
    assert_eq!(placed_total(&l, "postgres"), 0);
    // Hij komt terug met hetzelfde id: de wees wordt weer geplaatst.
    assert!(rejoin(&mut l, &mut net, "node-a", at(40)));
    assert_eq!(l.agents().len(), 1);
    assert_eq!(placed_total(&l, "postgres"), 1);
}

#[test]
fn run_reply_maps_status_codes() {
    use crate::RunReply;
    assert_eq!(RunReply::from_status(200), RunReply::Accepted);
    assert_eq!(RunReply::from_status(201), RunReply::Accepted);
    assert_eq!(RunReply::from_status(202), RunReply::Accepted);
    assert_eq!(RunReply::from_status(406), RunReply::AffinityMismatch);
    assert_eq!(RunReply::from_status(503), RunReply::NoCapacity);
    assert_eq!(RunReply::from_status(500), RunReply::Rejected(500));
    let _ = Time::ZERO;
    let mut net = FakeNet::new();
    assert!(net.tasks(&agent("x", "http://nowhere")).is_none());
}

// ---- De notify-route (`server.go` handleNotify, in v3 op de leider) ----

#[test]
fn leader_notify_topics() {
    use crate::Event;
    let mut l = leader();
    l.notify("job:web:started");
    l.notify("agent:a1");
    assert_eq!(
        l.drain_events(),
        [
            Event::Job("web".to_string()),
            Event::Agent("a1".to_string())
        ]
    );
    // Leeg, een lege naam of iets onbekends: kijk alles opnieuw.
    for t in ["", "job:", "agent:", "iets"] {
        l.notify(t);
        assert_eq!(l.drain_events(), [Event::Status], "{t:?}");
    }
}
