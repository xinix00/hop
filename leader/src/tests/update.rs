//! `update_test.go`, `update_preempt_test.go` en `deploying_test.go`.

use std::string::ToString;

use types::UpdatePolicy;

use crate::testkit::*;
use crate::{Error, JobStore as _, ROLLING_UPDATE_DELAY};

fn v(name: &str, cmd: &str, count: i64, policy: Option<UpdatePolicy>) -> types::Job {
    let mut j = job(name, count);
    j.command = cmd.to_string();
    j.update_policy = policy;
    j
}

#[test]
fn update_job_rolling() {
    let mut l = leader();
    let mut net = FakeNet::new();
    for id in ["agent-a", "agent-b", "agent-c"] {
        join(&mut l, &mut net, id, NOW);
    }
    l.dispatch_job(v("my-app", "./app-v1", 3, None), &mut net)
        .unwrap();
    assert_eq!(l.placed("my-app").unwrap().len(), 3);
    l.update_job(
        v("my-app", "./app-v2", 3, Some(UpdatePolicy::Rolling)),
        &mut net,
    )
    .unwrap();
    assert_eq!(l.placed("my-app").unwrap().len(), 3);
    assert_eq!(placed_total(&l, "my-app"), 3);
    assert_eq!(net.total_for_job("my-app"), 3);
    assert_eq!(l.job("my-app").unwrap().command, "./app-v2");
    // Elke agent draait de nieuwe versie.
    for m in net.agents.values() {
        assert_eq!(m.jobs.get("my-app").unwrap().command, "./app-v2");
    }
    // Tussen drie stappen twee pauzes.
    assert_eq!(net.paused, 2 * ROLLING_UPDATE_DELAY);
}

#[test]
fn update_job_recreate() {
    let mut l = leader();
    let mut net = FakeNet::new();
    join(&mut l, &mut net, "agent-1", NOW);
    l.dispatch_job(v("my-app", "./app-v1", 1, None), &mut net)
        .unwrap();
    l.update_job(
        v("my-app", "./app-v2", 1, Some(UpdatePolicy::Recreate)),
        &mut net,
    )
    .unwrap();
    assert_eq!(l.job("my-app").unwrap().command, "./app-v2");
    assert_eq!(net.get("agent-1").tasks_for_job("my-app"), 1);
    assert_eq!(net.get("agent-1").stops, ["my-app"]);
}

#[test]
fn update_job_blue_green() {
    let mut l = leader();
    let mut net = FakeNet::new();
    join(&mut l, &mut net, "agent-1", NOW);
    l.dispatch_job(v("my-app", "./app-v1", 1, None), &mut net)
        .unwrap();
    let before = net.get("agent-1").run_calls;
    l.update_job(
        v("my-app", "./app-v2", 1, Some(UpdatePolicy::BlueGreen)),
        &mut net,
    )
    .unwrap();
    assert!(net.get("agent-1").run_calls > before);
    assert_eq!(l.job("my-app").unwrap().command, "./app-v2");
    assert_eq!(net.get("agent-1").tasks_for_job("my-app"), 1);
    assert_eq!(placed_total(&l, "my-app"), 1);
}

#[test]
fn update_job_rolling_failure_keeps_old() {
    let mut l = leader();
    let mut net = FakeNet::new();
    join(&mut l, &mut net, "agent-1", NOW);
    l.dispatch_job(v("my-app", "./app-v1", 1, None), &mut net)
        .unwrap();
    assert_eq!(net.get("agent-1").task_count(), 1);
    net.get("agent-1").fail_runs = true;
    let err = l
        .update_job(
            v("my-app", "./app-v2", 1, Some(UpdatePolicy::Rolling)),
            &mut net,
        )
        .unwrap_err();
    assert!(matches!(err, Error::Rolling { instance: 1, .. }));
    // Het oude exemplaar draait nog: dat is de kernbewering.
    assert_eq!(net.get("agent-1").task_count(), 1);
    assert!(l.job("my-app").is_some());
}

#[test]
fn update_job_blue_green_failure_keeps_old() {
    let mut l = leader();
    let mut net = FakeNet::new();
    join(&mut l, &mut net, "agent-1", NOW);
    l.dispatch_job(v("my-app", "./app-v1", 1, None), &mut net)
        .unwrap();
    net.get("agent-1").fail_runs = true;
    let err = l
        .update_job(
            v("my-app", "./app-v2", 1, Some(UpdatePolicy::BlueGreen)),
            &mut net,
        )
        .unwrap_err();
    assert!(matches!(
        err,
        Error::BlueGreen {
            instance: 1,
            count: 1,
            ..
        }
    ));
    assert_eq!(net.get("agent-1").task_count(), 1);
    assert!(l.job("my-app").is_some());
}

#[test]
fn update_job_not_found() {
    let mut l = leader();
    let mut net = FakeNet::new();
    let err = l
        .update_job(v("nonexistent", "./app", 1, None), &mut net)
        .unwrap_err();
    assert!(matches!(err, Error::NotFound { .. }));
}

#[test]
fn find_job_by_name() {
    let mut l = leader();
    let mut net = FakeNet::new();
    let _ = l.dispatch_job(job("app-1", 1), &mut net);
    let _ = l.dispatch_job(job("app-2", 1), &mut net);
    assert_eq!(l.job("app-1").unwrap().name, "app-1");
    assert!(l.job("nonexistent").is_none());
}

// De echte agent verwijdert bij DELETE /delete/{naam} álle taken met die
// naam, ook de net gestarte nieuwe versie. Een update stopt daarom per
// taak-id. Met één agent staan oud en nieuw altijd naast elkaar.
#[test]
fn update_rolling_delete_by_name_bug() {
    let mut l = leader();
    let mut net = FakeNet::new();
    join(&mut l, &mut net, "agent-1", NOW);
    l.dispatch_job(v("my-app", "./app-v1", 1, None), &mut net)
        .unwrap();
    assert_eq!(net.get("agent-1").task_count(), 1);
    l.update_job(
        v("my-app", "./app-v2", 1, Some(UpdatePolicy::Rolling)),
        &mut net,
    )
    .unwrap();
    assert_eq!(net.get("agent-1").task_count(), 1);
    assert_eq!(net.get("agent-1").tasks_for_job("my-app"), 1);
    assert!(net.get("agent-1").deletes.is_empty());
}

#[test]
fn update_blue_green_delete_by_name_bug() {
    let mut l = leader();
    let mut net = FakeNet::new();
    join(&mut l, &mut net, "agent-1", NOW);
    l.dispatch_job(v("my-app", "./app-v1", 1, None), &mut net)
        .unwrap();
    l.update_job(
        v("my-app", "./app-v2", 1, Some(UpdatePolicy::BlueGreen)),
        &mut net,
    )
    .unwrap();
    assert_eq!(net.get("agent-1").task_count(), 1);
    assert_eq!(net.get("agent-1").tasks_for_job("my-app"), 1);
}

// ---- update_preempt_test.go ----

// De les van 01-08 op de LicheeRV (1 core, 2 kooien): een re-apply van
// welcome telde oud en nieuw even samen, de node meldde "vol", en de leider
// offerde cloudflared via de preemptie-pas. Een update mag nooit een buurman
// preempten: de terugval is vervangen ter plekke.
#[test]
fn update_rolling_preempt_geen_buurman() {
    let mut l = leader();
    let mut net = FakeNet::new();
    join(&mut l, &mut net, "node-1", NOW);
    net.get("node-1").max_capacity = 2;
    l.dispatch_job(job_prio("cloudflared", 1, 1), &mut net)
        .unwrap();
    l.dispatch_job(job_prio("welcome", 1, 0), &mut net).unwrap();
    let mut nieuw = job_prio("welcome", 1, 0);
    nieuw.command = "./welcome-v2".to_string();
    nieuw.update_policy = Some(UpdatePolicy::Rolling);
    l.update_job(nieuw, &mut net).unwrap();
    assert_eq!(net.get("node-1").tasks_for_job("cloudflared"), 1);
    assert_eq!(net.get("node-1").tasks_for_job("welcome"), 1);
    assert_eq!(l.job("welcome").unwrap().command, "./welcome-v2");
    assert!(net.get("node-1").stops.is_empty(), "de buurman is gestopt");
}

// De hand-back boekt af en reconcilet meteen; anders bleef de teller op 1
// en zag reconcile voorgoed een gezonde job waar niets draaide (01-08).
#[test]
fn mark_unplaced_boekt_af_en_reconcilet() {
    let mut l = leader();
    let mut net = FakeNet::new();
    join(&mut l, &mut net, "node-1", NOW);
    l.dispatch_job(job("cloudflared", 1), &mut net).unwrap();
    assert_eq!(l.placed("cloudflared").unwrap().len(), 1);
    net.get("node-1").clear_tasks();
    l.mark_unplaced("node-1", "cloudflared", &mut net).unwrap();
    assert_eq!(net.get("node-1").task_count(), 1);
    assert_eq!(placed_total(&l, "cloudflared"), 1);
}

// ---- deploying_test.go ----

// De vlag zegt de waarheid over een uitrol: een gewone dispatch is er geen,
// een geslaagde update wist hem, een gebroken update laat hem staan.
#[test]
fn deploying_reflects_rollout_truth() {
    let mut l = leader();
    let mut net = FakeNet::new();
    join(&mut l, &mut net, "a", NOW);
    l.dispatch_job(v("app", "./v1", 1, None), &mut net).unwrap();
    assert!(!l.job("app").unwrap().deploying);
    l.update_job(v("app", "./v2", 1, None), &mut net).unwrap();
    assert!(!l.job("app").unwrap().deploying);
    net.get("a").fail_runs = true;
    assert!(l.update_job(v("app", "./v3", 1, None), &mut net).is_err());
    assert!(l.job("app").unwrap().deploying);
}

#[test]
fn update_keeps_old_priority_when_unset() {
    let mut l = leader();
    let mut net = FakeNet::new();
    join(&mut l, &mut net, "a", NOW);
    l.dispatch_job(job_prio("x", 1, 0), &mut net).unwrap();
    l.dispatch_job(job_prio("app", 1, 1), &mut net).unwrap();
    l.update_job(v("app", "./v2", 1, None), &mut net).unwrap();
    assert_eq!(l.store().get("app").unwrap().priority, Some(1));
}
