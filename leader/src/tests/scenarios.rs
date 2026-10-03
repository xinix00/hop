//! `scenarios_test.go`: updates met gefaalde taken, op- en afschalen, de
//! juistheid van `placed`, nodes die komen en gaan, prioriteiten en de
//! levensloop deploy, delete, redeploy.
//!
//! Weggelaten: `TestConcurrentUpdates_SameJob`. Die toetste dat van twee
//! gelijktijdige updates er één de 409 `ErrJobLocked` kreeg; met één eigenaar
//! bestaan twee gelijktijdige updates niet, en de job-lock is er dus ook niet.

use std::format;
use std::string::ToString;

use types::time::MILLISECOND;
use types::{Job, Map, TaskState, UpdatePolicy};

use crate::testkit::*;
use crate::{Leader, MemStore};

fn v(name: &str, cmd: &str, count: i64, policy: Option<UpdatePolicy>) -> Job {
    Job {
        command: cmd.to_string(),
        update_policy: policy,
        ..job(name, count)
    }
}

fn vp(name: &str, count: i64, priority: i64) -> Job {
    Job {
        command: format!("./{name}"),
        ..job_prio(name, count, priority)
    }
}

/// Een leider met `n` nep-agents `a0`, `a1`, ... (Go's `setupLeader` plus
/// `registerAgent`).
fn cluster(ids: &[&str]) -> (Leader<MemStore>, FakeNet) {
    let mut l = leader();
    let mut net = FakeNet::new();
    for id in ids {
        join(&mut l, &mut net, id, NOW);
    }
    (l, net)
}

fn running(net: &FakeNet) -> usize {
    net.agents.values().map(|a| a.running_count()).sum()
}

/// Markeert de eerste taak van `name` die een van de agents heeft als gefaald.
fn fail_first(net: &mut FakeNet, ids: &[&str], name: &str) {
    for id in ids {
        if let Some(t) = net.get(id).task_ids(name).first().cloned() {
            net.get(id).mark_task_state(&t, TaskState::Failed);
            return;
        }
    }
    panic!("geen taak van {name} gevonden");
}

// ---- A. Updates met gefaalde taken ----

#[test]
fn update_rolling_one_task_failed() {
    let ids = ["a1", "a2", "a3"];
    let (mut l, mut net) = cluster(&ids);
    l.dispatch_job(v("api", "./api-v1", 3, None), &mut net)
        .unwrap();
    assert_eq!(net.total_tasks(), 3);
    fail_first(&mut net, &ids, "api");
    assert_eq!(running(&net), 2);

    l.update_job(
        v("api", "./api-v2", 3, Some(UpdatePolicy::Rolling)),
        &mut net,
    )
    .unwrap();
    // Alle oude taken weg, ook de gefaalde; drie nieuwe.
    assert_eq!(running(&net), 3);
    assert_eq!(net.total_tasks(), 3);
    assert_eq!(placed_total(&l, "api"), 3);
}

#[test]
fn update_rolling_all_tasks_failed() {
    let (mut l, mut net) = cluster(&["a1"]);
    l.dispatch_job(v("worker", "./worker-v1", 2, None), &mut net)
        .unwrap();
    assert_eq!(net.get("a1").task_count(), 2);
    net.get("a1")
        .mark_job_tasks_state("worker", TaskState::Failed);
    assert_eq!(net.get("a1").running_count(), 0);

    l.update_job(
        v("worker", "./worker-v2", 2, Some(UpdatePolicy::Rolling)),
        &mut net,
    )
    .unwrap();
    assert_eq!(net.get("a1").running_count(), 2);
    assert_eq!(placed_total(&l, "worker"), 2);
}

#[test]
fn update_recreate_with_failed_task() {
    let (mut l, mut net) = cluster(&["a1"]);
    l.dispatch_job(v("web", "./web-v1", 3, None), &mut net)
        .unwrap();
    fail_first(&mut net, &["a1"], "web");

    l.update_job(
        v("web", "./web-v2", 3, Some(UpdatePolicy::Recreate)),
        &mut net,
    )
    .unwrap();
    assert_eq!(net.get("a1").running_count(), 3);
    assert_eq!(net.get("a1").task_count(), 3);
    assert_eq!(placed_total(&l, "web"), 3);
}

#[test]
fn update_blue_green_with_failed_task() {
    let (mut l, mut net) = cluster(&["a1"]);
    l.dispatch_job(v("app", "./app-v1", 2, None), &mut net)
        .unwrap();
    fail_first(&mut net, &["a1"], "app");

    l.update_job(
        v("app", "./app-v2", 2, Some(UpdatePolicy::BlueGreen)),
        &mut net,
    )
    .unwrap();
    // Twee nieuwe ernaast, dan de twee oude weg (ook de gefaalde).
    assert_eq!(net.get("a1").running_count(), 2);
    assert_eq!(net.get("a1").task_count(), 2);
    assert_eq!(placed_total(&l, "app"), 2);
}

// ---- B. Op- en afschalen tijdens een update ----

#[test]
fn update_rolling_scale_up() {
    let (mut l, mut net) = cluster(&["a0", "a1", "a2"]);
    l.dispatch_job(v("api", "./api-v1", 2, None), &mut net)
        .unwrap();
    assert_eq!(net.total_tasks(), 2);

    l.update_job(
        v("api", "./api-v2", 5, Some(UpdatePolicy::Rolling)),
        &mut net,
    )
    .unwrap();
    // Twee vervangen, de reconcile daarna vult er drie bij.
    assert_eq!(placed_total(&l, "api"), 5);
    assert_eq!(net.total_tasks(), 5);
}

#[test]
fn update_recreate_scale_up() {
    let (mut l, mut net) = cluster(&["a0", "a1", "a2"]);
    l.dispatch_job(v("api", "./api-v1", 2, None), &mut net)
        .unwrap();

    l.update_job(
        v("api", "./api-v2", 5, Some(UpdatePolicy::Recreate)),
        &mut net,
    )
    .unwrap();
    assert_eq!(placed_total(&l, "api"), 5);
    assert_eq!(net.total_tasks(), 5);
}

#[test]
fn update_rolling_scale_down() {
    let (mut l, mut net) = cluster(&["a0", "a1", "a2"]);
    l.dispatch_job(v("api", "./api-v1", 5, None), &mut net)
        .unwrap();
    assert_eq!(net.total_tasks(), 5);

    l.update_job(
        v("api", "./api-v2", 3, Some(UpdatePolicy::Rolling)),
        &mut net,
    )
    .unwrap();
    // Drie vervangen, twee overtollige gestopt.
    assert_eq!(placed_total(&l, "api"), 3);
    assert_eq!(net.total_tasks(), 3);
}

#[test]
fn update_recreate_scale_down() {
    let (mut l, mut net) = cluster(&["a0", "a1", "a2"]);
    l.dispatch_job(v("api", "./api-v1", 5, None), &mut net)
        .unwrap();

    l.update_job(
        v("api", "./api-v2", 3, Some(UpdatePolicy::Recreate)),
        &mut net,
    )
    .unwrap();
    assert_eq!(placed_total(&l, "api"), 3);
    assert_eq!(net.total_tasks(), 3);
}

// ---- C. De juistheid van placed ----

#[test]
fn placed_count_not_updated_on_task_failure() {
    // `placed` telt plaatsingen, niet de staat van taken: een gefaalde taak
    // is de zaak van de agent (herstarten), niet van de leider.
    let (mut l, mut net) = cluster(&["a1"]);
    l.dispatch_job(v("app", "./app", 3, None), &mut net)
        .unwrap();
    assert_eq!(net.get("a1").task_count(), 3);
    fail_first(&mut net, &["a1"], "app");

    assert_eq!(placed_total(&l, "app"), 3);
    assert_eq!(net.get("a1").running_count(), 2);
    l.reconcile_jobs(&mut net).unwrap();
    assert_eq!(net.get("a1").run_calls, 3);
}

#[test]
fn placed_count_fixed_by_re_registration() {
    let (mut l, mut net) = cluster(&["a1"]);
    l.dispatch_job(v("app", "./app", 5, None), &mut net)
        .unwrap();
    assert_eq!(net.get("a1").task_count(), 5);

    // Twee taken definitief weg op de agent.
    let a1 = net.get("a1");
    let keep = a1.tasks.len() - 2;
    a1.tasks.truncate(keep);
    assert_eq!(net.get("a1").task_count(), 3);
    assert_eq!(placed_total(&l, "app"), 5);

    // De herregistratie met de echte telling zet de leider recht.
    assert!(rejoin(&mut l, &mut net, "a1", at(1)));
    assert_eq!(net.get("a1").task_count(), 5);
    assert_eq!(placed_total(&l, "app"), 5);
}

// ---- D. Nodes die komen en gaan, met gemengde taakstaten ----

#[test]
fn node_leave_with_mixed_task_states() {
    let (mut l, mut net) = cluster(&["a", "b"]);
    l.set_agent_timeout(200 * MILLISECOND);
    l.dispatch_job(v("api", "./api", 6, None), &mut net)
        .unwrap();
    let b_tasks = net.get("b").task_count();
    assert_eq!(net.get("a").task_count() + b_tasks, 6);
    fail_first(&mut net, &["a"], "api");

    // A sterft, met een draaiende en een gefaalde taak.
    net.get("a").down = true;
    l.heartbeat("b", "", Default::default(), at_ms(300));
    l.check_dead_agents(at_ms(300), &mut net).unwrap();

    // A's hele telling (3) is weg en gaat naar B.
    assert_eq!(net.get("b").task_count(), 6);
    assert!(l.placed("api").unwrap().get("a").is_none());
}

#[test]
fn node_join_gets_daemon_and_filled_regular_jobs() {
    let (mut l, mut net) = cluster(&["a1"]);
    l.dispatch_job(v("monitor", "./monitor", -1, None), &mut net)
        .unwrap();
    l.dispatch_job(v("web", "./web", 4, None), &mut net)
        .unwrap();
    assert_eq!(net.get("a1").tasks_for_job("monitor"), 1);
    assert_eq!(net.get("a1").tasks_for_job("web"), 4);

    join(&mut l, &mut net, "a2", NOW);
    assert_eq!(net.get("a2").tasks_for_job("monitor"), 1);
    // De gewone job stond al op 4/4 en krijgt er niets bij.
    assert_eq!(net.total_for_job("web"), 4);
}

#[test]
fn node_join_under_scheduled_job_gets_instances() {
    let mut l = leader();
    let mut net = FakeNet::new();
    net.add("a1");
    net.get("a1").max_capacity = 3;
    l.register_agent(agent("a1", "http://a1"), Map::new(), NOW, &mut net)
        .unwrap();

    // Vijf gevraagd, drie passen: de rest wacht.
    assert!(
        l.dispatch_job(v("api", "./api", 5, None), &mut net)
            .is_err()
    );
    assert_eq!(net.get("a1").task_count(), 3);
    assert_eq!(placed_total(&l, "api"), 3);

    join(&mut l, &mut net, "a2", NOW);
    assert_eq!(net.total_tasks(), 5);
    assert_eq!(net.get("a2").task_count(), 2);
}

#[test]
fn node_leave_during_rolling_update() {
    // Go liet a2 sterven terwijl de update in een goroutine liep; synchroon
    // is dat: a2 antwoordt niet meer als de update begint.
    let (mut l, mut net) = cluster(&["a1", "a2"]);
    l.set_agent_timeout(200 * MILLISECOND);
    l.dispatch_job(v("api", "./api-v1", 6, None), &mut net)
        .unwrap();
    assert_eq!(net.total_tasks(), 6);

    net.get("a2").down = true;
    // Mag half lukken; waar het om gaat is dat de cluster daarna convergeert.
    let _ = l.update_job(
        v("api", "./api-v2", 6, Some(UpdatePolicy::Rolling)),
        &mut net,
    );

    l.heartbeat("a1", "", Default::default(), at_ms(300));
    l.check_dead_agents(at_ms(300), &mut net).unwrap();
    assert!(net.get("a1").task_count() >= 6);
    assert_eq!(placed_total(&l, "api"), 6);
}

// ---- E. Prioriteiten tijdens een gedeeltelijke plaatsing ----

#[test]
fn priority_patch_during_active_dispatch() {
    // Go patchte de prioriteit terwijl een trage dispatch in een goroutine
    // liep (de race die de dispatching-vlag dichtzette). Met één eigenaar
    // komt de patch ná de dispatch, en de bewering blijft: geen overschot.
    let (mut l, mut net) = cluster(&["a1"]);
    let big = vp("big", 10, 5);
    l.store_put(big.clone()).unwrap();
    l.dispatch_job(big, &mut net).unwrap();
    l.patch_job_priority("big", 0, &mut net).unwrap();
    assert_eq!(net.get("a1").task_count(), 10);
    assert_eq!(placed_total(&l, "big"), 10);
}

#[test]
fn priority_patch_unscheduled_job_gets_capacity() {
    let mut l = leader();
    let mut net = FakeNet::new();
    net.add("a1");
    net.get("a1").max_capacity = 5;
    l.register_agent(agent("a1", "http://a1"), Map::new(), NOW, &mut net)
        .unwrap();

    l.dispatch_job(vp("batch", 5, 10), &mut net).unwrap();
    assert_eq!(net.get("a1").task_count(), 5);

    // Minder belangrijk en geen ruimte: niets.
    assert!(l.dispatch_job(vp("critical", 5, 20), &mut net).is_err());
    assert_eq!(net.get("a1").tasks_for_job("critical"), 0);

    // Naar de top: nu mag hij verdringen.
    l.patch_job_priority("critical", 0, &mut net).unwrap();
    assert!(net.get("a1").tasks_for_job("critical") > 0);
}

#[test]
fn normalize_priorities_after_deletion() {
    let (mut l, mut net) = cluster(&["a1"]);
    for (i, name) in ["job-a", "job-b", "job-c"].into_iter().enumerate() {
        l.dispatch_job(vp(name, 1, i as i64), &mut net).unwrap();
    }
    l.delete_job("job-b", &mut net).unwrap();
    assert_eq!(l.job("job-a").unwrap().priority, Some(0));
    assert_eq!(l.job("job-c").unwrap().priority, Some(1));
}

#[test]
fn priority_reorder_three_jobs() {
    let (mut l, mut net) = cluster(&["a1"]);
    for (i, name) in ["A", "B", "C"].into_iter().enumerate() {
        l.dispatch_job(vp(name, 1, i as i64), &mut net).unwrap();
    }
    l.patch_job_priority("C", 0, &mut net).unwrap();
    assert_eq!(l.job("C").unwrap().priority, Some(0));
    assert_eq!(l.job("A").unwrap().priority, Some(1));
    assert_eq!(l.job("B").unwrap().priority, Some(2));
}

#[test]
fn update_job_preserves_priority() {
    let (mut l, mut net) = cluster(&["a1"]);
    for (i, name) in ["A", "B", "C"].into_iter().enumerate() {
        l.dispatch_job(vp(name, 1, i as i64), &mut net).unwrap();
    }
    // Zonder eigen prioriteit houdt B zijn plek.
    l.update_job(v("B", "./B-v2", 1, None), &mut net).unwrap();
    let p = |n: &str| l.job(n).unwrap().priority.unwrap();
    assert!(p("A") < p("B"));
    assert!(p("B") < p("C"));
    assert_eq!(l.job("B").unwrap().command, "./B-v2");
}

// ---- F. Deploy, delete, redeploy ----

#[test]
fn deploy_delete_redeploy() {
    let (mut l, mut net) = cluster(&["a1"]);
    l.dispatch_job(v("api", "./api-v1", 3, None), &mut net)
        .unwrap();
    assert_eq!(net.get("a1").task_count(), 3);

    l.delete_job("api", &mut net).unwrap();
    assert!(l.job("api").is_none());
    assert_eq!(net.get("a1").tasks_for_job("api"), 0);
    assert_eq!(placed_total(&l, "api"), 0);

    // Dezelfde naam, andere definitie: geen spookstaat van de vorige.
    l.dispatch_job(v("api", "./api-v2", 2, None), &mut net)
        .unwrap();
    assert_eq!(net.get("a1").tasks_for_job("api"), 2);
    assert_eq!(placed_total(&l, "api"), 2);
    assert_eq!(l.job("api").unwrap().command, "./api-v2");
}

#[test]
fn delete_job_with_mixed_task_states() {
    let (mut l, mut net) = cluster(&["a1"]);
    l.dispatch_job(v("app", "./app", 4, None), &mut net)
        .unwrap();
    let ids = net.get("a1").task_ids("app");
    net.get("a1").mark_task_state(&ids[0], TaskState::Failed);
    net.get("a1").mark_task_state(&ids[1], TaskState::Failed);
    assert_eq!(net.get("a1").running_count(), 2);

    l.delete_job("app", &mut net).unwrap();
    assert_eq!(net.get("a1").tasks_for_job("app"), 0);
    assert_eq!(placed_total(&l, "app"), 0);
}

#[test]
fn multiple_jobs_independent_lifecycle() {
    let (mut l, mut net) = cluster(&["a0", "a1"]);
    for j in [vp("api", 4, 0), vp("worker", 2, 1), vp("cron", 1, 2)] {
        l.dispatch_job(j, &mut net).unwrap();
    }
    assert_eq!(net.total_tasks(), 7);

    l.update_job(
        v("worker", "./worker-v2", 2, Some(UpdatePolicy::Rolling)),
        &mut net,
    )
    .unwrap();
    assert_eq!(placed_total(&l, "api"), 4);
    assert_eq!(placed_total(&l, "cron"), 1);
    assert_eq!(l.job("worker").unwrap().command, "./worker-v2");

    l.delete_job("cron", &mut net).unwrap();
    assert!(l.job("cron").is_none());
    assert_eq!(placed_total(&l, "api"), 4);
}

// ---- G. Randgevallen ----

#[test]
fn update_rolling_no_old_tasks() {
    // De job staat in de store en `placed` zegt 2, maar de agent draait niets.
    let (mut l, mut net) = cluster(&["a1"]);
    l.store_put(v("ghost", "./ghost-v1", 2, None)).unwrap();
    force_placed(&mut l, "a1", "ghost", 2);

    l.update_job(
        v("ghost", "./ghost-v2", 2, Some(UpdatePolicy::Rolling)),
        &mut net,
    )
    .unwrap();
    // Nul oude taken, dus nul vervangen; de definitie is wel bijgewerkt.
    assert_eq!(l.job("ghost").unwrap().command, "./ghost-v2");
    assert_eq!(placed_total(&l, "ghost"), 2);
}

#[test]
fn update_rolling_count_one_single_agent() {
    let (mut l, mut net) = cluster(&["a1"]);
    l.dispatch_job(v("api", "./api-v1", 1, None), &mut net)
        .unwrap();
    assert_eq!(net.get("a1").task_count(), 1);

    l.update_job(
        v("api", "./api-v2", 1, Some(UpdatePolicy::Rolling)),
        &mut net,
    )
    .unwrap();
    // Stoppen op taak-id, niet op naam: dat laatste doodde ook de nieuwe.
    assert_eq!(net.get("a1").task_count(), 1);
    assert_eq!(placed_total(&l, "api"), 1);
}

#[test]
fn dispatch_during_settle_period() {
    let mut l = leader();
    l.settle(200 * MILLISECOND, NOW);
    let mut net = FakeNet::new();
    join(&mut l, &mut net, "a1", NOW);
    assert!(!l.is_settled());

    // Tijdens settle opgeslagen, niet verstuurd.
    l.dispatch_job(v("api", "./api", 3, None), &mut net)
        .unwrap();
    assert!(l.job("api").is_some());
    assert_eq!(net.get("a1").task_count(), 0);

    l.tick(at_ms(300), &mut net).unwrap();
    assert!(l.is_settled());
    assert_eq!(net.get("a1").task_count(), 3);
}

#[test]
fn update_rolling_multi_agent_failed_task_on_one_agent() {
    let (mut l, mut net) = cluster(&["a1", "a2"]);
    l.dispatch_job(v("api", "./api-v1", 4, None), &mut net)
        .unwrap();
    assert_eq!(net.total_tasks(), 4);
    net.get("a1").mark_job_tasks_state("api", TaskState::Failed);
    assert_eq!(net.get("a1").running_count(), 0);

    l.update_job(
        v("api", "./api-v2", 4, Some(UpdatePolicy::Rolling)),
        &mut net,
    )
    .unwrap();
    assert_eq!(running(&net), 4);
    assert_eq!(net.total_tasks(), 4);
    assert_eq!(placed_total(&l, "api"), 4);
}
