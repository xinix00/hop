//! De tests uit `internal/agent/*_test.go` en `internal/agentloop/loop_test.go`.
//!
//! Waar Go een goroutine blokkeerde om een race na te spelen, zet de test hier
//! de invoer in de volgorde van die race. De namen zijn de Go-namen in
//! snake_case; de bedoeling staat erbij als de vorm anders is.

use super::*;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use types::{Artifact, CheckType, Driver, HealthCheck, Job, Map, Task, TaskState, Time};

const S: u64 = types::time::SECOND;
const T0: u64 = 1_000 * S;

fn map<V>(pairs: &[(&str, V)]) -> Map<V>
where
    V: Clone,
{
    let mut m = Map::new();
    for (k, v) in pairs {
        m.insert(k.to_string(), v.clone()).unwrap();
    }
    m
}

/// Zoals Go's `testConfig`: 1000 shares, 1 GiB, op een ruime machine.
fn settings() -> Settings {
    let mut attrs = alloc::collections::BTreeMap::new();
    for (k, v) in [
        ("node.id", "test-agent"),
        ("node.arch", "arm64"),
        ("node.os", "linux"),
        ("node.docker", "false"),
    ] {
        attrs.insert(k.to_string(), v.to_string());
    }
    Settings {
        id: "test-agent".into(),
        endpoint: "http://127.0.0.1:8080".into(),
        attributes: attrs,
        cpu_cores: 8,
        memory_bytes: 16 << 30,
        cap_cpu_shares: 1000,
        cap_memory: 1 << 30,
        seed: 42,
        ..Settings::default()
    }
}

fn agent() -> Agent {
    Agent::new(settings())
}

fn job(name: &str) -> Job {
    Job {
        name: name.into(),
        command: "./app".into(),
        ..Job::default()
    }
}

fn starts(actions: &[Action]) -> Vec<String> {
    actions
        .iter()
        .filter_map(|a| match a {
            Action::Start { task_id, .. } => Some(task_id.clone()),
            _ => None,
        })
        .collect()
}

fn stops(actions: &[Action]) -> Vec<String> {
    actions
        .iter()
        .filter_map(|a| match a {
            Action::Stop { task_id, .. } => Some(task_id.clone()),
            _ => None,
        })
        .collect()
}

fn notes(actions: &[Action]) -> Vec<(String, Event)> {
    actions
        .iter()
        .filter_map(|a| match a {
            Action::Notify { job, event } => Some((job.clone(), *event)),
            _ => None,
        })
        .collect()
}

fn ok(pid: i64) -> core::result::Result<StartOk, StartError> {
    Ok(StartOk {
        pid,
        ports: Map::new(),
    })
}

/// Neemt een job aan en laat de runner hem starten; geeft het taak-id.
fn run_ok(a: &mut Agent, now: u64, j: Job) -> String {
    let id = a.run(now, j, false, None).unwrap();
    let acts = a.take_actions();
    assert_eq!(starts(&acts), core::slice::from_ref(&id));
    a.on_started(now, &id, Driver::Exec, ok(1001));
    a.take_actions();
    id
}

/// Een HopOS-node: de capaciteit is het aantal app-cores, zonder lagere grens.
fn hop_agent() -> Agent {
    let mut s = settings();
    s.cap_cpu_shares = 0;
    s.cpu_cores = 3;
    Agent::new(s)
}

// ---- agent_test.go -------------------------------------------------------------

#[test]
fn resource_usage_collapses_sharegroup() {
    let mut a = agent();
    let mut web = job("web");
    web.cpu_shares = 2048;
    web.tags = map(&[("sharegroup", "web".to_string())]);
    a.store_job(T0, web).unwrap();
    a.store_job(T0, job("solo")).unwrap();
    for (id, name, cpu) in [
        ("w1", "web", 2048),
        ("w2", "web", 2048),
        ("s1", "solo", 1024),
    ] {
        a.insert_task(Task {
            id: id.into(),
            job_name: name.into(),
            cpu_shares: cpu,
            memory_limit: 64 << 20,
            ..Task::default()
        });
    }
    assert_eq!(a.resource_usage(), (3072, 3 * (64 << 20)));
}

#[test]
fn every_task_in_state_holds_its_reservation() {
    let mut a = agent();
    for (i, st) in [
        TaskState::Queued,
        TaskState::Downloading,
        TaskState::Running,
        TaskState::Stopping,
        TaskState::Failed,
    ]
    .into_iter()
    .enumerate()
    {
        a.insert_task(Task {
            id: alloc::format!("t{i}"),
            job_name: "j".into(),
            state: st,
            cpu_shares: 100,
            memory_limit: 1,
            ..Task::default()
        });
    }
    assert_eq!(a.resource_usage(), (500, 5));
}

#[test]
fn agent_new() {
    let a = agent();
    assert_eq!(a.id(), "test-agent");
    assert_eq!(a.endpoint(), "http://127.0.0.1:8080");
    assert_eq!(a.tasks().count(), 0);
}

#[test]
fn agent_start_job() {
    let mut a = agent();
    let id = run_ok(&mut a, T0, job("test-job"));
    let t = a.task(&id).unwrap();
    assert_eq!(t.state, TaskState::Running);
    assert_eq!(t.pid, 1001);
    assert!(a.get_job("test-job").is_some());
}

#[test]
fn agent_start_job_runner_error() {
    let mut a = agent();
    let id = a.run(T0, job("fail"), false, None).unwrap();
    a.take_actions();
    a.on_started(T0, &id, Driver::Exec, Err(StartError::Failed));
    let acts = a.take_actions();
    // Mislukte start: failed zichtbaar, crash gemeld, één opruimpoging, en een verse poging.
    assert!(notes(&acts).contains(&("fail".into(), Event::Crash)));
    assert_eq!(stops(&acts), core::slice::from_ref(&id));
    assert_eq!(starts(&acts).len(), 1);
}

#[test]
fn agent_delete_job() {
    let mut a = agent();
    let id = run_ok(&mut a, T0, job("del"));
    assert_eq!(a.delete_job(T0, "del"), 1);
    let acts = a.take_actions();
    assert_eq!(stops(&acts), [id]);
    assert!(a.get_job("del").is_none());
    assert_eq!(a.tasks().count(), 0);
}

#[test]
fn agent_stop_all_tasks() {
    let mut a = agent();
    run_ok(&mut a, T0, job("a"));
    run_ok(&mut a, T0, job("b"));
    assert_eq!(a.stop_all(), 2);
    assert_eq!(stops(&a.take_actions()).len(), 2);
    assert_eq!(a.tasks().count(), 0);
}

#[test]
fn agent_delete_job_with_multiple_tasks() {
    let mut a = agent();
    for _ in 0..3 {
        run_ok(&mut a, T0, job("multi"));
    }
    assert_eq!(a.delete_job(T0, "multi"), 3);
    assert_eq!(stops(&a.take_actions()).len(), 3);
}

#[test]
fn agent_delete_job_non_existent() {
    let mut a = agent();
    assert_eq!(a.delete_job(T0, "nope"), 0);
}

#[test]
fn delete_job_removes_tasks_before_stop() {
    let mut a = agent();
    let id = run_ok(&mut a, T0, job("order"));
    a.delete_job(T0, "order");
    // Het record is weg vóór de runner-stop; de stop-actie draagt wat de runner nodig heeft.
    assert!(a.task(&id).is_none());
    assert!(matches!(
        &a.take_actions()[0],
        Action::Stop { pid: 1001, .. }
    ));
}

// ---- capacity_test.go ----------------------------------------------------------

fn fits(a: &mut Agent, cpu: i64, mem: u64) -> bool {
    let mut j = job("probe");
    j.cpu_shares = cpu;
    j.memory_limit = mem;
    let r = a.run(T0, j, false, None);
    if let Ok(id) = &r {
        a.take_actions();
        a.stop_task(id);
        a.take_actions();
    }
    r.is_ok()
}

#[test]
fn agent_has_capacity() {
    let mut a = agent();
    assert!(fits(&mut a, 500, 512 << 20));
}

#[test]
fn agent_capacity_with_running_tasks() {
    let mut a = agent();
    let mut j = job("big");
    j.cpu_shares = 800;
    run_ok(&mut a, T0, j);
    assert!(!fits(&mut a, 300, 0));
    assert!(fits(&mut a, 200, 0));
}

#[test]
fn agent_capacity_counts_failed_tasks() {
    let mut a = agent();
    a.insert_task(Task {
        id: "f".into(),
        job_name: "x".into(),
        state: TaskState::Failed,
        cpu_shares: 900,
        ..Task::default()
    });
    assert!(!fits(&mut a, 200, 0));
}

#[test]
fn agent_capacity_freed_when_task_is_gone() {
    let mut a = agent();
    let mut j = job("big");
    j.cpu_shares = 900;
    let id = run_ok(&mut a, T0, j);
    assert!(!fits(&mut a, 200, 0));
    a.stop_task(&id);
    assert!(fits(&mut a, 200, 0));
}

#[test]
fn agent_capacity_uses_system_defaults() {
    let mut s = settings();
    s.cap_cpu_shares = 0;
    s.cap_memory = 0;
    s.cpu_cores = 2;
    let a = Agent::new(s);
    assert_eq!(a.settings().effective_cpu_shares(), 2048);
    assert_eq!(a.settings().effective_memory_bytes(), 16 << 30);
}

#[test]
fn agent_capacity_job_with_no_limits() {
    let mut a = agent();
    assert!(fits(&mut a, 0, 0));
}

#[test]
fn agent_capacity_exceeds_memory() {
    let mut a = agent();
    assert!(!fits(&mut a, 0, 2 << 30));
}

#[test]
fn agent_capacity_exceeds_cpu() {
    let mut a = agent();
    assert!(!fits(&mut a, 1001, 0));
}

#[test]
fn agent_capacity_exact_limit() {
    let mut s = settings();
    s.cpu_cores = 1;
    s.memory_bytes = 1024;
    s.cap_cpu_shares = 0;
    s.cap_memory = 0;
    let mut a = Agent::new(s);
    assert!(fits(&mut a, 1024, 1024));
}

#[test]
fn agent_capacity_without_job_definition() {
    // Een taak zonder bekende job telt nog steeds mee.
    let mut a = agent();
    a.insert_task(Task {
        id: "orphan".into(),
        job_name: "gone".into(),
        cpu_shares: 1000,
        ..Task::default()
    });
    assert!(!fits(&mut a, 1, 0));
}

#[test]
fn concurrent_dispatch_respects_capacity() {
    // Tien dispatches van 200 shares op een node van 1000: precies vijf passen.
    let mut a = agent();
    let mut ok = 0;
    for i in 0..10 {
        let mut j = job(&alloc::format!("j{i}"));
        j.cpu_shares = 200;
        if a.run(T0, j, false, None).is_ok() {
            ok += 1;
        }
    }
    assert_eq!(ok, 5);
}

#[test]
fn task_in_state_immediately_after_accept() {
    let mut a = agent();
    let id = a.run(T0, job("q"), false, None).unwrap();
    assert_eq!(a.task(&id).unwrap().state, TaskState::Queued);
}

#[test]
fn agent_capacity_multiple_running_tasks() {
    let mut a = agent();
    for i in 0..4 {
        let mut j = job(&alloc::format!("j{i}"));
        j.cpu_shares = 250;
        run_ok(&mut a, T0, j);
    }
    assert_eq!(a.capacity().cpu_used_shares, 1000);
    assert_eq!(a.capacity().tasks_running, 4);
    assert!(!fits(&mut a, 1, 0));
}

#[test]
fn crashed_task_still_reserves_capacity() {
    let mut a = agent();
    let mut j = job("crash");
    j.cpu_shares = 900;
    let id = run_ok(&mut a, T0, j);
    a.on_status(T0 + S, &id, Status::Failed);
    a.take_actions();
    assert!(!fits(&mut a, 200, 0));
}

// ---- affinity_test.go ----------------------------------------------------------

#[test]
fn matches_affinity() {
    let a = agent();
    assert!(a.matches_affinity(&map(&[("node.os", "linux".to_string())])));
    assert!(!a.matches_affinity(&map(&[("node.os", "darwin".to_string())])));
    assert!(a.matches_affinity(&Map::new()));
}

#[test]
fn matches_affinity_with_config_attributes() {
    let mut s = settings();
    s.attributes.insert("zone".into(), "eu-west".into());
    let a = Agent::new(s);
    assert!(a.matches_affinity(&map(&[("zone", "eu-west".to_string())])));
    assert!(!a.matches_affinity(&map(&[("zone", "us-east".to_string())])));
}

#[test]
fn matches_affinity_multiple_constraints_and() {
    let a = agent();
    let both = map(&[
        ("node.os", "linux".to_string()),
        ("node.arch", "arm64".to_string()),
    ]);
    let one_off = map(&[
        ("node.os", "linux".to_string()),
        ("node.arch", "amd64".to_string()),
    ]);
    assert!(a.matches_affinity(&both));
    assert!(!a.matches_affinity(&one_off));
}

#[test]
fn handle_run_affinity_mismatch() {
    let mut a = agent();
    let mut j = job("pinned");
    j.affinity = map(&[("node.id", "other".to_string())]);
    assert_eq!(a.run(T0, j, false, None), Err(Error::AffinityMismatch));
    assert_eq!(a.tasks().count(), 0);
}

#[test]
fn handle_run_affinity_before_capacity() {
    let mut a = agent();
    let mut j = job("both-wrong");
    j.affinity = map(&[("node.os", "darwin".to_string())]);
    j.cpu_shares = 1_000_000;
    assert_eq!(a.run(T0, j, false, None), Err(Error::AffinityMismatch));
}

fn art(url: &str, m: &[(&str, &str)]) -> Artifact {
    Artifact {
        url: url.into(),
        matches: map(&m
            .iter()
            .map(|(k, v)| (*k, v.to_string()))
            .collect::<Vec<_>>()),
        ..Artifact::default()
    }
}

#[test]
fn resolve_artifact_first_match() {
    let a = agent();
    let arts = [
        art("amd", &[("node.arch", "amd64")]),
        art("arm", &[("node.arch", "arm64")]),
        art("any", &[]),
    ];
    assert_eq!(a.resolve_artifact(&arts).unwrap().url, "arm");
}

#[test]
fn resolve_artifact_catch_all() {
    let a = agent();
    let arts = [art("amd", &[("node.arch", "amd64")]), art("any", &[])];
    assert_eq!(a.resolve_artifact(&arts).unwrap().url, "any");
}

#[test]
fn resolve_artifact_no_match() {
    let a = agent();
    assert!(
        a.resolve_artifact(&[art("amd", &[("node.arch", "amd64")])])
            .is_none()
    );
}

#[test]
fn resolve_artifact_empty_slice() {
    assert!(agent().resolve_artifact(&[]).is_none());
}

#[test]
fn resolve_job_for_run_filters_to_single_match() {
    let a = agent();
    let mut j = job("x");
    j.artifacts = alloc::vec![
        art("amd", &[("node.arch", "amd64")]),
        art("arm", &[("node.arch", "arm64")])
    ];
    let r = a.resolve_job_for_run(&j).unwrap();
    assert_eq!(r.artifacts.len(), 1);
    assert_eq!(r.artifacts[0].url, "arm");
    assert_eq!(j.artifacts.len(), 2, "de opgeslagen job blijft ongemoeid");
}

#[test]
fn resolve_job_for_run_no_match() {
    let a = agent();
    let mut j = job("x");
    j.artifacts = alloc::vec![art("amd", &[("node.arch", "amd64")])];
    assert_eq!(a.resolve_job_for_run(&j).unwrap_err(), Error::NoArtifact);
}

#[test]
fn resolve_job_for_run_passthrough_without_artifacts() {
    let a = agent();
    assert!(
        a.resolve_job_for_run(&job("x"))
            .unwrap()
            .artifacts
            .is_empty()
    );
}

#[test]
fn runner_never_sees_mismatched_artifact_on_restart() {
    let mut a = agent();
    let mut j = job("plat");
    j.artifacts = alloc::vec![
        art("amd", &[("node.arch", "amd64")]),
        art("arm", &[("node.arch", "arm64")])
    ];
    let id = run_ok(&mut a, T0, j);
    a.on_status(T0 + S, &id, Status::Failed);
    for act in a.take_actions() {
        if let Action::Start { job, .. } = act {
            assert_eq!(job.artifacts.len(), 1);
            assert_eq!(job.artifacts[0].url, "arm");
        }
    }
}

// ---- agent_rollout_test.go / replace_test.go / poolhole_test.go -------------------

#[test]
fn keep_rollout_flag_follows_the_store() {
    let mut a = agent();
    let mut stored = job("r");
    stored.deploying = false;
    a.store_job(T0, stored).unwrap();
    let mut incoming = job("r");
    incoming.deploying = true;
    a.keep_rollout_flag(&mut incoming);
    assert!(!incoming.deploying);
    a.set_job_deploying(T0, "r", true);
    let mut again = job("r");
    a.keep_rollout_flag(&mut again);
    assert!(again.deploying);
    let mut fresh = job("new");
    fresh.deploying = true;
    a.keep_rollout_flag(&mut fresh);
    assert!(!fresh.deploying);
}

#[test]
fn handle_run_replace_vervang_eigen_taak_binnen_volle_node() {
    let mut a = agent();
    let mut j = job("web");
    j.cpu_shares = 1000;
    let old = run_ok(&mut a, T0, j.clone());
    // Zonder replace past de opvolger niet naast zijn voorganger.
    assert_eq!(a.run(T0, j.clone(), false, None), Err(Error::NoCapacity));
    assert_eq!(refusals(&a.take_actions()), [("web".to_string(), "cpu")]);
    let new = a.run(T0, j, true, None).unwrap();
    let acts = a.take_actions();
    // Eerst de voorganger weg, dan pas de opvolger starten.
    assert!(matches!(&acts[0], Action::Stop { task_id, .. } if *task_id == old));
    assert_eq!(starts(&acts), [new]);
}

fn hop_job(name: &str, mem: u64) -> Job {
    Job {
        name: name.into(),
        driver: Some(Driver::Hop),
        artifacts: alloc::vec![art("http://x/app.img", &[])],
        memory_limit: mem,
        ..Job::default()
    }
}

#[test]
fn hop_job_larger_than_the_largest_hole_is_refused() {
    let mut a = hop_agent();
    assert_eq!(
        a.run(T0, hop_job("big", 36 << 20), false, Some(32 << 20)),
        Err(Error::NoCapacity)
    );
    assert!(
        a.run(T0, hop_job("fits", 28 << 20), false, Some(32 << 20))
            .is_ok()
    );
}

fn refusals(actions: &[Action]) -> Vec<(String, &'static str)> {
    actions
        .iter()
        .filter_map(|a| match a {
            Action::Refused { job, why } => Some((job.clone(), *why)),
            _ => None,
        })
        .collect()
}

fn in_group(name: &str, group: &str) -> Job {
    let mut j = hop_job(name, 8 << 20);
    j.tags = map(&[("sharegroup", group.to_string())]);
    j
}

/// De LicheeRV van 03-10: Hop op de enige app-core, de kern deelt de
/// OS-core. Geen eigen core uit te delen, en toch past alles van één core.
#[test]
fn a_system_core_takes_what_has_no_core_of_its_own() {
    let mut s = settings();
    s.cap_cpu_shares = 0;
    s.cpu_cores = 0;
    s.free_groups = alloc::vec![HOP_GROUP.into(), SYSTEM_GROUP.into()];
    let mut a = Agent::new(s);
    // Zonder groep: de kern zet hem bij geen vrije core op de OS-core.
    let welcome = run_ok(&mut a, T0, hop_job("welcome", 8 << 20));
    run_ok(&mut a, T0, in_group("sys", SYSTEM_GROUP));
    run_ok(&mut a, T0, in_group("near-hop", HOP_GROUP));
    // De terugval telt als core tot de kern zegt waar hij staat; de groepen nooit.
    assert_eq!(a.resource_usage(), (1024, 3 * (8 << 20)));
    a.record_usage(&welcome, 0.0, 0.0, 1, Some(0));
    assert_eq!(a.resource_usage().0, 0);
    run_ok(&mut a, T0, hop_job("second", 8 << 20));

    // Twee cores, of een core-class, kan de terugval niet: één regel per job.
    let mut wide = hop_job("wide", 8 << 20);
    wide.cpu_shares = 2048;
    let mut classy = hop_job("classy", 8 << 20);
    classy.tags = map(&[("core-class", "big".to_string())]);
    for j in [wide.clone(), classy, wide.clone()] {
        assert_eq!(a.run(T0, j, false, None), Err(Error::NoCapacity));
    }
    assert_eq!(
        refusals(&a.take_actions()),
        [("wide".to_string(), "cpu"), ("classy".to_string(), "cpu")]
    );
    // Na een verwijdering zegt een nieuwe weigering het weer.
    a.delete_job_definition(T0, "wide");
    assert_eq!(a.run(T0, wide, false, None), Err(Error::NoCapacity));
    assert_eq!(refusals(&a.take_actions()).len(), 1);
    // Geheugen telt gewoon.
    let mut fat = in_group("fat", SYSTEM_GROUP);
    fat.memory_limit = 2 << 30;
    assert_eq!(a.run(T0, fat, false, None), Err(Error::NoCapacity));
    assert_eq!(refusals(&a.take_actions()), [("fat".to_string(), "memory")]);
}

/// Een oude kern (geen `HOPOS_SYSTEM_CORE`): `system` is een groep als elke
/// andere en er is geen terugval; `hop` past altijd.
#[test]
fn without_a_system_core_system_is_just_a_group() {
    let mut s = settings();
    s.cap_cpu_shares = 0;
    s.cpu_cores = 1;
    s.free_groups = alloc::vec![HOP_GROUP.into()];
    let mut a = Agent::new(s);
    run_ok(&mut a, T0, hop_job("welcome", 8 << 20));
    run_ok(&mut a, T0, in_group("near-hop", HOP_GROUP));
    for j in [in_group("sys", SYSTEM_GROUP), hop_job("second", 8 << 20)] {
        assert_eq!(a.run(T0, j, false, None), Err(Error::NoCapacity));
    }
    assert_eq!(refusals(&a.take_actions()).len(), 2);
}

#[test]
fn replace_is_not_blocked_by_the_hole() {
    let mut a = hop_agent();
    run_ok(&mut a, T0, hop_job("web", 36 << 20));
    assert!(
        a.run(T0, hop_job("web", 36 << 20), true, Some(32 << 20))
            .is_ok()
    );
}

// ---- handlers_test.go (de staatkant; de HTTP-kant zit in `api`) --------------------

#[test]
fn stop_during_start_does_not_resurrect_task() {
    let mut a = agent();
    let id = a.run(T0, job("race"), false, None).unwrap();
    a.take_actions();
    assert_eq!(a.stop_job_tasks("race"), 1);
    a.take_actions();
    // De runner meldt pas nu dat de start lukte: een geest, die meteen weer gestopt wordt.
    a.on_started(T0, &id, Driver::Exec, ok(7));
    assert!(a.task(&id).is_none());
    assert!(matches!(
        &a.take_actions()[..],
        [Action::Stop { pid: 7, .. }]
    ));
}

#[test]
fn handle_run_early_failure_task_stays_failed() {
    let mut a = agent();
    let mut j = job("early");
    j.max_restarts = Some(0);
    let id = a.run(T0, j, false, None).unwrap();
    a.take_actions();
    a.on_started(T0, &id, Driver::Exec, Err(StartError::Failed));
    assert_eq!(a.task(&id).unwrap().state, TaskState::Failed);
}

#[test]
fn handle_run_early_failure_visible_in_task_list() {
    let mut a = agent();
    let mut j = job("early");
    j.max_restarts = Some(0);
    let id = a.run(T0, j, false, None).unwrap();
    a.on_started(T0, &id, Driver::Exec, Err(StartError::Failed));
    assert_eq!(
        a.tasks().filter(|t| t.state == TaskState::Failed).count(),
        1
    );
}

#[test]
fn handle_run_early_failure_then_success() {
    let mut a = agent();
    let id = a.run(T0, job("flaky"), false, None).unwrap();
    a.take_actions();
    a.on_started(T0, &id, Driver::Exec, Err(StartError::Failed));
    let next = starts(&a.take_actions());
    assert_eq!(next.len(), 1);
    a.on_started(T0, &next[0], Driver::Exec, ok(5));
    let t = a.task(&next[0]).unwrap();
    assert_eq!((t.state, t.restart_count), (TaskState::Running, 1));
}

#[test]
fn max_restarts_zero_means_no_restarts() {
    let mut a = agent();
    let mut j = job("zero");
    j.max_restarts = Some(0);
    let id = run_ok(&mut a, T0, j);
    a.on_status(T0 + S, &id, Status::Failed);
    let acts = a.take_actions();
    assert!(starts(&acts).is_empty());
    assert_eq!(a.task(&id).unwrap().state, TaskState::Failed);
}

#[test]
fn max_restarts_one_allows_exactly_one_restart() {
    let mut a = agent();
    let mut j = job("one");
    j.max_restarts = Some(1);
    let id = run_ok(&mut a, T0, j);
    a.on_status(T0 + S, &id, Status::Failed);
    let second = starts(&a.take_actions());
    assert_eq!(second.len(), 1);
    a.on_started(T0 + S, &second[0], Driver::Exec, ok(2));
    a.on_status(T0 + 2 * S, &second[0], Status::Failed);
    assert!(starts(&a.take_actions()).is_empty());
    assert_eq!(a.task(&second[0]).unwrap().state, TaskState::Failed);
}

#[test]
fn slow_failing_start_does_not_reset_restart_count() {
    let mut a = agent();
    let mut j = job("slow");
    j.max_restarts = Some(2);
    j.restart_window = S;
    let id = a.run(T0, j, false, None).unwrap();
    a.take_actions();
    a.task_mut(&id).unwrap().restart_count = 1;
    // De start faalt pas lang na de aanmaak: geen uptime, dus geen schone lei.
    a.on_started(T0 + 60 * S, &id, Driver::Exec, Err(StartError::Failed));
    assert_eq!(a.task(&id).unwrap().restart_count, 1);
    assert!(a.restart_pending(&id).is_some());
}

#[test]
fn healthy_uptime_still_resets_restart_count() {
    let mut a = agent();
    let mut j = job("up");
    j.max_restarts = Some(1);
    j.restart_window = S;
    let id = run_ok(&mut a, T0, j);
    a.task_mut(&id).unwrap().restart_count = 1;
    a.on_status(T0 + 60 * S, &id, Status::Failed);
    // Echte uptime voorbij het venster: het budget begint opnieuw en de herstart gaat meteen.
    assert_eq!(starts(&a.take_actions()).len(), 1);
}

#[test]
fn unplaceable_job_is_not_restarted() {
    let mut a = agent();
    let id = a.run(T0, job("browser"), false, None).unwrap();
    a.take_actions();
    a.on_started(T0, &id, Driver::Hop, Err(StartError::NoCapacity));
    let acts = a.take_actions();
    assert!(starts(&acts).is_empty());
    assert_eq!(notes(&acts), [("browser".into(), Event::Unplaceable)]);
    assert_eq!(a.tasks().count(), 0);
}

// ---- lifecycle_test.go ---------------------------------------------------------

#[test]
fn restart_terminal_path_cleans_runner_before_failed() {
    let mut a = agent();
    let mut j = job("terminal");
    j.max_restarts = Some(0);
    let id = run_ok(&mut a, T0, j);
    a.on_status(T0 + S, &id, Status::Failed);
    let acts = a.take_actions();
    assert_eq!(stops(&acts), core::slice::from_ref(&id));
    assert_eq!(a.task(&id).unwrap().state, TaskState::Failed);
}

#[test]
fn restart_cleans_ghost_when_stop_wins_during_runner_start() {
    let mut a = agent();
    let mut j = job("restart-ghost");
    j.max_restarts = Some(1);
    let old = run_ok(&mut a, T0, j);
    a.on_status(T0 + S, &old, Status::Failed);
    let repl = starts(&a.take_actions()).pop().unwrap();
    assert!(a.is_starting(&repl));
    // De afsluiting pakt de vervanger terwijl de runner hem nog start.
    a.shutdown();
    a.take_actions();
    a.on_started(T0 + S, &repl, Driver::Exec, ok(9));
    assert_eq!(stops(&a.take_actions()), [repl]);
    assert_eq!(a.tasks().count(), 0);
}

#[test]
fn stop_failure_still_removes_logical_task() {
    // Het record gaat weg vóór de runner-stop; een mislukte stop is runner-quarantaine.
    let mut a = agent();
    let id = run_ok(&mut a, T0, job("stop"));
    assert!(a.stop_task(&id));
    assert!(a.task(&id).is_none());
}

#[test]
fn shutdown_stops_once_and_removes_logical_task() {
    let mut a = agent();
    let id = run_ok(&mut a, T0, job("sd"));
    assert_eq!(a.shutdown(), 1);
    assert_eq!(stops(&a.take_actions()), [id]);
    assert_eq!(a.shutdown(), 0);
    assert!(a.take_actions().is_empty());
}

#[test]
fn restart_delay_saturates_without_overflow() {
    for count in [6, 63, 64, 1000, i64::MAX] {
        for rand in [0, 1, u64::MAX] {
            let d = health::restart_delay(count, rand);
            assert!(d > 0 && d <= MAX_RESTART_DELAY, "{count}: {d}");
        }
    }
}

#[test]
fn check_states_pruned_by_monitor_owner() {
    // De gezondheidsteller woont in het taakrecord: weg met de taak is weg met de teller.
    let mut a = agent();
    let mut j = job("hc");
    j.health_check = Some(HealthCheck::default());
    j.ports = map(&[("http", 0u16)]);
    let id = a.run(T0, j, false, None).unwrap();
    a.on_started(
        T0,
        &id,
        Driver::Exec,
        Ok(StartOk {
            pid: 1,
            ports: map(&[("http", 8000u16)]),
        }),
    );
    a.on_probe(T0, &id, Outcome::Http(None));
    a.stop_task(&id);
    assert!(a.tasks_map().get(&id).is_none());
}

#[test]
fn failed_tasks_included_in_shutdown_cleanup() {
    let mut a = agent();
    a.insert_task(Task {
        id: "failed".into(),
        state: TaskState::Failed,
        ..Task::default()
    });
    assert_eq!(a.shutdown(), 1);
    assert_eq!(stops(&a.take_actions()), ["failed"]);
}

// ---- monitor_test.go -----------------------------------------------------------

fn hc_job(kind: Option<CheckType>, threshold: i64) -> Job {
    let mut j = job("hc");
    j.health_check = Some(HealthCheck {
        kind,
        path: "/health".into(),
        failure_threshold: threshold,
        ..HealthCheck::default()
    });
    j.ports = map(&[("http", 0u16)]);
    j
}

fn run_hc(a: &mut Agent, j: Job) -> String {
    let id = a.run(T0, j, false, None).unwrap();
    a.take_actions();
    a.on_started(
        T0,
        &id,
        Driver::Exec,
        Ok(StartOk {
            pid: 1,
            ports: map(&[("http", 18080u16)]),
        }),
    );
    a.take_actions();
    id
}

fn probe_of(acts: &[Action]) -> Option<Probe> {
    acts.iter().find_map(|a| match a {
        Action::Probe { probe, .. } => Some(probe.clone()),
        _ => None,
    })
}

#[test]
fn check_health_success() {
    let mut a = agent();
    let id = run_hc(&mut a, hc_job(None, 0));
    a.on_probe(T0, &id, Outcome::Http(Some(200)));
    assert_eq!(a.task(&id).unwrap().state, TaskState::Running);
}

#[test]
fn check_health_bad_status() {
    let mut a = agent();
    let id = run_hc(&mut a, hc_job(None, 1));
    a.on_probe(T0, &id, Outcome::Http(Some(500)));
    assert_ne!(a.task(&id).map(|t| t.state), Some(TaskState::Running));
}

#[test]
fn check_health3xx_redirect() {
    let mut a = agent();
    let id = run_hc(&mut a, hc_job(None, 1));
    a.on_probe(T0, &id, Outcome::Http(Some(302)));
    assert_eq!(a.task(&id).unwrap().state, TaskState::Running);
}

#[test]
fn check_health_timeout() {
    let mut a = agent();
    let id = run_hc(&mut a, hc_job(None, 1));
    a.on_probe(T0, &id, Outcome::Http(None));
    assert_ne!(a.task(&id).map(|t| t.state), Some(TaskState::Running));
}

#[test]
fn check_health_missing_port() {
    let mut a = agent();
    let mut j = hc_job(None, 1);
    if let Some(hc) = j.health_check.as_mut() {
        hc.port = "admin".into();
    }
    let id = run_hc(&mut a, j);
    a.on_status(T0 + S, &id, Status::Running);
    // Geen poort met die naam: een mislukte controle, geen probe.
    assert_ne!(a.task(&id).map(|t| t.state), Some(TaskState::Running));
}

#[test]
fn check_health_default_port() {
    let mut a = agent();
    let id = run_hc(&mut a, hc_job(None, 0));
    a.on_status(T0 + S, &id, Status::Running);
    let p = probe_of(&a.take_actions()).unwrap();
    assert!(matches!(p, Probe::Http { port: 18080, .. }));
}

#[test]
fn check_tasks_detects_crashed_process() {
    let mut a = agent();
    let id = run_ok(&mut a, T0, job("crash"));
    let polls = a.tick(T0);
    assert!(
        polls
            .iter()
            .any(|p| matches!(p, Action::Poll { task_id, .. } if *task_id == id))
    );
    a.on_status(T0, &id, Status::Failed);
    let acts = a.take_actions();
    assert!(notes(&acts).contains(&("crash".into(), Event::Crash)));
    assert_eq!(starts(&acts).len(), 1);
}

#[test]
fn check_tasks_health_check_fails() {
    let mut a = agent();
    let id = run_hc(&mut a, hc_job(None, 1));
    a.on_status(T0 + S, &id, Status::Running);
    assert!(probe_of(&a.take_actions()).is_some());
    a.on_probe(T0 + S, &id, Outcome::Http(Some(503)));
    let acts = a.take_actions();
    assert!(notes(&acts).contains(&("hc".into(), Event::Crash)));
    assert_eq!(stops(&acts), [id]);
}

#[test]
fn check_tasks_health_check_succeeds() {
    let mut a = agent();
    let id = run_hc(&mut a, hc_job(None, 0));
    a.on_probe(T0 + S, &id, Outcome::Http(Some(200)));
    assert_eq!(a.task(&id).unwrap().state, TaskState::Running);
}

#[test]
fn check_health_failure_threshold() {
    let mut a = agent();
    let id = run_hc(&mut a, hc_job(None, 3));
    a.on_probe(T0, &id, Outcome::Http(None));
    a.on_probe(T0, &id, Outcome::Http(None));
    assert_eq!(a.task(&id).unwrap().state, TaskState::Running);
    a.on_probe(T0, &id, Outcome::Http(None));
    assert_ne!(a.task(&id).map(|t| t.state), Some(TaskState::Running));
}

#[test]
fn check_health_failure_threshold_resets() {
    let mut a = agent();
    let id = run_hc(&mut a, hc_job(None, 3));
    for _ in 0..2 {
        a.on_probe(T0, &id, Outcome::Http(None));
    }
    a.on_probe(T0, &id, Outcome::Http(Some(200)));
    for _ in 0..2 {
        a.on_probe(T0, &id, Outcome::Http(None));
    }
    assert_eq!(a.task(&id).unwrap().state, TaskState::Running);
}

#[test]
fn check_health_tcp() {
    let mut a = agent();
    let id = run_hc(&mut a, hc_job(Some(CheckType::Tcp), 1));
    a.on_status(T0 + S, &id, Status::Running);
    assert!(matches!(
        probe_of(&a.take_actions()),
        Some(Probe::Tcp { port: 18080, .. })
    ));
    a.on_probe(T0 + S, &id, Outcome::Tcp(true));
    assert_eq!(a.task(&id).unwrap().state, TaskState::Running);
}

#[test]
fn check_health_tcp_refused() {
    let mut a = agent();
    let id = run_hc(&mut a, hc_job(Some(CheckType::Tcp), 1));
    a.on_probe(T0, &id, Outcome::Tcp(false));
    assert_ne!(a.task(&id).map(|t| t.state), Some(TaskState::Running));
}

#[test]
fn check_health_file() {
    let mut a = agent();
    let id = run_hc(&mut a, hc_job(Some(CheckType::File), 1));
    a.on_probe(T0, &id, Outcome::File(Some(Time(T0))));
    a.on_probe(T0 + S, &id, Outcome::File(Some(Time(T0 + S / 2))));
    assert_eq!(a.task(&id).unwrap().state, TaskState::Running);
}

#[test]
fn check_health_file_not_modified() {
    let mut a = agent();
    let id = run_hc(&mut a, hc_job(Some(CheckType::File), 1));
    a.on_probe(T0, &id, Outcome::File(Some(Time(T0 - S))));
    a.on_probe(T0 + S, &id, Outcome::File(Some(Time(T0 - S))));
    assert_ne!(a.task(&id).map(|t| t.state), Some(TaskState::Running));
}

#[test]
fn check_health_file_not_exists() {
    let mut a = agent();
    let id = run_hc(&mut a, hc_job(Some(CheckType::File), 1));
    a.on_probe(T0, &id, Outcome::File(None));
    assert_ne!(a.task(&id).map(|t| t.state), Some(TaskState::Running));
}

#[test]
fn restart_task_success() {
    let mut a = agent();
    let id = run_ok(&mut a, T0, job("r"));
    a.on_status(T0, &id, Status::Failed);
    let new = starts(&a.take_actions()).pop().unwrap();
    a.on_started(T0, &new, Driver::Exec, ok(2));
    assert!(a.task(&id).is_none());
    assert_eq!(a.task(&new).unwrap().restart_count, 1);
}

#[test]
fn restart_task_max_restarts_exceeded() {
    let mut a = agent();
    let mut j = job("r");
    j.max_restarts = Some(2);
    let id = run_ok(&mut a, T0, j);
    a.task_mut(&id).unwrap().restart_count = 2;
    a.on_status(T0, &id, Status::Failed);
    assert!(starts(&a.take_actions()).is_empty());
    assert_eq!(a.task(&id).unwrap().state, TaskState::Failed);
}

#[test]
fn restart_task_unlimited_restarts() {
    let mut a = agent();
    let id = run_ok(&mut a, T0, job("r"));
    a.task_mut(&id).unwrap().restart_count = 50;
    a.on_status(T0, &id, Status::Failed);
    let at = a.restart_pending(&id).unwrap();
    assert!(at > T0 && at <= T0 + MAX_RESTART_DELAY);
    assert_eq!(a.task(&id).unwrap().next_restart_at, Time(at));
    // De backoff loopt af in de tick, en dan komt de vervanger.
    assert!(starts(&a.tick(at)).len() == 1);
}

#[test]
fn restart_task_grace_period_resets_count() {
    let mut a = agent();
    let id = run_ok(&mut a, T0, job("r"));
    a.task_mut(&id).unwrap().restart_count = 3;
    a.on_status(T0 + DEFAULT_RESTART_WINDOW + S, &id, Status::Failed);
    assert_eq!(starts(&a.take_actions()).len(), 1);
}

#[test]
fn restart_task_job_not_found() {
    let mut a = agent();
    let id = run_ok(&mut a, T0, job("r"));
    a.delete_job_definition(T0, "r");
    a.on_status(T0, &id, Status::Failed);
    assert!(a.task(&id).is_none());
}

// ---- notify_test.go ------------------------------------------------------------

#[test]
fn start_job_notify_without_health_check() {
    let mut a = agent();
    let id = a.run(T0, job("n"), false, None).unwrap();
    a.take_actions();
    a.on_started(T0, &id, Driver::Exec, ok(1));
    assert_eq!(notes(&a.take_actions()), [("n".into(), Event::Started)]);
}

#[test]
fn start_job_notify_with_health_check() {
    let mut a = agent();
    let id = a.run(T0, hc_job(None, 0), false, None).unwrap();
    a.take_actions();
    a.on_started(T0, &id, Driver::Exec, ok(1));
    assert_eq!(notes(&a.take_actions()), [("hc".into(), Event::Start)]);
}

#[test]
fn delete_job_notify() {
    let mut a = agent();
    run_ok(&mut a, T0, job("d"));
    a.delete_job(T0, "d");
    assert!(notes(&a.take_actions()).contains(&("d".into(), Event::Stop)));
}

#[test]
fn first_health_check_pass_fires_started() {
    let mut a = agent();
    let id = run_hc(&mut a, hc_job(None, 0));
    a.on_probe(T0, &id, Outcome::Http(Some(200)));
    assert_eq!(notes(&a.take_actions()), [("hc".into(), Event::Started)]);
}

#[test]
fn health_check_pass_only_notifies_once() {
    let mut a = agent();
    let id = run_hc(&mut a, hc_job(None, 0));
    for _ in 0..3 {
        a.on_probe(T0, &id, Outcome::Http(Some(200)));
    }
    assert_eq!(notes(&a.take_actions()).len(), 1);
}

#[test]
fn crash_event_fired() {
    let mut a = agent();
    let id = run_ok(&mut a, T0, job("c"));
    a.on_status(T0, &id, Status::Failed);
    assert!(notes(&a.take_actions()).contains(&("c".into(), Event::Crash)));
}

// ---- state_test.go -------------------------------------------------------------

#[test]
fn agent_store_and_get_job() {
    let mut a = agent();
    a.store_job(T0, job("s")).unwrap();
    assert_eq!(a.get_job("s").unwrap().command, "./app");
}

#[test]
fn agent_get_jobs() {
    let mut a = agent();
    a.store_job(T0, job("a")).unwrap();
    a.store_job(T0, job("b")).unwrap();
    assert_eq!(a.jobs().count(), 2);
}

#[test]
fn agent_get_job_not_found() {
    assert!(agent().get_job("nope").is_none());
}

#[test]
fn agent_store_job_overwrite() {
    let mut a = agent();
    a.store_job(T0, job("o")).unwrap();
    let mut j = job("o");
    j.command = "./v2".into();
    a.store_job(T0, j).unwrap();
    assert_eq!(a.get_job("o").unwrap().command, "./v2");
    assert_eq!(a.jobs().count(), 1);
}

#[test]
fn agent_sync_jobs() {
    let mut a = agent();
    a.sync_jobs(alloc::vec![job("x"), job("y")], Time(T0))
        .unwrap();
    assert_eq!(a.jobs().count(), 2);
    assert_eq!(a.state_time(), Time(T0));
}

#[test]
fn agent_get_state_time() {
    assert!(agent().state_time().is_zero());
}

#[test]
fn agent_state_time_updates_on_store_job() {
    let mut a = agent();
    a.store_job(T0, job("t")).unwrap();
    assert_eq!(a.state_time(), Time(T0));
}

#[test]
fn update_job_never_resurrects_deleted() {
    let mut a = agent();
    assert!(!a.update_job(T0, job("gone")));
    assert!(a.get_job("gone").is_none());
    assert!(!a.set_job_priority(T0, "gone", 1));
}

// ---- agentloop/loop_test.go ----------------------------------------------------

/// Een discoverer met vaste antwoorden.
#[derive(Default)]
struct FakeDisc {
    leader: Option<String>,
    can_lead: bool,
    renew: (bool, bool),
    reachable: bool,
    released: usize,
}

impl Discoverer for FakeDisc {
    fn get_leader(&mut self) -> Option<String> {
        self.leader.clone()
    }
    fn try_become_leader(&mut self) -> bool {
        self.can_lead
    }
    fn renew_lease(&mut self) -> (bool, bool) {
        self.renew
    }
    fn release_leadership(&mut self) {
        self.released += 1;
    }
    fn store_reachable(&self) -> bool {
        self.reachable
    }
}

fn loop_setup() -> (Election, FakeDisc, Agent) {
    (
        Election::new("10.0.0.5", 8080),
        FakeDisc::default(),
        agent(),
    )
}

#[test]
fn step_down_is_idempotent() {
    let (mut l, mut d, mut a) = loop_setup();
    d.can_lead = true;
    assert_eq!(l.become_leader_now(&mut d, &mut a), [Request::StartLeader]);
    assert_eq!(l.step_down(&mut d, &mut a, true), [Request::StopLeader]);
    assert!(l.step_down(&mut d, &mut a, true).is_empty());
    assert_eq!(d.released, 1);
}

#[test]
fn tick_no_leader_triggers_after4() {
    let (mut l, mut d, mut a) = loop_setup();
    d.reachable = true;
    d.can_lead = true;
    for _ in 0..3 {
        assert!(l.tick(&mut d, &mut a, 0).is_empty());
    }
    assert_eq!(l.tick(&mut d, &mut a, 0), [Request::StartLeader]);
    assert!(l.is_leading());
    assert_eq!(a.leader_addr(), "10.0.0.5:9080");
}

#[test]
fn tick_no_leader_not_yet_at3() {
    let (mut l, mut d, mut a) = loop_setup();
    d.reachable = true;
    d.can_lead = true;
    for _ in 0..3 {
        l.tick(&mut d, &mut a, 0);
    }
    assert!(!l.is_leading());
    assert_eq!(l.fail_count(), 3);
}

#[test]
fn tick_register_fails_triggers_after4() {
    let (mut l, mut d, mut a) = loop_setup();
    d.leader = Some("10.0.0.1:9080".into());
    d.can_lead = true;
    for i in 0..4 {
        let reqs = l.tick(&mut d, &mut a, 0);
        let out = l.on_reply(&mut d, &mut a, &reqs[0], Err(LinkError::Failed));
        if i == 3 {
            assert_eq!(out, [Request::StartLeader]);
        }
    }
    assert!(l.is_leading());
}

#[test]
fn tick_register_fails7_stops_all_tasks() {
    let (mut l, mut d, mut a) = loop_setup();
    d.leader = Some("10.0.0.1:9080".into());
    run_ok(&mut a, T0, job("t"));
    for _ in 0..7 {
        let reqs = l.tick(&mut d, &mut a, 0);
        l.on_reply(&mut d, &mut a, &reqs[0], Err(LinkError::Failed));
    }
    assert_eq!(a.tasks().count(), 0);
    assert_eq!(l.fail_count(), 4);
}

#[test]
fn tick_register_success_resets_state() {
    let (mut l, mut d, mut a) = loop_setup();
    d.leader = Some("10.0.0.1:9080".into());
    let reqs = l.tick(&mut d, &mut a, 0);
    assert_eq!(
        reqs,
        [Request::Register {
            leader: "10.0.0.1:9080".into()
        }]
    );
    l.on_reply(&mut d, &mut a, &reqs[0], Ok(()));
    assert!(l.is_registered());
    assert_eq!(a.leader_addr(), "10.0.0.1:9080");
    assert_eq!(
        l.tick(&mut d, &mut a, 0),
        [Request::Heartbeat {
            leader: "10.0.0.1:9080".into()
        }]
    );
}

#[test]
fn tick_heartbeat_not_registered_reregisters() {
    let (mut l, mut d, mut a) = loop_setup();
    d.leader = Some("10.0.0.1:9080".into());
    let r = l.tick(&mut d, &mut a, 0);
    l.on_reply(&mut d, &mut a, &r[0], Ok(()));
    let hb = l.tick(&mut d, &mut a, 0);
    l.on_reply(&mut d, &mut a, &hb[0], Err(LinkError::NotRegistered));
    assert!(matches!(
        &l.tick(&mut d, &mut a, 0)[..],
        [Request::Register { .. }]
    ));
    assert_eq!(l.fail_count(), 0);
}

#[test]
fn tick_heartbeat_success_resets_fail_count() {
    let (mut l, mut d, mut a) = loop_setup();
    d.leader = Some("10.0.0.1:9080".into());
    let r = l.tick(&mut d, &mut a, 0);
    l.on_reply(&mut d, &mut a, &r[0], Ok(()));
    let hb = l.tick(&mut d, &mut a, 0);
    l.on_reply(&mut d, &mut a, &hb[0], Err(LinkError::Failed));
    assert_eq!(l.fail_count(), 1);
    let hb = l.tick(&mut d, &mut a, 0);
    l.on_reply(&mut d, &mut a, &hb[0], Ok(()));
    assert_eq!(l.fail_count(), 0);
}

#[test]
fn tick_leader_raft_down_stays_leader() {
    let (mut l, mut d, mut a) = loop_setup();
    d.can_lead = true;
    l.become_leader_now(&mut d, &mut a);
    d.renew = (false, false);
    assert!(matches!(
        &l.tick(&mut d, &mut a, 2)[..],
        [Request::SelfHeartbeat { .. }]
    ));
    assert!(l.is_leading());
}

#[test]
fn tick_leader_raft_down_no_agents_loses_leadership() {
    let (mut l, mut d, mut a) = loop_setup();
    d.can_lead = true;
    l.become_leader_now(&mut d, &mut a);
    d.renew = (false, false);
    assert_eq!(l.tick(&mut d, &mut a, 0), [Request::StopLeader]);
    assert!(!l.is_leading());
}

#[test]
fn tick_self_heartbeat_not_registered_reregisters() {
    let (mut l, mut d, mut a) = loop_setup();
    d.can_lead = true;
    l.become_leader_now(&mut d, &mut a);
    d.renew = (true, false);
    let r = l.tick(&mut d, &mut a, 1);
    let out = l.on_reply(&mut d, &mut a, &r[0], Err(LinkError::NotRegistered));
    assert_eq!(
        out,
        [Request::SelfRegister {
            leader: "10.0.0.5:9080".into()
        }]
    );
}

#[test]
fn tick_self_heartbeat_transport_fout_alleen_tellen() {
    let (mut l, mut d, mut a) = loop_setup();
    d.can_lead = true;
    l.become_leader_now(&mut d, &mut a);
    d.renew = (true, false);
    let r = l.tick(&mut d, &mut a, 1);
    assert!(
        l.on_reply(&mut d, &mut a, &r[0], Err(LinkError::Failed))
            .is_empty()
    );
    assert_eq!(l.self_beat_fails(), 1);
    assert!(l.is_leading());
}

#[test]
fn tick_no_leader_never_stops_tasks() {
    let (mut l, mut d, mut a) = loop_setup();
    d.reachable = true;
    run_ok(&mut a, T0, job("keep"));
    for _ in 0..20 {
        l.tick(&mut d, &mut a, 0);
    }
    assert_eq!(a.tasks().count(), 1);
}

#[test]
fn tick_leader_and_store_unreachable_stops_tasks() {
    let (mut l, mut d, mut a) = loop_setup();
    d.reachable = false;
    run_ok(&mut a, T0, job("t"));
    for _ in 0..7 {
        l.tick(&mut d, &mut a, 0);
    }
    assert_eq!(a.tasks().count(), 0);
}

#[test]
fn tick_takeover_succeeds_sets_stop_leader() {
    let (mut l, mut d, mut a) = loop_setup();
    d.can_lead = true;
    d.reachable = true;
    for _ in 0..4 {
        l.tick(&mut d, &mut a, 0);
    }
    assert!(l.is_leading());
    d.renew = (false, true);
    assert_eq!(l.tick(&mut d, &mut a, 5), [Request::StopLeader]);
}

// ---- Settings uit config::Config (Go: agent.New las *config.Config) ----

#[test]
fn settings_from_config() {
    let cfg = config::Config::from_json(
        br#"{
            "node": {"id": "node-1", "ip": "10.0.0.7", "port": 9000,
                     "attributes": {"rack": "r2", "node.id": "override"}},
            "capacity": {"cpu_shares": 2048, "memory": 1073741824},
            "timeouts": {"health_check_interval": "7s", "health_check_timeout": "2s"}
        }"#,
    )
    .unwrap();
    let s = Settings::from_config(&cfg).unwrap();
    assert_eq!(s.id, "node-1");
    assert_eq!(s.endpoint, "http://10.0.0.7:9000");
    assert_eq!(s.attributes.get("rack").map(String::as_str), Some("r2"));
    // De config gaat over het automatische attribuut heen, zoals in Go.
    assert_eq!(
        s.attributes.get("node.id").map(String::as_str),
        Some("override")
    );
    assert_eq!(s.cap_cpu_shares, 2048);
    assert_eq!(s.cap_memory, 1 << 30);
    assert_eq!(s.monitor_interval(), 7 * types::time::SECOND);
    assert_eq!(s.health_timeout(), 2 * types::time::SECOND);
    // Wat alleen het ijzer weet, vult de executor.
    assert_eq!((s.cpu_cores, s.memory_bytes, s.seed), (0, 0, 0));

    // Met gemeten ijzer werkt de plafond-logica van Settings erop door.
    let s = Settings {
        cpu_cores: 4,
        memory_bytes: 16 << 30,
        ..s
    };
    assert_eq!(s.effective_cpu_shares(), 2048);
    assert_eq!(s.effective_memory_bytes(), 1 << 30);
    // En de agent is ermee te bouwen.
    let a = Agent::new(s);
    assert_eq!(a.id(), "node-1");
    assert_eq!(a.endpoint(), "http://10.0.0.7:9000");
    assert!(a.matches_affinity(&map(&[("rack", "r2".to_string())])));
}

#[test]
fn settings_from_default_config() {
    let s = Settings::from_config(&config::Config::default()).unwrap();
    // Geen id en geen IP: leeg, niet `http://:8080`.
    assert!(s.id.is_empty());
    assert!(s.endpoint.is_empty());
    assert!(s.attributes.is_empty());
    assert_eq!((s.cap_cpu_shares, s.cap_memory), (0, 0));
    assert_eq!(s.monitor_interval(), 5 * types::time::SECOND);
    assert_eq!(s.health_timeout(), 5 * types::time::SECOND);
}
