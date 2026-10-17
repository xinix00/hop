//! De toetsen van de kern: de Go-test van de exporter, de lezers op vaste
//! JSON, de collector met zijn tellers, het tekstformaat en de pollronde.

use alloc::string::{String, ToString};
use alloc::vec;
use alloc::vec::Vec;
use core::future::Future;
use core::pin::pin;
use core::task::{Context, Poll as TaskPoll, Waker};

use types::time::SECOND;
use types::{Agent, Job, Task, TaskState, Time};

use crate::*;

/// Een vast tijdstip: 2026-09-30T08:00:00Z.
const NOW: Time = Time(1_790_755_200 * SECOND);

fn at(secs_before: u64) -> Time {
    Time(NOW.0 - secs_before * SECOND)
}

fn agent(id: &str, seen: Time) -> Agent {
    Agent {
        id: id.to_string(),
        endpoint: alloc::format!("http://{id}:8080"),
        last_seen: seen,
        ..Agent::default()
    }
}

fn job(name: &str, count: i64) -> Job {
    Job {
        name: name.to_string(),
        count,
        ..Job::default()
    }
}

fn task(id: &str, job: &str, state: TaskState, restarts: i64) -> Task {
    Task {
        id: id.to_string(),
        job_name: job.to_string(),
        state,
        restart_count: restarts,
        ..Task::default()
    }
}

fn on(agent: &str, tasks: Vec<Task>) -> AgentTasks {
    AgentTasks {
        agent: agent.to_string(),
        tasks: Some(tasks),
    }
}

fn poll_of(tasks: Vec<AgentTasks>) -> Poll {
    Poll {
        agents: vec![agent("a1", at(5))],
        jobs: vec![job("api", 2)],
        tasks,
        ..Poll::default()
    }
}

fn text(m: &Metrics, now: Option<Time>) -> String {
    let mut out = String::new();
    render(m, now, &mut out).unwrap();
    out
}

/// Pollt een future die nooit wacht (de nep-bron hieronder).
fn block_on<F: Future>(f: F) -> F::Output {
    let mut f = pin!(f);
    let mut cx = Context::from_waker(Waker::noop());
    loop {
        if let TaskPoll::Ready(v) = f.as_mut().poll(&mut cx) {
            return v;
        }
    }
}

// ---- De Go-test (OLD/internal/exporter/exporter_test.go) -------------------

#[test]
fn leader_and_lease_metrics() {
    let m = Metrics {
        leader: "10.0.0.2:9080".to_string(),
        lease_expires_at: Time(NOW.0 + 90 * SECOND),
        leader_changes: 3,
        deploying: vec!["api".to_string()],
        settling: true,
        ..Metrics::default()
    };
    let out = text(&m, Some(NOW));
    for want in [
        r#"hop_leader_info{leader="10.0.0.2:9080"} 1"#,
        "hop_leader_changes_total 3",
        "hop_cluster_settling 1",
        "hop_jobs_deploying 1",
        r#"hop_job_deploying{job="api"} 1"#,
        // Go toetste "8x of 9x" tegen time.Now(); met een vaste klok exact.
        "hop_leader_lease_seconds 90.0",
    ] {
        assert!(out.contains(want), "metrics output lacks {want:?}\n{out}");
    }
}

#[test]
fn lease_is_zero_without_a_clock_or_when_lapsed() {
    let m = Metrics {
        lease_expires_at: Time(NOW.0 + 90 * SECOND),
        ..Metrics::default()
    };
    assert!(text(&m, None).contains("hop_leader_lease_seconds 0.0\n"));
    let lapsed = Metrics {
        lease_expires_at: at(1),
        ..Metrics::default()
    };
    assert!(text(&lapsed, Some(NOW)).contains("hop_leader_lease_seconds 0.0\n"));
    let none = text(&Metrics::default(), Some(NOW));
    assert!(none.contains("hop_leader_info{leader=\"\"} 0\n"), "{none}");
}

// ---- Van vaste JSON (via de lezers van hoplib) tot tekst -------------------

/// Leest `body` zoals hoplib een antwoord van de agent leest.
fn parse<T>(call: hoplib::Result<hoplib::Call<T>>, body: &[u8]) -> T {
    let resp = hoplib::Response {
        status: 200,
        body: body.to_vec(),
    };
    call.unwrap().parse(&resp).unwrap()
}

#[test]
fn fixed_json_through_the_collector_to_text() {
    // De vormen van hop's api (leader.rs, node.rs) en types (task.rs), zoals
    // agentd-hopos ze op QEMU gaf.
    let c = hoplib::Client::new("http://HOP:9080", None).unwrap();
    let agents = parse(
        c.agents(),
        br#"[{"id":"hopos-qemu","endpoint":"http://10.0.2.15:8080","version":"3.0.0",
             "last_seen":"2026-09-30T07:59:55Z","temp_milli_c":47250},
            {"id":"n2","endpoint":"http://10.0.2.16:8080","version":"3.0.0",
             "last_seen":"2026-09-30T07:58:00Z"}]"#,
    );
    assert_eq!(agents[0].last_seen, at(5));
    let jobs = parse(
        c.jobs(),
        br#"[{"name":"hopprom","driver":"hop","count":1,"ports":{"metrics":9090},"deploying":true},
            {"name":"dns","count":-1,"future_field":{"x":1}}]"#,
    );
    let tasks = parse(
        c.tasks(),
        br#"{"tasks_by_agent":{"hopos-qemu":[{"id":"t1","job_name":"hopprom","driver":"hop",
              "ports":{"metrics":9090},"pid":2,"state":"running",
              "started_at":"2026-09-30T07:59:00Z","restart_count":0,
              "last_failed_at":"0001-01-01T00:00:00Z","next_restart_at":"0001-01-01T00:00:00Z",
              "cpu_percent":1.5,"mem_percent":12.25}]},
            "unreachable":["n2"]}"#,
    )
    .unwrap();
    let cap = parse(
        c.capacity(&agents[0]),
        br#"{"cpu_cores":3,"memory_bytes":3221225472,"cpu_used_shares":1536,
            "memory_used_bytes":134217728,"tasks_running":1,"attributes":{"node.os":"hopos"}}"#,
    );
    let status = parse(
        c.status(),
        br#"{"cluster_name":"hopos","agents":1,"jobs":2,"total_placed":1,"settling":false,
            "placed":{"hopprom":1},"deploying":["hopprom"]}"#,
    );
    let leader = parse(
        c.leader(),
        br#"{"leader":"10.0.2.15:9080","lease_expires_at":"2026-09-30T08:00:30Z"}"#,
    );
    let p = Poll {
        agents,
        jobs,
        tasks,
        capacities: vec![("hopos-qemu".to_string(), cap), ("n2".to_string(), None)],
        status: Some(status),
        leader: Some(leader),
    };
    assert_eq!(p.task_count(), 1);
    let m = Collector::new().observe(&p, Some(NOW)).unwrap();
    let out = text(&m, Some(NOW));
    for want in [
        "hop_agents_total 2\n",
        "hop_agents_healthy 1\n",
        "hop_agent_cpu_cores{agent=\"hopos-qemu\"} 3\n",
        "hop_agent_cpu_used_cores{agent=\"hopos-qemu\"} 1.50\n",
        "hop_agent_temperature_celsius{agent=\"hopos-qemu\"} 47.2\n",
        "hop_tasks_total{state=\"running\"} 1\n",
        "hop_tasks_placed{job=\"hopprom\"} 1\n",
        "hop_job_instances_expected{job=\"dns\"} 2\n",
        "hop_job_healthy{job=\"hopprom\"} 1\n",
        "hop_job_mem_percent{job=\"hopprom\"} 12.2\n",
        "hop_task_starts_total{job=\"hopprom\"} 1\n",
        "hop_leader_info{leader=\"10.0.2.15:9080\"} 1\n",
        "hop_leader_lease_seconds 30.0\n",
        "hop_job_deploying{job=\"hopprom\"} 1\n",
    ] {
        assert!(out.contains(want), "lacks {want:?}\n{out}");
    }
    assert_well_formed(&out);
}

// ---- De collector: gauges ---------------------------------------------------

#[test]
fn agents_total_healthy_and_capacity() {
    let mut c = Collector::new();
    let p = Poll {
        agents: vec![agent("a1", at(5)), agent("a2", at(59)), agent("a3", at(61))],
        capacities: vec![
            (
                "a1".to_string(),
                Some(Capacity {
                    cpu_cores: 4,
                    memory_bytes: 8 << 30,
                    cpu_used_shares: 2048,
                    memory_used_bytes: 1 << 30,
                    ..Capacity::default()
                }),
            ),
            ("a2".to_string(), None),
        ],
        ..Poll::default()
    };
    let m = c.observe(&p, Some(NOW)).unwrap();
    assert_eq!((m.agents_total, m.agents_healthy), (3, 2));
    assert!(m.agents.get("a1").unwrap().capacity.is_some());
    assert!(m.agents.get("a2").unwrap().capacity.is_none());
    let out = text(&m, Some(NOW));
    assert!(
        out.contains("hop_agent_cpu_cores{agent=\"a1\"} 4\n"),
        "{out}"
    );
    assert!(out.contains("hop_agent_cpu_used_cores{agent=\"a1\"} 2.00\n"));
    assert!(out.contains("hop_agent_memory_bytes{agent=\"a1\"} 8589934592\n"));
    assert!(out.contains("hop_agent_memory_used_bytes{agent=\"a1\"} 1073741824\n"));
    assert!(
        !out.contains("agent=\"a2\""),
        "no capacity, no series: {out}"
    );
}

#[test]
fn without_a_wall_clock_health_is_measured_against_the_newest_heartbeat() {
    let mut c = Collector::new();
    let p = Poll {
        agents: vec![
            agent("a1", at(100)),
            agent("a2", at(130)),
            agent("a3", at(200)),
        ],
        ..Poll::default()
    };
    let m = c.observe(&p, None).unwrap();
    assert_eq!(m.agents_healthy, 2);
}

#[test]
fn capacity_miss_keeps_the_last_one_and_a_gone_agent_drops_it() {
    let mut c = Collector::new();
    let cap = Capacity {
        cpu_cores: 2,
        ..Capacity::default()
    };
    let mut p = Poll {
        agents: vec![agent("a1", at(1))],
        capacities: vec![("a1".to_string(), Some(cap.clone()))],
        ..Poll::default()
    };
    c.observe(&p, Some(NOW)).unwrap();
    p.capacities = vec![("a1".to_string(), None)];
    let m = c.observe(&p, Some(NOW)).unwrap();
    assert_eq!(
        m.agents.get("a1").unwrap().capacity,
        Some(CapacityMetric::from(&cap))
    );
    p.agents = vec![agent("a2", at(1))];
    p.capacities = vec![("a2".to_string(), None)];
    let m = c.observe(&p, Some(NOW)).unwrap();
    assert!(m.agents.get("a1").is_none());
    assert!(m.agents.get("a2").unwrap().capacity.is_none());
}

#[test]
fn tasks_by_state_placed_restarts_and_jobs() {
    let mut c = Collector::new();
    let mut run1 = task("t1", "api", TaskState::Running, 0);
    run1.cpu_percent = 10.0;
    run1.mem_percent = 20.0;
    let mut run2 = task("t2", "api", TaskState::Running, 2);
    run2.cpu_percent = 30.0;
    run2.mem_percent = 40.0;
    let p = Poll {
        agents: vec![agent("a1", at(1)), agent("a2", at(1)), agent("a3", at(1))],
        jobs: vec![job("api", 2), job("dns", -1), job("once", 0), job("big", 5)],
        tasks: vec![
            on("a1", vec![run1, task("t3", "dns", TaskState::Failed, 1)]),
            on(
                "a2",
                vec![run2, task("t4", "dns", TaskState::Downloading, 0)],
            ),
            on("a3", vec![task("t5", "once", TaskState::Running, 0)]),
        ],
        ..Poll::default()
    };
    let m = c.observe(&p, Some(NOW)).unwrap();
    assert_eq!(m.tasks_by_state.get("running"), Some(&3));
    assert_eq!(m.tasks_by_state.get("failed"), Some(&1));
    assert_eq!(m.tasks_by_state.get("downloading"), Some(&1));
    assert_eq!(m.tasks_placed.get("api"), Some(&2));
    assert_eq!(m.tasks_placed.get("dns"), None);
    assert_eq!(m.task_restarts.get("api"), Some(&2));
    assert_eq!(m.task_restarts.get("dns"), Some(&1));
    assert_eq!(m.jobs_total, 4);
    let api = m.jobs.get("api").unwrap();
    assert_eq!((api.placed, api.expected, api.healthy), (2, 2, true));
    assert_eq!((api.cpu_percent, api.mem_percent), (20.0, 30.0));
    let dns = m.jobs.get("dns").unwrap();
    assert_eq!((dns.placed, dns.expected, dns.healthy), (0, 3, false));
    let once = m.jobs.get("once").unwrap();
    assert_eq!((once.expected, once.healthy), (1, true));
    assert_eq!(m.jobs.get("big").unwrap().expected, 5);
}

// ---- De collector: de tellers als delta tussen twee polls ------------------

#[test]
fn starts_count_ids_the_previous_poll_did_not_have() {
    let mut c = Collector::new();
    let t1 = task("t1", "api", TaskState::Running, 0);
    let t2 = task("t2", "api", TaskState::Queued, 0);
    // De eerste ronde telt wat hij vindt, zoals Go.
    let m = c
        .observe(&poll_of(vec![on("a1", vec![t1.clone()])]), Some(NOW))
        .unwrap();
    assert_eq!(m.starts_total.get("api"), Some(&1));
    // Dezelfde taak nog eens: geen start.
    let m = c
        .observe(&poll_of(vec![on("a1", vec![t1.clone()])]), Some(NOW))
        .unwrap();
    assert_eq!(m.starts_total.get("api"), Some(&1));
    // Een nieuwe taak, en een staatswissel van een bekende: één start.
    let mut t1b = t1.clone();
    t1b.state = TaskState::Stopping;
    let m = c
        .observe(&poll_of(vec![on("a1", vec![t1b, t2])]), Some(NOW))
        .unwrap();
    assert_eq!(m.starts_total.get("api"), Some(&2));
    assert_eq!(m.failures_total.get("api"), None);
    assert_eq!(c.rounds(), 3);
}

#[test]
fn failures_count_transitions_to_failed_once() {
    let mut c = Collector::new();
    let run = task("t1", "api", TaskState::Running, 0);
    let mut failed = run.clone();
    failed.state = TaskState::Failed;
    c.observe(&poll_of(vec![on("a1", vec![run])]), Some(NOW))
        .unwrap();
    let m = c
        .observe(&poll_of(vec![on("a1", vec![failed.clone()])]), Some(NOW))
        .unwrap();
    assert_eq!(m.failures_total.get("api"), Some(&1));
    // Blijft hij failed, dan telt hij niet opnieuw.
    let m = c
        .observe(&poll_of(vec![on("a1", vec![failed])]), Some(NOW))
        .unwrap();
    assert_eq!(m.failures_total.get("api"), Some(&1));
    // Een taak die al failed binnenkomt is een start, geen failure (Go).
    let m = c
        .observe(
            &poll_of(vec![on(
                "a1",
                vec![task("t9", "api", TaskState::Failed, 0)],
            )]),
            Some(NOW),
        )
        .unwrap();
    assert_eq!(m.failures_total.get("api"), Some(&1));
    assert_eq!(m.starts_total.get("api"), Some(&2));
}

#[test]
fn restarts_count_a_restart_count_that_grew() {
    let mut c = Collector::new();
    let first = task("t1", "api", TaskState::Running, 0);
    c.observe(&poll_of(vec![on("a1", vec![first])]), Some(NOW))
        .unwrap();
    // Hop v3: een herstart is een nieuw id met de teller van de voorganger plus één.
    let replacement = task("t2", "api", TaskState::Running, 1);
    let m = c
        .observe(&poll_of(vec![on("a1", vec![replacement])]), Some(NOW))
        .unwrap();
    assert_eq!(m.restarts_total.get("api"), Some(&1));
    assert_eq!(m.starts_total.get("api"), Some(&2));
    // Op hetzelfde id telt het verschil (een agent die in place herstart).
    let grown = task("t2", "api", TaskState::Running, 3);
    let m = c
        .observe(&poll_of(vec![on("a1", vec![grown])]), Some(NOW))
        .unwrap();
    assert_eq!(m.restarts_total.get("api"), Some(&3));
    // Een teller die zakt (een gezonde run zet hem op nul) telt niets.
    let reset = task("t2", "api", TaskState::Running, 0);
    let m = c
        .observe(&poll_of(vec![on("a1", vec![reset])]), Some(NOW))
        .unwrap();
    assert_eq!(m.restarts_total.get("api"), Some(&3));
    let out = text(&m, Some(NOW));
    assert!(
        out.contains("# TYPE hop_task_restarts_total counter\n"),
        "{out}"
    );
    assert!(out.contains("hop_task_restarts_total{job=\"api\"} 3\n"));
}

#[test]
fn an_unreachable_agent_keeps_its_tasks_for_the_next_poll() {
    let mut c = Collector::new();
    let t1 = task("t1", "api", TaskState::Running, 0);
    let t2 = task("t2", "api", TaskState::Running, 0);
    let both = vec![on("a1", vec![t1.clone()]), on("a2", vec![t2.clone()])];
    c.observe(&poll_of(both.clone()), Some(NOW)).unwrap();
    let gone = vec![
        on("a1", vec![t1.clone()]),
        AgentTasks {
            agent: "a2".to_string(),
            tasks: None,
        },
    ];
    let m = c.observe(&poll_of(gone), Some(NOW)).unwrap();
    assert_eq!(m.tasks_placed.get("api"), Some(&1), "only what was reached");
    let m = c.observe(&poll_of(both), Some(NOW)).unwrap();
    assert_eq!(
        m.starts_total.get("api"),
        Some(&2),
        "a2 came back, no new start"
    );
}

#[test]
fn a_vanished_task_is_forgotten() {
    let mut c = Collector::new();
    let t1 = task("t1", "api", TaskState::Running, 0);
    c.observe(&poll_of(vec![on("a1", vec![t1.clone()])]), Some(NOW))
        .unwrap();
    c.observe(&poll_of(vec![on("a1", vec![])]), Some(NOW))
        .unwrap();
    let m = c
        .observe(&poll_of(vec![on("a1", vec![t1])]), Some(NOW))
        .unwrap();
    assert_eq!(m.starts_total.get("api"), Some(&2));
}

#[test]
fn leader_changes_count_switches_between_known_leaders() {
    let mut c = Collector::new();
    let with = |l: &str| Poll {
        leader: Some(Leader {
            leader: l.to_string(),
            lease_expires_at: Time::ZERO,
        }),
        ..Poll::default()
    };
    assert_eq!(
        c.observe(&with("n1:9080"), Some(NOW))
            .unwrap()
            .leader_changes,
        0
    );
    assert_eq!(
        c.observe(&with("n1:9080"), Some(NOW))
            .unwrap()
            .leader_changes,
        0
    );
    let m = c.observe(&with("n2:9080"), Some(NOW)).unwrap();
    assert_eq!((m.leader_changes, m.leader.as_str()), (1, "n2:9080"));
    // Een misser houdt de vorige leader, status idem.
    let m = c.observe(&Poll::default(), Some(NOW)).unwrap();
    assert_eq!((m.leader_changes, m.leader.as_str()), (1, "n2:9080"));
    let st = Poll {
        status: Some(Status {
            settling: true,
            deploying: vec!["api".to_string()],
            ..Status::default()
        }),
        ..Poll::default()
    };
    c.observe(&st, Some(NOW)).unwrap();
    let m = c.observe(&Poll::default(), Some(NOW)).unwrap();
    assert!(m.settling);
    assert_eq!(m.deploying, ["api"]);
}

// ---- Het tekstformaat -------------------------------------------------------

#[test]
fn the_text_format_is_exact() {
    let mut c = Collector::new();
    let mut run = task("t1", "hopprom", TaskState::Running, 0);
    run.cpu_percent = 1.25;
    run.mem_percent = 3.0;
    let mut a = agent("hopos-qemu", at(2));
    a.telemetry.temp_milli_c = 51_500;
    let p = Poll {
        agents: vec![a],
        jobs: vec![job("hopprom", 1)],
        tasks: vec![on("hopos-qemu", vec![run])],
        capacities: vec![(
            "hopos-qemu".to_string(),
            Some(Capacity {
                cpu_cores: 3,
                memory_bytes: 1 << 30,
                cpu_used_shares: 512,
                memory_used_bytes: 32 << 20,
                ..Capacity::default()
            }),
        )],
        status: Some(Status::default()),
        leader: Some(Leader {
            leader: "10.0.2.15:9080".to_string(),
            lease_expires_at: Time(NOW.0 + 12 * SECOND),
        }),
    };
    let m = c.observe(&p, Some(NOW)).unwrap();
    let want = "\
# HELP hop_agents_total Total number of registered agents
# TYPE hop_agents_total gauge
hop_agents_total 1

# HELP hop_agents_healthy Number of healthy agents (seen in last 60s)
# TYPE hop_agents_healthy gauge
hop_agents_healthy 1

# HELP hop_agent_cpu_cores Total CPU cores per agent
# TYPE hop_agent_cpu_cores gauge
hop_agent_cpu_cores{agent=\"hopos-qemu\"} 3

# HELP hop_agent_cpu_used_cores Used CPU cores per agent
# TYPE hop_agent_cpu_used_cores gauge
hop_agent_cpu_used_cores{agent=\"hopos-qemu\"} 0.50

# HELP hop_agent_memory_bytes Total memory bytes per agent
# TYPE hop_agent_memory_bytes gauge
hop_agent_memory_bytes{agent=\"hopos-qemu\"} 1073741824

# HELP hop_agent_memory_used_bytes Used memory bytes per agent
# TYPE hop_agent_memory_used_bytes gauge
hop_agent_memory_used_bytes{agent=\"hopos-qemu\"} 33554432

# HELP hop_agent_temperature_celsius CPU temperature per agent from its heartbeat (the hottest sensor)
# TYPE hop_agent_temperature_celsius gauge
hop_agent_temperature_celsius{agent=\"hopos-qemu\"} 51.5

# HELP hop_tasks_total Number of tasks by state
# TYPE hop_tasks_total gauge
hop_tasks_total{state=\"running\"} 1

# HELP hop_tasks_placed Placed tasks per job
# TYPE hop_tasks_placed gauge
hop_tasks_placed{job=\"hopprom\"} 1

# HELP hop_jobs_total Total number of jobs
# TYPE hop_jobs_total gauge
hop_jobs_total 1

# HELP hop_job_instances_placed Placed instances per job
# TYPE hop_job_instances_placed gauge
hop_job_instances_placed{job=\"hopprom\"} 1

# HELP hop_job_instances_expected Expected instances per job
# TYPE hop_job_instances_expected gauge
hop_job_instances_expected{job=\"hopprom\"} 1

# HELP hop_job_healthy Job is healthy (placed >= expected)
# TYPE hop_job_healthy gauge
hop_job_healthy{job=\"hopprom\"} 1

# HELP hop_job_cpu_percent Average CPU usage percent per job (relative to allocation)
# TYPE hop_job_cpu_percent gauge
hop_job_cpu_percent{job=\"hopprom\"} 1.2

# HELP hop_job_mem_percent Average memory usage percent per job (relative to allocation)
# TYPE hop_job_mem_percent gauge
hop_job_mem_percent{job=\"hopprom\"} 3.0

# HELP hop_task_starts_total Total task starts per job
# TYPE hop_task_starts_total counter
hop_task_starts_total{job=\"hopprom\"} 1

# HELP hop_leader_info Current leader address as a label (1 = a leader is known)
# TYPE hop_leader_info gauge
hop_leader_info{leader=\"10.0.2.15:9080\"} 1

# HELP hop_leader_lease_seconds Seconds until the local leader's lease lapses unless renewed (0 when this node does not lead)
# TYPE hop_leader_lease_seconds gauge
hop_leader_lease_seconds 12.0

# HELP hop_leader_changes_total Leader switches observed since hopprom started
# TYPE hop_leader_changes_total counter
hop_leader_changes_total 0

# HELP hop_cluster_settling 1 while a new leader waits before its first reconcile
# TYPE hop_cluster_settling gauge
hop_cluster_settling 0

# HELP hop_jobs_deploying Jobs whose last rollout did not finish
# TYPE hop_jobs_deploying gauge
hop_jobs_deploying 0

";
    let got = text(&m, Some(NOW));
    assert_eq!(got, want, "\n--- got ---\n{got}");
    assert_well_formed(&got);
}

#[test]
fn label_values_are_escaped() {
    let mut m = Metrics::default();
    types::Map::insert(&mut m.tasks_placed, "we\"ird\\job\nname".to_string(), 1).unwrap();
    m.leader = "caf\u{e9}:9080".to_string();
    let out = text(&m, Some(NOW));
    assert!(
        out.contains(r#"hop_tasks_placed{job="we\"ird\\job\nname"} 1"#),
        "{out}"
    );
    // Niet-ASCII blijft UTF-8 (Go's %q maakte er é van).
    assert!(out.contains("hop_leader_info{leader=\"caf\u{e9}:9080\"} 1"));
    assert_well_formed(&out);
}

#[test]
fn series_are_sorted_and_empty_families_are_left_out() {
    let mut c = Collector::new();
    let p = Poll {
        agents: vec![agent("b", at(1)), agent("a", at(1))],
        jobs: vec![job("zeta", 1), job("alpha", 1)],
        tasks: vec![on(
            "a",
            vec![
                task("t2", "zeta", TaskState::Running, 0),
                task("t1", "alpha", TaskState::Queued, 0),
            ],
        )],
        ..Poll::default()
    };
    let m = c.observe(&p, Some(NOW)).unwrap();
    let out = text(&m, Some(NOW));
    let alpha = out.find("hop_job_healthy{job=\"alpha\"}").unwrap();
    let zeta = out.find("hop_job_healthy{job=\"zeta\"}").unwrap();
    assert!(alpha < zeta);
    let queued = out.find("hop_tasks_total{state=\"queued\"}").unwrap();
    let running = out.find("hop_tasks_total{state=\"running\"}").unwrap();
    assert!(queued < running);
    // Geen capaciteit, geen herstarts, geen failures: die families ontbreken.
    for absent in [
        "hop_agent_cpu_cores",
        "hop_task_restarts ",
        "hop_task_failures_total",
        "hop_agent_temperature_celsius",
        "hop_job_deploying",
    ] {
        assert!(!out.contains(absent), "{absent} in\n{out}");
    }
    assert_eq!(text(&m, Some(NOW)), out, "twee scrapes, dezelfde bytes");
    assert_well_formed(&out);
}

#[test]
fn non_finite_values_are_written_as_prometheus_reads_them() {
    let mut m = Metrics::default();
    let j = JobMetric {
        cpu_percent: f64::NAN,
        mem_percent: f64::INFINITY,
        ..JobMetric::default()
    };
    types::Map::insert(&mut m.jobs, "x".to_string(), j).unwrap();
    let out = text(&m, None);
    assert!(
        out.contains("hop_job_cpu_percent{job=\"x\"} NaN\n"),
        "{out}"
    );
    assert!(out.contains("hop_job_mem_percent{job=\"x\"} +Inf\n"));
}

/// Elke regel is leeg, een HELP of TYPE, of een reeks van een familie
/// waarvan de TYPE al kwam; elke familie heeft precies één HELP en één TYPE.
fn assert_well_formed(out: &str) {
    let mut typed: Vec<&str> = Vec::new();
    let mut helped: Vec<&str> = Vec::new();
    assert!(out.ends_with('\n'));
    for line in out.lines() {
        if line.is_empty() {
            continue;
        }
        if let Some(rest) = line.strip_prefix("# HELP ") {
            let name = rest.split(' ').next().unwrap();
            assert!(!helped.contains(&name), "second HELP for {name}");
            helped.push(name);
        } else if let Some(rest) = line.strip_prefix("# TYPE ") {
            let mut it = rest.split(' ');
            let name = it.next().unwrap();
            assert!(matches!(it.next(), Some("gauge" | "counter")), "{line}");
            assert!(helped.last() == Some(&name), "TYPE without HELP: {line}");
            assert!(!typed.contains(&name), "second TYPE for {name}");
            typed.push(name);
        } else {
            let name = line.split(['{', ' ']).next().unwrap();
            assert_eq!(
                typed.last(),
                Some(&name),
                "sample outside its family: {line}"
            );
            let value = line.rsplit(' ').next().unwrap();
            assert!(
                value.parse::<f64>().is_ok() || matches!(value, "NaN" | "+Inf" | "-Inf"),
                "{line}"
            );
        }
    }
}

// ---- De pollronde over een nep-bron ----------------------------------------

/// Een bron met vaste antwoorden; `fail` noemt de stappen die falen.
struct Fake {
    fail: Vec<Step>,
    capacity_fails_for: Option<&'static str>,
    calls: Vec<Step>,
}

impl Fake {
    fn new(fail: &[Step]) -> Self {
        Self {
            fail: fail.to_vec(),
            capacity_fails_for: None,
            calls: Vec::new(),
        }
    }

    fn step<T>(&mut self, s: Step, v: T) -> core::result::Result<T, &'static str> {
        self.calls.push(s);
        if self.fail.contains(&s) {
            Err("status 500")
        } else {
            Ok(v)
        }
    }
}

impl Source for Fake {
    type Error = &'static str;

    async fn agents(&mut self) -> core::result::Result<Vec<Agent>, Self::Error> {
        self.step(Step::Agents, vec![agent("a1", at(1)), agent("a2", at(1))])
    }

    async fn jobs(&mut self) -> core::result::Result<Vec<Job>, Self::Error> {
        self.step(Step::Jobs, vec![job("api", 1)])
    }

    async fn tasks(&mut self) -> core::result::Result<Vec<AgentTasks>, Self::Error> {
        let t = task("t1", "api", TaskState::Running, 0);
        self.step(Step::Tasks, vec![on("a1", vec![t])])
    }

    async fn capacity(&mut self, a: &Agent) -> core::result::Result<Capacity, Self::Error> {
        if self.capacity_fails_for == Some(a.id.as_str()) {
            self.calls.push(Step::Capacity);
            return Err("connection refused");
        }
        self.step(
            Step::Capacity,
            Capacity {
                cpu_cores: 1,
                ..Capacity::default()
            },
        )
    }

    async fn status(&mut self) -> core::result::Result<Status, Self::Error> {
        self.step(Step::Status, Status::default())
    }

    async fn leader(&mut self) -> core::result::Result<Leader, Self::Error> {
        self.step(Step::Leader, Leader::default())
    }

    fn out_of_memory(&self) -> Self::Error {
        "out of memory"
    }
}

#[test]
fn a_full_round_asks_every_route_once() {
    let mut src = Fake::new(&[]);
    let (p, misses) = block_on(poll(&mut src)).unwrap();
    assert!(misses.is_empty());
    assert_eq!(
        src.calls,
        [
            Step::Agents,
            Step::Jobs,
            Step::Tasks,
            Step::Capacity,
            Step::Capacity,
            Step::Status,
            Step::Leader
        ]
    );
    assert_eq!((p.agents.len(), p.task_count()), (2, 1));
    assert!(p.capacities.iter().all(|(_, c)| c.is_some()));
}

#[test]
fn a_required_step_that_fails_fails_the_round() {
    for step in [Step::Agents, Step::Jobs, Step::Tasks] {
        let mut src = Fake::new(&[step]);
        let miss = block_on(poll(&mut src)).unwrap_err();
        assert_eq!(miss.step, step);
        assert_eq!(
            miss.to_string(),
            alloc::format!("{}: status 500", step.path())
        );
    }
}

#[test]
fn best_effort_steps_miss_without_failing_the_round() {
    let mut src = Fake::new(&[Step::Status, Step::Leader]);
    src.capacity_fails_for = Some("a2");
    let (p, misses) = block_on(poll(&mut src)).unwrap();
    assert_eq!(p.status, None);
    assert_eq!(p.leader, None);
    assert!(p.capacities[0].1.is_some());
    assert!(p.capacities[1].1.is_none());
    let steps: Vec<Step> = misses.iter().map(|m| m.step).collect();
    assert_eq!(steps, [Step::Capacity, Step::Status, Step::Leader]);
    // En de collector houdt het vorige vast.
    let mut c = Collector::new();
    let m = c.observe(&p, Some(NOW)).unwrap();
    assert_eq!(m.agents_total, 2);
}

// ---- De env van de bewoner --------------------------------------------------

#[test]
fn hop_in_the_agent_url_is_hops_slot_address() {
    let ip = [10, 100, 0, 2];
    assert_eq!(
        config::with_hop_address(config::DEFAULT_AGENT, ip).unwrap(),
        "http://10.100.0.2:8080"
    );
    assert_eq!(
        config::with_hop_address("hop:8080", ip).unwrap(),
        "10.100.0.2:8080"
    );
    assert_eq!(
        config::with_hop_address("http://HOP/x", ip).unwrap(),
        "http://10.100.0.2/x"
    );
    for other in ["http://10.0.2.15:9080", "http://hoppy:9080", "node1:8080"] {
        assert_eq!(config::with_hop_address(other, ip).unwrap(), other);
    }
}

#[test]
fn interval_and_port_come_from_the_env_with_loud_defaults() {
    use core::time::Duration;
    assert_eq!(config::interval(None), Ok(Duration::from_secs(5)));
    assert_eq!(config::interval(Some("1m30s")), Ok(Duration::from_secs(90)));
    assert_eq!(
        config::interval(Some("250ms")),
        Ok(Duration::from_millis(250))
    );
    assert_eq!(config::interval(Some("0s")), Err(Duration::from_secs(5)));
    assert_eq!(config::interval(Some("soon")), Err(Duration::from_secs(5)));
    assert_eq!(config::port(None), Ok(9090));
    assert_eq!(config::port(Some("9191")), Ok(9191));
    assert_eq!(config::port(Some("0")), Err(9090));
    assert_eq!(config::port(Some("metrics")), Err(9090));
}
