//! De test van `OLD/internal/lb/watcher_test.go`, naam voor naam, de
//! benchmarks `BuildRoutes` en `ParseJobFromData`, en de regels van
//! `watcher.go` die Go niet los toetste.

use super::*;
use crate::testutil::bench;
use alloc::format;
use alloc::string::ToString;
use alloc::vec;
use hoplib::Map;

#[test]
fn test_classify_event() {
    let relevant = ["api"];
    let known = ["api", "batch"];
    let cases: &[(&str, &str, &str, bool)] = &[
        (
            "job event on relevant job",
            r#"data: {"name":"api"}"#,
            "api",
            true,
        ),
        (
            "job event on known irrelevant job (tags may have changed)",
            r#"data: {"name":"batch"}"#,
            "batch",
            true,
        ),
        (
            "job event on unknown job",
            r#"data: {"name":"new"}"#,
            "new",
            true,
        ),
        (
            "task event on relevant job",
            r#"data: {"job":"api","event":"started"}"#,
            "api",
            false,
        ),
        (
            "task event on known irrelevant job",
            r#"data: {"job":"batch","event":"crash"}"#,
            "",
            false,
        ),
        (
            "task event on unknown job",
            r#"data: {"job":"new","event":"started"}"#,
            "new",
            true,
        ),
        ("ping", "data: {}", "", false),
        ("garbage", "data: not json", "", false),
    ];
    for (name, line, want_job, want_full) in cases {
        let got = classify_event(line, |j| relevant.contains(&j), |j| known.contains(&j));
        let (job, full) = got.map_or((String::new(), false), |(j, f)| (j.into_owned(), f));
        assert_eq!(
            (job.as_str(), full),
            (*want_job, *want_full),
            "{name}: classifyEvent({line:?})"
        );
    }
}

fn job(name: &str, tags: &[(&str, &str)]) -> Job {
    let mut t = Map::new();
    for (k, v) in tags {
        t.insert(k.to_string(), v.to_string()).unwrap();
    }
    Job {
        name: name.to_string(),
        tags: t,
        ..Job::default()
    }
}

fn task(job: &str, state: TaskState, ports: &[(&str, u16)]) -> Task {
    let mut p = Map::new();
    for (k, v) in ports {
        p.insert(k.to_string(), *v).unwrap();
    }
    Task {
        id: format!("{job}-task"),
        job_name: job.to_string(),
        state,
        ports: p,
        ..Task::default()
    }
}

fn agent(id: &str, endpoint: &str) -> AgentInfo {
    AgentInfo {
        id: id.to_string(),
        endpoint: endpoint.to_string(),
        ..AgentInfo::default()
    }
}

fn on(agent: &str, tasks: Vec<Task>) -> AgentTasks {
    AgentTasks {
        agent: agent.to_string(),
        tasks: Some(tasks),
    }
}

fn addrs(rt: &RouteTable, host: &str) -> Vec<String> {
    rt.match_host(host)
        .map(|r| r.backends.iter().map(|b| b.address.clone()).collect())
        .unwrap_or_default()
}

#[test]
fn only_running_tasks_of_relevant_jobs_become_backends() {
    let mut w = Watcher::new("").unwrap();
    let agents = [
        agent("a1", "http://10.0.0.1:8080"),
        agent("a2", "http://10.0.0.2:8080"),
    ];
    let jobs = [
        job(
            "web",
            &[(TAG_URLPREFIX, "web.example.com"), (TAG_PORT, "http")],
        ),
        job("batch", &[]),
    ];
    let tasks = vec![
        on(
            "a1",
            vec![
                task(
                    "web",
                    TaskState::Running,
                    &[("admin", 9000), ("http", 8080)],
                ),
                task("batch", TaskState::Running, &[("http", 1)]),
            ],
        ),
        on(
            "a2",
            vec![
                task("web", TaskState::Running, &[("http", 8081)]),
                task("web", TaskState::Queued, &[("http", 8082)]),
                task("web", TaskState::Failed, &[("http", 8083)]),
            ],
        ),
    ];
    w.apply_full(&agents, &jobs, tasks).unwrap();
    assert!(w.is_relevant("web"));
    assert!(!w.is_relevant("batch"));
    assert!(w.is_known("batch"));
    let rt = w.build_routes().unwrap();
    assert_eq!(rt.len(), 1);
    assert_eq!(
        addrs(&rt, "web.example.com"),
        ["10.0.0.1:8080", "10.0.0.2:8081"]
    );
}

#[test]
fn the_tag_filter_picks_the_jobs() {
    let mut w = Watcher::new("lb:haas").unwrap();
    let agents = [agent("a1", "http://10.0.0.1:8080")];
    let jobs = [
        job("mine", &[("lb", "haas"), (TAG_URLPREFIX, "*.haas.eu")]),
        job(
            "theirs",
            &[("lb", "staging"), (TAG_URLPREFIX, "*.staging.eu")],
        ),
        job("untagged", &[(TAG_URLPREFIX, "*.other.eu")]),
    ];
    let tasks = vec![on(
        "a1",
        vec![
            task("mine", TaskState::Running, &[("http", 80)]),
            task("theirs", TaskState::Running, &[("http", 81)]),
            task("untagged", TaskState::Running, &[("http", 82)]),
        ],
    )];
    w.apply_full(&agents, &jobs, tasks).unwrap();
    assert_eq!(w.relevant().collect::<Vec<_>>(), ["mine"]);
    let rt = w.build_routes().unwrap();
    assert_eq!(addrs(&rt, "app.haas.eu"), ["10.0.0.1:80"]);
    assert!(rt.match_host("app.staging.eu").is_none());
}

#[test]
fn a_filter_without_value_wants_the_tag_absent() {
    // Go: `job.Tags[key] == value`, met value "" na `parseTagFilter("lb")`.
    assert_eq!(parse_tag_filter("lb:haas"), ("lb", "haas"));
    assert_eq!(parse_tag_filter("lb"), ("lb", ""));
    let w = Watcher::new("lb").unwrap();
    assert!(w.matches_filter(&job("x", &[])));
    assert!(!w.matches_filter(&job("y", &[("lb", "haas")])));
}

#[test]
fn a_job_status_replaces_the_tasks_of_one_job() {
    let mut w = Watcher::new("").unwrap();
    let jobs = [
        job("a", &[(TAG_URLPREFIX, "a.example.com")]),
        job("b", &[(TAG_URLPREFIX, "b.example.com")]),
    ];
    let tasks = vec![on(
        "n1",
        vec![
            task("a", TaskState::Running, &[("http", 1000)]),
            task("b", TaskState::Running, &[("http", 2000)]),
        ],
    )];
    w.apply_full(&[agent("n1", "http://10.0.0.1:8080")], &jobs, tasks)
        .unwrap();
    let st = JobStatus {
        agents: vec![agent("n2", "http://10.0.0.2:8080")],
        tasks: vec![on(
            "n2",
            vec![task("a", TaskState::Running, &[("http", 1001)])],
        )],
    };
    w.apply_job("a", st).unwrap();
    let rt = w.build_routes().unwrap();
    assert_eq!(addrs(&rt, "a.example.com"), ["10.0.0.2:1001"]);
    assert_eq!(addrs(&rt, "b.example.com"), ["10.0.0.1:2000"]);
}

#[test]
fn an_agent_without_host_or_a_task_without_port_gives_nothing() {
    let mut w = Watcher::new("").unwrap();
    let jobs = [job("a", &[(TAG_URLPREFIX, "a.example.com")])];
    let tasks = vec![
        on("ghost", vec![task("a", TaskState::Running, &[("http", 1)])]),
        on("n1", vec![task("a", TaskState::Running, &[])]),
    ];
    w.apply_full(
        &[
            agent("n1", "http://10.0.0.1:8080"),
            agent("bad", "nonsense"),
        ],
        &jobs,
        tasks,
    )
    .unwrap();
    assert!(w.build_routes().unwrap().is_empty());
}

#[test]
fn hosts_come_out_of_endpoints() {
    assert_eq!(extract_host("http://10.0.2.15:8080"), "10.0.2.15");
    assert_eq!(
        extract_host("https://node.example.com/x"),
        "node.example.com"
    );
    assert_eq!(extract_host("http://[fd00::1]:8080"), "fd00::1");
    assert_eq!(extract_host("http://user@h:1"), "h");
    assert_eq!(extract_host("10.0.0.1:8080"), "");
    assert_eq!(backend("fd00::1", 80).unwrap().address, "[fd00::1]:80");
    assert_eq!(
        backend("10.0.0.1", 65535).unwrap().address,
        "10.0.0.1:65535"
    );
    assert_eq!(backend("h", 0).unwrap().address, "h:0");
}

#[test]
fn the_named_port_wins_else_the_first() {
    let t = task("a", TaskState::Running, &[("admin", 9000), ("http", 8080)]);
    assert_eq!(task_port(&t, "http"), Some(8080));
    assert_eq!(task_port(&t, "grpc"), Some(9000));
    assert_eq!(task_port(&t, ""), Some(9000));
}

#[test]
fn pending_waits_half_a_second_and_folds() {
    let mut p = Pending::new();
    assert_eq!(p.take(0), None);
    p.push("a", false, 1_000).unwrap();
    p.push("b", false, 200_000_000).unwrap();
    p.push("a", false, 300_000_000).unwrap();
    assert_eq!(p.due(), Some(500_001_000));
    assert_eq!(p.take(500_000_999), None);
    assert_eq!(
        p.take(500_001_000),
        Some(Sync::Jobs(vec!["a".into(), "b".into()]))
    );
    assert_eq!(p.due(), None);
    p.push("a", false, 1).unwrap();
    p.push("new", true, 2).unwrap();
    assert_eq!(p.take(u64::MAX), Some(Sync::Full));
}

#[test]
fn benchmark_build_routes() {
    // 100 jobs, 10 agents, 5 taken per job (Go: `BenchmarkBuildRoutes`).
    let mut w = Watcher::new("").unwrap();
    let agents: Vec<AgentInfo> = (0..10)
        .map(|i| agent(&format!("agent-{i}"), &format!("http://10.0.0.{i}:8080")))
        .collect();
    let jobs: Vec<Job> = (0..100)
        .map(|i| {
            job(
                &format!("job-{i}"),
                &[
                    (TAG_URLPREFIX, &format!("*.job{i}.example.com")),
                    (TAG_PORT, "http"),
                ],
            )
        })
        .collect();
    let mut per_agent: Vec<AgentTasks> =
        (0..10).map(|i| on(&format!("agent-{i}"), vec![])).collect();
    for i in 0..100 {
        for a in 0..5 {
            let mut t = task(
                &format!("job-{i}"),
                TaskState::Running,
                &[("http", 8080 + a)],
            );
            t.id = format!("task-{i}-{a}");
            per_agent[usize::from(a) % 10]
                .tasks
                .as_mut()
                .unwrap()
                .push(t);
        }
    }
    w.apply_full(&agents, &jobs, per_agent).unwrap();
    let rt = w.build_routes().unwrap();
    assert_eq!((rt.len(), rt.backends()), (100, 500));
    let ns = bench("BuildRoutes (100 jobs)", 2_000, |_| {
        let rt = w.build_routes().unwrap();
        assert_eq!(rt.len(), 100);
    });
    // Go: 58.686 ns/op.
    assert!(ns < 50_000_000.0, "build routes {ns:.0} ns/op");
}

#[test]
fn benchmark_parse_job_from_data() {
    let lines = [
        r#"data: {"name":"my-api","type":"task_started","task_id":"abc123"}"#,
        r#"data: {"name":"nginx-proxy","type":"task_stopped"}"#,
        r#"data: {"name":"redis-cache","type":"state_changed","state":"running"}"#,
        r#"data: {"name":"worker-pool-long-name","type":"task_failed","error":"exit 1"}"#,
    ];
    let ns = bench("ParseJobFromData", 1_000_000, |i| {
        let got = classify_event(lines[i % 4], |_| true, |_| true);
        assert!(got.is_some_and(|(j, _)| !j.is_empty()));
    });
    // Go (hoplib.ParseJobFromSSE): 359 ns/op, 7 allocs.
    assert!(ns < 100_000.0, "parse {ns:.1} ns/op");
}

#[test]
fn a_failed_sync_retries_in_full() {
    let mut p = Pending::new();
    p.retry(10, Duration::from_secs(5));
    assert_eq!(p.due(), Some(5_000_000_010));
    assert_eq!(p.take(5_000_000_010), Some(Sync::Full));
}
