//! De toetsen van de watcher: Go's `watcher_test.go`, naam voor naam.
//!
//! Go draaide de SSE-toetsen tegen een `httptest`-server met echte slaap
//! (300 ms, 800 ms). Hier is de watcher sans-I/O, dus draait dezelfde
//! kring in een simulatie met een nepklok: een nep-Hop met jobs en taken,
//! een rij gebeurtenissen met hun tijd, en een lus die de acties van de
//! watcher uitvoert zoals een driver dat doet. Dezelfde volgorde, dezelfde
//! termijnen, zonder één milliseconde echt te wachten.

use super::*;
use crate::Cache;
use alloc::collections::VecDeque;
use alloc::string::ToString;
use alloc::vec;
use hoplib::{Client, Response};

const MS: u64 = 1_000_000;

fn ip(s: &str) -> Ipv4Addr {
    s.parse().unwrap()
}

fn agent(id: &str, endpoint: &str) -> AgentInfo {
    AgentInfo {
        id: id.to_string(),
        endpoint: endpoint.to_string(),
        ..AgentInfo::default()
    }
}

fn task(id: &str, job: &str, state: TaskState) -> Task {
    Task {
        id: id.to_string(),
        job_name: job.to_string(),
        state,
        ..Task::default()
    }
}

fn on(agent: &str, tasks: Vec<Task>) -> AgentTasks {
    AgentTasks {
        agent: agent.to_string(),
        tasks: Some(tasks),
    }
}

fn ev(kind: &str, data: &str) -> Event {
    Event {
        kind: kind.to_string(),
        data: data.to_string(),
    }
}

// TestWatcherRefresh: twee lopende taken op één agent zijn één adres; een
// gestopte job heeft er geen.
#[test]
fn watcher_refresh() {
    let agents = [agent("agent1", "http://127.0.0.1:18080")];
    let tasks = [on(
        "agent1",
        vec![
            task("task1", "myapp", TaskState::Running),
            task("task2", "myapp", TaskState::Running),
            task("task3", "other", TaskState::Stopping),
        ],
    )];
    let mut w = Watcher::new("http://127.0.0.1:18080").unwrap();
    w.discovered(0, "test-cluster").unwrap();
    let mut cache = Cache::new();
    w.refreshed_full(&agents, &tasks, None)
        .unwrap()
        .apply(&mut cache)
        .unwrap();
    assert_eq!(cache.get_cluster("test-cluster", "myapp").unwrap().len(), 1);
    assert!(
        cache
            .get_cluster("test-cluster", "other")
            .unwrap_or_default()
            .is_empty()
    );
}

// TestExtractIP: het adres uit een endpoint.
#[test]
fn extract_ip_from_endpoint() {
    for (endpoint, want) in [
        ("http://192.168.1.10:8080", "192.168.1.10"),
        ("https://10.0.0.1:443", "10.0.0.1"),
        ("http://127.0.0.1:8080", "127.0.0.1"),
    ] {
        assert_eq!(extract_ip(endpoint), Some(ip(want)), "{endpoint}");
    }
}

// TestExtractIPInvalid: geen URL, geen adres.
#[test]
fn extract_ip_invalid() {
    assert_eq!(extract_ip("not-a-url"), None);
    assert_eq!(extract_ip("http://node1:8080"), None);
    assert_eq!(extract_ip("http://[::1]:8080"), None);
}

// TestWatcherNoAgents: een verversing zonder jobs leegt het cluster.
#[test]
fn watcher_no_agents() {
    let mut cache = Cache::new();
    cache
        .set("test-cluster", "oldapp", vec![ip("10.0.0.1")])
        .unwrap();
    let mut w = Watcher::new("http://x").unwrap();
    w.discovered(0, "test-cluster").unwrap();
    w.refreshed_full(&[], &[], None)
        .unwrap()
        .apply(&mut cache)
        .unwrap();
    assert!(
        cache
            .get_cluster("test-cluster", "oldapp")
            .unwrap_or_default()
            .is_empty(),
        "cache should be empty after refresh with no jobs"
    );
}

/// `/v1/status` door hoplib gelezen, zoals Go's `discoverCluster`.
fn status_name(body: &str) -> String {
    let call = Client::new("http://127.0.0.1:8080", None)
        .unwrap()
        .status()
        .unwrap();
    call.parse(&Response {
        status: 200,
        body: body.as_bytes().to_vec(),
    })
    .unwrap()
    .cluster_name
}

// TestWatcherDiscoverCluster: de naam uit `/v1/status`.
#[test]
fn watcher_discover_cluster() {
    let name = status_name(r#"{"cluster_name":"prod-eu","agents":1}"#);
    let mut w = Watcher::new("http://x").unwrap();
    assert_eq!(w.next(0), Action::Discover);
    w.discovered(0, &name).unwrap();
    assert_eq!(w.cluster(), Some("prod-eu"));
}

// TestWatcherDiscoverClusterMissing: zonder `cluster_name` faalt de
// ontdekking, en de volgende poging wacht de interval.
#[test]
fn watcher_discover_cluster_missing() {
    let name = status_name(r#"{"agents":1}"#);
    let mut w = Watcher::new("http://x").unwrap();
    assert_eq!(w.discovered(0, &name), Err(Error::NoClusterName));
    assert_eq!(w.cluster(), None);
    let five = nanos(RECONNECT);
    assert_eq!(w.next(1), Action::Wait(Some(five)));
    assert_eq!(w.next(five), Action::Discover);
}

// ============== De SSE-kring, gesimuleerd ==============

/// Wat de stroom brengt, en wanneer.
enum Arrival {
    Event(Event),
    Lost,
}

/// Een nep-Hop (Go's `mockHop`): jobs met hun taken op `agent1`, en hoe
/// vaak elke route gevraagd werd.
#[derive(Default)]
struct FakeHop {
    jobs: Vec<(String, Vec<Task>)>,
    full_calls: usize,
    job_calls: Vec<String>,
}

impl FakeHop {
    fn set_job(&mut self, name: &str, tasks: Vec<Task>) {
        self.jobs.retain(|(n, _)| n != name);
        self.jobs.push((name.to_string(), tasks));
    }

    fn agents(&self) -> Vec<AgentInfo> {
        vec![agent("agent1", "http://127.0.0.1:18080")]
    }

    fn tasks(&self) -> Vec<AgentTasks> {
        vec![on(
            "agent1",
            self.jobs.iter().flat_map(|(_, t)| t.clone()).collect(),
        )]
    }
}

/// De kring: watcher, nep-Hop, cache en een nepklok.
struct Sim {
    w: Watcher,
    hop: FakeHop,
    cache: Cache,
    now: u64,
    arrivals: VecDeque<(u64, Arrival)>,
}

impl Sim {
    /// Een verbonden watcher: de ontdekking lukt, en de stroom begint met
    /// een ping na 10 ms (Go's mock stuurt hem meteen).
    fn new(hop: FakeHop) -> Self {
        let mut s = Sim {
            w: Watcher::new("http://127.0.0.1:18080").unwrap(),
            hop,
            cache: Cache::new(),
            now: 0,
            arrivals: VecDeque::new(),
        };
        s.at(10 * MS, Arrival::Event(ev("ping", "{}")));
        s
    }

    fn at(&mut self, t: u64, a: Arrival) {
        self.arrivals.push_back((t, a));
    }

    fn send_sse(&mut self, t: u64, kind: &str, data: &str) {
        self.at(t, Arrival::Event(ev(kind, data)));
    }

    /// Voert de acties van de watcher uit tot de klok op `until` staat.
    fn run_until(&mut self, until: u64) {
        loop {
            match self.w.next(self.now) {
                Action::Discover => self.w.discovered(self.now, "test-cluster").unwrap(),
                Action::Refresh(Refresh::Full) => {
                    self.hop.full_calls += 1;
                    let (a, t) = (self.hop.agents(), self.hop.tasks());
                    let u = self.w.refreshed_full(&a, &t, None).unwrap();
                    u.apply(&mut self.cache).unwrap();
                }
                Action::Refresh(Refresh::Jobs(jobs)) => {
                    for job in jobs {
                        self.hop.job_calls.push(job.clone());
                        let st = JobStatus::from_tasks(&job, self.hop.agents(), self.hop.tasks());
                        let u = self.w.refreshed_job(&job, &st, None).unwrap();
                        u.apply(&mut self.cache).unwrap();
                    }
                }
                Action::Wait(deadline) => {
                    let next = self.arrivals.front().map(|(t, _)| *t);
                    let wake = match (deadline, next) {
                        (Some(d), Some(n)) => d.min(n),
                        (Some(d), None) => d,
                        (None, Some(n)) => n,
                        (None, None) => u64::MAX,
                    };
                    if wake > until {
                        self.now = until;
                        return;
                    }
                    self.now = self.now.max(wake);
                    if next == Some(wake)
                        && let Some((_, a)) = self.arrivals.pop_front()
                    {
                        match a {
                            Arrival::Event(e) => self.w.event(self.now, &e).unwrap(),
                            Arrival::Lost => self.w.lost(),
                        }
                    }
                }
            }
        }
    }

    fn ips(&self, job: &str) -> usize {
        self.cache
            .get_cluster("test-cluster", job)
            .map_or(0, <[Ipv4Addr]>::len)
    }
}

fn running(id: &str, job: &str) -> Task {
    task(id, job, TaskState::Running)
}

// TestSSE_JobEvent: een job-melding, dan de samenvoeging, dan de
// verversing van die job; twee taken op dezelfde agent blijven één adres.
#[test]
fn sse_job_event() {
    let mut hop = FakeHop::default();
    hop.set_job("api", vec![running("t1", "api")]);
    let mut s = Sim::new(hop);
    s.run_until(300 * MS);
    assert_eq!(s.ips("api"), 1, "expected 1 IP after initial refresh");
    s.hop
        .set_job("api", vec![running("t1", "api"), running("t2", "api")]);
    s.send_sse(300 * MS, "job", r#"{"name":"api"}"#);
    s.run_until(1100 * MS);
    assert_eq!(s.hop.job_calls, ["api"]);
    assert_eq!(s.ips("api"), 1, "expected 1 IP (same agent)");
}

// TestSSE_TaskEvent: een taak-melding (het veld `job`) na een crash leegt
// de job.
#[test]
fn sse_task_event() {
    let mut hop = FakeHop::default();
    hop.set_job("worker", vec![running("t1", "worker")]);
    let mut s = Sim::new(hop);
    s.run_until(300 * MS);
    assert_eq!(s.ips("worker"), 1);
    s.hop
        .set_job("worker", vec![task("t1", "worker", TaskState::Failed)]);
    s.send_sse(300 * MS, "task", r#"{"job":"worker","event":"crash"}"#);
    s.run_until(1100 * MS);
    assert_eq!(s.ips("worker"), 0, "expected 0 IPs after task crash");
}

// TestSSE_NewJobAppears: een nieuwe job verschijnt via een melding.
#[test]
fn sse_new_job_appears() {
    let mut s = Sim::new(FakeHop::default());
    s.run_until(300 * MS);
    assert_eq!(s.ips("newapp"), 0);
    s.hop.set_job("newapp", vec![running("t1", "newapp")]);
    s.send_sse(300 * MS, "job", r#"{"name":"newapp"}"#);
    s.run_until(1100 * MS);
    assert_eq!(s.ips("newapp"), 1, "expected 1 IP after new job event");
}

// TestSSE_Debounce: vijf meldingen binnen 50 ms worden één verversing.
#[test]
fn sse_debounce() {
    let mut hop = FakeHop::default();
    hop.set_job("api", vec![running("t1", "api")]);
    let mut s = Sim::new(hop);
    s.run_until(300 * MS);
    assert_eq!(s.hop.full_calls, 1, "the seed");
    for i in 0..5 {
        s.send_sse(
            (300 + 10 * i) * MS,
            "task",
            r#"{"job":"api","event":"start"}"#,
        );
    }
    s.run_until(1140 * MS);
    assert_eq!(s.hop.job_calls, ["api"], "expected 1 debounced refresh");
    assert_eq!(s.hop.full_calls, 1);
}

// TestSSE_Disconnect: een weggevallen stroom laat de cache staan
// (verouderd maar bruikbaar), en er komt geen verversing tot de volgende
// verbinding.
#[test]
fn sse_disconnect() {
    let mut hop = FakeHop::default();
    hop.set_job("api", vec![running("t1", "api")]);
    let mut s = Sim::new(hop);
    s.run_until(300 * MS);
    assert_eq!(s.ips("api"), 1, "expected 1 IP before disconnect");
    s.send_sse(300 * MS, "job", r#"{"name":"api"}"#);
    s.at(310 * MS, Arrival::Lost);
    s.hop.jobs.clear();
    s.run_until(500 * MS);
    assert_eq!(
        s.ips("api"),
        1,
        "expected stale cache kept after disconnect"
    );
    assert!(
        s.hop.job_calls.is_empty(),
        "no refresh against a dead stream"
    );
    // De volgende verbinding begint met een ping, en die zaait opnieuw.
    s.at(5310 * MS, Arrival::Event(ev("ping", "{}")));
    s.run_until(5400 * MS);
    assert_eq!(s.hop.full_calls, 2);
    assert_eq!(s.ips("api"), 0);
}

// TestSSE_MultipleJobEvents: een melding voor één job raakt de andere niet.
#[test]
fn sse_multiple_job_events() {
    let mut hop = FakeHop::default();
    hop.set_job("api", vec![running("t1", "api")]);
    hop.set_job("worker", vec![running("t2", "worker")]);
    let mut s = Sim::new(hop);
    s.run_until(300 * MS);
    assert_eq!((s.ips("api"), s.ips("worker")), (1, 1));
    s.hop
        .set_job("worker", vec![task("t2", "worker", TaskState::Stopping)]);
    s.send_sse(300 * MS, "job", r#"{"name":"worker"}"#);
    s.run_until(1100 * MS);
    assert_eq!(s.ips("api"), 1, "api should still have 1 IP");
    assert_eq!(s.ips("worker"), 0, "worker should have 0 IPs after stop");
}

#[test]
fn status_and_agent_events_refresh_everything() {
    let mut s = Sim::new(FakeHop::default());
    s.run_until(300 * MS);
    s.hop.set_job("a", vec![running("t1", "a")]);
    s.send_sse(300 * MS, "job", r#"{"name":"a"}"#);
    s.send_sse(320 * MS, "status", "{}");
    s.send_sse(330 * MS, "agent", r#"{"id":"n2"}"#);
    s.run_until(1000 * MS);
    assert_eq!(s.hop.full_calls, 2);
    assert!(s.hop.job_calls.is_empty());
    assert_eq!(s.ips("a"), 1);
}

#[test]
fn a_failed_job_refresh_falls_back_to_a_full_one() {
    let mut w = Watcher::new("http://x").unwrap();
    w.discovered(0, "c").unwrap();
    w.event(0, &ev("job", r#"{"name":"Web"}"#)).unwrap();
    let Action::Refresh(r) = w.next(nanos(DEBOUNCE)) else {
        panic!("no refresh");
    };
    assert_eq!(r, Refresh::Jobs(vec![String::from("web")]));
    w.refresh_failed(nanos(DEBOUNCE), &r);
    assert_eq!(w.next(nanos(DEBOUNCE)), Action::Refresh(Refresh::Full));
    w.refresh_failed(nanos(DEBOUNCE), &Refresh::Full);
    let later = nanos(DEBOUNCE) + nanos(RECONNECT);
    assert_eq!(w.next(nanos(DEBOUNCE)), Action::Wait(Some(later)));
}

#[test]
fn a_storm_of_jobs_becomes_one_full_refresh() {
    let mut w = Watcher::new("http://x").unwrap();
    w.discovered(0, "c").unwrap();
    for i in 0..=MAX_PENDING {
        let data = alloc::format!(r#"{{"name":"job-{i}"}}"#);
        w.event(0, &ev("job", &data)).unwrap();
    }
    assert_eq!(w.next(nanos(DEBOUNCE)), Action::Refresh(Refresh::Full));
}

#[test]
fn own_node_tasks_get_their_slot_address() {
    fn slot_ip(n: u64) -> [u8; 4] {
        [10, 100, 0, 1 + n as u8]
    }
    let local = Local {
        node: "n1",
        slot_ip,
    };
    let agents = [
        agent("n1", "http://10.0.2.15:8080"),
        agent("n2", "http://192.168.1.7:8080"),
    ];
    let mut here = running("t1", "Welcome");
    here.driver = String::from("hop");
    here.pid = 2;
    let mut there = running("t2", "welcome");
    there.driver = String::from("hop");
    there.pid = 2;
    let tasks = [on("n1", vec![here]), on("n2", vec![there])];
    let jobs = build_jobs(&agents, &tasks, Some(&local)).unwrap();
    assert_eq!(
        jobs.get("welcome").unwrap(),
        &[ip("10.100.0.3"), ip("192.168.1.7")]
    );
    // Zonder Local (de host): het adres van de agent, zoals Go.
    let jobs = build_jobs(&agents, &tasks, None).unwrap();
    assert_eq!(
        jobs.get("welcome").unwrap(),
        &[ip("10.0.2.15"), ip("192.168.1.7")]
    );
    // Een agent die de leader niet antwoordde, draagt niets bij.
    let silent = [AgentTasks {
        agent: String::from("n1"),
        tasks: None,
    }];
    assert!(build_jobs(&agents, &silent, None).unwrap().is_empty());
}
