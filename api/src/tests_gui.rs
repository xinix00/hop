//! Elke route die het dashboard (hop-gui, `app.js`) aanroept, tegen de
//! [`NodeApi`] (de browser praat met een agent) en de [`LeaderApi`] (waar
//! de agent het verzoek heen geeft).
//!
//! De lijst staat niet hier: de test haalt hem uit `app.js` zelf, zodat hij
//! meeloopt als het dashboard verandert. Het dashboard is een eigen repo;
//! de test zoekt het naast deze (`../hop-gui`, of `HOP_GUI_DIR`) en zegt
//! luid dat hij niets toetste als het er niet is.
//!
//! Wat "bestaat" hier betekent: op de agent een ondertekend verzoek dat geen
//! 401, 404 of 405 geeft, de CORS-koppen draagt, en voor `/v1/...` een
//! doorgifte naar de leader wordt (een stroom voor SSE en logs); een
//! preflight zonder handtekening die de methode toestaat; en op de leader
//! een antwoord onder de 400 of een effect dat de adapter uitvoert. Daarna
//! de velden die `app.js` uit `/v1/status` en de capaciteit leest.

use super::*;
use agent::{Agent, Settings};
use alloc::string::ToString;
use std::path::PathBuf;
use std::string::String as StdString;
use std::vec::Vec as StdVec;

const T0: u64 = 1_000 * types::time::SECOND;
const KEY: &[u8] = b"dashboard-key";

/// Een aanroep van het dashboard: methode en pad met voorbeeldwaarden.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Call {
    method: Method,
    path: StdString,
}

/// Waar `app.js` staat, of `None` als het dashboard niet naast deze repo staat.
fn app_js() -> Option<StdString> {
    let dir = std::env::var_os("HOP_GUI_DIR").map_or_else(
        || PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../hop-gui"),
        PathBuf::from,
    );
    std::fs::read_to_string(dir.join("app.js")).ok()
}

/// Een voorbeeldwaarde voor een `${...}` in een pad, op zijn naam.
fn sample(expr: &str) -> &'static str {
    let e = expr.to_ascii_lowercase();
    if e.contains("agent") {
        "a1"
    } else if e.contains("task") {
        "t1"
    } else if e.contains("stream") {
        "stdout"
    } else {
        "web"
    }
}

/// Vervangt elke `${...}` door zijn voorbeeldwaarde.
fn fill(template: &str) -> StdString {
    let mut out = StdString::new();
    let mut rest = template;
    while let Some(i) = rest.find("${") {
        out.push_str(&rest[..i]);
        let after = &rest[i + 2..];
        let end = after.find('}').unwrap_or(after.len());
        out.push_str(sample(&after[..end]));
        rest = after.get(end + 1..).unwrap_or("");
    }
    out.push_str(rest);
    out
}

/// Het letterlijke stuk JavaScript (tussen `'` of een backtick) rond `at`,
/// op dezelfde regel; `None` als `at` niet in een letterlijke string staat.
fn literal_around(src: &str, at: usize) -> Option<(usize, usize)> {
    let line_start = src[..at].rfind('\n').map_or(0, |i| i + 1);
    let open = src[line_start..at].rfind(['\'', '`'])? + line_start;
    let quote = src[open..].chars().next()?;
    let close = src[at..].find(quote)? + at;
    if src[at..close].contains('\n') {
        return None;
    }
    Some((open + 1, close))
}

/// De methode van de aanroep na een letterlijke string: `method: 'X'` vóór
/// het einde van het statement, anders GET.
fn method_after(src: &str, end: usize) -> Method {
    let stmt = &src[end..];
    let stmt = &stmt[..stmt.find(';').unwrap_or(stmt.len())];
    match stmt.find("method: '") {
        Some(i) => {
            let m = &stmt[i + "method: '".len()..];
            Method::parse(&m[..m.find('\'').unwrap_or(0)])
        }
        None => Method::Get,
    }
}

/// Alle aanroepen van `app.js`: elk letterlijk pad met `/v1/` of `/leader`.
fn calls(src: &str) -> StdVec<Call> {
    let mut out: StdVec<Call> = StdVec::new();
    for needle in ["/v1/", "'/leader'"] {
        for (at, _) in src.match_indices(needle) {
            let at = if needle.starts_with('\'') { at + 1 } else { at };
            let Some((start, end)) = literal_around(src, at) else {
                continue;
            };
            let lit = &src[start..end];
            let from = lit.find("/v1/").or_else(|| lit.find("/leader"));
            let Some(from) = from else { continue };
            let call = Call {
                method: method_after(src, end + 1),
                path: fill(&lit[from..]),
            };
            if !out.contains(&call) {
                out.push(call);
            }
        }
    }
    out
}

/// De velden die `app.js` leest als `<naam>.<veld>` (niet `x.<naam>.`).
fn fields(src: &str, name: &str) -> StdVec<StdString> {
    let pat = alloc::format!("{name}.");
    let mut out = StdVec::new();
    for (at, _) in src.match_indices(&pat) {
        let before = src[..at].chars().next_back().unwrap_or(' ');
        if before.is_ascii_alphanumeric() || before == '_' || before == '.' {
            continue;
        }
        let rest = &src[at + pat.len()..];
        let f: StdString = rest
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect();
        if !f.is_empty() && !out.contains(&f) {
            out.push(f);
        }
    }
    out
}

fn body_for(c: &Call) -> &'static str {
    match c.method {
        Method::Post => r#"{"name":"web","command":"sleep 1"}"#,
        Method::Patch => r#"{"priority":0}"#,
        _ => "",
    }
}

/// Een verzoek zoals de browser het stuurt: ondertekend met Web Crypto
/// (`X-Hop-Auth`), met een `Origin` (een cross-origin fetch).
fn browser(c: &Call) -> Request {
    let body = body_for(c);
    let mut req = Request::new(c.method, &c.path, body.as_bytes());
    let sig = auth::sign(KEY, c.method.as_str(), &req.path, body.as_bytes());
    req.headers.push((
        auth::AUTH_HEADER.to_string(),
        core::str::from_utf8(&sig).unwrap().to_string(),
    ));
    req.headers
        .push(("Origin".to_string(), "http://localhost:3000".to_string()));
    if c.method == Method::Post || c.method == Method::Patch {
        req.headers
            .push(("Content-Type".to_string(), "application/json".to_string()));
    }
    req
}

fn node_agent() -> Agent {
    let mut attrs = alloc::collections::BTreeMap::new();
    attrs.insert("node.os".to_string(), "hopos".to_string());
    let mut a = Agent::new(Settings {
        id: "a1".into(),
        endpoint: "http://10.0.0.1:8080".into(),
        attributes: attrs,
        cpu_cores: 4,
        memory_bytes: 1 << 30,
        ..Settings::default()
    });
    a.set_leader_addr("10.0.0.1:9080");
    a
}

/// Een leader met agent `a1` en job `web` daarop.
#[derive(Default)]
struct OneJob {
    jobs: StdVec<types::Job>,
}

fn agent_a1() -> types::Agent {
    types::Agent {
        id: "a1".into(),
        endpoint: "http://10.0.0.1:8080".into(),
        version: "3.0.0".into(),
        ..types::Agent::default()
    }
}

impl OneJob {
    fn new() -> Self {
        Self {
            jobs: alloc::vec![types::Job {
                name: "web".into(),
                command: "sleep 1".into(),
                priority: Some(0),
                ..types::Job::default()
            }],
        }
    }
}

impl Cluster for OneJob {
    fn agents(&self) -> alloc::vec::Vec<types::Agent> {
        alloc::vec![agent_a1()]
    }
    fn register_agent(&mut self, _: u64, _: &str, _: &str, _: &str, _: &[(String, i64)]) -> bool {
        true
    }
    fn heartbeat(&mut self, _: u64, _: &str, _: &str, _: types::Telemetry) -> bool {
        true
    }
    fn unregister_agent(&mut self, _: &str) {}
    fn jobs(&self) -> alloc::vec::Vec<types::Job> {
        self.jobs.clone()
    }
    fn has_job(&self, name: &str) -> bool {
        self.jobs.iter().any(|j| j.name == name)
    }
    fn update_job(&mut self, _: u64, _: types::Job) -> core::result::Result<(), ClusterError> {
        Ok(())
    }
    fn dispatch_job(&mut self, _: u64, job: types::Job) -> core::result::Result<(), String> {
        self.jobs.push(job);
        Ok(())
    }
    fn next_priority(&self) -> i64 {
        1
    }
    fn patch_priority(&mut self, _: u64, name: &str, p: i64) -> bool {
        self.jobs
            .iter_mut()
            .find(|j| j.name == name)
            .map(|j| j.priority = Some(p))
            .is_some()
    }
    fn delete_job(&mut self, _: u64, name: &str) {
        self.jobs.retain(|j| j.name != name);
    }
    fn placed_counts(&self) -> alloc::vec::Vec<(String, i64)> {
        alloc::vec![("web".into(), 1)]
    }
    fn placed_agents(&self, name: &str) -> alloc::vec::Vec<types::Agent> {
        if self.has_job(name) {
            alloc::vec![agent_a1()]
        } else {
            alloc::vec::Vec::new()
        }
    }
    fn is_settled(&self) -> bool {
        true
    }
    fn state_time(&self) -> types::Time {
        types::Time(T0)
    }
    fn mark_unplaced(&mut self, _: &str, _: &str) {}
    fn notify(&mut self, _: &str) {}
}

fn is_stream_route(path: &str) -> bool {
    path == "/v1/events" || path.contains("/logs/")
}

/// De aanroepen van het dashboard, of `None` (luid) zonder dashboard.
fn dashboard() -> Option<(StdString, StdVec<Call>)> {
    let Some(src) = app_js() else {
        std::eprintln!(
            "hop-gui/app.js not found next to this repo (set HOP_GUI_DIR): the dashboard routes were NOT checked"
        );
        return None;
    };
    let list = calls(&src);
    let names: StdVec<StdString> = list
        .iter()
        .map(|c| alloc::format!("{} {}", c.method.as_str(), c.path))
        .collect();
    std::eprintln!("app.js calls {} routes: {}", list.len(), names.join(", "));
    Some((src, list))
}

#[test]
fn the_route_list_comes_out_of_app_js() {
    let src = "this.fetchAPI('/v1/status'),\n\
        const url = `${this.getEndpoint()}/v1/agents/${agentId}/capacity`;\n\
        await this.fetchAPI(`/v1/jobs/${moved.name}/priority`, {\n method: 'PATCH', body: x });\n\
        // a comment about /v1/nothing\n\
        this.fetchAPI('/leader')";
    let got: StdVec<(Method, StdString)> =
        calls(src).into_iter().map(|c| (c.method, c.path)).collect();
    assert_eq!(
        got,
        [
            (Method::Get, "/v1/status".into()),
            (Method::Get, "/v1/agents/a1/capacity".into()),
            (Method::Patch, "/v1/jobs/web/priority".into()),
            (Method::Get, "/leader".into()),
        ]
    );
}

#[test]
fn every_dashboard_route_exists_on_the_agent_with_cors() {
    let Some((_, list)) = dashboard() else { return };
    // Wat het dashboard in september 2026 deed; minder is een kapotte grep.
    assert!(
        list.len() >= 10,
        "only {} routes found: {list:?}",
        list.len()
    );
    let api = NodeApi::new(KEY, false);
    for c in &list {
        let mut a = node_agent();
        let req = browser(c);
        let (r, e) = api.handle(&mut a, T0, None, &req);
        let what = alloc::format!("{} {}", c.method.as_str(), c.path);
        assert!(
            !matches!(r.status, 401 | 404 | 405),
            "{what}: {} {:?}",
            r.status,
            core::str::from_utf8(&r.body)
        );
        assert_eq!(r.header("Access-Control-Allow-Origin"), Some("*"), "{what}");
        if c.path.starts_with("/v1/") {
            assert_eq!(
                e,
                Effect::Proxy {
                    leader: "10.0.0.1:9080".into(),
                    stream: is_stream_route(&c.path),
                },
                "{what}"
            );
        }
        // De preflight: geen handtekening, wel de methode en de koppen.
        let mut pre = Request::new(Method::Options, &c.path, b"");
        pre.headers.push((
            "Access-Control-Request-Method".to_string(),
            c.method.as_str().to_string(),
        ));
        let (p, pe) = api.handle(&mut a, T0, None, &pre);
        assert_eq!((p.status, pe), (200, Effect::None), "preflight {what}");
        let allowed = p.header("Access-Control-Allow-Methods").unwrap_or("");
        assert!(
            allowed.contains(c.method.as_str()),
            "preflight {what}: {allowed}"
        );
        let headers = p.header("Access-Control-Allow-Headers").unwrap_or("");
        assert!(
            headers.contains("X-Hop-Auth"),
            "preflight {what}: {headers}"
        );
    }
}

#[test]
fn every_dashboard_route_exists_on_the_leader() {
    let Some((_, list)) = dashboard() else { return };
    let api = LeaderApi::new(KEY, "gui");
    // `/leader` is een agent-route: de agent weet wie leidt.
    for c in list.iter().filter(|c| c.path.starts_with("/v1/")) {
        let mut cluster = OneJob::new();
        let (r, e) = api.handle(&mut cluster, T0, &browser(c));
        let what = alloc::format!("{} {}", c.method.as_str(), c.path);
        assert!(
            r.status < 400,
            "{what}: {} {:?}",
            r.status,
            core::str::from_utf8(&r.body)
        );
        match (&e, c.path.as_str()) {
            (LeaderEffect::Events, "/v1/events") => {}
            (
                LeaderEffect::Agent {
                    stream: true, path, ..
                },
                p,
            ) if p.contains("/logs/") => {
                // Het dashboard vraagt zonder query en verwacht een levende tail.
                assert!(path.ends_with("follow=1"), "{what}: {path}");
            }
            (
                LeaderEffect::Agent {
                    stream: false,
                    path,
                    ..
                },
                p,
            ) if p.ends_with("/capacity") => {
                assert_eq!(path, "/capacity", "{what}");
            }
            (LeaderEffect::Tasks { scope, .. }, p) if p.ends_with("/status") => {
                assert!(matches!(scope, TasksScope::Job { .. }), "{what}");
            }
            (LeaderEffect::None, _) => {}
            _ => panic!("{what}: unexpected effect {e:?}"),
        }
    }
}

#[test]
fn the_fields_app_js_reads_are_in_the_answers() {
    let Some((src, _)) = dashboard() else { return };
    // /v1/status: status.agents, status.total_placed, ...
    let api = LeaderApi::new(KEY, "gui");
    let c = Call {
        method: Method::Get,
        path: "/v1/status".into(),
    };
    let (r, _) = api.handle(&mut OneJob::new(), T0, &browser(&c));
    let v = r.json_body().unwrap();
    let want = fields(&src, "status");
    assert!(!want.is_empty(), "no status.<field> in app.js");
    for f in &want {
        assert!(
            v.as_object().unwrap().get(f).is_some(),
            "/v1/status lacks {f}"
        );
    }
    // De capaciteit: cap.cpu_used_shares, cap.memory_bytes, ...
    let mut a = node_agent();
    let c = Call {
        method: Method::Get,
        path: "/capacity".into(),
    };
    let (r, _) = NodeApi::new(KEY, false).handle(&mut a, T0, None, &browser(&c));
    let v = r.json_body().unwrap();
    let want = fields(&src, "cap");
    assert!(!want.is_empty(), "no cap.<field> in app.js");
    for f in &want {
        assert!(
            v.as_object().unwrap().get(f).is_some(),
            "/capacity lacks {f}"
        );
    }
    // De jobstatus: js?.tasks_by_agent, per agent een lijst taken.
    assert!(
        src.contains("js?.tasks_by_agent"),
        "app.js no longer reads tasks_by_agent"
    );
    let t = types::Task {
        id: "t1".into(),
        job_name: "web".into(),
        ..types::Task::default()
    };
    let r = job_status_reply("web", &[agent_a1()], &[("a1".into(), Some(alloc::vec![t]))]);
    let v = r.json_body().unwrap();
    let by = v.as_object().unwrap().get("tasks_by_agent").unwrap();
    assert_eq!(
        by.as_object()
            .unwrap()
            .get("a1")
            .unwrap()
            .as_array()
            .unwrap()
            .len(),
        1
    );
}
