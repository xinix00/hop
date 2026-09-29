//! De tests van `OLD/cmd/cli/main_test.go`, naam voor naam, plus de
//! vlaggen, de tabel en de body van `apply`.

use crate::jobspec::{ApplyFlags, build_job, parse_kv, parse_memory, parse_pairs};
use crate::table::Table;
use crate::{apply_body, fmt_temp, globals, parse_apply};

fn s(v: &[&str]) -> Vec<String> {
    v.iter().map(|x| x.to_string()).collect()
}

// TestContract_ApplyCount: `hop apply --count` bereikt Job.count, ook -1.
#[test]
fn contract_apply_count() {
    let mut f = ApplyFlags::new();
    f.name = "web".into();
    f.command = "./app".into();
    f.count = 3;
    assert_eq!(build_job(&f).unwrap().count, 3);
    f.name = "dns".into();
    f.count = -1;
    assert_eq!(build_job(&f).unwrap().count, -1);
    // En via de echte vlaggen.
    let (f, file) =
        parse_apply(&s(&["--name", "web", "--command", "./app", "--count", "3"])).unwrap();
    assert!(file.is_none());
    assert_eq!(build_job(&f).unwrap().count, 3);
    let (f, _) = parse_apply(&s(&["--name=dns", "--command=./dns", "--count=-1"])).unwrap();
    assert_eq!(build_job(&f).unwrap().count, -1);
}

// TestParsePairsHoudtKommasEnIsgelijktekens.
#[test]
fn parse_pairs_houdt_kommas_en_isgelijktekens() {
    let m = parse_pairs(&s(&[
        "GREETING=hallo, wereld",
        "EXPR=a=b=c",
        "zonder-is-teken",
    ]));
    assert_eq!(m.get("GREETING").unwrap(), "hallo, wereld");
    assert_eq!(m.get("EXPR").unwrap(), "a=b=c");
    assert_eq!(m.len(), 2);
}

// TestStringListVerzameltHerhaaldeFlags: herhaalde vlaggen verzamelen.
#[test]
fn string_list_verzamelt_herhaalde_flags() {
    let (f, _) = parse_apply(&s(&[
        "--name",
        "a",
        "--command",
        "x",
        "--env",
        "a=1",
        "--env",
        "b=2",
    ]))
    .unwrap();
    assert_eq!(f.env, s(&["a=1", "b=2"]));
    let job = build_job(&f).unwrap();
    assert_eq!(job.env.get("a").unwrap(), "1");
    assert_eq!(job.env.get("b").unwrap(), "2");
}

#[test]
fn build_job_artifacts_affinity_check_and_memory() {
    let (f, _) = parse_apply(&s(&[
        "--name",
        "app",
        "--driver",
        "hop",
        "--artifact",
        "node.arch=arm64::https://x/app-arm64",
        "--artifact",
        "https://x/app",
        "--affinity",
        "node.os=linux",
        "--tag",
        "env=prod,team=a",
        "--memory",
        "512M",
        "--priority",
        "0",
        "--update-policy",
        "blue-green",
        "--check-type",
        "tcp",
        "--check-port",
        "http",
        "--check-failures",
        "5",
    ]))
    .unwrap();
    let job = build_job(&f).unwrap();
    assert_eq!(job.driver, Some(types::Driver::Hop));
    assert_eq!(job.artifacts.len(), 2);
    assert_eq!(job.artifacts[0].url, "https://x/app-arm64");
    assert_eq!(job.artifacts[0].matches.get("node.arch").unwrap(), "arm64");
    assert!(job.artifacts[1].matches.is_empty());
    assert_eq!(job.affinity.get("node.os").unwrap(), "linux");
    assert_eq!(job.tags.len(), 2);
    assert_eq!(job.memory_limit, 512 << 20);
    assert_eq!(job.priority, Some(0));
    assert_eq!(job.update_policy, Some(types::UpdatePolicy::BlueGreen));
    let hc = job.health_check.unwrap();
    assert_eq!(hc.kind, Some(types::CheckType::Tcp));
    assert_eq!(hc.failure_threshold, 5);
    assert!(
        build_job(&ApplyFlags {
            driver: "vm".into(),
            ..ApplyFlags::new()
        })
        .is_err()
    );
}

#[test]
fn parse_memory_units() {
    assert_eq!(parse_memory("64k").unwrap(), 64 << 10);
    assert_eq!(parse_memory("1G").unwrap(), 1 << 30);
    assert_eq!(parse_memory("1000").unwrap(), 1000);
    assert!(parse_memory("lots").is_err());
    assert!(parse_memory("99999999999G").is_err());
}

#[test]
fn parse_kv_drops_pairs_without_equals() {
    let m = parse_kv("a=1,b,c=3");
    assert_eq!(m.len(), 2);
}

#[test]
fn apply_body_from_file_is_sent_unchanged() {
    let dir = std::env::temp_dir().join(format!("hop-cli-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let p = dir.join("job.json");
    let spec = br#"{"name":"sleeper","command":"sleep 30","future_field":1}"#;
    std::fs::write(&p, spec).unwrap();
    let (body, name) = apply_body(&s(&[p.to_str().unwrap()])).unwrap();
    assert_eq!(name, "sleeper");
    assert_eq!(body, spec);
    std::fs::write(&p, br#"{"command":"x"}"#).unwrap();
    assert!(apply_body(&s(&[p.to_str().unwrap()])).is_err());
    std::fs::remove_dir_all(&dir).unwrap();
    // Zonder bestand en zonder naam: de gebruiksregel.
    assert!(apply_body(&[]).unwrap_err().contains("--name is required"));
    assert!(
        apply_body(&s(&["--name", "a"]))
            .unwrap_err()
            .contains("--command")
    );
    assert!(
        apply_body(&s(&["--name", "a", "--driver", "hop"]))
            .unwrap_err()
            .contains("--artifact")
    );
}

#[test]
fn globals_anywhere_and_inline() {
    let (c, rest) = globals(s(&["jobs", "--leader", "10.0.0.1:9080", "--api-key=k"])).unwrap();
    assert_eq!(c.leader, "10.0.0.1:9080");
    assert_eq!(rest, s(&["jobs"]));
    assert!(globals(s(&["--agent"])).is_err());
}

#[test]
fn table_aligns_columns() {
    let mut t = Table::new(&["ID", "STATE"]);
    t.row(vec!["abc".into(), "running".into()]);
    t.row(vec!["a".into(), "failed".into()]);
    assert_eq!(t.render(), "ID   STATE\nabc  running\na    failed\n");
}

#[test]
fn temp_is_a_dash_without_sensor() {
    assert_eq!(fmt_temp(0), "-");
    assert_eq!(fmt_temp(41_500), "41.5\u{b0}C");
}
