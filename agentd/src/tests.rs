//! De tests van `OLD/cmd/agent/node_id_test.go` naam voor naam, plus de
//! vlaggen en de netwerkkeuze van de boot.

use std::net::Ipv4Addr;
use std::path::PathBuf;

use config::Config;

use crate::boot::{self, Flags, NODE_ID_LEN};

/// Een lege tijdelijke map per test.
fn temp_dir(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("agentd-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

// TestGetOrCreateNodeID_FromConfig.
#[test]
fn get_or_create_node_id_from_config() {
    let mut cfg = Config::default();
    cfg.node.id = String::from("my-custom-id");
    let (id, note) = boot::node_id(&cfg).unwrap();
    assert_eq!(id, "my-custom-id");
    assert!(note.is_none());
}

// TestGetOrCreateNodeID_Persistence.
#[test]
fn get_or_create_node_id_persistence() {
    let dir = temp_dir("persist");
    let mut cfg = Config::default();
    cfg.paths.state_file = dir.join("state.json").to_string_lossy().into_owned();
    let (id1, _) = boot::node_id(&cfg).unwrap();
    assert_eq!(id1.len(), NODE_ID_LEN);
    assert!(dir.join("node-id").exists());
    let (id2, note) = boot::node_id(&cfg).unwrap();
    assert_eq!(id1, id2);
    assert!(note.unwrap().contains("persisted"));
    std::fs::remove_dir_all(&dir).unwrap();
}

// TestGetOrCreateNodeID_ConfigOverridesPersisted.
#[test]
fn get_or_create_node_id_config_overrides_persisted() {
    let dir = temp_dir("override");
    let mut cfg = Config::default();
    cfg.paths.state_file = dir.join("state.json").to_string_lossy().into_owned();
    let (id1, _) = boot::node_id(&cfg).unwrap();
    cfg.node.id = String::from("override-id");
    let (id2, _) = boot::node_id(&cfg).unwrap();
    assert_eq!(id2, "override-id");
    assert_ne!(id1, id2);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn flags_override_config_and_pick_standalone() {
    let args: Vec<String> = ["--cluster", "prod", "-node=n1", "--api-key", "k"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    let f = boot::parse_flags(&args).unwrap();
    let mut cfg = Config::default();
    let standalone = boot::apply_flags(&mut cfg, &f).unwrap();
    // Geen lock-backend: standalone, en een staatbestand per cluster.
    assert!(standalone);
    assert_eq!(cfg.cluster.name, "prod");
    assert_eq!(cfg.node.id, "n1");
    assert_eq!(cfg.api_key, "k");
    assert_eq!(cfg.paths.state_file, "./data/state-prod.json");

    let f = Flags {
        lock: String::from("http://lock:8090"),
        ..Flags::default()
    };
    let mut cfg = Config::default();
    assert!(!boot::apply_flags(&mut cfg, &f).unwrap());
    assert_eq!(cfg.cluster.lock.kind, "hoplockserver");
    // --standalone wint van --lock.
    let f = Flags {
        lock: String::from("http://lock:8090"),
        standalone: true,
        ..Flags::default()
    };
    let mut cfg = Config::default();
    assert!(boot::apply_flags(&mut cfg, &f).unwrap());
    assert!(cfg.cluster.lock.url.is_empty());

    assert!(boot::parse_flags(&["--bogus".to_string()]).is_err());
    assert!(boot::parse_flags(&["--node".to_string()]).is_err());
    let mut cfg = Config::default();
    cfg.cluster.name.clear();
    assert!(boot::apply_flags(&mut cfg, &Flags::default()).is_err());
}

#[test]
fn missing_config_file_is_the_default() {
    let (cfg, found) = boot::load_config("/nonexistent/hop.json").unwrap();
    assert!(!found);
    assert_eq!(cfg, Config::default());
    let dir = temp_dir("cfg");
    let p = dir.join("hop.json");
    std::fs::write(&p, br#"{"node": {"port": 7070}, "cluster": {"name": "c"}}"#).unwrap();
    let (cfg, found) = boot::load_config(p.to_str().unwrap()).unwrap();
    assert!(found);
    assert_eq!(cfg.node.port, 7070);
    std::fs::write(&p, b"node:\n  port: 1\n").unwrap();
    assert!(boot::load_config(p.to_str().unwrap()).is_err());
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn cidr_contains() {
    let net = boot::parse_cidr("10.0.0.0/24").unwrap();
    assert!(boot::in_cidr(Ipv4Addr::new(10, 0, 0, 7), net));
    assert!(!boot::in_cidr(Ipv4Addr::new(10, 0, 1, 7), net));
    assert!(boot::in_cidr(
        Ipv4Addr::new(1, 2, 3, 4),
        boot::parse_cidr("0.0.0.0/0").unwrap()
    ));
    assert!(boot::parse_cidr("10.0.0.0/33").is_none());
    assert!(boot::parse_cidr("nope").is_none());
}

#[test]
fn host_facts_are_sane() {
    assert!(boot::cpu_cores() >= 1);
    assert!(boot::memory_bytes() > 0);
    assert!(matches!(boot::go_os(), "linux" | "darwin"));
}
