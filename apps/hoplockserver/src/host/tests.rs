//! `store_test.go` tegen de bestanden-store, plus de schijfvorm en de vlaggen.

use std::string::{String, ToString};
use std::vec::Vec;
use std::{format, fs};

use super::*;
use crate::tests::{get, put};

/// Een verse map onder de temp-map van het OS.
pub(crate) fn temp_dir(name: &str) -> PathBuf {
    use core::sync::atomic::{AtomicU64, Ordering::Relaxed};
    static N: AtomicU64 = AtomicU64::new(0);
    let d = std::env::temp_dir().join(format!(
        "hoplockserver-{name}-{}-{}",
        std::process::id(),
        N.fetch_add(1, Relaxed)
    ));
    let _ = fs::remove_dir_all(&d);
    d
}

/// Go's `newStore`: een store op `<tmp>/data`, die de map zelf maakt.
fn new_store() -> FileStore {
    FileStore::open(temp_dir("store").join("data")).unwrap()
}

mod file_store {
    crate::tests::go_store_tests!(super::new_store());
}

// De schijfvorm is die van Go: een kaal bestand per sleutel, geen tijdelijke erbij.
#[test]
fn the_disk_layout_is_gos() {
    let mut s = new_store();
    put(
        &mut s,
        "leases/prod",
        b"{\"owner\":\"a\"}",
        &Condition::create(),
    )
    .unwrap();
    let path = s.dir().join("leases/prod");
    assert_eq!(fs::read(&path).unwrap(), b"{\"owner\":\"a\"}");
    let names: Vec<String> = fs::read_dir(s.dir().join("leases"))
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
        .collect();
    assert_eq!(names, ["prod"]);
    // 0600, zoals Go's CreateTemp: een lease is van de server.
    use std::os::unix::fs::PermissionsExt;
    assert_eq!(
        fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
}

// Een Go-datamap blijft werken: een bestand dat Go schreef, is een sleutel.
#[test]
fn a_go_data_dir_is_read_as_is() {
    let s = new_store();
    fs::create_dir_all(s.dir().join("leases")).unwrap();
    fs::write(s.dir().join("leases/old"), b"from go").unwrap();
    let mut s = s;
    let got = get(&mut s, "leases/old").unwrap();
    assert_eq!(got.etag, Etag::of(b"from go"));
    assert_eq!(s.count_keys().unwrap(), 1);
}

// Een tijdelijk bestand overschrijft nooit een sleutel met dezelfde naam.
#[test]
fn a_tmp_name_never_clobbers_a_key() {
    let mut s = new_store();
    let clash = format!("{TMP_PREFIX}{}-1", std::process::id());
    put(&mut s, &clash, b"keep me", &Condition::create()).unwrap();
    s.seq = 0;
    put(&mut s, "other", b"x", &Condition::create()).unwrap();
    assert_eq!(get(&mut s, &clash).unwrap().body, b"keep me");
}

// Een sleutel die een map is: een leesfout, geen aanmaak eroverheen (Go: 500).
#[test]
fn a_directory_is_not_a_key() {
    let mut s = new_store();
    put(&mut s, "a/b", b"x", &Condition::create()).unwrap();
    assert!(matches!(
        get(&mut s, "a"),
        Err(Error::Io { op: "read", .. })
    ));
    assert!(matches!(
        put(&mut s, "a", b"y", &Condition::create()),
        Err(Error::Io { op: "read", .. })
    ));
}

fn args(a: &[&str]) -> Vec<String> {
    a.iter().map(|s| s.to_string()).collect()
}

// Go's flag: -x v, -x=v, --x v; de standaarden van Go; HOPLOCK_API_KEY als terugval.
#[test]
fn flags_are_gos() {
    assert_eq!(parse_flags(&[], None).unwrap(), Flags::default());
    assert_eq!(Flags::default().listen, ":8090");
    assert_eq!(Flags::default().data, "./data");
    let f = parse_flags(
        &args(&["-listen", "127.0.0.1:9", "--data=/tmp/x", "-api-key=k"]),
        Some("env"),
    )
    .unwrap();
    assert_eq!(
        f,
        Flags {
            listen: String::from("127.0.0.1:9"),
            data: String::from("/tmp/x"),
            api_key: String::from("k"),
        }
    );
    assert_eq!(parse_flags(&[], Some("env")).unwrap().api_key, "env");
    assert_eq!(parse_flags(&args(&["-h"]), None), Err(FlagError::Help));
    assert_eq!(
        parse_flags(&args(&["-port", "1"]), None),
        Err(FlagError::Unknown(String::from("port")))
    );
    assert_eq!(
        parse_flags(&args(&["-data"]), None),
        Err(FlagError::Missing(String::from("data")))
    );
    assert_eq!(
        parse_flags(&args(&["serve"]), None),
        Err(FlagError::Stray(String::from("serve")))
    );
}

// Go's authLabel.
#[test]
fn auth_label_is_gos() {
    assert_eq!(auth_label(&Server::new("")), "off");
    assert_eq!(auth_label(&Server::new("abcd")), "on (4 chars)");
}

// `:0` is elke interface op een vrije poort, zoals Go's ":0".
#[test]
fn a_bare_port_binds_every_interface() {
    let l = bind(":0").unwrap();
    assert!(l.local_addr().unwrap().ip().is_unspecified());
    assert!(bind(":nope").is_err());
}
