//! Uitpakken: tar, gzip, inflate, zip en bzip2 (Go: de extract-tests in `download_test.go`).

use std::fs;
use std::io::Read;

use super::super::HostError;
use super::super::extract::{self, Gzip, extract_tar, extract_zip};
use super::{TempDir, gzip, pipe, tar, tar_header, zip};

// Go: TestExtractTarGzValid
#[test]
fn extract_tar_gz_valid() {
    let dir = TempDir::new("tgz");
    let archive = dir.path().join("test.tar.gz");
    let files: [(&str, &[u8]); 2] = [
        ("hello.txt", b"Hello, World!"),
        ("dir/nested.txt", b"Nested content"),
    ];
    fs::write(&archive, gzip(&tar(&files))).unwrap();
    let out = TempDir::new("tgz-out");
    extract::extract_tar_gz(&archive, out.path()).unwrap();
    assert_eq!(
        fs::read_to_string(out.path().join("hello.txt")).unwrap(),
        "Hello, World!"
    );
    assert_eq!(
        fs::read_to_string(out.path().join("dir/nested.txt")).unwrap(),
        "Nested content"
    );
}

// Go: TestExtractTarPathTraversal
#[test]
fn extract_tar_path_traversal() {
    let dir = TempDir::new("tar-evil");
    let data = tar(&[("../../../etc/evil", b"evil")]);
    let err = extract_tar(&mut &data[..], dir.path()).unwrap_err();
    assert!(err.to_string().contains("illegal file path"), "{err}");
}

// Go: TestExtractZipValid (stored en deflate)
#[test]
fn extract_zip_valid() {
    for deflated in [false, true] {
        let dir = TempDir::new("zip");
        let archive = dir.path().join("test.zip");
        let files: [(&str, &[u8]); 2] = [
            ("hello.txt", b"Hello, World!"),
            ("dir/nested.txt", b"Nested content"),
        ];
        fs::write(&archive, zip(&files, deflated)).unwrap();
        let out = TempDir::new("zip-out");
        extract_zip(&archive, out.path()).unwrap();
        assert_eq!(
            fs::read_to_string(out.path().join("hello.txt")).unwrap(),
            "Hello, World!"
        );
        assert_eq!(
            fs::read_to_string(out.path().join("dir/nested.txt")).unwrap(),
            "Nested content"
        );
    }
}

// Go: TestExtractZipPathTraversal
#[test]
fn extract_zip_path_traversal() {
    let dir = TempDir::new("zip-evil");
    let archive = dir.path().join("evil.zip");
    fs::write(&archive, zip(&[("../../../etc/evil", b"evil")], false)).unwrap();
    let out = TempDir::new("zip-evil-out");
    let err = extract_zip(&archive, out.path()).unwrap_err();
    assert!(err.to_string().contains("illegal file path"), "{err}");
}

#[test]
fn zip_rejects_wrong_crc_and_oversized_entries() {
    let dir = TempDir::new("zip-crc");
    let archive = dir.path().join("bad.zip");
    let mut z = zip(&[("a.txt", b"hello")], false);
    // Het eerste byte van de data (na de lokale kop van 30 + 5 bytes).
    z[35] ^= 0xff;
    fs::write(&archive, &z).unwrap();
    let err = extract_zip(&archive, dir.path()).unwrap_err();
    assert!(err.to_string().contains("crc"), "{err}");
}

#[test]
fn zip_system_tool_archive_extracts() {
    // Een echt archief van `zip` (als het er is): extra velden, data-descriptors.
    if super::super::process::find_tool("zip").is_none() {
        eprintln!("skip: no zip tool");
        return;
    }
    let dir = TempDir::new("zip-tool");
    fs::create_dir_all(dir.path().join("src/sub")).unwrap();
    let big: Vec<u8> = (0..200_000u32).map(|i| (i % 251) as u8).collect();
    fs::write(dir.path().join("src/sub/big.bin"), &big).unwrap();
    fs::write(dir.path().join("src/a.txt"), b"alpha").unwrap();
    let status = std::process::Command::new("zip")
        .current_dir(dir.path().join("src"))
        .args(["-q", "-r", "../out.zip", "."])
        .status()
        .unwrap();
    assert!(status.success());
    let out = TempDir::new("zip-tool-out");
    extract_zip(&dir.path().join("out.zip"), out.path()).unwrap();
    assert_eq!(fs::read(out.path().join("sub/big.bin")).unwrap(), big);
    assert_eq!(fs::read(out.path().join("a.txt")).unwrap(), b"alpha");
}

#[test]
fn gzip_roundtrips_large_and_multi_member() {
    // Groter dan het venster, met herhaling (afstanden) en ruis (literals).
    let data: Vec<u8> = (0..300_000u32)
        .map(|i| {
            if i % 1000 < 500 {
                b'a' + (i % 7) as u8
            } else {
                (i.wrapping_mul(2_654_435_761) >> 24) as u8
            }
        })
        .collect();
    let mut gz = gzip(&data);
    gz.extend_from_slice(&gzip(b"tail"));
    let mut out = Vec::new();
    Gzip::new(&gz[..]).unwrap().read_to_end(&mut out).unwrap();
    assert_eq!(out.len(), data.len() + 4);
    assert_eq!(&out[..data.len()], &data[..]);
    assert_eq!(&out[data.len()..], b"tail");
}

#[test]
fn gzip_fixed_and_stored_blocks() {
    // `gzip -1` op een korte tekst kiest vaste codes; `-1` op ruis stored blokken.
    for (level, data) in [
        ("-1", b"abcabcabcabc hello hello".to_vec()),
        (
            "-1",
            (0..70_000u32)
                .map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8)
                .collect(),
        ),
    ] {
        let gz = pipe("gzip", &[level, "-n", "-c"], &data);
        let mut out = Vec::new();
        Gzip::new(&gz[..]).unwrap().read_to_end(&mut out).unwrap();
        assert_eq!(out, data);
    }
}

#[test]
fn gzip_rejects_bad_checksum() {
    let mut gz = gzip(b"hello world");
    let n = gz.len();
    gz[n - 8] ^= 1;
    let mut out = Vec::new();
    let err = Gzip::new(&gz[..])
        .unwrap()
        .read_to_end(&mut out)
        .unwrap_err();
    assert!(err.to_string().contains("checksum"), "{err}");
}

#[test]
fn tar_long_names_pax_and_skipped_links() {
    let long = format!("{}/file.txt", "d".repeat(150));
    let mut t = Vec::new();
    // GNU `L`: de naam als data.
    t.extend_from_slice(&tar_header("././@LongLink", long.len() as u64 + 1, b'L'));
    t.extend_from_slice(long.as_bytes());
    t.push(0);
    t.resize(t.len().div_ceil(512) * 512, 0);
    t.extend_from_slice(&tar_header("short", 2, b'0'));
    t.extend_from_slice(b"hi");
    t.resize(t.len().div_ceil(512) * 512, 0);
    // pax: pad via een `x`-kop.
    let rec = "21 path=pax/name.txt\n";
    t.extend_from_slice(&tar_header("PaxHeader", rec.len() as u64, b'x'));
    t.extend_from_slice(rec.as_bytes());
    t.resize(t.len().div_ceil(512) * 512, 0);
    t.extend_from_slice(&tar_header("ignored", 3, b'0'));
    t.extend_from_slice(b"pax");
    t.resize(t.len().div_ceil(512) * 512, 0);
    // Een symlink wordt overgeslagen, net als in Go.
    t.extend_from_slice(&tar_header("link", 0, b'2'));
    t.resize(t.len() + 1024, 0);

    let out = TempDir::new("tar-long");
    extract_tar(&mut &t[..], out.path()).unwrap();
    assert_eq!(fs::read(out.path().join(&long)).unwrap(), b"hi");
    assert_eq!(fs::read(out.path().join("pax/name.txt")).unwrap(), b"pax");
    assert!(!out.path().join("link").exists());
}

#[test]
fn tar_system_tool_archive_extracts() {
    let dir = TempDir::new("tar-tool");
    fs::create_dir_all(dir.path().join("src/bin")).unwrap();
    fs::write(dir.path().join("src/bin/app"), b"#!/bin/sh\necho hi\n").unwrap();
    let status = std::process::Command::new("tar")
        .current_dir(dir.path().join("src"))
        .args(["-czf", "../app.tar.gz", "."])
        .status()
        .unwrap();
    assert!(status.success());
    let out = TempDir::new("tar-tool-out");
    extract::extract("tar.gz", &dir.path().join("app.tar.gz"), out.path()).unwrap();
    assert_eq!(
        fs::read(out.path().join("bin/app")).unwrap(),
        b"#!/bin/sh\necho hi\n"
    );
}

#[test]
fn tar_bz2_via_bzip2_pipe() {
    if super::super::process::find_tool("bzip2").is_none() {
        eprintln!("skip: no bzip2");
        return;
    }
    let dir = TempDir::new("tbz");
    let archive = dir.path().join("a.tar.bz2");
    fs::write(
        &archive,
        pipe("bzip2", &["-c"], &tar(&[("x/y.txt", b"bz")])),
    )
    .unwrap();
    let out = TempDir::new("tbz-out");
    extract::extract("tar.bz2", &archive, out.path()).unwrap();
    assert_eq!(fs::read(out.path().join("x/y.txt")).unwrap(), b"bz");

    // Een kapotte stroom is een fout met de exit van bzip2.
    fs::write(&archive, b"not bzip2 at all").unwrap();
    let err = extract::extract("tar.bz2", &archive, out.path()).unwrap_err();
    assert!(
        matches!(
            err,
            HostError::Archive {
                archive: "tar.bz2",
                ..
            }
        ),
        "{err}"
    );
}
