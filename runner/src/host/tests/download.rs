//! Downloads: HTTP(S) tegen een server-thread, S3-adressering, en de vorm van een kale download (Go: `download_test.go`).

use std::fs;
use std::os::unix::fs::PermissionsExt;

use types::{Artifact, Map};

use super::super::download::{base64, download_artifact, download_http, s3_client};
use super::super::{HostError, prepare};
use super::{TempDir, config, gzip, ok, one_shot, spec, tar, zip};

fn artifact(url: &str) -> Artifact {
    Artifact {
        url: url.to_string(),
        ..Artifact::default()
    }
}

fn smap(pairs: &[(&str, &str)]) -> Map<String> {
    let mut m = Map::new();
    for (k, v) in pairs {
        m.insert(k.to_string(), v.to_string()).unwrap();
    }
    m
}

// Go: TestDownloadArtifactNil (in Rust: een spec zonder artifact)
#[test]
fn download_artifact_nil() {
    let base = TempDir::new("nil");
    let p = prepare(
        &config(base.path(), false),
        types::Driver::Exec,
        &spec("t-nil", "true"),
        &mut |_, _| {},
    )
    .unwrap();
    assert!(p.task_dir.join("tmp").is_dir());
}

// Go: TestDownloadArtifactEmptyURL
#[test]
fn download_artifact_empty_url() {
    let dir = TempDir::new("empty");
    download_artifact(&artifact(""), dir.path(), &mut |_, _| {}).unwrap();
}

// Go: TestDownloadArtifactUnsupportedScheme
#[test]
fn download_artifact_unsupported_scheme() {
    let dir = TempDir::new("ftp");
    let err = download_artifact(
        &artifact("ftp://example.com/file.tar.gz"),
        dir.path(),
        &mut |_, _| {},
    )
    .unwrap_err();
    assert!(err.to_string().contains("unsupported URL scheme"), "{err}");
}

// Go: TestDownloadHTTPSuccess
#[test]
fn download_http_success() {
    let (addr, _h) = one_shot(ok(b"binary content here"));
    let mut out = Vec::new();
    download_http(
        &artifact(&format!("http://{addr}/app.bin")),
        &mut out,
        &mut |_, _| {},
    )
    .unwrap();
    assert_eq!(out, b"binary content here");
}

// Go: TestDownloadHTTPBasicAuth
#[test]
fn download_http_basic_auth() {
    let (addr, h) = one_shot(ok(b"ok"));
    let mut a = artifact(&format!("http://{addr}/app.bin"));
    a.auth = smap(&[("username", "deploy"), ("password", "secret123")]);
    download_http(&a, &mut Vec::new(), &mut |_, _| {}).unwrap();
    let head = h.join().unwrap();
    let want = format!("Authorization: Basic {}\r\n", base64(b"deploy:secret123"));
    assert!(head.contains(&want), "{head}");
}

// Go: TestDownloadHTTPCustomHeaders
#[test]
fn download_http_custom_headers() {
    let (addr, h) = one_shot(ok(b"ok"));
    let mut a = artifact(&format!("http://{addr}/app.bin"));
    a.headers = smap(&[
        ("Authorization", "Bearer token123"),
        ("X-API-Key", "secret-key"),
    ]);
    download_http(&a, &mut Vec::new(), &mut |_, _| {}).unwrap();
    let head = h.join().unwrap();
    assert!(
        head.contains("Authorization: Bearer token123\r\n"),
        "{head}"
    );
    assert!(head.contains("X-API-Key: secret-key\r\n"), "{head}");
}

// Go: TestDownloadHTTP404
#[test]
fn download_http_404() {
    let (addr, _h) = one_shot(
        b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec(),
    );
    let err = download_http(
        &artifact(&format!("http://{addr}/missing.bin")),
        &mut Vec::new(),
        &mut |_, _| {},
    )
    .unwrap_err();
    assert!(err.to_string().contains("404"), "{err}");
}

// Go: TestDownloadHTTPWithExtractTarGz
#[test]
fn download_http_with_extract_tar_gz() {
    let body = gzip(&tar(&[
        ("app", b"binary content"),
        ("config.yml", b"port: 8080"),
    ]));
    let (addr, _h) = one_shot(ok(&body));
    let dir = TempDir::new("dl-tgz");
    let mut a = artifact(&format!("http://{addr}/app.tar.gz"));
    a.extract = "tar.gz".to_string();
    let mut seen = 0;
    download_artifact(&a, dir.path(), &mut |got, _| seen = got).unwrap();
    assert_eq!(fs::read(dir.path().join("app")).unwrap(), b"binary content");
    assert_eq!(seen, body.len() as u64);
    // Het tijdelijke archief is weg.
    assert!(!dir.path().join(".download.tar.gz").exists());
}

// Go: TestDownloadHTTPWithExtractZip
#[test]
fn download_http_with_extract_zip() {
    let body = zip(
        &[("app", b"binary content"), ("config.yml", b"port: 8080")],
        true,
    );
    let (addr, _h) = one_shot(ok(&body));
    let dir = TempDir::new("dl-zip");
    let mut a = artifact(&format!("http://{addr}/app.zip"));
    a.extract = "zip".to_string();
    download_artifact(&a, dir.path(), &mut |_, _| {}).unwrap();
    assert_eq!(fs::read(dir.path().join("app")).unwrap(), b"binary content");
}

// Go: TestDownloadHTTPRawBinary
#[test]
fn download_http_raw_binary() {
    let (addr, _h) = one_shot(ok(b"#!/bin/sh\necho hello"));
    let dir = TempDir::new("dl-raw");
    download_artifact(
        &artifact(&format!("http://{addr}/myapp")),
        dir.path(),
        &mut |_, _| {},
    )
    .unwrap();
    let path = dir.path().join("myapp");
    assert_eq!(fs::read(&path).unwrap(), b"#!/bin/sh\necho hello");
    assert_ne!(fs::metadata(&path).unwrap().permissions().mode() & 0o111, 0);
}

#[test]
fn download_raw_uses_filename_and_refuses_escaping_names() {
    let (addr, _h) = one_shot(ok(b"x"));
    let dir = TempDir::new("dl-name");
    let mut a = artifact(&format!("http://{addr}/path/ignored?v=1"));
    a.filename = "renamed".to_string();
    download_artifact(&a, dir.path(), &mut |_, _| {}).unwrap();
    assert!(dir.path().join("renamed").is_file());

    a.filename = "../escape".to_string();
    let err = download_artifact(&a, dir.path(), &mut |_, _| {}).unwrap_err();
    assert!(matches!(err, HostError::Url(_)), "{err}");
}

// Go: TestDownloadArtifactUnsupportedExtract
#[test]
fn download_artifact_unsupported_extract() {
    let dir = TempDir::new("dl-rar");
    let mut a = artifact("http://127.0.0.1:9/app.rar");
    a.extract = "rar".to_string();
    let err = download_artifact(&a, dir.path(), &mut |_, _| {}).unwrap_err();
    assert!(
        err.to_string().contains("unsupported extract type"),
        "{err}"
    );
}

// Go: TestDownloadS3MissingCredentials
#[test]
fn download_s3_missing_credentials() {
    let dir = TempDir::new("s3");
    let err = download_artifact(&artifact("s3://mybucket/mykey"), dir.path(), &mut |_, _| {})
        .unwrap_err();
    assert!(err.to_string().contains("access_key"), "{err}");
}

// Go: TestSignS3GetRequest. De SigV4-handtekening zelf is naar leans3
// verhuisd (en daar getest tegen de AWS-voorbeelden); hier de adressering:
// dezelfde host en hetzelfde pad als Go ze tekende.
#[test]
fn sign_s3_get_request() {
    let mut a = artifact("s3://mybucket.s3.us-east-1.amazonaws.com/path/to/file.tar.gz");
    a.auth = smap(&[("access_key", "AKIAEXAMPLE"), ("secret_key", "secretkey")]);
    let (client, key) = s3_client(&a).unwrap();
    let url = client.url_for(&key).unwrap();
    assert!(url.is_https());
    assert_eq!(url.host(), "mybucket.s3.us-east-1.amazonaws.com");
    assert_eq!(url.path(), "/path/to/file.tar.gz");
    assert_eq!(client.access_key_id, "AKIAEXAMPLE");
    assert_eq!(client.region, "us-east-1");
}

// Go: TestSignS3GetRequestCustomEndpoint
#[test]
fn sign_s3_get_request_custom_endpoint() {
    let mut a = artifact("s3://haas-builds.fsn1.your-objectstorage.com/ravendb/file.tar.bz2");
    a.auth = smap(&[("access_key", "AKIAEXAMPLE"), ("secret_key", "secretkey")]);
    let (client, key) = s3_client(&a).unwrap();
    let url = client.url_for(&key).unwrap();
    assert_eq!(
        format!("{}{}", url.host(), url.path()),
        "haas-builds.fsn1.your-objectstorage.com/ravendb/file.tar.bz2"
    );

    // Met een expliciet endpoint en path-style (MinIO) is de host de bucket.
    a.url = "s3://builds/app.tgz".to_string();
    a.auth = smap(&[
        ("access_key", "k"),
        ("secret_key", "s"),
        ("endpoint", "http://minio:9000"),
        ("path_style", "true"),
    ]);
    let (client, key) = s3_client(&a).unwrap();
    let url = client.url_for(&key).unwrap();
    assert!(!url.is_https());
    assert_eq!((url.host(), url.path()), ("minio:9000", "/builds/app.tgz"));
}

// Go: TestSignS3GetRequestDefaultRegion (zonder netwerk: Go liet het
// verzoek echt vertrekken en verwachtte dat het faalde).
#[test]
fn sign_s3_get_request_default_region() {
    let mut a = artifact("s3://mybucket/mykey");
    a.auth = smap(&[("access_key", "AKIAEXAMPLE"), ("secret_key", "secretkey")]);
    let (client, key) = s3_client(&a).unwrap();
    assert_eq!(client.region, "us-east-1");
    assert_eq!(key, "mykey");
    assert_eq!(
        client.url_for(&key).unwrap().host(),
        "mybucket.s3.us-east-1.amazonaws.com"
    );
}

#[test]
fn base64_matches_rfc4648() {
    for (i, o) in [
        ("", ""),
        ("f", "Zg=="),
        ("fo", "Zm8="),
        ("foo", "Zm9v"),
        ("foob", "Zm9vYg=="),
        ("fooba", "Zm9vYmE="),
        ("foobar", "Zm9vYmFy"),
    ] {
        assert_eq!(base64(i.as_bytes()), o);
    }
}
