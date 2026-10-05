//! Het artifact van een taak ophalen: `http://`, `https://` en `s3://`, en daarna uitpakken.
//!
//! Bezit het tijdelijke downloadbestand zolang het bestaat; de taakmap is
//! van de aanroeper. Zoals Go (`download.go`): met `extract` gaat de download
//! naar `.download.<extract>` in de taakmap en wordt daar uitgepakt, zonder
//! `extract` wordt het bestand zelf de download (`filename` of de basename
//! van de URL) en uitvoerbaar gemaakt.

use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use hostnet::{Call, Http, S3Transport, block_on};
use types::Artifact;

use super::{HostError, Result, extract};

/// De termijn per fase van een download (Go: `downloadTimeout`, daar voor
/// het hele verzoek). Hier geldt hij per verbinden, kop en elke lees: een
/// grote download mag lang duren, een zwijgende server niet.
const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(5 * 60);

/// De regio als het artifact er geen noemt (Go: `us-east-1`).
const DEFAULT_REGION: &str = "us-east-1";

/// Het schema van een URL (`http`, `s3`, ...), of leeg zonder `://`.
fn scheme(url: &str) -> &str {
    url.split_once("://").map_or("", |(s, _)| s)
}

/// Toetst vóór er iets gebeurt of dit artifact ooit kan lukken: schema en extract.
pub(crate) fn check(a: &Artifact) -> Result {
    if a.url.is_empty() {
        return Ok(());
    }
    match scheme(&a.url) {
        "http" | "https" | "s3" => {}
        other => return Err(HostError::Scheme(other.to_string())),
    }
    if !a.extract.is_empty() && !extract::is_known(&a.extract) {
        return Err(HostError::ExtractType(a.extract.clone()));
    }
    Ok(())
}

/// De bestandsnaam van een kale download: `filename`, anders de basename van het URL-pad.
fn raw_name(a: &Artifact) -> Result<String> {
    let name = if a.filename.is_empty() {
        let rest = a.url.split_once("://").map_or("", |(_, r)| r);
        let path = rest.split_once('/').map_or("", |(_, p)| p);
        let path = path.split(['?', '#']).next().unwrap_or("");
        path.rsplit('/').next().unwrap_or("").to_string()
    } else {
        a.filename.clone()
    };
    // Een naam met een `/` of een puntnaam zou buiten de taakmap schrijven.
    if name.is_empty() || name == "." || name == ".." || name.contains('/') {
        return Err(HostError::Url(format!(
            "{}: no usable file name (set filename)",
            a.url
        )));
    }
    Ok(name)
}

/// Downloadt `a` in `dest` en pakt het uit (Go: `downloadArtifact`).
///
/// Een leeg artifact is geen fout: er is niets te doen.
pub(crate) fn download_artifact(
    a: &Artifact,
    dest: &Path,
    progress: &mut dyn FnMut(u64, Option<u64>),
) -> Result {
    if a.url.is_empty() {
        return Ok(());
    }
    check(a)?;
    let extracting = !a.extract.is_empty();
    let target = if extracting {
        dest.join(format!(".download.{}", a.extract))
    } else {
        dest.join(raw_name(a)?)
    };
    let fetched = fetch(a, &target, progress);
    if extracting {
        let res = fetched.and_then(|()| extract::extract(&a.extract, &target, dest));
        // Het archief gaat altijd weg, ook na een fout (Go: `defer os.Remove`).
        let _ = fs::remove_file(&target);
        return res;
    }
    fetched?;
    fs::set_permissions(&target, fs::Permissions::from_mode(0o755))
        .map_err(|e| HostError::io("chmod", &target, e))
}

/// Haalt de bytes van `a` naar `path`, volgens het schema.
fn fetch(a: &Artifact, path: &Path, progress: &mut dyn FnMut(u64, Option<u64>)) -> Result {
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .map_err(|e| HostError::io("create", path, e))?;
    match scheme(&a.url) {
        "s3" => download_s3(a, &mut file, path, progress),
        _ => download_http(a, &mut file, progress),
    }?;
    file.flush().map_err(|e| HostError::io("write", path, e))
}

/// Standaard base64 (RFC 4648, met opvulling), voor de Basic-kop.
pub(crate) fn base64(data: &[u8]) -> String {
    const ABC: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b = [
            chunk.first().copied().unwrap_or(0),
            chunk.get(1).copied().unwrap_or(0),
            chunk.get(2).copied().unwrap_or(0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        for i in 0..4 {
            if i <= chunk.len() {
                let idx = usize::try_from((n >> (18 - 6 * i)) & 63).unwrap_or(0);
                out.push(char::from(ABC.get(idx).copied().unwrap_or(b'=')));
            } else {
                out.push('=');
            }
        }
    }
    out
}

/// HTTP(S)-download (Go: `downloadHTTP`): de headers van het artifact, en
/// Basic-auth uit `username`/`password` als die er allebei zijn (die wint
/// van een eigen `Authorization`, zoals in Go).
pub(crate) fn download_http(
    a: &Artifact,
    sink: &mut dyn Write,
    progress: &mut dyn FnMut(u64, Option<u64>),
) -> Result {
    let mut headers: Vec<(String, String)> = a
        .headers
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect();
    let user = a.auth.get("username").map_or("", String::as_str);
    let pass = a.auth.get("password").map_or("", String::as_str);
    if !user.is_empty() && !pass.is_empty() {
        headers.retain(|(k, _)| !k.eq_ignore_ascii_case("authorization"));
        headers.push((
            "Authorization".to_string(),
            format!("Basic {}", base64(format!("{user}:{pass}").as_bytes())),
        ));
    }
    let pairs: Vec<(&str, &str)> = headers
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    let call = Call {
        method: "GET",
        url: &a.url,
        headers: &pairs,
        body: None,
        timeout: DOWNLOAD_TIMEOUT,
    };
    let mut sink = sink;
    Http::new()
        .stream(&call, &mut sink, progress)
        .map(|_| ())
        .map_err(HostError::Http)
}

/// De leans3-client van een `s3://host/sleutel`-URL; geeft ook de sleutel.
///
/// Go tekende `https://<host>/<sleutel>` zelf, met `host` de hele
/// virtual-hosted naam (`bucket.endpoint`). leans3 wil bucket en endpoint
/// apart; zonder `endpoint` in `auth` is het eerste label van de host de
/// bucket en de rest het endpoint, zodat de URL over de draad dezelfde is
/// als in Go. Een host zonder punt is een kale bucket op AWS
/// (`s3.<regio>.amazonaws.com`). Met `endpoint` in `auth` is de host de
/// bucket, en `path_style = "true"` zet hem in het pad (MinIO).
pub(crate) fn s3_client(a: &Artifact) -> Result<(leans3::Client, String)> {
    let rest = a.url.strip_prefix("s3://").unwrap_or("");
    let (host, key) = rest.split_once('/').unwrap_or((rest, ""));
    if host.is_empty() || key.is_empty() {
        return Err(HostError::Url(format!("{}: want s3://host/key", a.url)));
    }
    let auth = |k: &str| a.auth.get(k).cloned().unwrap_or_default();
    let (access, secret) = (auth("access_key"), auth("secret_key"));
    if access.is_empty() || secret.is_empty() {
        return Err(HostError::S3Credentials);
    }
    let mut region = auth("region");
    if region.is_empty() {
        region = DEFAULT_REGION.to_string();
    }
    let endpoint = auth("endpoint");
    let (bucket, endpoint) = if !endpoint.is_empty() {
        let e = if endpoint.contains("://") {
            endpoint
        } else {
            format!("https://{endpoint}")
        };
        (host.to_string(), e)
    } else if let Some((bucket, base)) = host.split_once('.') {
        (bucket.to_string(), format!("https://{base}"))
    } else {
        (
            host.to_string(),
            format!("https://s3.{region}.amazonaws.com"),
        )
    };
    let client = leans3::Client {
        endpoint,
        bucket,
        region,
        access_key_id: access,
        secret_access_key: secret,
        session_token: auth("session_token"),
        path_style: auth("path_style") == "true",
        now: Some(hostnet::unix_secs),
    };
    Ok((client, key.to_string()))
}

/// S3-download via `leans3::Client::get_to` over het host-transport (Go: `downloadS3`).
pub(crate) fn download_s3(
    a: &Artifact,
    sink: &mut dyn Write,
    path: &Path,
    progress: &mut dyn FnMut(u64, Option<u64>),
) -> Result {
    let (client, key) = s3_client(a)?;
    let mut transport = S3Transport::new(DOWNLOAD_TIMEOUT);
    let mut w = S3Sink {
        out: sink,
        written: 0,
        progress,
        err: None,
    };
    let res = block_on(client.get_to(&mut transport, &key, &mut w));
    match (res, w.err.take()) {
        (Ok(_), _) => Ok(()),
        (Err(_), Some(e)) => Err(HostError::io("write", path, e)),
        (Err(e), None) => Err(HostError::S3(e)),
    }
}

/// Een `Write` als leans3-schrijver, met voortgang en de echte schrijffout bewaard.
struct S3Sink<'a> {
    out: &'a mut dyn Write,
    written: u64,
    progress: &'a mut dyn FnMut(u64, Option<u64>),
    err: Option<io::Error>,
}

impl leans3::AsyncWrite for S3Sink<'_> {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<core::result::Result<usize, leans3::IoError>> {
        let this = self.get_mut();
        match this.out.write(buf) {
            Ok(n) => {
                this.written = this
                    .written
                    .saturating_add(u64::try_from(n).unwrap_or(u64::MAX));
                (this.progress)(this.written, None);
                Poll::Ready(Ok(n))
            }
            Err(e) => {
                this.err = Some(e);
                Poll::Ready(Err(leans3::IoError::Other("write failed")))
            }
        }
    }
}
