//! Het artifact van een taak ophalen en uitpakken; de download zelf is
//! [`crate::artifact`], dezelfde als op HopOS.
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
use std::time::Duration;

use hostnet::{WebDialer, block_on};
use types::Artifact;

use crate::artifact;

use super::{HostError, Result, extract};

/// De termijn per fase van een download (Go: `downloadTimeout`, daar voor
/// het hele verzoek). Hier geldt hij per verbinden, kop en elke lees: een
/// grote download mag lang duren, een zwijgende server niet.
const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(5 * 60);

/// Toetst vóór er iets gebeurt of dit artifact ooit kan lukken: schema en extract.
pub(crate) fn check(a: &Artifact) -> Result {
    if a.url.is_empty() {
        return Ok(());
    }
    if let Err(artifact::Error::Scheme(s)) = artifact::check(a) {
        return Err(HostError::Scheme(s));
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

/// Haalt de bytes van `a` naar `path`, met de downloader van elke node.
fn fetch(a: &Artifact, path: &Path, progress: &mut dyn FnMut(u64, Option<u64>)) -> Result {
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .map_err(|e| HostError::io("create", path, e))?;
    download_to(a, path, &mut file, progress)?;
    file.flush().map_err(|e| HostError::io("write", path, e))
}

/// Downloadt `a` naar `out` met [`artifact::fetch`] over de webdialer van de
/// host; `path` noemt het doel in een schrijffout.
pub(crate) fn download_to(
    a: &Artifact,
    path: &Path,
    out: &mut dyn Write,
    progress: &mut dyn FnMut(u64, Option<u64>),
) -> Result {
    let mut sink = FileSink {
        out,
        written: 0,
        total: None,
        progress,
        err: None,
    };
    let mut net = WebDialer::new(DOWNLOAD_TIMEOUT);
    let (got, tls) = {
        let mut dial = net.dial();
        let got = block_on(artifact::fetch(&mut dial, &HOW, a, &mut sink));
        (got, dial.last_error())
    };
    if let Some(e) = sink.err.take() {
        return Err(HostError::io("write", path, e));
    }
    got.map_err(|e| match e {
        artifact::Error::Scheme(s) => HostError::Scheme(s),
        artifact::Error::Url(u) => HostError::Url(u),
        artifact::Error::S3Credentials => HostError::S3Credentials,
        artifact::Error::S3(e) => HostError::S3(e),
        artifact::Error::Net(e) => HostError::Http(net.failure(e, tls)),
        artifact::Error::Status { code, body } => {
            HostError::Http(hostnet::Error::Status { code, body })
        }
        other => HostError::Http(hostnet::Error::Sink(other.to_string())),
    })
}

/// Hoe de host downloadt: de termijn per fase, brokken van 64 KiB, de systeemklok voor S3.
const HOW: artifact::Fetch = artifact::Fetch {
    timeout: DOWNLOAD_TIMEOUT,
    chunk: 64 << 10,
    now: Some(hostnet::unix_secs),
};

/// Een bestand als sink, met voortgang en de echte schrijffout bewaard.
struct FileSink<'a> {
    out: &'a mut dyn Write,
    written: u64,
    total: Option<u64>,
    progress: &'a mut dyn FnMut(u64, Option<u64>),
    err: Option<io::Error>,
}

impl artifact::Sink for FileSink<'_> {
    async fn begin(&mut self, size: Option<u64>) -> core::result::Result<(), String> {
        self.total = size;
        Ok(())
    }

    async fn chunk(&mut self, bytes: &[u8]) -> core::result::Result<(), String> {
        if let Err(e) = self.out.write_all(bytes) {
            let why = e.to_string();
            self.err = Some(e);
            return Err(why);
        }
        self.written = self
            .written
            .saturating_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
        (self.progress)(self.written, self.total);
        Ok(())
    }
}
