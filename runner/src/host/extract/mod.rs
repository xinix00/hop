//! Het uitpakken van een artifact in de taakmap: tar.gz, tar.bz2 en zip.
//!
//! Bezit alleen de keuze van het formaat en de padtoets die alle formaten
//! delen. De taakmap is van [`super::prepare`]; dit schrijft erin en laat
//! hem staan.
//!
//! Go deed dit in-process met `archive/tar`, `archive/zip` en
//! `compress/{gzip,bzip2}`. Hier staan tar, zip en inflate zelf geschreven
//! (geen crates van buiten, handboek §8). bzip2 is de uitzondering: een
//! bzip2-decoder is een Burrows-Wheeler-inverse met eigen Huffman-tabellen en
//! is groter dan de rest van deze map samen, voor één formaat dat vooral
//! oude artifacts gebruiken. Daarom gaat het archief door `bzip2 -dc` (een
//! pijp, geen tijdelijk bestand), dat op elke Linux en macOS staat. Ontbreekt
//! het, dan zegt de fout dat.

mod inflate;
mod tar;
mod zip;

use std::fs::File;
use std::io::{self, BufReader, Read};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use super::{HostError, Result};

pub(crate) use inflate::{Crc32, Deflate, Gzip};
pub(crate) use tar::extract_tar;
pub(crate) use zip::extract_zip;

/// Hoe het formaat heet in een foutmelding.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Kind {
    /// tar, al dan niet gecomprimeerd.
    Tar,
    /// zip.
    Zip,
}

impl Kind {
    /// Het woord in "illegal file path in {kind}".
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Tar => "tar",
            Self::Zip => "zip",
        }
    }
}

/// De extract-waarden die Hop kent (plus Go's aliassen `tgz` en `tbz2`).
pub(crate) fn is_known(extract: &str) -> bool {
    matches!(extract, "tar.gz" | "tgz" | "tar.bz2" | "tbz2" | "zip")
}

/// Pakt `archive` uit in `dest` volgens `extract`.
pub(crate) fn extract(extract: &str, archive: &Path, dest: &Path) -> Result {
    match extract {
        "tar.gz" | "tgz" => extract_tar_gz(archive, dest),
        "tar.bz2" | "tbz2" => extract_tar_bz2(archive, dest),
        "zip" => extract_zip(archive, dest),
        other => Err(HostError::ExtractType(other.to_string())),
    }
}

/// Pakt een `.tar.gz` uit.
pub(crate) fn extract_tar_gz(archive: &Path, dest: &Path) -> Result {
    let f = File::open(archive).map_err(|e| HostError::io("open", archive, e))?;
    let mut gz = Gzip::new(BufReader::new(f)).map_err(|e| HostError::archive("tar.gz", &e))?;
    extract_tar(&mut gz, dest)
}

/// Pakt een `.tar.bz2` uit via `bzip2 -dc` (zie de module-doc voor waarom).
pub(crate) fn extract_tar_bz2(archive: &Path, dest: &Path) -> Result {
    let f = File::open(archive).map_err(|e| HostError::io("open", archive, e))?;
    let bzip2 = super::process::find_tool("bzip2").ok_or_else(|| HostError::Archive {
        archive: "tar.bz2",
        why: "bzip2 not found in /usr/bin, /bin or /usr/local/bin".to_string(),
    })?;
    let mut child = Command::new(&bzip2)
        .arg("-dc")
        .stdin(f)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| HostError::Spawn {
            program: bzip2.display().to_string(),
            source: e,
        })?;
    let Some(mut out) = child.stdout.take() else {
        let _ = child.kill();
        let _ = child.wait();
        return Err(HostError::archive_msg("tar.bz2", "bzip2 has no stdout"));
    };
    let res = extract_tar(&mut out, dest);
    if res.is_err() {
        let _ = child.kill();
    }
    // Leeg de pijp: een tar eindigt vóór de opvulling, en een bzip2 die nog
    // schrijft op een volle pijp komt anders nooit bij zijn exit.
    let _ = io::copy(&mut out, &mut io::sink());
    drop(out);
    let mut err = String::new();
    if let Some(mut e) = child.stderr.take() {
        let _ = e.by_ref().take(4 << 10).read_to_string(&mut err);
    }
    let status = child.wait().map_err(|e| HostError::Spawn {
        program: bzip2.display().to_string(),
        source: e,
    })?;
    res?;
    if !status.success() {
        return Err(HostError::Archive {
            archive: "tar.bz2",
            why: format!("bzip2 -dc exited with {status}: {}", err.trim()),
        });
    }
    Ok(())
}

/// Het doel van `name` in `dest`, lexicaal opgeschoond zoals Go's `filepath.Join`.
///
/// `Ok(None)` is de wortel zelf (`.`, `./`), die wordt overgeslagen. Een naam
/// die via `..` buiten `dest` komt, is een fout: "illegal file path". Een
/// absoluut pad komt, net als in Go, binnen `dest` terecht.
pub(crate) fn safe_join(kind: Kind, dest: &Path, name: &str) -> Result<Option<PathBuf>> {
    let mut parts: Vec<&str> = Vec::new();
    for part in name.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                if parts.pop().is_none() {
                    return Err(HostError::IllegalPath {
                        archive: kind.name(),
                        name: name.to_string(),
                    });
                }
            }
            p => parts.push(p),
        }
    }
    if parts.is_empty() {
        return Ok(None);
    }
    let mut out = dest.to_path_buf();
    for p in parts {
        out.push(p);
    }
    Ok(Some(out))
}
