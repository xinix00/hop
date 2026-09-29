//! De clusterstaat in een lokaal bestand: standalone en de in-memory lock.
//!
//! Bezit het pad en niets anders. Een schrijf is crashbestendig zoals in Go:
//! een tijdelijk bestand ernaast, fsync, een atomische rename en een fsync
//! van de map, zodat een crash of een volle schijf nooit een half of
//! afgekapt staatbestand achterlaat (een truncate-in-place zou dat wel doen).

use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

use crate::{Error, Result, StateStore};

/// De clusterstaat in een lokaal bestand.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileStateStore {
    path: PathBuf,
}

impl FileStateStore {
    /// Een opslag op `path`; de map wordt bij de eerste schrijf gemaakt.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    /// Het pad van het staatbestand.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Het tijdelijke bestand naast het echte.
    ///
    /// Een vaste naam en geen willekeurige: na een crash blijft er hooguit
    /// één over, en de volgende schrijf kapt precies die af.
    fn tmp_path(&self) -> PathBuf {
        let mut name = self.path.clone().into_os_string();
        name.push(".tmp");
        PathBuf::from(name)
    }

    /// Schrijft en synct het tijdelijke bestand, en hernoemt het over het echte.
    fn replace(&self, tmp: &Path, snapshot: &[u8]) -> Result {
        // 0600: de staat noemt jobs, env en soms geheimen van taken.
        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(tmp)
            .map_err(|e| fail("create", tmp, &e))?;
        f.write_all(snapshot).map_err(|e| fail("write", tmp, &e))?;
        f.sync_all().map_err(|e| fail("fsync", tmp, &e))?;
        drop(f);
        fs::rename(tmp, &self.path).map_err(|e| fail("rename", &self.path, &e))
    }
}

impl StateStore for FileStateStore {
    fn save(&mut self, snapshot: &[u8]) -> Result {
        let dir = match self.path.parent() {
            Some(d) if !d.as_os_str().is_empty() => d.to_path_buf(),
            _ => PathBuf::from("."),
        };
        fs::create_dir_all(&dir).map_err(|e| fail("mkdir", &dir, &e))?;
        let tmp = self.tmp_path();
        let result = self.replace(&tmp, snapshot);
        if result.is_err() {
            // Na een geslaagde rename is er niets meer; na een fout ruimen we op.
            let _ = fs::remove_file(&tmp);
            return result;
        }
        // De map syncen maakt de rename zelf duurzaam over een crash. Naar
        // beste kunnen, zoals in Go: de data staat al veilig in het bestand.
        if let Ok(d) = File::open(&dir) {
            let _ = d.sync_all();
        }
        Ok(())
    }

    fn load(&mut self) -> Result<Option<Vec<u8>>> {
        match fs::read(&self.path) {
            Ok(data) => Ok(Some(data)),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
            Err(e) => Err(fail("read", &self.path, &e)),
        }
    }

    fn describe(&self) -> String {
        format!("file {}", self.path.display())
    }
}

/// Een I/O-fout met stap en pad.
fn fail(op: &'static str, path: &Path, e: &std::io::Error) -> Error {
    Error::File {
        op,
        path: path.display().to_string(),
        why: e.to_string(),
    }
}
