//! Een zip-lezer (centrale directory, stored en deflate, zip64) die uitpakt in een map.
//!
//! Bezit het archiefbestand zolang hij leest, en de centrale directory in het
//! geheugen (begrensd). Zoals Go: mappen en gewone bestanden, de modus uit
//! de Unix-attributen als die er zijn, anders 0666 (min de umask).

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

use super::{Crc32, Deflate, Kind, safe_join};
use crate::host::{HostError, Result};

/// De handtekening van het einde van de centrale directory.
const EOCD_SIG: u32 = 0x0605_4b50;
/// De handtekening van de zip64-locator.
const LOC64_SIG: u32 = 0x0706_4b50;
/// De handtekening van het zip64-einde.
const EOCD64_SIG: u32 = 0x0606_4b50;
/// De handtekening van een entry in de centrale directory.
const CDIR_SIG: u32 = 0x0201_4b50;
/// De handtekening van een lokale kop.
const LOCAL_SIG: u32 = 0x0403_4b50;

/// De vaste maat van het einde van de centrale directory.
const EOCD_LEN: usize = 22;

/// De grootste centrale directory die we in het geheugen nemen: 64 MiB,
/// ruim een half miljoen entries.
const MAX_CDIR: u64 = 64 << 20;

/// De kopieerbuffer.
const BUF: usize = 64 << 10;

/// Leest little-endian velden uit een byteslice.
struct Le<'a>(&'a [u8]);

impl Le<'_> {
    fn u16(&self, at: usize) -> u16 {
        let b = self.0.get(at..at + 2).unwrap_or(&[0, 0]);
        u16::from_le_bytes([
            b.first().copied().unwrap_or(0),
            b.get(1).copied().unwrap_or(0),
        ])
    }

    fn u32(&self, at: usize) -> u32 {
        u32::from(self.u16(at)) | (u32::from(self.u16(at + 2)) << 16)
    }

    fn u64(&self, at: usize) -> u64 {
        u64::from(self.u32(at)) | (u64::from(self.u32(at + 4)) << 32)
    }
}

/// Een fout van dit formaat.
fn bad(why: &'static str) -> HostError {
    HostError::archive_msg("zip", why)
}

/// Eén entry uit de centrale directory.
struct Entry {
    name: String,
    method: u16,
    flags: u16,
    crc: u32,
    csize: u64,
    usize: u64,
    offset: u64,
    mode: Option<u32>,
}

/// Pakt het zip-archief `archive` uit in `dest`.
pub(crate) fn extract_zip(archive: &Path, dest: &Path) -> Result {
    let mut f = File::open(archive).map_err(|e| HostError::io("open", archive, e))?;
    let entries = central_directory(&mut f).map_err(|e| match e {
        HostError::Io { source, .. } => HostError::archive("zip", &source),
        e => e,
    })?;
    for e in &entries {
        let Some(target) = safe_join(Kind::Zip, dest, &e.name)? else {
            continue;
        };
        let is_dir = e.name.ends_with('/') || e.mode.is_some_and(|m| m & 0o170_000 == 0o040_000);
        if is_dir {
            fs::create_dir_all(&target).map_err(|err| HostError::io("mkdir", &target, err))?;
            continue;
        }
        extract_entry(&mut f, e, &target)?;
    }
    Ok(())
}

/// Leest de centrale directory, via het (zip64-)einde.
fn central_directory(f: &mut File) -> Result<Vec<Entry>> {
    let io = |e: io::Error| HostError::archive("zip", &e);
    let len = f.seek(SeekFrom::End(0)).map_err(io)?;
    // Het einde staat in de laatste 22 bytes plus hoogstens 64 KiB commentaar.
    let tail_len = len.min(u64::try_from(EOCD_LEN + 0xffff).unwrap_or(u64::MAX));
    let tail_at = len - tail_len;
    let mut tail = vec![0u8; usize::try_from(tail_len).map_err(|_| bad("archive too large"))?];
    f.seek(SeekFrom::Start(tail_at)).map_err(io)?;
    f.read_exact(&mut tail).map_err(io)?;
    let pos = (0..=tail.len().saturating_sub(EOCD_LEN))
        .rev()
        .find(|&i| Le(&tail).u32(i) == EOCD_SIG && tail.len() >= i + EOCD_LEN)
        .ok_or_else(|| bad("not a valid zip file"))?;
    let eocd = Le(tail.get(pos..).unwrap_or(&[]));
    let mut count = u64::from(eocd.u16(10));
    let mut cd_size = u64::from(eocd.u32(12));
    let mut cd_off = u64::from(eocd.u32(16));
    if count == 0xffff || cd_size == 0xffff_ffff || cd_off == 0xffff_ffff {
        let eocd_at = tail_at + u64::try_from(pos).unwrap_or(0);
        (count, cd_size, cd_off) = zip64_end(f, eocd_at)?;
    }
    if cd_size > MAX_CDIR {
        return Err(HostError::Archive {
            archive: "zip",
            why: format!("central directory of {cd_size} bytes exceeds {MAX_CDIR}"),
        });
    }
    let mut cd = vec![0u8; usize::try_from(cd_size).unwrap_or(0)];
    f.seek(SeekFrom::Start(cd_off)).map_err(io)?;
    f.read_exact(&mut cd).map_err(io)?;
    parse_entries(&cd, count)
}

/// De maten uit het zip64-einde: (entries, maat, offset van de centrale directory).
fn zip64_end(f: &mut File, eocd_at: u64) -> Result<(u64, u64, u64)> {
    let io = |e: io::Error| HostError::archive("zip", &e);
    let loc_at = eocd_at
        .checked_sub(20)
        .ok_or_else(|| bad("missing zip64 locator"))?;
    let mut loc = [0u8; 20];
    f.seek(SeekFrom::Start(loc_at)).map_err(io)?;
    f.read_exact(&mut loc).map_err(io)?;
    if Le(&loc).u32(0) != LOC64_SIG {
        return Err(bad("missing zip64 locator"));
    }
    let mut end = [0u8; 56];
    f.seek(SeekFrom::Start(Le(&loc).u64(8))).map_err(io)?;
    f.read_exact(&mut end).map_err(io)?;
    let e = Le(&end);
    if e.u32(0) != EOCD64_SIG {
        return Err(bad("invalid zip64 end of central directory"));
    }
    Ok((e.u64(32), e.u64(40), e.u64(48)))
}

/// Leest `count` entries uit de centrale directory.
fn parse_entries(cd: &[u8], count: u64) -> Result<Vec<Entry>> {
    let mut out = Vec::new();
    let mut at = 0usize;
    for _ in 0..count {
        let h = Le(cd.get(at..).unwrap_or(&[]));
        if cd.len() < at + 46 || h.u32(0) != CDIR_SIG {
            return Err(bad("invalid central directory entry"));
        }
        let (nlen, xlen, clen) = (
            usize::from(h.u16(28)),
            usize::from(h.u16(30)),
            usize::from(h.u16(32)),
        );
        let name = cd
            .get(at + 46..at + 46 + nlen)
            .ok_or_else(|| bad("truncated entry name"))?;
        let extra = cd
            .get(at + 46 + nlen..at + 46 + nlen + xlen)
            .ok_or_else(|| bad("truncated extra field"))?;
        let made_by_unix = h.u16(4) >> 8 == 3;
        let attrs = h.u32(38) >> 16;
        let mut e = Entry {
            name: String::from_utf8_lossy(name).into_owned(),
            method: h.u16(10),
            flags: h.u16(8),
            crc: h.u32(16),
            csize: u64::from(h.u32(20)),
            usize: u64::from(h.u32(24)),
            offset: u64::from(h.u32(42)),
            mode: (made_by_unix && attrs != 0).then_some(attrs),
        };
        zip64_extra(extra, &mut e);
        out.push(e);
        at += 46 + nlen + xlen + clen;
    }
    Ok(out)
}

/// Vult de 64-bits maten in uit het zip64-extraveld (0x0001), in de volgorde van de spec.
fn zip64_extra(mut extra: &[u8], e: &mut Entry) {
    while extra.len() >= 4 {
        let x = Le(extra);
        let (id, size) = (x.u16(0), usize::from(x.u16(2)));
        let Some(data) = extra.get(4..4 + size) else {
            return;
        };
        if id == 1 {
            let d = Le(data);
            let mut at = 0;
            for v in [&mut e.usize, &mut e.csize, &mut e.offset] {
                if *v == 0xffff_ffff && data.len() >= at + 8 {
                    *v = d.u64(at);
                    at += 8;
                }
            }
        }
        extra = extra.get(4 + size..).unwrap_or(&[]);
    }
}

/// Schrijft één bestand uit het archief, met toets van lengte en CRC.
fn extract_entry(f: &mut File, e: &Entry, target: &Path) -> Result {
    if e.flags & 1 != 0 {
        return Err(HostError::Archive {
            archive: "zip",
            why: format!("{}: encrypted entries are not supported", e.name),
        });
    }
    let io = |err: io::Error| HostError::archive("zip", &err);
    let mut local = [0u8; 30];
    f.seek(SeekFrom::Start(e.offset)).map_err(io)?;
    f.read_exact(&mut local).map_err(io)?;
    let l = Le(&local);
    if l.u32(0) != LOCAL_SIG {
        return Err(bad("invalid local file header"));
    }
    let skip = u64::from(l.u16(26)) + u64::from(l.u16(28));
    f.seek(SeekFrom::Current(i64::try_from(skip).unwrap_or(0)))
        .map_err(io)?;
    if let Some(dir) = target.parent() {
        fs::create_dir_all(dir).map_err(|err| HostError::io("mkdir", dir, err))?;
    }
    let mode = e.mode.map_or(0o666, |m| m & 0o777);
    let mut out = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(if mode == 0 { 0o666 } else { mode })
        .open(target)
        .map_err(|err| HostError::io("create", target, err))?;
    let raw = (&mut *f).take(e.csize);
    let (written, crc) = match e.method {
        0 => copy_checked(raw, &mut out, e.usize, target)?,
        8 => copy_checked(Deflate::new(raw), &mut out, e.usize, target)?,
        m => {
            return Err(HostError::Archive {
                archive: "zip",
                why: format!("{}: unsupported compression method {m}", e.name),
            });
        }
    };
    if written != e.usize || crc != e.crc {
        return Err(HostError::Archive {
            archive: "zip",
            why: format!(
                "{}: {written} bytes with crc {crc:08x}, want {} bytes with crc {:08x}",
                e.name, e.usize, e.crc
            ),
        });
    }
    Ok(())
}

/// Kopieert tot hoogstens `max` bytes (plus één om een overschrijding te zien) en telt de CRC.
fn copy_checked<R: Read>(mut src: R, out: &mut File, max: u64, path: &Path) -> Result<(u64, u32)> {
    let mut buf = vec![0u8; BUF];
    let mut crc = Crc32::new();
    let mut total = 0u64;
    loop {
        let n = match src.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(HostError::archive("zip", &e)),
        };
        let chunk = buf.get(..n).unwrap_or(&[]);
        total = total.saturating_add(u64::try_from(n).unwrap_or(u64::MAX));
        if total > max {
            // Meer dan de directory belooft: een zip-bom stopt hier.
            return Err(HostError::Archive {
                archive: "zip",
                why: format!("{}: more than the declared {max} bytes", path.display()),
            });
        }
        crc.update(chunk);
        out.write_all(chunk)
            .map_err(|e| HostError::io("write", path, e))?;
    }
    Ok((total, crc.sum()))
}
