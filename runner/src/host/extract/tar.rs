//! Een tar-lezer (ustar, GNU lange namen, pax-paden en -lengtes) die uitpakt in een map.
//!
//! Bezit alleen het blok dat hij leest; de bron is van de aanroeper, de
//! bestanden die hij schrijft zijn van de taakmap. Net als de Go-versie
//! schrijft hij alleen mappen en gewone bestanden: links, apparaten en fifo's
//! worden overgeslagen, zodat een archief nooit een verwijzing naar buiten de
//! taakmap kan leggen.

use std::fs::{self, OpenOptions};
use std::io::{self, Read};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

use super::{Kind, safe_join};
use crate::host::{HostError, Result};

/// Een tar-blok.
const BLOCK: usize = 512;

/// De langste GNU-naam (`L`) die we aannemen: 64 KiB. Linux' PATH_MAX is 4 KiB.
const MAX_LONG_NAME: u64 = 64 << 10;

/// De grootste pax-kop die we aannemen: 1 MiB (Go: `maxSpecialFileSize`).
const MAX_PAX: u64 = 1 << 20;

/// Wat een pax- of GNU-kop zegt over het volgende item.
#[derive(Default)]
struct Pending {
    path: Option<String>,
    size: Option<u64>,
}

/// Pakt het tar-archief uit `src` uit in `dest`.
///
/// Leest tot het eerste nulblok (het einde van het archief) of het einde van
/// de bron. Een pad buiten `dest` is een fout, ook voor een item dat anders
/// overgeslagen zou worden (Go toetst ook eerst).
pub(crate) fn extract_tar<R: Read + ?Sized>(src: &mut R, dest: &Path) -> Result {
    let mut block = [0u8; BLOCK];
    let mut pending = Pending::default();
    loop {
        if !read_block(src, &mut block)? {
            return Ok(());
        }
        if block.iter().all(|&b| b == 0) {
            return Ok(());
        }
        check_sum(&block)?;
        let header_size = octal(field(&block, 124, 12))?;
        let size = pending.size.take().unwrap_or(header_size);
        let kind = block.get(156).copied().unwrap_or(0);
        match kind {
            b'L' => {
                pending.path = Some(text(&read_special(src, size, MAX_LONG_NAME)?));
                skip_padding(src, size)?;
                continue;
            }
            b'x' => {
                let records = read_special(src, size, MAX_PAX)?;
                skip_padding(src, size)?;
                parse_pax(&records, &mut pending)?;
                continue;
            }
            _ => {}
        }
        let name = match pending.path.take() {
            Some(p) => p,
            None => header_name(&block),
        };
        let target = safe_join(Kind::Tar, dest, &name)?;
        match (kind, target) {
            (b'5', Some(dir)) => {
                fs::create_dir_all(&dir).map_err(|e| HostError::io("mkdir", &dir, e))?;
                skip(src, size)?;
            }
            (b'0' | 0 | b'7', Some(file)) => {
                let mode = u32::try_from(octal(field(&block, 100, 8))? & 0o777).unwrap_or(0o644);
                write_file(src, &file, size, mode)?;
            }
            // De wortel, links, apparaten, fifo's, globale pax-koppen: overslaan.
            _ => skip(src, size)?,
        }
        skip_padding(src, size)?;
    }
}

/// Leest één blok; `false` bij een schoon einde van de bron vóór het blok.
fn read_block<R: Read + ?Sized>(src: &mut R, block: &mut [u8; BLOCK]) -> Result<bool> {
    let mut got = 0;
    while got < BLOCK {
        let dst = block.get_mut(got..).unwrap_or(&mut []);
        match src.read(dst) {
            Ok(0) if got == 0 => return Ok(false),
            Ok(0) => return Err(HostError::archive_msg("tar", "truncated header block")),
            Ok(n) => got += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(HostError::archive("tar", &e)),
        }
    }
    Ok(true)
}

/// Een veld van de kop.
fn field(block: &[u8; BLOCK], at: usize, len: usize) -> &[u8] {
    block.get(at..at + len).unwrap_or(&[])
}

/// Een nul-afgesloten tekstveld.
fn text(f: &[u8]) -> String {
    let end = f.iter().position(|&b| b == 0).unwrap_or(f.len());
    String::from_utf8_lossy(f.get(..end).unwrap_or(&[])).into_owned()
}

/// De naam uit de kop: ustar-prefix plus naam.
fn header_name(block: &[u8; BLOCK]) -> String {
    let name = text(field(block, 0, 100));
    let is_ustar = field(block, 257, 5) == b"ustar";
    let prefix = if is_ustar {
        text(field(block, 345, 155))
    } else {
        String::new()
    };
    if prefix.is_empty() {
        name
    } else {
        format!("{prefix}/{name}")
    }
}

/// Een getal: octaal (met spaties en nullen eromheen), of base-256 als de hoogste bit staat.
fn octal(f: &[u8]) -> Result<u64> {
    if let Some((&first, rest)) = f.split_first()
        && first & 0x80 != 0
    {
        let mut v = u64::from(first & 0x7f);
        for &b in rest {
            v = v
                .checked_mul(256)
                .and_then(|v| v.checked_add(u64::from(b)))
                .ok_or_else(|| HostError::archive_msg("tar", "base-256 number overflows"))?;
        }
        return Ok(v);
    }
    let mut v: u64 = 0;
    for &b in f.iter().skip_while(|&&b| b == b' ' || b == 0) {
        match b {
            b'0'..=b'7' => {
                v = v
                    .checked_mul(8)
                    .and_then(|v| v.checked_add(u64::from(b - b'0')))
                    .ok_or_else(|| HostError::archive_msg("tar", "octal number overflows"))?;
            }
            b' ' | 0 => break,
            _ => return Err(HostError::archive_msg("tar", "invalid octal number")),
        }
    }
    Ok(v)
}

/// Toetst de kopsom: alle bytes, met het somveld als spaties (unsigned of signed, zoals Go).
fn check_sum(block: &[u8; BLOCK]) -> Result {
    let want = octal(field(block, 148, 8))?;
    let (mut unsigned, mut signed) = (0u64, 0i64);
    for (i, &b) in block.iter().enumerate() {
        let b = if (148..156).contains(&i) { b' ' } else { b };
        unsigned += u64::from(b);
        signed += i64::from(b.cast_signed());
    }
    if want == unsigned || i64::try_from(want).is_ok_and(|w| w == signed) {
        Ok(())
    } else {
        Err(HostError::Archive {
            archive: "tar",
            why: format!("header checksum {want} does not match {unsigned}"),
        })
    }
}

/// Leest `size` bytes van een speciale kop (een lange naam of pax-records), tot `max`.
fn read_special<R: Read + ?Sized>(src: &mut R, size: u64, max: u64) -> Result<Vec<u8>> {
    if size > max {
        return Err(HostError::Archive {
            archive: "tar",
            why: format!("special header of {size} bytes exceeds {max}"),
        });
    }
    let mut buf = Vec::new();
    src.take(size)
        .read_to_end(&mut buf)
        .map_err(|e| HostError::archive("tar", &e))?;
    if u64::try_from(buf.len()).unwrap_or(u64::MAX) != size {
        return Err(HostError::archive_msg("tar", "truncated special header"));
    }
    Ok(buf)
}

/// Leest de pax-records `"<len> <sleutel>=<waarde>\n"`; alleen `path` en `size` tellen.
///
/// Op bytes, niet op tekst: macOS' bsdtar zet xattrs als binaire waarden
/// (met nullen) in dezelfde kop.
fn parse_pax(mut s: &[u8], pending: &mut Pending) -> Result {
    let bad = || HostError::archive_msg("tar", "invalid pax record");
    while !s.is_empty() {
        let sp = s.iter().position(|&b| b == b' ').ok_or_else(bad)?;
        let len = std::str::from_utf8(s.get(..sp).ok_or_else(bad)?).map_err(|_| bad())?;
        let n: usize = len.parse().map_err(|_| bad())?;
        let record = s.get(..n).ok_or_else(bad)?;
        s = s.get(n..).ok_or_else(bad)?;
        let body = record
            .get(sp + 1..)
            .and_then(|r| r.strip_suffix(b"\n"))
            .ok_or_else(bad)?;
        let eq = body.iter().position(|&b| b == b'=').ok_or_else(bad)?;
        let (key, value) = (
            body.get(..eq).unwrap_or(&[]),
            body.get(eq + 1..).unwrap_or(&[]),
        );
        match key {
            b"path" => pending.path = Some(String::from_utf8_lossy(value).into_owned()),
            b"size" => {
                let v = std::str::from_utf8(value).map_err(|_| bad())?;
                pending.size = Some(v.parse().map_err(|_| bad())?);
            }
            _ => {}
        }
    }
    Ok(())
}

/// Slaat `n` bytes van de bron over; een te kort archief is een fout.
fn skip<R: Read + ?Sized>(src: &mut R, n: u64) -> Result {
    let got =
        io::copy(&mut src.take(n), &mut io::sink()).map_err(|e| HostError::archive("tar", &e))?;
    if got != n {
        return Err(HostError::archive_msg("tar", "unexpected end of archive"));
    }
    Ok(())
}

/// Slaat de opvulling tot het volgende blok over.
fn skip_padding<R: Read + ?Sized>(src: &mut R, size: u64) -> Result {
    let block = BLOCK as u64;
    skip(src, (block - size % block) % block)
}

/// Schrijft een gewoon bestand van `size` bytes met `mode`.
fn write_file<R: Read + ?Sized>(src: &mut R, path: &Path, size: u64, mode: u32) -> Result {
    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir).map_err(|e| HostError::io("mkdir", dir, e))?;
    }
    let mut f = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(mode)
        .open(path)
        .map_err(|e| HostError::io("create", path, e))?;
    let got = io::copy(&mut src.take(size), &mut f).map_err(|e| HostError::io("write", path, e))?;
    if got != size {
        return Err(HostError::archive_msg("tar", "unexpected end of archive"));
    }
    Ok(())
}
