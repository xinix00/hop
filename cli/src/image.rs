//! `hop image`: de node-config in het config-venster van een HopOS-image,
//! een flipbundel of een kaart, en een kaart of stick schrijven met die
//! config erin. De Hop-imager van v2 (hop-imager, `internal/cfgwin`, en
//! `image/hopcfg` van HopOS op tag v2.2.8), als subcommando van `hop`.
//!
//! ```text
//! hop image <bestand|device>                        de config in het venster
//! hop image <bestand|device> --config <cfg>         de config erin, ter plekke
//! hop image <image> --config <cfg> --write <device> de kaart of stick schrijven
//! hop image <image> --keep --write <device>         idem, met de config die er al op stond
//! ```
//!
//! # Het venster
//!
//! Elke HopOS-kern draagt een venster van vaste maat (HopOS
//! `board/src/cfgwin.rs`, 16 KiB): een kopregel, de config, en `#`-regels
//! als padding.
//!
//! ```text
//! #HOPCFG1 window=16384 len=0000000432
//! hopos.node=hop-1
//! ...
//! ################################################################ (padding)
//! ```
//!
//! `len` telt de configbytes na de kopregel (tien vaste cijfers: de kopregel
//! blijft even lang), `window` de hele maat (een 512-voud). Voor de kern is
//! het hele venster een geldig configbestand; voor deze tool een blok bytes
//! dat hij vindt aan zijn kopregel en ter plekke herschrijft, zonder
//! filesystem: in een `.img`, een `.flip`, `fip.bin`, of op `/dev/rdiskN`.
//! FORMAATCONTRACT met HopOS `board/src/cfgwin.rs` en `image/hopcfg.py`.
//!
//! De zoektocht gaat over elke byte (in de FIP van de LicheeRV ligt het
//! venster niet uitgelijnd), tot 256 MiB diep, en eist precies één venster.
//! Schrijven gaat met lezen-wijzigen-schrijven van hele sectoren: een rauw
//! device neemt alleen I/O op 512-grenzen. Ligt het venster in de
//! MONITOR-payload van een Sophgo-FIP (de LicheeRV), dan rekent de tool
//! MONITOR_CKSUM en PARAM2_CKSUM na, anders weigert de FSBL de kern.
//!
//! Een config die zelf al een venster is, wordt eerst gestript; een NUL,
//! tekst die geen UTF-8 is of een config die niet past: weigeren, want de
//! kern zou hem negeren.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};

/// Het begin van de kopregel.
const MAGIC: &[u8] = b"#HOPCFG1 window=";
/// Hoe diep de zoektocht gaat (een kaart is groot, het venster staat voorin).
const SCAN_LIMIT: u64 = 256 << 20;
/// Het leesblok van de zoektocht.
const CHUNK: usize = 4 << 20;
/// De sector van een rauw device.
const SECTOR: u64 = 512;

pub(crate) const USAGE: &str = "Usage: hop image <file|device>                         show the config in the window
       hop image <file|device> --config <cfg>          put a config in the window, in place
       hop image <image> --config <cfg> --write <dev>  write a card or stick with that config, and verify
       hop image <image> --keep --write <dev>          the same, keeping the config already on <dev>

<file> is a HopOS image (.img, unpacked: gunzip first), a flip bundle (.flip,
its sha256 is printed and <file>.sha256 rewritten), or a kernel; <dev> is a
card or stick (/dev/rdiskN on macOS, /dev/sdX on Linux) and needs root. The
window is the config window every HopOS kernel carries (16 KiB).
--force writes a <dev> that does not look removable.";

/// De kopregel van een venster.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) struct Head {
    /// De hele maat van het venster.
    pub(crate) size: usize,
    /// De lengte van de kopregel.
    pub(crate) head: usize,
    /// De lengte van de config.
    pub(crate) len: usize,
}

/// Leest de kopregel aan het begin van `b` (Go: `parseHeader`).
pub(crate) fn parse_head(b: &[u8]) -> Option<Head> {
    let rest = b.strip_prefix(MAGIC)?;
    let sp = rest.iter().take(11).position(|&c| c == b' ')?;
    let size = digits(rest.get(..sp)?)?;
    let d = rest.get(sp..)?.strip_prefix(b" len=")?;
    if d.get(10) != Some(&b'\n') {
        return None;
    }
    let len = digits(d.get(..10)?)?;
    let head = MAGIC.len() + sp + 5 + 11;
    (sp > 0 && (size as u64).is_multiple_of(SECTOR) && head + len <= size).then_some(Head {
        size,
        head,
        len,
    })
}

fn digits(d: &[u8]) -> Option<usize> {
    if d.is_empty() || d.len() > 10 || !d.iter().all(u8::is_ascii_digit) {
        return None;
    }
    std::str::from_utf8(d).ok()?.parse().ok()
}

/// Exact `size` bytes: de kopregel, `content` en de `#`-padding (Go:
/// `makeWindow`). Een bestaand venster in `content` wordt eerst gestript.
pub(crate) fn make_window(content: &[u8], size: usize) -> Result<Vec<u8>, String> {
    let mut c = match parse_head(content) {
        Some(h) if h.head + h.len <= content.len() => content[h.head..h.head + h.len].to_vec(),
        _ => content.to_vec(),
    };
    if c.contains(&0) {
        return Err(String::from("the config contains a NUL byte"));
    }
    if std::str::from_utf8(&c).is_err() {
        return Err(String::from("the config is not UTF-8"));
    }
    if c.last().is_some_and(|&b| b != b'\n') {
        c.push(b'\n');
    }
    let mut w = format!("#HOPCFG1 window={size} len={:010}\n", c.len()).into_bytes();
    if w.len() + c.len() > size {
        return Err(format!(
            "a config of {} bytes does not fit the {size}-byte window",
            c.len()
        ));
    }
    w.extend_from_slice(&c);
    while w.len() < size {
        let n = (size - w.len()).min(65);
        w.extend(std::iter::repeat_n(b'#', n - 1));
        w.push(b'\n');
    }
    Ok(w)
}

/// Iets met bytes op een plek: een bestand, een device, of (de tests) een
/// buffer in het geheugen.
pub(crate) trait Medium {
    /// Leest vanaf `off` in `buf`; minder dan gevraagd alleen aan het eind.
    fn read_at(&mut self, off: u64, buf: &mut [u8]) -> io::Result<usize>;
    /// Schrijft `data` op `off`.
    fn write_at(&mut self, off: u64, data: &[u8]) -> io::Result<()>;
}

impl Medium for Vec<u8> {
    fn read_at(&mut self, off: u64, buf: &mut [u8]) -> io::Result<usize> {
        let from = usize::try_from(off).unwrap_or(usize::MAX).min(self.len());
        let n = buf.len().min(self.len() - from);
        buf[..n].copy_from_slice(&self[from..from + n]);
        Ok(n)
    }

    fn write_at(&mut self, off: u64, data: &[u8]) -> io::Result<()> {
        let at = usize::try_from(off).map_err(|_| io::Error::other("offset"))?;
        let end = at + data.len();
        if end > self.len() {
            return Err(io::Error::other("write past the end"));
        }
        self[at..end].copy_from_slice(data);
        Ok(())
    }
}

/// Een bestand of een rauw device, altijd met I/O op hele sectoren
/// (`/dev/rdiskN` geeft anders EINVAL; Go mat het 06-08).
pub(crate) struct Disk(pub(crate) File);

impl Medium for Disk {
    fn read_at(&mut self, off: u64, buf: &mut [u8]) -> io::Result<usize> {
        let lo = off - off % SECTOR;
        let hi = (off + buf.len() as u64).next_multiple_of(SECTOR);
        let mut tmp = vec![0u8; usize::try_from(hi - lo).map_err(io::Error::other)?];
        self.0.seek(SeekFrom::Start(lo))?;
        let mut got = 0;
        while got < tmp.len() {
            match self.0.read(&mut tmp[got..]) {
                Ok(0) => break,
                Ok(n) => got += n,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        let skip = usize::try_from(off - lo).map_err(io::Error::other)?;
        let n = got.saturating_sub(skip).min(buf.len());
        buf[..n].copy_from_slice(&tmp[skip..skip + n]);
        Ok(n)
    }

    fn write_at(&mut self, off: u64, data: &[u8]) -> io::Result<()> {
        let lo = off - off % SECTOR;
        let hi = (off + data.len() as u64).next_multiple_of(SECTOR);
        let mut tmp = vec![0u8; usize::try_from(hi - lo).map_err(io::Error::other)?];
        let got = self.read_at(lo, &mut tmp)?;
        let skip = usize::try_from(off - lo).map_err(io::Error::other)?;
        if got < skip + data.len() {
            return Err(io::Error::other("write past the end"));
        }
        tmp[skip..skip + data.len()].copy_from_slice(data);
        // Een bestand dat niet op een sectorgrens eindigt: niet verlengen.
        tmp.truncate(got);
        self.0.seek(SeekFrom::Start(lo))?;
        self.0.write_all(&tmp)
    }
}

/// Elke plek van `needle` in de eerste [`SCAN_LIMIT`] bytes van `m` waar
/// `ok` de bytes vanaf daar goedkeurt.
fn scan(m: &mut dyn Medium, needle: &[u8], ok: &dyn Fn(&[u8]) -> bool) -> io::Result<Vec<u64>> {
    let mut hits = Vec::new();
    let mut buf = vec![0u8; CHUNK + 4096];
    let mut off = 0u64;
    while off < SCAN_LIMIT {
        let n = m.read_at(off, &mut buf)?;
        let mut i = 0;
        // Alleen vondsten die in dit blok beginnen; de staart van 4 KiB is
        // er om een kop op de blokgrens heel te kunnen toetsen.
        while i < CHUNK.min(n) {
            let Some(k) = buf[i..n].windows(needle.len()).position(|w| w == needle) else {
                break;
            };
            i += k;
            if i >= CHUNK {
                break;
            }
            if ok(&buf[i..n]) {
                hits.push(off + i as u64);
            }
            i += 1;
        }
        if n < buf.len() {
            break;
        }
        off += CHUNK as u64;
    }
    Ok(hits)
}

/// Het enige venster in `m`: plek en kopregel.
pub(crate) fn find(m: &mut dyn Medium) -> Result<(u64, Head), String> {
    let hits = scan(m, MAGIC, &|b| parse_head(b).is_some()).map_err(|e| e.to_string())?;
    let [at] = hits.as_slice() else {
        if hits.is_empty() {
            return Err(String::from(
                "no config window found: not a HopOS image with a config window (HopOS v3.1 or later), or not unpacked",
            ));
        }
        return Err(format!(
            "{} config windows found (at {}), refusing",
            hits.len(),
            hits.iter()
                .map(|h| format!("{h:#x}"))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    };
    let mut head = [0u8; 64];
    let n = m.read_at(*at, &mut head).map_err(|e| e.to_string())?;
    let h =
        parse_head(&head[..n]).ok_or_else(|| String::from("the window head changed under us"))?;
    Ok((*at, h))
}

/// De config in het venster op `at`.
pub(crate) fn config(m: &mut dyn Medium, at: u64, h: Head) -> Result<Vec<u8>, String> {
    let mut c = vec![0u8; h.len];
    let n = m
        .read_at(at + h.head as u64, &mut c)
        .map_err(|e| e.to_string())?;
    if n != h.len {
        return Err(String::from("the window runs past the end"));
    }
    Ok(c)
}

/// Herschrijft het venster op `at` met `cfg`, en de FIP-checksums als het
/// venster in de monitor van een Sophgo-FIP ligt. Geeft de configlengte.
pub(crate) fn replace(m: &mut dyn Medium, at: u64, h: Head, cfg: &[u8]) -> Result<usize, String> {
    let w = make_window(cfg, h.size)?;
    let len = parse_head(&w).map_or(0, |h| h.len);
    m.write_at(at, &w).map_err(|e| e.to_string())?;
    fip_fix(m, at, h.size).map_err(|e| format!("fip: {e}"))?;
    Ok(len)
}

// --- Sophgo-FIP (LicheeRV): de checksums over de monitor-payload ----------

/// CRC-16/XMODEM (Python's `binascii.crc_hqx` met 0), zoals fiptool.py.
fn crc_hqx(b: &[u8]) -> u16 {
    let mut crc = 0u16;
    for &c in b {
        crc ^= u16::from(c) << 8;
        for _ in 0..8 {
            crc = if crc & 0x8000 != 0 {
                (crc << 1) ^ 0x1021
            } else {
                crc << 1
            };
        }
    }
    crc
}

/// Een FIP-checksum: de CRC little-endian, dan 0xCAFE.
fn fip_cksum(b: &[u8]) -> [u8; 4] {
    let c = crc_hqx(b).to_le_bytes();
    [c[0], c[1], 0xFE, 0xCA]
}

fn le32(b: &[u8], at: usize) -> u32 {
    let mut w = [0u8; 4];
    if let Some(s) = b.get(at..at + 4) {
        w.copy_from_slice(s);
    }
    u32::from_le_bytes(w)
}

/// Herstelt MONITOR_CKSUM (param2 + 48) en PARAM2_CKSUM (param2 + 8) als
/// het venster `[win, win + size)` in de monitor-payload ligt (Go:
/// `fipFix`, de layout van fiptool.py). Geen FIP: niets.
fn fip_fix(m: &mut dyn Medium, win: u64, size: usize) -> Result<(), String> {
    let e = |e: io::Error| e.to_string();
    let p2s = scan(m, b"CVLD02\n\0", &|b| {
        let ra = le32(b, 60);
        b.len() >= 64 && (ra == 0 || (0x8000_0000..0x9000_0000).contains(&ra))
    })
    .map_err(e)?;
    let p2 = match p2s.as_slice() {
        [] => return Ok(()),
        [p] => *p,
        _ => return Err(format!("{} param2 blocks, expected 1", p2s.len())),
    };
    let mut param2 = vec![0u8; 4096];
    let n = m.read_at(p2, &mut param2).map_err(e)?;
    param2.truncate(n);
    let start = scan(m, b"CVBL01\n\0", &|_| true)
        .map_err(e)?
        .into_iter()
        .filter(|&s| s <= p2)
        .max()
        .ok_or_else(|| format!("param2 at {p2:#x} but no fip header (CVBL01) before it"))?;
    let mon = start + u64::from(le32(&param2, 52));
    let mon_size = le32(&param2, 56) as usize;
    if win < mon || win + size as u64 > mon + mon_size as u64 {
        return Ok(());
    }
    let mut monitor = vec![0u8; mon_size];
    if m.read_at(mon, &mut monitor).map_err(e)? != mon_size {
        return Err(String::from("the monitor runs past the end"));
    }
    if param2.len() < 64 {
        return Err(String::from("param2 is cut short"));
    }
    param2[48..52].copy_from_slice(&fip_cksum(&monitor));
    // PARAM2_CKSUM dekt param2 vanaf byte 12, de verse MONITOR_CKSUM incluis.
    let c = fip_cksum(&param2[12..]);
    param2[8..12].copy_from_slice(&c);
    m.write_at(p2 + 8, &param2[8..52]).map_err(e)
}

// --- het commando -----------------------------------------------------------

/// De vlaggen van `hop image`.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Flags {
    pub(crate) target: String,
    pub(crate) config: Option<String>,
    pub(crate) write: Option<String>,
    pub(crate) keep: bool,
    pub(crate) force: bool,
}

pub(crate) fn parse(args: &[String]) -> Result<Flags, String> {
    let mut f = Flags::default();
    let mut it = args.iter();
    while let Some(a) = it.next() {
        let (name, inline) = match a.split_once('=') {
            Some((n, v)) if n.starts_with('-') => (n, Some(v.to_string())),
            _ => (a.as_str(), None),
        };
        let mut value = || {
            inline
                .clone()
                .or_else(|| it.next().cloned())
                .ok_or_else(|| format!("{name} needs a value"))
        };
        match name {
            "--config" | "-config" => f.config = Some(value()?),
            "--write" | "-write" => f.write = Some(value()?),
            "--keep" | "-keep" => f.keep = true,
            "--force" | "-force" => f.force = true,
            "--help" | "-h" | "help" => return Err(String::from(USAGE)),
            s if s.starts_with('-') => return Err(format!("image: unknown flag {s}\n{USAGE}")),
            _ if f.target.is_empty() => f.target = a.clone(),
            _ => return Err(format!("image takes one file or device\n{USAGE}")),
        }
    }
    if f.target.is_empty() {
        return Err(String::from(USAGE));
    }
    if f.keep && f.config.is_some() {
        return Err(String::from("--keep and --config exclude each other"));
    }
    if f.keep && f.write.is_none() {
        return Err(String::from("--keep needs --write <device>"));
    }
    Ok(f)
}

pub(crate) fn run(args: &[String]) -> Result<(), String> {
    let f = parse(args)?;
    match (&f.write, &f.config) {
        (Some(dev), _) => write(&f, dev),
        (None, Some(cfg)) => set(&f.target, cfg, f.force),
        (None, None) => show(&f.target),
    }
}

fn open(path: &str, rw: bool) -> Result<Disk, String> {
    OpenOptions::new()
        .read(true)
        .write(rw)
        .open(path)
        .map(Disk)
        .map_err(|e| format!("{path}: {e}"))
}

fn show(path: &str) -> Result<(), String> {
    let mut d = open(path, false)?;
    let (at, h) = find(&mut d).map_err(|e| format!("{path}: {e}"))?;
    let c = config(&mut d, at, h)?;
    eprintln!(
        "{path}: config window of {} bytes at {at:#x}, {} bytes of config",
        h.size, h.len
    );
    io::stdout().write_all(&c).map_err(|e| e.to_string())
}

fn set(path: &str, cfg: &str, force: bool) -> Result<(), String> {
    let text = std::fs::read(cfg).map_err(|e| format!("{cfg}: {e}"))?;
    if is_device(path) {
        check_removable(path, force)?;
        unmount(path)?;
    }
    let mut d = open(path, true)?;
    let (at, h) = find(&mut d).map_err(|e| format!("{path}: {e}"))?;
    let n = replace(&mut d, at, h, &text).map_err(|e| format!("{path}: {e}"))?;
    sync(&d.0, path)?;
    drop(d);
    eprintln!("{path}: {cfg} ({n} bytes) in the config window at {at:#x}");
    if !is_device(path) {
        let sum = hex(&auth::sha256(
            &std::fs::read(path).map_err(|e| format!("{path}: {e}"))?,
        ));
        let side = format!("{path}.sha256");
        if std::path::Path::new(&side).exists() {
            std::fs::write(&side, format!("{sum}\n")).map_err(|e| format!("{side}: {e}"))?;
            eprintln!("{side} rewritten");
        }
        println!("{sum}  {path}");
    }
    Ok(())
}

fn write(f: &Flags, dev: &str) -> Result<(), String> {
    if f.target.ends_with(".gz") {
        return Err(format!("{}: unpack it first (gunzip)", f.target));
    }
    let mut img = std::fs::read(&f.target).map_err(|e| format!("{}: {e}", f.target))?;
    let (at, h) = find(&mut img).map_err(|e| format!("{}: {e}", f.target))?;
    let device = is_device(dev);
    if device {
        check_removable(dev, f.force)?;
    }
    // De config: de opgegeven, die van de kaart (--keep), of die van het image.
    let text = match (&f.config, f.keep) {
        (Some(c), _) => Some(std::fs::read(c).map_err(|e| format!("{c}: {e}"))?),
        (None, true) => {
            let mut d = open(dev, false)?;
            let (dat, dh) = find(&mut d).map_err(|e| format!("{dev}: nothing to keep: {e}"))?;
            Some(config(&mut d, dat, dh)?)
        }
        (None, false) => None,
    };
    if let Some(t) = &text {
        let n = replace(&mut img, at, h, t)?;
        eprintln!("{}: {n} bytes of config in the window at {at:#x}", f.target);
    }
    if device {
        unmount(dev)?;
    }
    // Een rauw device neemt alleen hele sectoren: nullen erachter.
    if device {
        img.resize((img.len() as u64).next_multiple_of(SECTOR) as usize, 0);
    }
    let mut out = if device {
        open(dev, true)?.0
    } else {
        File::create(dev).map_err(|e| format!("{dev}: {e}"))?
    };
    for (i, chunk) in img.chunks(CHUNK).enumerate() {
        out.write_all(chunk).map_err(|e| format!("{dev}: {e}"))?;
        eprint!(
            "\r{dev}: {} of {} MiB",
            ((i + 1) * CHUNK).min(img.len()) >> 20,
            img.len() >> 20
        );
    }
    eprintln!();
    sync(&out, dev)?;
    drop(out);
    // De proef: alles terug lezen en vergelijken, op dezelfde bytes die
    // erheen gingen (de config incluis).
    let mut d = open(dev, false)?;
    let mut back = vec![0u8; CHUNK];
    let mut off = 0usize;
    while off < img.len() {
        let want = &img[off..(off + CHUNK).min(img.len())];
        let n = d
            .read_at(off as u64, &mut back[..want.len()])
            .map_err(|e| format!("{dev}: {e}"))?;
        if n != want.len() || back[..n] != *want {
            return Err(format!("{dev}: verify failed in the MiB at {}", off >> 20));
        }
        off += want.len();
    }
    eprintln!(
        "{dev}: {} written and verified ({} bytes){}",
        f.target,
        img.len(),
        match (&f.config, f.keep) {
            (Some(c), _) => format!(", config {c}"),
            (None, true) => String::from(", the config of the card kept"),
            (None, false) => String::from(", the config of the image"),
        }
    );
    Ok(())
}

/// Een fsync die een rauw device verdraagt: `/dev/rdiskN` is een character
/// device en geeft ENOTTY (Go, 06-08); daar gaat het schrijven ongebufferd.
fn sync(f: &File, path: &str) -> Result<(), String> {
    match f.sync_all() {
        Err(e) if !is_device(path) => Err(format!("{path}: {e}")),
        _ => Ok(()),
    }
}

fn is_device(path: &str) -> bool {
    path.starts_with("/dev/")
}

/// Weigert een device dat niet uitneembaar lijkt (de systeemschijf), tenzij
/// `--force`. macOS: `diskutil info`; Linux: `/sys/block/<naam>/removable`.
fn check_removable(dev: &str, force: bool) -> Result<(), String> {
    if force {
        return Ok(());
    }
    let ok = if cfg!(target_os = "macos") {
        let disk = dev.replace("/dev/rdisk", "/dev/disk");
        std::process::Command::new("diskutil")
            .args(["info", &disk])
            .output()
            .ok()
            .is_some_and(|o| {
                let t = String::from_utf8_lossy(&o.stdout);
                t.lines().any(|l| {
                    let l = l.trim();
                    l.starts_with("Device Location:") && l.ends_with("External")
                        || l.starts_with("Removable Media:") && l.ends_with("Removable")
                })
            })
    } else {
        let name = dev.trim_start_matches("/dev/");
        std::fs::read_to_string(format!("/sys/block/{name}/removable"))
            .is_ok_and(|s| s.trim() == "1")
    };
    if ok {
        Ok(())
    } else {
        Err(format!(
            "{dev} does not look like a removable card or stick; --force writes it anyway"
        ))
    }
}

/// macOS: de volumes van de kaart eraf (`diskutil unmountDisk`), anders
/// schrijft het OS er tijdens het flashen zelf in.
fn unmount(dev: &str) -> Result<(), String> {
    if !cfg!(target_os = "macos") {
        return Ok(());
    }
    let disk = dev.replace("/dev/rdisk", "/dev/disk");
    let st = std::process::Command::new("diskutil")
        .args(["unmountDisk", &disk])
        .status()
        .map_err(|e| format!("diskutil: {e}"))?;
    if st.success() {
        Ok(())
    } else {
        Err(format!("diskutil unmountDisk {disk} failed"))
    }
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

#[cfg(test)]
mod tests;
