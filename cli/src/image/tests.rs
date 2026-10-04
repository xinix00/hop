//! De toetsen van `hop image`: het formaat (Go's `makeWindow` en de kern
//! van HopOS), de zoektocht, het herschrijven, de FIP van de LicheeRV, een
//! bestand dat niet op een sectorgrens eindigt, en de som van een bundel.

use super::*;

/// Het lege venster zoals de linker van HopOS het neerlegt
/// (`board/src/cfgwin.rs`): kopregel, en regels van 64 '#' plus newline.
fn empty(size: usize) -> Vec<u8> {
    let mut w = format!("#HOPCFG1 window={size} len=0000000000\n").into_bytes();
    while w.len() < size {
        let n = (size - w.len()).min(65);
        w.extend(std::iter::repeat_n(b'#', n - 1));
        w.push(b'\n');
    }
    w
}

/// Een image: rommel, een venster op een oneven plek, rommel.
fn image(at: usize, tail: usize) -> Vec<u8> {
    let mut img: Vec<u8> = (0..at).map(|i| (i * 7 % 251) as u8).collect();
    img.extend_from_slice(&empty(16384));
    img.extend(std::iter::repeat_n(0xaa, tail));
    img
}

fn tmp(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("hop-image-{}-{name}", std::process::id()))
}

#[test]
fn the_window_is_gos_window() {
    let w = make_window(b"hopos.node=hop-1", 1024).unwrap();
    assert_eq!(w.len(), 1024);
    assert!(w.starts_with(b"#HOPCFG1 window=1024 len=0000000017\nhopos.node=hop-1\n####"));
    assert_eq!(
        parse_head(&w),
        Some(Head {
            size: 1024,
            head: 36,
            len: 17
        })
    );
    // De padding: regels van 64 '#' en een newline, de laatste korter.
    let pad = &w[36 + 17..];
    assert!(
        pad.split(|&b| b == b'\n')
            .all(|l| l.len() <= 64 && l.iter().all(|&b| b == b'#'))
    );
    assert_eq!(w.last(), Some(&b'\n'));
    // Een leeg venster is byte voor byte dat van de kern.
    assert_eq!(make_window(b"", 16384).unwrap(), empty(16384));
    // Een venster als config: eerst strippen (Go: idempotent).
    assert_eq!(make_window(&w, 1024).unwrap(), w);
    assert_eq!(make_window(&w, 2048).unwrap()[36..53], w[36..53]);
    // De kopregel van de kern (16384, vijf cijfers) is 37 bytes.
    assert_eq!(parse_head(&empty(16384)).unwrap().head, 37);
}

#[test]
fn what_the_kernel_would_ignore_is_refused() {
    let big = vec![b'x'; 16384 - 37];
    assert!(
        make_window(&big, 16384)
            .unwrap_err()
            .contains("does not fit")
    );
    assert!(
        make_window(&big[..16384 - 38], 16384).is_ok(),
        "met de newline past het net"
    );
    assert!(
        make_window(b"hopos.node=a\0b\n", 1024)
            .unwrap_err()
            .contains("NUL")
    );
    assert!(
        make_window(b"hopos.node=\xff\n", 1024)
            .unwrap_err()
            .contains("UTF-8")
    );
}

#[test]
fn a_crooked_head_is_no_window() {
    let w = make_window(b"x=1\n", 1024).unwrap();
    let s = String::from_utf8(w).unwrap();
    for (from, to) in [
        ("window=1024", "window=1000"),
        ("window=1024", "window=10x4"),
        ("len=0000000004", "len=000000004"),
        ("len=0000000004", "len=0000002000"),
        ("len=0000000004\n", "len=0000000004 "),
    ] {
        assert_eq!(parse_head(s.replacen(from, to, 1).as_bytes()), None, "{to}");
    }
}

#[test]
fn find_wants_exactly_one_window_at_any_byte() {
    let mut img = image(0x1235, 100);
    assert_eq!(find(&mut img).unwrap().0, 0x1235);
    // Een losse magic zonder geldige kopregel (een string in .rodata) telt niet.
    let mut stray = b"#HOPCFG1 window=".to_vec();
    stray.extend_from_slice(&img);
    assert_eq!(find(&mut stray).unwrap().0, 0x1235 + 16);
    // Twee vensters: weigeren; geen: weigeren.
    let mut two = img.clone();
    two.extend_from_slice(&empty(16384));
    assert!(find(&mut two).unwrap_err().contains("2 config windows"));
    let mut none = vec![0u8; 4096];
    assert!(find(&mut none).unwrap_err().contains("no config window"));
}

#[test]
fn a_window_across_the_chunk_border_is_found() {
    let at = CHUNK - 10;
    let mut img = image(at, 64);
    assert_eq!(find(&mut img).unwrap().0, at as u64);
}

#[test]
fn replace_writes_the_window_and_nothing_else() {
    let mut img = image(0x1235, 100);
    let before = img.clone();
    let (at, h) = find(&mut img).unwrap();
    assert_eq!(
        replace(&mut img, at, h, b"hopos.node=lrv\nhopos.apikey=k").unwrap(),
        30
    );
    let (at2, h2) = find(&mut img).unwrap();
    assert_eq!((at2, h2.size, h2.len), (at, 16384, 30));
    assert_eq!(
        config(&mut img, at, h2).unwrap(),
        b"hopos.node=lrv\nhopos.apikey=k\n"
    );
    assert_eq!(img.len(), before.len());
    assert_eq!(img[..0x1235], before[..0x1235]);
    assert_eq!(img[0x1235 + 16384..], before[0x1235 + 16384..]);
    // En weer leeg.
    replace(&mut img, at, h2, b"").unwrap();
    assert_eq!(img, before);
}

#[test]
fn the_crc_is_xmodem() {
    // De controlewaarde van CRC-16/XMODEM.
    assert_eq!(crc_hqx(b"123456789"), 0x31C3);
    assert_eq!(fip_cksum(b"123456789"), [0xC3, 0x31, 0xFE, 0xCA]);
}

/// Een FIP zoals fiptool.py hem legt: "CVBL01" vooraan, param2 ("CVLD02")
/// met de monitor op +52/+56, en de monitor (de kern) met het venster erin.
fn fip(win_at: usize) -> (Vec<u8>, usize, usize, usize) {
    let (p2, mon, mon_size) = (0x1000, 0x2000, 0x8000);
    let mut b = vec![0u8; mon + mon_size + 0x100];
    b[..8].copy_from_slice(b"CVBL01\n\0");
    b[p2..p2 + 8].copy_from_slice(b"CVLD02\n\0");
    b[p2 + 52..p2 + 56].copy_from_slice(&(mon as u32).to_le_bytes());
    b[p2 + 56..p2 + 60].copy_from_slice(&(mon_size as u32).to_le_bytes());
    for (i, x) in b[mon..mon + mon_size].iter_mut().enumerate() {
        *x = (i % 13) as u8;
    }
    b[mon + win_at..mon + win_at + 16384].copy_from_slice(&empty(16384));
    (b, p2, mon, mon_size)
}

#[test]
fn a_window_in_the_licheerv_monitor_fixes_the_fip_checksums() {
    let (mut b, p2, mon, size) = fip(0x123);
    let (at, h) = find(&mut b).unwrap();
    assert_eq!(at, (mon + 0x123) as u64);
    replace(&mut b, at, h, b"hopos.node=lrv\n").unwrap();
    assert_eq!(b[p2 + 48..p2 + 52], fip_cksum(&b[mon..mon + size]));
    assert_eq!(b[p2 + 8..p2 + 12], fip_cksum(&b[p2 + 12..p2 + 4096]));
    // Zonder FIP (een kaal image) blijft alles buiten het venster staan.
    let mut img = image(0x10, 0);
    let (at, h) = find(&mut img).unwrap();
    replace(&mut img, at, h, b"x=1\n").unwrap();
    assert_eq!(img[..0x10], image(0x10, 0)[..0x10]);
}

#[test]
fn a_file_that_ends_off_a_sector_keeps_its_length() {
    let p = tmp("odd.img");
    let img = image(700, 333);
    std::fs::write(&p, &img).unwrap();
    let path = p.to_str().unwrap();
    let cfg = tmp("odd.cfg");
    std::fs::write(&cfg, "hopos.node=pi4\n").unwrap();
    set(path, cfg.to_str().unwrap(), false).unwrap();
    let got = std::fs::read(&p).unwrap();
    assert_eq!(got.len(), img.len());
    let mut d = open(path, false).unwrap();
    let (at, h) = find(&mut d).unwrap();
    assert_eq!(at, 700);
    assert_eq!(config(&mut d, at, h).unwrap(), b"hopos.node=pi4\n");
    assert_eq!(got[..700], img[..700]);
    assert_eq!(got[700 + 16384..], img[700 + 16384..]);
    std::fs::remove_file(&p).unwrap();
    std::fs::remove_file(&cfg).unwrap();
}

#[test]
fn a_bundle_gets_its_new_sum_next_to_it() {
    let p = tmp("b.flip");
    let side = tmp("b.flip.sha256");
    std::fs::write(&p, image(64, 64)).unwrap();
    std::fs::write(&side, "oud\n").unwrap();
    let cfg = tmp("b.cfg");
    std::fs::write(&cfg, "hopos.node=m4\n").unwrap();
    set(p.to_str().unwrap(), cfg.to_str().unwrap(), false).unwrap();
    let sum = hex(&auth::sha256(&std::fs::read(&p).unwrap()));
    assert_eq!(std::fs::read_to_string(&side).unwrap(), format!("{sum}\n"));
    for f in [&p, &side, &cfg] {
        std::fs::remove_file(f).unwrap();
    }
}

#[test]
fn write_puts_the_config_in_and_verifies() {
    let src = tmp("w.img");
    let dst = tmp("w.out");
    let cfg = tmp("w.cfg");
    std::fs::write(&src, image(4096, 4096)).unwrap();
    std::fs::write(&cfg, "hopos.node=radxa-2\n").unwrap();
    let args: Vec<String> = [
        src.to_str().unwrap(),
        "--config",
        cfg.to_str().unwrap(),
        "--write",
        dst.to_str().unwrap(),
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    run(&args).unwrap();
    let mut out = std::fs::read(&dst).unwrap();
    let (at, h) = find(&mut out).unwrap();
    assert_eq!(config(&mut out, at, h).unwrap(), b"hopos.node=radxa-2\n");
    // Het bronimage zelf is niet veranderd.
    let mut img = std::fs::read(&src).unwrap();
    assert_eq!(find(&mut img).unwrap().1.len, 0);
    for f in [&src, &dst, &cfg] {
        std::fs::remove_file(f).unwrap();
    }
}

#[test]
fn the_flags() {
    let s = |v: &[&str]| v.iter().map(|x| x.to_string()).collect::<Vec<_>>();
    assert_eq!(
        parse(&s(&["a.img", "--config=x.cfg", "--write", "/dev/rdisk4"])).unwrap(),
        Flags {
            target: "a.img".into(),
            config: Some("x.cfg".into()),
            write: Some("/dev/rdisk4".into()),
            keep: false,
            force: false,
        }
    );
    assert!(
        parse(&s(&["a.img", "--keep"])).is_err(),
        "--keep zonder --write"
    );
    assert!(parse(&s(&["a.img", "--keep", "--config", "x", "--write", "d"])).is_err());
    assert!(parse(&s(&["a.img", "b.img"])).is_err());
    assert!(parse(&s(&[])).is_err());
    assert!(parse(&s(&["a.img", "--bla"])).is_err());
}
