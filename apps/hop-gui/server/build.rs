//! Maakt de tabel van ingebakken bestanden, en linkt de HopOS-app met het app-script van applib.
//!
//! Het dashboard ligt één map hoger: `index.html`, `app.js`,
//! `appearance.js`, `style.css` en alles in `vendor/`. Dit script schrijft
//! per bestand één regel `File { ... include_bytes!(...) ... }` naar
//! `$OUT_DIR/files.rs`, met het content-type op de extensie en een ETag uit
//! de inhoud (FNV-1a 64). Zo komt een nieuw bestand in `vendor/` vanzelf
//! mee, en is de ETag een getal van de build in plaats van een lus van vier
//! megabyte in `const`-evaluatie. Het script leest alleen die bestanden en
//! praat met niemand (handboek §8).
//!
//! Voor een bare-metal target zet het ook `-Thopapp.ld` op de link van de
//! binaries: applib legt het script in een zoekpad dat meereist (zoals
//! `apps/welcome` in HopOS).

use std::env;
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};

/// De bestanden aan de root van het dashboard, in de volgorde van de tabel.
const ROOT_FILES: [&str; 4] = ["index.html", "app.js", "appearance.js", "style.css"];

fn main() {
    if env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("none") {
        println!("cargo:rustc-link-arg-bins=-Thopapp.ld");
    }
    println!("cargo:rerun-if-changed=build.rs");
    let manifest = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap_or_default());
    let root = manifest.join("..");
    let mut files: Vec<(String, PathBuf)> = ROOT_FILES
        .iter()
        .map(|n| (format!("/{n}"), root.join(n)))
        .collect();
    let vendor = root.join("vendor");
    println!("cargo:rerun-if-changed={}", vendor.display());
    let mut names: Vec<String> = match fs::read_dir(&vendor) {
        Ok(rd) => rd
            .filter_map(Result::ok)
            .filter(|e| e.file_type().is_ok_and(|t| t.is_file()))
            .filter_map(|e| e.file_name().into_string().ok())
            .filter(|n| !n.starts_with('.'))
            .collect(),
        Err(e) => fail(&format!("{}: {e}", vendor.display())),
    };
    names.sort();
    for n in names {
        files.push((format!("/vendor/{n}"), vendor.join(&n)));
    }
    let mut out = String::from("&[\n");
    for (url, path) in &files {
        println!("cargo:rerun-if-changed={}", path.display());
        let bytes = fs::read(path).unwrap_or_else(|e| fail(&format!("{}: {e}", path.display())));
        let abs = path.canonicalize().unwrap_or_else(|_| path.clone());
        let _ = writeln!(
            out,
            "    File {{ path: {url:?}, body: include_bytes!({:?}), content_type: {:?}, etag: \"\\\"{:016x}\\\"\" }},",
            abs.display().to_string(),
            content_type(path),
            fnv1a(&bytes),
        );
    }
    out.push_str("]\n");
    let dest = PathBuf::from(env::var_os("OUT_DIR").unwrap_or_default()).join("files.rs");
    if let Err(e) = fs::write(&dest, out) {
        fail(&format!("{}: {e}", dest.display()));
    }
}

/// Stopt de build met een reden.
fn fail(why: &str) -> ! {
    eprintln!("hop-gui build: {why}");
    std::process::exit(1)
}

/// FNV-1a 64 over de inhoud: een ETag hoeft niet cryptografisch te zijn,
/// alleen te veranderen als de inhoud verandert.
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// Het content-type op de extensie; onbekend is platte tekst (een licentie).
fn content_type(path: &Path) -> &'static str {
    match path.extension().and_then(|e| e.to_str()) {
        Some("html") => "text/html; charset=utf-8",
        Some("js") => "text/javascript; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("woff2") => "font/woff2",
        Some("svg") => "image/svg+xml",
        Some("png") => "image/png",
        Some("ico") => "image/x-icon",
        Some("json") => "application/json",
        Some("md") => "text/markdown; charset=utf-8",
        _ => "text/plain; charset=utf-8",
    }
}
