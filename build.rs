//! Linkt cloudflared-lean met het app-script van applib.
//!
//! applib zet `hopapp.ld` in een zoekpad dat meereist naar deze link (zie
//! applib/build.rs); hier alleen de vlag, en alleen voor een bare-metal
//! target. Op de host is dit een lege binary met de toetsen van de
//! protocollagen.

use std::env;

fn main() {
    if env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("none") {
        println!("cargo:rustc-link-arg-bins=-Thopapp.ld");
    }
    println!("cargo:rerun-if-changed=build.rs");
}
