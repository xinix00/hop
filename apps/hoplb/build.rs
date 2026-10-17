//! Linkt de bewoner met het app-script van applib.
//!
//! applib zet `hopapp.ld` in een zoekpad dat meereist naar deze link (zie
//! applib/build.rs in HopOS); hier alleen de vlag, en alleen voor een
//! bare-metal target. Op de host bouwt alleen de host-bin, of is
//! `hoplb-hopos` een lege `main` die de bewoner typecheckt.

use std::env;

fn main() {
    if env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("none") {
        println!("cargo:rustc-link-arg-bins=-Thopapp.ld");
    }
    println!("cargo:rerun-if-changed=build.rs");
}
