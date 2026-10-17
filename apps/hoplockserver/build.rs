//! Linkt de bewoner met het app-script van applib.
//!
//! applib zet `hopapp.ld` in een zoekpad dat meereist naar deze link (zie
//! applib/build.rs in HopOS); hier alleen de vlag, alleen voor een
//! bare-metal target en alleen voor de bewoner: de host-bin linkt gewoon.

use std::env;

fn main() {
    if env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("none") {
        println!("cargo:rustc-link-arg-bin=hoplockserver-hopos=-Thopapp.ld");
    }
    println!("cargo:rerun-if-changed=build.rs");
}
