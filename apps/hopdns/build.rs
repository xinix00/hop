//! Linkt de bewoner met het app-script van applib.
//!
//! applib zet `hopapp.ld` in een zoekpad dat meereist naar deze link (zie
//! applib/build.rs in HopOS); hier alleen de vlag, alleen voor de bewoner en
//! alleen voor een bare-metal target. Op de host is `hopdns-hopos` een lege
//! `main` die de bewoner alleen typecheckt.

use std::env;

fn main() {
    if env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("none") {
        println!("cargo:rustc-link-arg-bin=hopdns-hopos=-Thopapp.ld");
    }
    println!("cargo:rerun-if-changed=build.rs");
}
