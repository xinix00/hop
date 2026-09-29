//! De `ER_PORT_*`- en `ER_ATTR_*`-variabelen die elke taak krijgt.
//!
//! Bezit alleen de naamgeving; de runner beslist waar ze heen gaan.

use alloc::collections::BTreeMap;
use alloc::format;
use alloc::string::String;

/// Maakt van een naam een env-sleutel: hoofdletters, al het andere wordt `_`.
///
/// `node.os` wordt `NODE_OS`, `http-port` wordt `HTTP_PORT`.
pub fn env_key(name: &str) -> String {
    name.chars()
        .map(|c| match c {
            'a'..='z' => c.to_ascii_uppercase(),
            'A'..='Z' | '0'..='9' => c,
            _ => '_',
        })
        .collect()
}

/// Zet `ER_PORT_<NAAM>=<poort>` in `env` voor elke poort.
pub fn port_env_vars(ports: &BTreeMap<String, u16>, env: &mut BTreeMap<String, String>) {
    for (name, port) in ports {
        env.insert(format!("ER_PORT_{}", env_key(name)), format!("{port}"));
    }
}

/// Zet `ER_ATTR_<SLEUTEL>=<waarde>` in `env` voor elk node-attribuut.
pub fn attr_env_vars(attrs: &BTreeMap<String, String>, env: &mut BTreeMap<String, String>) {
    for (name, val) in attrs {
        env.insert(format!("ER_ATTR_{}", env_key(name)), val.clone());
    }
}
