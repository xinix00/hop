//! De configuratie uit de env van de jobspec, puur en op de host getoetst.

use alloc::string::String;

/// De verkeerspoort zonder `ER_PORT_HTTP`.
pub(crate) const DEFAULT_HTTP: u16 = 80;

/// De admin-poort zonder `ER_PORT_ADMIN`.
pub(crate) const DEFAULT_ADMIN: u16 = 9091;

/// De agent zonder `HOPLB_AGENT`: de leader-API van Hop in zijn eigen slot.
pub(crate) const DEFAULT_AGENT: &str = "http://HOP:9080";

/// De naam die voor het slot van Hop staat.
pub(crate) const HOP_NAME: &str = "HOP";

/// Het slot van Hop (de eerste bewoner).
pub(crate) const HOP_SLOT: u64 = 1;

/// Een poort uit de env; zonder of bij onzin `default` (en `Err` met de
/// waarde, zodat de aanroeper het luid kan zeggen).
pub(crate) fn port(env: Option<&str>, default: u16) -> Result<u16, u16> {
    match env.map(str::parse::<u16>) {
        None => Ok(default),
        Some(Ok(p)) if p != 0 => Ok(p),
        Some(_) => Err(default),
    }
}

/// De agent-URL: `HOP` als host wordt het adres van het slot van Hop,
/// zonder schema komt er `http://` voor (Go: `HOP_ADDR` zonder `://`).
pub(crate) fn agent_url(env: Option<&str>, hop_ip: [u8; 4]) -> String {
    let raw = env.filter(|s| !s.is_empty()).unwrap_or(DEFAULT_AGENT);
    let (scheme, rest) = match raw.split_once("://") {
        Some((s, r)) => (s, r),
        None => ("http", raw),
    };
    let (host, tail) = match rest.find([':', '/']) {
        Some(i) => rest.split_at(i),
        None => (rest, ""),
    };
    let [a, b, c, d] = hop_ip;
    if host == HOP_NAME {
        alloc::format!("{scheme}://{a}.{b}.{c}.{d}{tail}")
    } else {
        alloc::format!("{scheme}://{rest}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ports_come_from_the_env() {
        assert_eq!(port(None, 80), Ok(80));
        assert_eq!(port(Some("8081"), 80), Ok(8081));
        assert_eq!(port(Some("0"), 80), Err(80));
        assert_eq!(port(Some("http"), 9091), Err(9091));
    }

    #[test]
    fn hop_is_the_slot_of_hop() {
        let ip = [10, 100, 0, 2];
        assert_eq!(agent_url(None, ip), "http://10.100.0.2:9080");
        assert_eq!(agent_url(Some(""), ip), "http://10.100.0.2:9080");
        assert_eq!(
            agent_url(Some("http://HOP:8080/x"), ip),
            "http://10.100.0.2:8080/x"
        );
        assert_eq!(agent_url(Some("HOP:9080"), ip), "http://10.100.0.2:9080");
        assert_eq!(agent_url(Some("10.0.0.5:9080"), ip), "http://10.0.0.5:9080");
        assert_eq!(
            agent_url(Some("https://leader.example.com"), ip),
            "https://leader.example.com"
        );
    }
}
