//! Wat de daemon bij de start vaststelt: vlaggen, config, node-id, IP en de maat van de host.
//!
//! Bezit alleen de afleiding; alles hier draait één keer, vóór de eerste
//! thread (handboek §2: eenmalige initialisatie in `main`). De regels zijn
//! die van `OLD/cmd/agent/main.go`: vlaggen gaan over de config heen, geen
//! lock-backend betekent standalone, en het node-id is config, dan
//! `data/node-id`, dan een nieuw id dat daar bewaard wordt.

use std::net::{Ipv4Addr, UdpSocket};
use std::path::{Path, PathBuf};
use std::process::Command;

use config::Config;

/// Hoe lang een gegenereerd node-id is: acht tekens, een naam die een mens
/// in een log leest en overtypt in een curl (Go: `leanrand.ID(8)`, 40 bits).
pub(crate) const NODE_ID_LEN: usize = 8;

/// De vlaggen van de daemon (Go: `cmd/agent`).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct Flags {
    pub(crate) config: String,
    pub(crate) node: String,
    pub(crate) cluster: String,
    pub(crate) lock: String,
    pub(crate) standalone: bool,
    pub(crate) api_key: String,
}

/// Leest de vlaggen (`--x v`, `--x=v`, en Go's enkele streep).
pub(crate) fn parse_flags(args: &[String]) -> Result<Flags, String> {
    let mut f = Flags::default();
    let mut it = args.iter();
    while let Some(a) = it.next() {
        let name = a
            .strip_prefix("--")
            .or_else(|| a.strip_prefix('-'))
            .ok_or_else(|| format!("unexpected argument {a:?}"))?;
        let (name, inline) = match name.split_once('=') {
            Some((n, v)) => (n, Some(v.to_string())),
            None => (name, None),
        };
        if name == "standalone" {
            f.standalone = inline.as_deref().is_none_or(|v| v == "true" || v == "1");
            continue;
        }
        let slot = match name {
            "config" => &mut f.config,
            "node" => &mut f.node,
            "cluster" => &mut f.cluster,
            "lock" => &mut f.lock,
            "api-key" => &mut f.api_key,
            "help" | "h" => return Err(String::from(USAGE)),
            other => return Err(format!("unknown flag --{other}\n{USAGE}")),
        };
        *slot = match inline {
            Some(v) => v,
            None => it
                .next()
                .cloned()
                .ok_or_else(|| format!("--{name} needs a value"))?,
        };
    }
    Ok(f)
}

/// De gebruiksregel.
pub(crate) const USAGE: &str = "Usage: agentd [--config FILE] [--node ID] [--cluster NAME] [--lock URL] [--standalone] [--api-key KEY]";

/// Leest de config; een ontbrekend bestand is de standaard (Go: `config.Load`).
pub(crate) fn load_config(path: &str) -> Result<(Config, bool), String> {
    if path.is_empty() {
        return Ok((Config::default(), false));
    }
    match std::fs::read(path) {
        Ok(data) => Config::from_json(&data)
            .map(|c| (c, true))
            .map_err(|e| format!("{path}: {e}")),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok((Config::default(), false)),
        Err(e) => Err(format!("{path}: {e}")),
    }
}

/// Zet de vlaggen over de config heen en beslist standalone, zoals Go's `main`.
pub(crate) fn apply_flags(cfg: &mut Config, f: &Flags) -> Result<bool, String> {
    if !f.cluster.is_empty() {
        cfg.cluster.name.clone_from(&f.cluster);
    }
    if !f.lock.is_empty() && !f.standalone {
        cfg.cluster.lock.kind = String::from("hoplockserver");
        cfg.cluster.lock.url.clone_from(&f.lock);
    }
    if !f.api_key.is_empty() {
        cfg.api_key.clone_from(&f.api_key);
    }
    if !f.node.is_empty() {
        cfg.node.id.clone_from(&f.node);
    }
    // Geen lock-backend: standalone (in geheugen). Hop is dan bruikbaar met
    // niets dan een clusternaam.
    let standalone = f.standalone || !store::lock_configured(&cfg.cluster.lock);
    if cfg.cluster.name.is_empty() {
        return Err(String::from(
            "cluster name required (use --cluster or config file)",
        ));
    }
    // Een staatbestand per cluster, zodat twee clusters op één host elkaar
    // niet overschrijven.
    if cfg.paths.state_file == "./data/state.json" {
        cfg.paths.state_file = format!("./data/state-{}.json", cfg.cluster.name);
    }
    Ok(standalone)
}

/// De map met het staatbestand: daar staan ook `node-id` en de agent-staat.
pub(crate) fn data_dir(cfg: &Config) -> PathBuf {
    Path::new(&cfg.paths.state_file)
        .parent()
        .map_or_else(|| PathBuf::from("."), Path::to_path_buf)
}

/// Een willekeurig id van `n` tekens uit `[a-z0-9]`.
fn random_id(n: usize) -> Result<String, String> {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let seed = hostnet::entropy().map_err(|e| format!("no entropy for a node id: {e}"))?;
    Ok(seed
        .iter()
        .take(n)
        .map(|b| {
            let i = usize::from(*b) % ALPHABET.len();
            char::from(ALPHABET.get(i).copied().unwrap_or(b'x'))
        })
        .collect())
}

/// Het node-id: config, dan het bewaarde `node-id`, dan een nieuw (Go: `getOrCreateNodeID`).
///
/// Geeft ook een regel voor de log (of `None`).
pub(crate) fn node_id(cfg: &Config) -> Result<(String, Option<String>), String> {
    if !cfg.node.id.is_empty() {
        return Ok((cfg.node.id.clone(), None));
    }
    let dir = data_dir(cfg);
    let file = dir.join("node-id");
    if let Ok(data) = std::fs::read_to_string(&file) {
        let id = data.trim();
        if !id.is_empty() {
            return Ok((
                id.to_string(),
                Some(format!("hop: using persisted node ID {id}")),
            ));
        }
    }
    let id = random_id(NODE_ID_LEN)?;
    let note = match std::fs::create_dir_all(&dir).and_then(|()| std::fs::write(&file, &id)) {
        Ok(()) => format!("hop: generated and persisted node ID {id}"),
        Err(e) => format!(
            "hop: WARNING node ID {id} not persisted to {}: {e}",
            file.display()
        ),
    };
    Ok((id, Some(note)))
}

/// Het IP waarmee een UDP-socket naar buiten zou gaan (Go: `getOutboundIP`).
///
/// Een UDP-`connect` stuurt niets; hij laat alleen de kernel de route
/// kiezen, en het lokale adres daarvan is het adres dat anderen zien.
pub(crate) fn outbound_ip() -> Option<Ipv4Addr> {
    let s = UdpSocket::bind("0.0.0.0:0").ok()?;
    s.connect("8.8.8.8:80").ok()?;
    match s.local_addr().ok()?.ip() {
        std::net::IpAddr::V4(ip) if !ip.is_unspecified() => Some(ip),
        _ => None,
    }
}

/// Een CIDR (`10.0.0.0/24`) als adres en masker.
pub(crate) fn parse_cidr(cidr: &str) -> Option<(u32, u32)> {
    let (ip, bits) = cidr.split_once('/')?;
    let ip: Ipv4Addr = ip.parse().ok()?;
    let bits: u32 = bits.parse().ok().filter(|b| *b <= 32)?;
    let mask = if bits == 0 {
        0
    } else {
        u32::MAX << (32 - bits)
    };
    Some((u32::from(ip) & mask, mask))
}

/// Of `ip` binnen het netwerk van `cidr` valt.
pub(crate) fn in_cidr(ip: Ipv4Addr, net: (u32, u32)) -> bool {
    u32::from(ip) & net.1 == net.0
}

/// De IPv4-adressen van de interfaces, uit `ip -o -4 addr` (Linux) of `ifconfig` (macOS).
///
/// std heeft geen `getifaddrs`, en FFI zou `unsafe` zijn in een crate die
/// dat niet draagt; de twee systeemcommando's zijn er op elke host.
fn interface_ips() -> Vec<Ipv4Addr> {
    let out = Command::new("ip")
        .args(["-o", "-4", "addr", "show"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .or_else(|| Command::new("ifconfig").output().ok());
    let Some(out) = out else {
        return Vec::new();
    };
    let text = String::from_utf8_lossy(&out.stdout);
    let mut ips = Vec::new();
    for word in text.split_whitespace().collect::<Vec<_>>().windows(2) {
        if let [k, v] = word
            && *k == "inet"
        {
            let addr = v.split('/').next().unwrap_or(v);
            if let Ok(ip) = addr.parse() {
                ips.push(ip);
            }
        }
    }
    ips
}

/// Het eerste interface-IP binnen `cidr` (Go: `pickIPInNetwork`, zonder de wachtlus).
pub(crate) fn pick_ip_in_network(cidr: &str) -> Result<Ipv4Addr, String> {
    let net = parse_cidr(cidr).ok_or_else(|| format!("invalid CIDR {cidr:?}"))?;
    if let Some(ip) = outbound_ip().filter(|ip| in_cidr(*ip, net)) {
        return Ok(ip);
    }
    interface_ips()
        .into_iter()
        .find(|ip| in_cidr(*ip, net))
        .ok_or_else(|| format!("no interface IP found in {cidr}"))
}

/// Het aantal cores van de host.
pub(crate) fn cpu_cores() -> u32 {
    std::thread::available_parallelism()
        .ok()
        .and_then(|n| u32::try_from(n.get()).ok())
        .unwrap_or(1)
}

/// Het geheugen van de host in bytes (Linux `/proc/meminfo`, macOS `sysctl hw.memsize`).
pub(crate) fn memory_bytes() -> u64 {
    if let Ok(info) = std::fs::read_to_string("/proc/meminfo") {
        for line in info.lines() {
            if let Some(rest) = line.strip_prefix("MemTotal:") {
                let kb: u64 = rest
                    .trim()
                    .trim_end_matches("kB")
                    .trim()
                    .parse()
                    .unwrap_or(0);
                return kb.saturating_mul(1024);
            }
        }
    }
    Command::new("sysctl")
        .args(["-n", "hw.memsize"])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0)
}

/// De architectuur in Go's namen (`arm64`, `amd64`), zodat affinity-labels van v1 blijven passen.
pub(crate) fn go_arch() -> &'static str {
    match std::env::consts::ARCH {
        "aarch64" => "arm64",
        "x86_64" => "amd64",
        "riscv64" => "riscv64",
        other => other,
    }
}

/// Het OS in Go's namen (`linux`, `darwin`).
pub(crate) fn go_os() -> &'static str {
    match std::env::consts::OS {
        "macos" => "darwin",
        other => other,
    }
}

/// De heetste thermische zone in milligraden (Linux); 0 zonder sensor (Go: `temp_linux.go`).
pub(crate) fn cpu_temp_milli_c() -> i64 {
    let Ok(dir) = std::fs::read_dir("/sys/class/thermal") else {
        return 0;
    };
    dir.flatten()
        .filter(|e| e.file_name().to_string_lossy().starts_with("thermal_zone"))
        .filter_map(|e| std::fs::read_to_string(e.path().join("temp")).ok())
        .filter_map(|s| s.trim().parse::<i64>().ok())
        .max()
        .unwrap_or(0)
}
