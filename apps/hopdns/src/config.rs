//! De config: de vlaggen, de peers en het optionele YAML-bestand.
//!
//! Dezelfde vlaggen als de Go-daemon (Go's `flag`: `-naam waarde`,
//! `-naam=waarde`, ook met twee streepjes):
//!
//! | Vlag | Standaard | Betekenis |
//! | --- | --- | --- |
//! | `-listen` | `:5353` | het UDP-adres; `:53` voor gewone DNS |
//! | `-peer` | (verplicht, herhaalbaar) | een cluster-endpoint, `http://key@host:8080` mag |
//! | `-domain` | `hop.local` | het domein |
//! | `-api-key` | leeg | de HMAC-sleutel, tenzij de peer-URL een eigen `key@` draagt |
//! | `-config` | geen | een YAML-bestand met statische CNAMEs |
//!
//! De standaarden zijn die van de README (`:5353`, `hop.local`); de
//! Go-code zei `:8053` en `internal`, en de README won omdat dat het
//! contract met de gebruiker is.
//!
//! Het YAML-bestand kent één sleutel, `cnames`, met naam-doel-paren. De
//! lezer hier is een kleine deelverzameling van YAML (blokvorm, `#`-commentaar,
//! enkele en dubbele aanhalingstekens), genoeg voor dat ene bestand en
//! zonder een YAML-crate (handboek §8):
//!
//! ```yaml
//! cnames:
//!   mail.hop.local: mailserver.example.com
//!   "*.apps.hop.local": ingress.prod-eu.hop.local
//! ```

use alloc::string::String;
use alloc::vec::Vec;

use crate::{DEFAULT_DOMAIN, Error, Result};

/// Het luisteradres zonder `-listen`.
pub const DEFAULT_LISTEN: &str = ":5353";

/// Het gebruik, voor `-h`.
pub const USAGE: &str = "usage: hopdns -peer URL [-peer URL ...] [-listen ADDR] [-domain NAME] [-api-key KEY] [-config FILE]

  -listen ADDR    DNS address to listen on (default :5353, use :53 for standard DNS)
  -peer URL       cluster agent endpoint, repeatable (e.g. -peer http://host:8080 -peer http://key@host:8080)
  -domain NAME    DNS domain suffix (default hop.local)
  -api-key KEY    default API key (overridden by key@ in a peer URL)
  -config FILE    optional YAML config (static CNAMEs)";

/// De vlaggen van de daemon.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Flags {
    /// `-listen`.
    pub listen: String,
    /// Elke `-peer`, in volgorde.
    pub peers: Vec<String>,
    /// `-domain`.
    pub domain: String,
    /// `-api-key`.
    pub api_key: String,
    /// `-config`.
    pub config: Option<String>,
}

/// Een kopie van `s`, faalbaar.
pub(crate) fn owned(s: &str) -> Result<String> {
    let mut o = String::new();
    o.try_reserve_exact(s.len())?;
    o.push_str(s);
    Ok(o)
}

impl Flags {
    /// Leest de vlaggen uit `args` (zonder de programmanaam).
    ///
    /// Een fout noemt de plek van het argument, zodat de aanroeper het kan
    /// tonen. Zonder één `-peer` is het [`Error::NoPeer`], zoals Go's
    /// `log.Fatal`.
    pub fn parse<'a>(args: impl IntoIterator<Item = &'a str>) -> Result<Self> {
        let mut f = Flags {
            listen: owned(DEFAULT_LISTEN)?,
            peers: Vec::new(),
            domain: owned(DEFAULT_DOMAIN)?,
            api_key: String::new(),
            config: None,
        };
        let mut it = args.into_iter().enumerate();
        while let Some((at, arg)) = it.next() {
            let Some(flag) = arg.strip_prefix("--").or_else(|| arg.strip_prefix('-')) else {
                return Err(Error::UnexpectedArgument { at });
            };
            let (name, inline) = match flag.split_once('=') {
                Some((n, v)) => (n, Some(v)),
                None => (flag, None),
            };
            if matches!(name, "h" | "help") {
                return Err(Error::Help);
            }
            if !matches!(name, "listen" | "peer" | "domain" | "api-key" | "config") {
                return Err(Error::UnknownFlag { at });
            }
            let value = match inline {
                Some(v) => v,
                None => it
                    .next()
                    .map(|(_, v)| v)
                    .ok_or(Error::MissingValue { at })?,
            };
            match name {
                "listen" => f.listen = owned(value)?,
                "domain" => f.domain = owned(value)?,
                "api-key" => f.api_key = owned(value)?,
                "config" => f.config = Some(owned(value)?),
                _ => {
                    if !value.trim().is_empty() {
                        f.peers.try_reserve(1)?;
                        f.peers.push(owned(value.trim())?);
                    }
                }
            }
        }
        if f.peers.is_empty() {
            return Err(Error::NoPeer);
        }
        Ok(f)
    }
}

/// Het luisteradres als `(ip, poort)`: `:5353` is elk adres (`0.0.0.0`),
/// zoals Go's `net.Listen`.
pub fn parse_listen(s: &str) -> Result<(core::net::Ipv4Addr, u16)> {
    let (host, port) = s.rsplit_once(':').ok_or(Error::BadListen)?;
    let port: u16 = port.parse().map_err(|_| Error::BadListen)?;
    let ip = match host {
        "" => core::net::Ipv4Addr::UNSPECIFIED,
        "localhost" => core::net::Ipv4Addr::LOCALHOST,
        h => h.parse().map_err(|_| Error::BadListen)?,
    };
    Ok((ip, port))
}

/// Een peer: het endpoint zonder sleutel, en de sleutel waarmee hij tekent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Peer {
    /// `http://host:8080`, zonder `key@` en zonder slot-`/`.
    pub endpoint: String,
    /// De HMAC-sleutel; `None` is zonder authenticatie.
    pub api_key: Option<String>,
}

impl Peer {
    /// Leest een peer-URL. Een `key@` in de URL wint van `default_key`
    /// (Go's `parsePeer`); een lege sleutel is geen sleutel.
    pub fn parse(raw: &str, default_key: &str) -> Result<Self> {
        let raw = raw.trim();
        let (scheme, rest) = raw.split_once("://").ok_or(Error::BadPeer)?;
        if !(scheme.eq_ignore_ascii_case("http") || scheme.eq_ignore_ascii_case("https")) {
            return Err(Error::BadPeer);
        }
        let (authority, path) = match rest.find('/') {
            Some(i) => rest.split_at(i),
            None => (rest, ""),
        };
        let (key, host) = match authority.rsplit_once('@') {
            Some((user, host)) => (user.split(':').next().unwrap_or(""), host),
            None => (default_key, authority),
        };
        if host.is_empty() {
            return Err(Error::BadPeer);
        }
        let mut endpoint = String::new();
        endpoint.try_reserve_exact(scheme.len() + 3 + host.len() + path.len())?;
        endpoint.push_str(scheme);
        endpoint.push_str("://");
        endpoint.push_str(host);
        endpoint.push_str(path.trim_end_matches('/'));
        let api_key = if key.is_empty() {
            None
        } else {
            Some(owned(key)?)
        };
        Ok(Self { endpoint, api_key })
    }
}

/// De peers uit een lijst met komma's (de env van de bewoner:
/// `HOPDNS_PEER=http://a:9080,http://key@b:9080`), zonder lege.
pub fn peer_list(s: &str) -> impl Iterator<Item = &str> {
    s.split(',').map(str::trim).filter(|p| !p.is_empty())
}

/// `raw` met host `alias` vervangen door `ip`: de bewoner schrijft zijn
/// eigen Hop als `http://HOP:9080`, want het slot-adres van Hop is een feit
/// van de node en geen config. Een URL met een andere host komt
/// ongewijzigd terug.
pub fn with_host_alias(raw: &str, alias: &str, ip: [u8; 4]) -> Result<String> {
    let raw = raw.trim();
    let Some((scheme, rest)) = raw.split_once("://") else {
        return owned(raw);
    };
    let (userinfo, hostport) = match rest.split(['/', '?', '#']).next() {
        Some(auth) => match auth.rsplit_once('@') {
            Some((u, h)) => (Some(u), h),
            None => (None, auth),
        },
        None => (None, rest),
    };
    let host = hostport.split(':').next().unwrap_or(hostport);
    if host != alias {
        return owned(raw);
    }
    let after_host = rest
        .get(userinfo.map_or(0, |u| u.len() + 1) + host.len()..)
        .unwrap_or("");
    let [a, b, c, d] = ip;
    let mut out = String::new();
    out.try_reserve(raw.len() + 16)?;
    out.push_str(scheme);
    out.push_str("://");
    if let Some(u) = userinfo {
        out.push_str(u);
        out.push('@');
    }
    let _ = core::fmt::write(&mut out, format_args!("{a}.{b}.{c}.{d}"));
    out.push_str(after_host);
    Ok(out)
}

/// Het YAML-bestand.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Config {
    /// `cnames`: naam naar doel, in de volgorde van het bestand.
    pub cnames: Vec<(String, String)>,
}

/// In welke sectie van het bestand de lezer staat.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Section {
    /// Nog geen sleutel gezien.
    Top,
    /// Onder `cnames:`.
    Cnames,
    /// Onder een sleutel die hopdns niet kent (Go's yaml negeert die).
    Other,
}

impl Config {
    /// Leest het bestand.
    pub fn parse(text: &str) -> Result<Self> {
        let mut c = Config::default();
        let mut section = Section::Top;
        for (i, raw) in text.lines().enumerate() {
            let bad = Error::Yaml { line: i + 1 };
            let line = strip_comment(raw).trim_end();
            if line.trim().is_empty() || line == "---" {
                continue;
            }
            if line.starts_with('\t') {
                return Err(bad);
            }
            if !line.starts_with(' ') {
                let (key, rest) = split_key(line).ok_or(bad)?;
                let rest = rest.trim();
                section = if key == "cnames" {
                    if !(rest.is_empty() || rest == "{}") {
                        return Err(bad);
                    }
                    Section::Cnames
                } else {
                    Section::Other
                };
                continue;
            }
            match section {
                Section::Top => return Err(bad),
                Section::Other => {}
                Section::Cnames => {
                    let (key, value) = split_key(line.trim_start()).ok_or(bad)?;
                    let value = unquote(value.trim()).ok_or(bad)?;
                    c.cnames.try_reserve(1)?;
                    c.cnames.push((owned(key)?, value));
                }
            }
        }
        Ok(c)
    }

    /// Het doel van `name` in `cnames`, zoals het in het bestand staat (de
    /// laatste wint).
    #[must_use]
    pub fn cname(&self, name: &str) -> Option<&str> {
        self.cnames
            .iter()
            .rev()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }

    /// De [`crate::CNAMEs`] van dit bestand.
    pub fn cname_table(&self) -> Result<crate::CNAMEs> {
        crate::CNAMEs::new(self.cnames.iter().map(|(k, v)| (k.as_str(), v.as_str())))
    }
}

/// `line` zonder `#`-commentaar (een `#` aan het begin of na witruimte,
/// buiten aanhalingstekens).
fn strip_comment(line: &str) -> &str {
    let mut quote: Option<char> = None;
    let mut prev_space = true;
    for (i, ch) in line.char_indices() {
        match quote {
            Some(q) if ch == q => quote = None,
            Some(_) => {}
            None if ch == '"' || ch == '\'' => quote = Some(ch),
            None if ch == '#' && prev_space => return line.get(..i).unwrap_or(line),
            None => {}
        }
        prev_space = ch == ' ' || ch == '\t';
    }
    line
}

/// `key: rest` met een kale of geciteerde sleutel. De sleutel komt zonder
/// aanhalingstekens terug.
fn split_key(line: &str) -> Option<(&str, &str)> {
    let (key, after) = match line.chars().next()? {
        q @ ('"' | '\'') => {
            let body = line.get(1..)?;
            let end = body.find(q)?;
            (body.get(..end)?, body.get(end + 1..)?.trim_start())
        }
        _ => {
            // De eerste `:` gevolgd door een spatie of het einde.
            let i = line
                .char_indices()
                .find(|&(i, c)| {
                    c == ':' && matches!(line.get(i + 1..i + 2), None | Some(" ") | Some(""))
                })?
                .0;
            (line.get(..i)?.trim_end(), line.get(i..)?)
        }
    };
    let rest = after.strip_prefix(':')?;
    if !(rest.is_empty() || rest.starts_with(' ')) || key.is_empty() {
        return None;
    }
    Some((key, rest))
}

/// Een scalaire waarde: kaal, of tussen `"..."` (met `\"` en `\\`) of
/// `'...'` (met `''`).
fn unquote(v: &str) -> Option<String> {
    let mut out = String::new();
    out.try_reserve_exact(v.len()).ok()?;
    if let Some(body) = v.strip_prefix('"') {
        let body = body.strip_suffix('"')?;
        let mut chars = body.chars();
        while let Some(c) = chars.next() {
            if c == '\\' {
                out.push(match chars.next()? {
                    'n' => '\n',
                    't' => '\t',
                    other => other,
                });
            } else {
                out.push(c);
            }
        }
    } else if let Some(body) = v.strip_prefix('\'') {
        out.push_str(&body.strip_suffix('\'')?.replace("''", "'"));
    } else {
        if v.starts_with(['{', '[', '&', '|', '>']) {
            return None;
        }
        out.push_str(v);
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flags_like_go() {
        let f = Flags::parse([
            "-listen",
            ":53",
            "-peer",
            "http://127.0.0.1:8080",
            "--peer=http://key@10.0.1.100:8080",
            "-domain=example.internal",
            "-api-key",
            "secret",
        ])
        .unwrap();
        assert_eq!(f.listen, ":53");
        assert_eq!(
            f.peers,
            ["http://127.0.0.1:8080", "http://key@10.0.1.100:8080"]
        );
        assert_eq!(f.domain, "example.internal");
        assert_eq!(f.api_key, "secret");
        assert_eq!(f.config, None);
    }

    #[test]
    fn flags_defaults_and_refusals() {
        let f = Flags::parse(["-peer", "http://h:8080"]).unwrap();
        assert_eq!(f.listen, DEFAULT_LISTEN);
        assert_eq!(f.domain, DEFAULT_DOMAIN);
        assert_eq!(Flags::parse([]), Err(Error::NoPeer));
        assert_eq!(
            Flags::parse(["-peer", "http://h", "-bogus", "x"]),
            Err(Error::UnknownFlag { at: 2 })
        );
        assert_eq!(Flags::parse(["-peer"]), Err(Error::MissingValue { at: 0 }));
        assert_eq!(
            Flags::parse(["stray"]),
            Err(Error::UnexpectedArgument { at: 0 })
        );
        assert_eq!(Flags::parse(["-h"]), Err(Error::Help));
    }

    #[test]
    fn listen_addresses() {
        use core::net::Ipv4Addr;
        assert_eq!(parse_listen(":5353"), Ok((Ipv4Addr::UNSPECIFIED, 5353)));
        assert_eq!(parse_listen("127.0.0.1:53"), Ok((Ipv4Addr::LOCALHOST, 53)));
        assert_eq!(parse_listen("5353"), Err(Error::BadListen));
        assert_eq!(parse_listen(":99999"), Err(Error::BadListen));
    }

    // parsePeer (Go, cmd/hopdns): de sleutel uit de userinfo wint, het
    // endpoint verliest hem.
    #[test]
    fn parse_peer() {
        let p = Peer::parse("http://key@host:8080", "dflt").unwrap();
        assert_eq!(p.endpoint, "http://host:8080");
        assert_eq!(p.api_key.as_deref(), Some("key"));
        let p = Peer::parse("http://host:8080/", "dflt").unwrap();
        assert_eq!(p.endpoint, "http://host:8080");
        assert_eq!(p.api_key.as_deref(), Some("dflt"));
        let p = Peer::parse(" https://host ", "").unwrap();
        assert_eq!(p.endpoint, "https://host");
        assert_eq!(p.api_key, None);
        assert_eq!(Peer::parse("host:8080", ""), Err(Error::BadPeer));
        assert_eq!(Peer::parse("ftp://host", ""), Err(Error::BadPeer));
        let list: Vec<&str> = peer_list(" http://a:9080, ,http://b:9080 ").collect();
        assert_eq!(list, ["http://a:9080", "http://b:9080"]);
    }

    #[test]
    fn the_hop_alias() {
        let ip = [10, 100, 0, 2];
        assert_eq!(
            with_host_alias("http://HOP:9080", "HOP", ip).unwrap(),
            "http://10.100.0.2:9080"
        );
        assert_eq!(
            with_host_alias("http://key@HOP:9080/", "HOP", ip).unwrap(),
            "http://key@10.100.0.2:9080/"
        );
        assert_eq!(
            with_host_alias("http://HOPPER:9080", "HOP", ip).unwrap(),
            "http://HOPPER:9080"
        );
        assert_eq!(
            with_host_alias("http://10.0.0.1:9080", "HOP", ip).unwrap(),
            "http://10.0.0.1:9080"
        );
    }

    // TestLoadConfig (het lezen; het bestand zelf is van `host`): een
    // kale en een geciteerde sleutel.
    #[test]
    fn load_config_text() {
        let cfg = Config::parse(
            "cnames:\n  mail.hop.local: mailserver.example.com\n  \"*.apps.hop.local\": ingress.prod-eu.hop.local\n",
        )
        .unwrap();
        assert_eq!(cfg.cname("mail.hop.local"), Some("mailserver.example.com"));
        assert_eq!(
            cfg.cname("*.apps.hop.local"),
            Some("ingress.prod-eu.hop.local")
        );
        assert_eq!(cfg.cname_table().unwrap().len(), 2);
    }

    #[test]
    fn yaml_subset() {
        let cfg = Config::parse(
            "# hopdns\n---\nother: 1\nnested:\n  x: y\ncnames:   # the table\n  'a.hop.local': 'it''s.example.com' # trailing\n  b.hop.local: \"q\\\"uote\"\n\n",
        )
        .unwrap();
        assert_eq!(cfg.cname("a.hop.local"), Some("it's.example.com"));
        assert_eq!(cfg.cname("b.hop.local"), Some("q\"uote"));
        assert_eq!(Config::parse("  x: y\n"), Err(Error::Yaml { line: 1 }));
        assert_eq!(
            Config::parse("cnames:\n  bad line\n"),
            Err(Error::Yaml { line: 2 })
        );
        assert_eq!(
            Config::parse("cnames: {a: b}\n"),
            Err(Error::Yaml { line: 1 })
        );
        assert_eq!(Config::parse("").unwrap(), Config::default());
    }
}
