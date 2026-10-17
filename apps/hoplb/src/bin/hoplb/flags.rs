//! De vlaggen van de Go-versie, met Go's regels: `-naam waarde`,
//! `-naam=waarde`, en `--naam` mag ook.

use std::fmt;

/// De uitleg bij `-h`.
pub(crate) const USAGE: &str = "usage: hoplb [flags]
  -listen string        Address to listen on for HTTP traffic (default \":80\")
  -admin-listen string  Address to listen on for admin endpoints (/health, /metrics) (default \":9091\")
  -agent string         Local hop agent address (default \"http://127.0.0.1:8080\")
  -tag string           Only route jobs with this tag (e.g., lb:haas)
  -api-key string       API key for hop agent authentication";

/// De vlaggen.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Flags {
    pub(crate) listen: String,
    pub(crate) admin_listen: String,
    pub(crate) agent: String,
    pub(crate) tag: String,
    pub(crate) api_key: String,
}

impl Default for Flags {
    fn default() -> Self {
        Self {
            listen: ":80".into(),
            admin_listen: ":9091".into(),
            agent: "http://127.0.0.1:8080".into(),
            tag: String::new(),
            api_key: String::new(),
        }
    }
}

/// Wat er mis kan zijn met de vlaggen.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Error {
    /// `-h` of `-help`.
    Help,
    /// Een vlag die niet bestaat.
    Unknown(String),
    /// Een vlag zonder waarde.
    Missing(String),
    /// Een argument dat geen vlag is.
    Stray(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Help => f.write_str("help requested"),
            Error::Unknown(n) => write!(f, "flag provided but not defined: -{n}"),
            Error::Missing(n) => write!(f, "flag needs an argument: -{n}"),
            Error::Stray(a) => write!(f, "unexpected argument {a:?}"),
        }
    }
}

impl Flags {
    /// Leest de argumenten (zonder de programmanaam).
    pub(crate) fn parse(args: impl IntoIterator<Item = String>) -> Result<Self, Error> {
        let mut f = Self::default();
        let mut args = args.into_iter();
        while let Some(arg) = args.next() {
            let Some(flag) = arg.strip_prefix("--").or_else(|| arg.strip_prefix('-')) else {
                return Err(Error::Stray(arg));
            };
            let (name, inline) = match flag.split_once('=') {
                Some((n, v)) => (n, Some(v.to_string())),
                None => (flag, None),
            };
            if matches!(name, "h" | "help") {
                return Err(Error::Help);
            }
            let slot = match name {
                "listen" => &mut f.listen,
                "admin-listen" => &mut f.admin_listen,
                "agent" => &mut f.agent,
                "tag" => &mut f.tag,
                "api-key" => &mut f.api_key,
                _ => return Err(Error::Unknown(name.to_string())),
            };
            *slot = match inline {
                Some(v) => v,
                None => args
                    .next()
                    .ok_or_else(|| Error::Missing(name.to_string()))?,
            };
        }
        Ok(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Result<Flags, Error> {
        Flags::parse(args.iter().map(ToString::to_string))
    }

    #[test]
    fn the_go_defaults_and_both_spellings() {
        assert_eq!(parse(&[]).unwrap(), Flags::default());
        let f = parse(&[
            "-listen",
            ":8080",
            "--admin-listen=:9999",
            "-tag=lb:haas",
            "-agent",
            "http://a:1",
            "-api-key",
            "k",
        ])
        .unwrap();
        assert_eq!(f.listen, ":8080");
        assert_eq!(f.admin_listen, ":9999");
        assert_eq!(f.tag, "lb:haas");
        assert_eq!(f.agent, "http://a:1");
        assert_eq!(f.api_key, "k");
    }

    #[test]
    fn mistakes_are_named() {
        assert_eq!(parse(&["-nope"]), Err(Error::Unknown("nope".into())));
        assert_eq!(parse(&["-tag"]), Err(Error::Missing("tag".into())));
        assert_eq!(parse(&["x"]), Err(Error::Stray("x".into())));
        assert_eq!(parse(&["-h"]), Err(Error::Help));
    }
}
