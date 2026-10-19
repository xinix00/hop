//! De twee antwoordkoppen die de Cloudflare-edge eist.
//!
//! Bezit de vorm waarin de koppen van een antwoord naar de edge gaan. Dit is
//! de val waar een eigen tunnel in loopt, en hij is stil: stuur je de koppen
//! van de oorsprong gewoon als HTTP/2-koppen mee, dan krijg je een 200 en
//! ziet de bezoeker je HTML als platte tekst. Gemeten 19-08 met de
//! Go-voorganger: van `content-type: text/html` kwam bij de browser niets aan.
//! De edge negeert wat hij niet kent.
//!
//! Zo doet cloudflared het (`connection/header.go`, `WriteRespHeaders`):
//!
//! - alle koppen van de oorsprong gaan in één kop,
//!   `cf-cloudflared-response-headers`, als `base64(naam):base64(waarde)`
//!   per paar, gescheiden door puntkomma's, in het standaard-alfabet zonder
//!   opvulling (Go's `RawStdEncoding`);
//! - `cf-cloudflared-response-meta` zegt waar het antwoord vandaan komt:
//!   `{"src":"origin"}` of `{"src":"cloudflared"}` voor een antwoord dat de
//!   tunnel zelf maakte (een 502 omdat de dienst niet opnam);
//! - koppen die de edge zelf bestuurt, gaan niet in de bundel: alles met een
//!   `:`-, `cf-int-`-, `cf-cloudflared-`- of `cf-proxy-`-voorvoegsel.
//!
//! `content-length` gaat er ook niet in: leanh2 weigert dat veld in een
//! antwoord (DATA plus END_STREAM is de lengte), en de edge leidt de lengte
//! af uit het einde van de stream.

#![forbid(unsafe_code)]

use alloc::string::String;

use crate::b64;

/// De kop met de bundel.
pub(crate) const HEADER_USER: &str = "cf-cloudflared-response-headers";
/// De kop met de herkomst.
pub(crate) const HEADER_META: &str = "cf-cloudflared-response-meta";

/// Wie het antwoord maakte. De edge onderscheidt het in zijn eigen
/// foutpagina's en statistiek; een tunnel die altijd "origin" zegt, liegt
/// over zijn eigen fouten.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Source {
    /// De lokale dienst antwoordde.
    Origin,
    /// De tunnel zelf (geen route, dienst dood).
    Tunnel,
}

impl Source {
    /// De waarde van [`HEADER_META`].
    pub(crate) fn meta(self) -> &'static str {
        match self {
            Self::Origin => r#"{"src":"origin"}"#,
            Self::Tunnel => r#"{"src":"cloudflared"}"#,
        }
    }
}

/// Of de edge deze kop zelf bestuurt (cloudflared's
/// `IsControlResponseHeader`). `lower` is in kleine letters.
pub(crate) fn is_control(lower: &str) -> bool {
    lower.starts_with(':')
        || lower.starts_with("cf-int-")
        || lower.starts_with("cf-cloudflared-")
        || lower.starts_with("cf-proxy-")
}

/// Een bundel in opbouw: de waarde van [`HEADER_USER`].
#[derive(Debug, Default)]
pub(crate) struct Bundle {
    /// De waarde tot nu toe.
    out: String,
    /// De heap weigerde ergens; de bundel is dan onbruikbaar.
    failed: bool,
}

impl Bundle {
    /// Een lege bundel.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Voegt één kop toe. De naam gaat in kleine letters (HTTP/2); een kop
    /// die de edge bestuurt of `content-length` slaat hij over.
    pub(crate) fn add(&mut self, name: &str, value: &str) {
        let need = 2 + b64::encoded_len(name.len()) + b64::encoded_len(value.len());
        if self.out.try_reserve(need).is_err() {
            self.failed = true;
            return;
        }
        // De naam in kleine letters zonder tweede buffer: de base64 van de
        // naam gaat per teken, in kleine letters, door een klein venster.
        let mut lower = [0u8; 64];
        let name_lower: &[u8] = if name.len() <= lower.len() {
            let dst = lower.get_mut(..name.len()).unwrap_or(&mut []);
            for (d, s) in dst.iter_mut().zip(name.bytes()) {
                *d = s.to_ascii_lowercase();
            }
            dst
        } else {
            // Een naam van meer dan 64 bytes is geen kop die een dienst
            // zinnig stuurt; hij gaat ongewijzigd mee.
            name.as_bytes()
        };
        let lower_str = core::str::from_utf8(name_lower).unwrap_or(name);
        if is_control(lower_str) || lower_str == "content-length" {
            return;
        }
        if !self.out.is_empty() {
            self.out.push(';');
        }
        let out = &mut self.out;
        b64::encode_raw(name_lower, |c| out.push(char::from(c)));
        out.push(':');
        b64::encode_raw(value.as_bytes(), |c| out.push(char::from(c)));
    }

    /// De waarde, of `None` als de heap onderweg weigerde.
    pub(crate) fn finish(self) -> Option<String> {
        (!self.failed).then_some(self.out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Vastgelegd met de Go-voorganger (edgeproto.Headers, 30-09).
    #[test]
    fn matches_go() {
        let mut b = Bundle::new();
        b.add("Content-Type", "text/html; charset=utf-8");
        assert_eq!(
            b.finish().unwrap(),
            "Y29udGVudC10eXBl:dGV4dC9odG1sOyBjaGFyc2V0PXV0Zi04"
        );
        let mut b = Bundle::new();
        b.add("set-cookie", "a=1");
        b.add("content-length", "5");
        b.add("cf-int-x", "y");
        b.add("set-cookie", "b=2");
        assert_eq!(
            b.finish().unwrap(),
            "c2V0LWNvb2tpZQ:YT0x;c2V0LWNvb2tpZQ:Yj0y"
        );
        assert_eq!(Source::Origin.meta(), r#"{"src":"origin"}"#);
        assert_eq!(Source::Tunnel.meta(), r#"{"src":"cloudflared"}"#);
    }

    #[test]
    fn control_headers_stay_out() {
        for h in [":status", "cf-int-foo", "cf-cloudflared-x", "cf-proxy-y"] {
            assert!(is_control(h), "{h}");
        }
        assert!(!is_control("cf-ray"));
        let mut b = Bundle::new();
        b.add("CF-Cloudflared-Proxy-Src", "x");
        assert_eq!(b.finish().unwrap(), "");
    }
}
