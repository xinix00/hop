//! De HMAC-verzoekauthenticatie (X-Hop-Auth): ondertekenen, toetsen, de weigeringen.
//!
//! Deze crate bezit het protocol en de cryptografie die het nodig heeft
//! (SHA-256 en HMAC-SHA256, hier zelf geschreven en getoetst tegen RFC 6234
//! en RFC 4231). Hij bezit geen HTTP: de adapter geeft methode, pad, body en
//! de header als waarden, en krijgt een [`Rejection`] of `Ok` terug.
//!
//! De gesigneerde string is `METHOD \n PATH \n hex(sha256(body))`; de
//! handtekening is `hex(HMAC-SHA256(key, string))` in de header `X-Hop-Auth`.
//! De sleutel reist nooit over de draad. Methode, pad en body zitten in de
//! handtekening, dus een onderschept verzoek is niet om te buigen naar een
//! ander endpoint of een andere body.
//!
//! Een tijdvenster is er bewust niet, en dat is geen vergeten stuk: de
//! Go-generatie koos voor "geen klok, geen nonce, geen serverstaat", zodat
//! een handtekening een failover overleeft en de proxy van een agent hem
//! ongewijzigd naar de leader kan doorzetten (`docs/api.md` van de
//! Go-generatie, github.com/xinix00/hop, tag v1.0.7). Een letterlijke replay
//! van een onderschept verzoek blijft daardoor mogelijk; het dreigingsmodel
//! staat in `SECURITY.md` op dezelfde tag. Een venster toevoegen
//! verandert het protocol voor CLI, GUI en satellieten tegelijk, en hoort dus
//! bij een protocolversie, niet bij een port.

#![cfg_attr(not(test), no_std)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )
)]
#![forbid(unsafe_code)]

mod sha256;

use core::fmt;

pub use sha256::{Sha256, hmac_sha256, sha256};

/// De header die de handtekening draagt.
pub const AUTH_HEADER: &str = "X-Hop-Auth";

/// De grootste body die de toets aanneemt: 512 KiB.
///
/// De body moet gelezen zijn vóór de handtekening te toetsen is (die dekt
/// `sha256(body)`, en er staat geen sleutel op de draad om eerst te
/// checken), dus dit is de rem op een geheugen-DoS vóór authenticatie. Ver
/// boven elke echte payload (een jobspec is kilobytes).
///
/// Hij moet strikt onder de 1 MiB-grens van de HTTP-server blijven: op of
/// boven die grens weigert de server de body al vóór deze toets, en is deze
/// grens onbereikbaar en ontestbaar (gemeten 21-08: bij 8 MiB paniekte
/// `TestRequireHMAC_BodyTooLarge` in de recorder in plaats van een 413).
pub const MAX_BODY_BYTES: usize = 512 << 10;

/// Een hex-handtekening: 64 ASCII-tekens.
pub type Signature = [u8; 64];

/// Waarom een verzoek geweigerd wordt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rejection {
    /// De body is groter dan [`MAX_BODY_BYTES`] (HTTP 413).
    BodyTooLarge {
        /// De maat van de body in bytes.
        len: usize,
    },
    /// Geen of een verkeerde handtekening (HTTP 401).
    Unauthorized,
}

impl Rejection {
    /// De HTTP-status die bij deze weigering hoort.
    pub fn status(self) -> u16 {
        match self {
            Self::BodyTooLarge { .. } => 413,
            Self::Unauthorized => 401,
        }
    }
}

impl fmt::Display for Rejection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BodyTooLarge { len } => {
                write!(f, "request body too large ({len} > {MAX_BODY_BYTES} bytes)")
            }
            Self::Unauthorized => f.write_str("unauthorized"),
        }
    }
}

impl core::error::Error for Rejection {}

/// Het resultaat van een toets.
pub type Result<T = (), E = Rejection> = core::result::Result<T, E>;

/// Voert de gesigneerde string `METHOD \n PATH \n hex(sha256(body))` aan `mac`.
///
/// De string wordt niet gebouwd maar gestroomd: geen allocatie, en de body
/// wordt maar één keer gehasht.
fn feed_signing_string(mac: &mut Hmac, method: &str, path: &str, body: &[u8]) {
    let mut body_hex = [0u8; 64];
    hex(&sha256(body), &mut body_hex);
    mac.update(method.as_bytes());
    mac.update(b"\n");
    mac.update(path.as_bytes());
    mac.update(b"\n");
    mac.update(&body_hex);
}

/// De hex-HMAC-SHA256 van de gesigneerde string onder `key`.
///
/// `path` is het gedecodeerde URL-pad zonder query; `body` de exacte bytes
/// (leeg hasht als `sha256("")`). Client en server rekenen dit identiek uit.
pub fn sign(key: &[u8], method: &str, path: &str, body: &[u8]) -> Signature {
    let mut mac = Hmac::new(key);
    feed_signing_string(&mut mac, method, path, body);
    let mut out = [0u8; 64];
    hex(&mac.finish(), &mut out);
    out
}

/// De handtekening voor een uitgaand verzoek, of `None` bij een lege sleutel.
///
/// Een lege sleutel is de ongeauthenticeerde modus (dev, standalone): dan
/// geen header. `url` mag een volledige URL zijn; alleen het pad telt, want
/// de query zit bewust niet in de handtekening. Een lege methode is `GET`.
pub fn sign_call(key: &[u8], method: &str, url: &str, body: &[u8]) -> Option<Signature> {
    if key.is_empty() {
        return None;
    }
    let method = if method.is_empty() { "GET" } else { method };
    Some(sign(key, method, path_of(url), body))
}

/// Het pad van een URL: zonder schema en host, zonder query en fragment.
pub fn path_of(url: &str) -> &str {
    let rest = match url.find("://") {
        Some(i) => {
            let after = url.get(i + 3..).unwrap_or_default();
            match after.find('/') {
                Some(j) => after.get(j..).unwrap_or_default(),
                None => "",
            }
        }
        None => url,
    };
    let end = rest.find(['?', '#']).unwrap_or(rest.len());
    rest.get(..end).unwrap_or_default()
}

/// Toetst een binnenkomend verzoek.
///
/// Een lege sleutel laat alles door. Anders eerst de body-grens (vóór de
/// handtekening, want dat is de rem op een DoS vóór authenticatie), dan de
/// handtekening, in constante tijd vergeleken.
pub fn verify(key: &[u8], method: &str, path: &str, body: &[u8], header: Option<&str>) -> Result {
    if key.is_empty() {
        return Ok(());
    }
    if body.len() > MAX_BODY_BYTES {
        return Err(Rejection::BodyTooLarge { len: body.len() });
    }
    let expected = sign(key, method, path, body);
    let got = header.unwrap_or_default().as_bytes();
    if constant_time_eq(&expected, got) {
        Ok(())
    } else {
        Err(Rejection::Unauthorized)
    }
}

/// Vergelijkt twee byte-reeksen in een tijd die niet van de inhoud afhangt.
///
/// Een vroege `return` op het eerste verschil lekt via de responstijd hoe
/// veel van een handtekening goed was. De lengte is geen geheim (64 tekens
/// hex), dus die mag vroeg afhaken; de inhoud niet.
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let diff = a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y));
    // `black_box` houdt de optimizer weg van een kortsluiting op `diff`.
    core::hint::black_box(diff) == 0
}

/// Schrijft `bytes` als kleine-letter-hex in `out` (dat twee keer zo lang is).
fn hex(bytes: &[u8; 32], out: &mut [u8; 64]) {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    for (b, pair) in bytes.iter().zip(out.chunks_exact_mut(2)) {
        if let [hi, lo] = pair {
            *hi = DIGITS[usize::from(b >> 4)];
            *lo = DIGITS[usize::from(b & 0x0f)];
        }
    }
}

/// HMAC-SHA256 in stromende vorm (RFC 2104).
struct Hmac {
    inner: Sha256,
    outer_key: [u8; 64],
}

impl Hmac {
    fn new(key: &[u8]) -> Self {
        let mut block = [0u8; 64];
        if key.len() > 64 {
            block[..32].copy_from_slice(&sha256(key));
        } else if let Some(dst) = block.get_mut(..key.len()) {
            dst.copy_from_slice(key);
        }
        let mut ipad = [0u8; 64];
        let mut opad = [0u8; 64];
        for ((i, o), k) in ipad.iter_mut().zip(opad.iter_mut()).zip(block) {
            *i = k ^ 0x36;
            *o = k ^ 0x5c;
        }
        let mut inner = Sha256::new();
        inner.update(&ipad);
        Self {
            inner,
            outer_key: opad,
        }
    }

    fn update(&mut self, data: &[u8]) {
        self.inner.update(data);
    }

    fn finish(self) -> [u8; 32] {
        let inner = self.inner.finish();
        let mut outer = Sha256::new();
        outer.update(&self.outer_key);
        outer.update(&inner);
        outer.finish()
    }
}

#[cfg(test)]
mod tests {
    //! De tests van `pkg/httputil/auth_test.go` (v1.0.7). Go testte de
    //! middleware met een recorder; hier is de middleware een functie, dus de
    //! toets krijgt het verzoek als waarden en de status komt uit de weigering.

    use super::*;

    const KEY: &[u8] = b"secret123";

    /// Tekent zoals een client (via `sign_call` op een volledige URL), zodat
    /// de test bewijst dat client en server het eens zijn.
    fn signed(key: &[u8], method: &str, path: &str, body: &[u8]) -> Option<Signature> {
        sign_call(key, method, &std::format!("http://node{path}"), body)
    }

    fn header(sig: &Option<Signature>) -> Option<&str> {
        sig.as_ref().map(|s| core::str::from_utf8(s).unwrap())
    }

    #[test]
    fn require_hmac_empty_key_passes_through() {
        assert_eq!(verify(b"", "GET", "/", b"", None), Ok(()));
    }

    #[test]
    fn require_hmac_valid_signature() {
        let sig = signed(KEY, "GET", "/v1/jobs", b"");
        assert_eq!(verify(KEY, "GET", "/v1/jobs", b"", header(&sig)), Ok(()));
    }

    #[test]
    fn require_hmac_valid_signature_with_body() {
        // De body blijft van de aanroeper: verify leent hem alleen.
        let body = br#"{"name":"api","count":3}"#;
        let sig = signed(KEY, "POST", "/v1/jobs", body);
        assert_eq!(verify(KEY, "POST", "/v1/jobs", body, header(&sig)), Ok(()));
    }

    #[test]
    fn require_hmac_body_too_large() {
        // Te grote body zonder geldige handtekening: de grens moet eerst
        // raken, vóór de handtekening getoetst wordt.
        let body = std::vec![b'a'; MAX_BODY_BYTES + 1];
        let err = verify(KEY, "POST", "/v1/jobs", &body, None).unwrap_err();
        assert_eq!(err.status(), 413);
    }

    // Precies de grens mag nog: een off-by-one daar weigert de grootste
    // legitieme jobspec.
    #[test]
    fn require_hmac_body_at_the_cap() {
        let body = std::vec![b'a'; MAX_BODY_BYTES];
        let sig = signed(KEY, "POST", "/v1/jobs", &body);
        assert_eq!(verify(KEY, "POST", "/v1/jobs", &body, header(&sig)), Ok(()));
    }

    #[test]
    fn require_hmac_missing_signature() {
        let err = verify(KEY, "GET", "/v1/jobs", b"", None).unwrap_err();
        assert_eq!(err, Rejection::Unauthorized);
        assert_eq!(err.status(), 401);
    }

    #[test]
    fn require_hmac_wrong_key() {
        let sig = signed(b"wrongkey", "GET", "/v1/jobs", b"");
        assert_eq!(
            verify(KEY, "GET", "/v1/jobs", b"", header(&sig)),
            Err(Rejection::Unauthorized)
        );
    }

    #[test]
    fn require_hmac_tampered_path() {
        // Getekend voor /v1/jobs, afgespeeld tegen een destructief endpoint.
        let sig = signed(KEY, "GET", "/v1/jobs", b"");
        assert_eq!(
            verify(KEY, "DELETE", "/v1/agents/node-1", b"", header(&sig)),
            Err(Rejection::Unauthorized)
        );
    }

    #[test]
    fn require_hmac_tampered_body() {
        let sig = signed(KEY, "POST", "/v1/jobs", br#"{"count":1}"#);
        assert_eq!(
            verify(KEY, "POST", "/v1/jobs", br#"{"count":9999}"#, header(&sig)),
            Err(Rejection::Unauthorized)
        );
    }

    // De query zit bewust niet in de handtekening, en een lege sleutel
    // tekent niet.
    #[test]
    fn sign_call_signs_what_it_sends() {
        let sig = sign_call(b"k", "POST", "http://node:7878/v1/jobs?dry=1", b"spec");
        assert_eq!(sig, Some(sign(b"k", "POST", "/v1/jobs", b"spec")));
        assert_eq!(sign_call(b"", "POST", "http://node/v1/jobs", b""), None);
    }

    #[test]
    fn sign_matches_independent_implementation() {
        // Uitgerekend met Python's hmac/hashlib (29-09-2026).
        type Case<'a> = (&'a [u8], &'a str, &'a str, &'a [u8], &'a str);
        let cases: [Case<'_>; 3] = [
            (
                b"k",
                "POST",
                "/v1/jobs",
                b"spec",
                "1aa919363960e319766547f83a64c816f0302182b8f637dbbf0e8457b6a18818",
            ),
            (
                b"secret123",
                "GET",
                "/v1/jobs",
                b"",
                "205db2f439beceb0a19264c65fc29ea421e2b33fe7827b30fc80520af0e57338",
            ),
            // Het curl-voorbeeld uit docs/api.md van v1.0.7.
            (
                b"your-secret-key",
                "POST",
                "/v1/jobs",
                br#"{"name":"api","count":3}"#,
                "54f6f8899ba8af25f69825f54c0c52a61f3ea224b6dc5542f4646d936f746f59",
            ),
        ];
        for (key, m, p, body, want) in cases {
            assert_eq!(core::str::from_utf8(&sign(key, m, p, body)).unwrap(), want);
        }
    }

    #[test]
    fn path_of_strips_scheme_host_and_query() {
        assert_eq!(path_of("http://node:7878/v1/jobs?dry=1"), "/v1/jobs");
        assert_eq!(path_of("https://h/a/b#frag"), "/a/b");
        assert_eq!(path_of("http://host"), "");
        assert_eq!(path_of("/run?replace=1"), "/run");
    }

    #[test]
    fn constant_time_eq_compares_content_and_length() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"ab"));
        assert!(constant_time_eq(b"", b""));
    }
}
