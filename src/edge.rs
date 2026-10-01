//! De weg naar de edge: TCP, TLS met Cloudflare's eigen CA's, en het
//! transport onder leanh2.
//!
//! Bezit de CA's (`cfroots.pem`, dezelfde drie die cloudflared inbakt), de
//! dial naar één edge-adres met de handshake binnen een termijn, en de
//! adapter die een leanhttps-verbinding de vorm geeft die leanh2 leest. Niet
//! wat er daarna over de verbinding gaat (`crate::tunnel`), en niet de
//! willekeur zelf: die is van `applib::rand`, [`Rand`] geeft hem alleen de
//! vorm van lean.
//!
//! Drie dingen die het protocol anders doet dan je zou denken, gemeten door
//! de Go-voorganger tegen `region1.v2.argotunnel.com:7844` (19-08) en
//! opnieuw met openssl op 30-09:
//!
//! 1. Geen ALPN: de http2-transport zet alleen de SNI-naam
//!    `h2.cftunnel.com` (alleen de quic-transport eist ALPN `argotunnel`).
//! 2. De edge bewijst zich met een "CloudFlare Origin Certificate" (ECDSA
//!    P-256, SAN `*.cftunnel.com`) dat niet publiek vertrouwd is: tegen de
//!    Mozilla-wortels faalt elke handshake, tegen deze drie niet.
//! 3. TLS 1.3 met `TLS_AES_128_GCM_SHA256` en X25519 is precies wat
//!    leantls kan en wat de edge aanbiedt.

#![forbid(unsafe_code)]

use alloc::vec::Vec;
use core::fmt;
use core::pin::Pin;
use core::task::{Context, Poll};
use core::time::Duration;

use applib::appnet::TcpStream;
use applib::rand::Rng;
use applib::rt::Exec;
use applib::tcp::TcpConn;
use leanhttp::{Close, Dial, IoError, Target};
use leanhttps::{TlsConn, TlsDial};
use leantls::{ChainVerifier, Roots, Trust};
use sync::{Either, select};

use crate::b64;

/// Waar origintunneld luistert.
pub(crate) const PORT: u16 = 7844;

/// De SNI-naam van de http2-transport.
pub(crate) const SERVER_NAME: &str = "h2.cftunnel.com";

/// Hoe lang TCP plus de TLS-handshake samen mogen duren: een edge die de
/// dans halverwege stil laat vallen, houdt anders een verbindingstaak vast.
pub(crate) const DIAL_TIMEOUT: Duration = Duration::from_secs(15);

/// De CA's, als PEM, met de herkomst in de kop van het bestand.
const ROOTS_PEM: &str = include_str!("cfroots.pem");

/// Waarom de CA's niet te lezen zijn (een fout in de build, niet op de node).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RootsError {
    /// Een blok zonder einde.
    Unterminated,
    /// Base64 dat niet klopt.
    Base64(b64::Error),
    /// leantls weigert de DER.
    Tls(leantls::Error),
    /// De heap weigerde.
    OutOfMemory,
}

impl fmt::Display for RootsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unterminated => f.write_str("cfroots.pem: certificate block without an end"),
            Self::Base64(e) => write!(f, "cfroots.pem: {e}"),
            Self::Tls(e) => write!(f, "cfroots.pem: {e}"),
            Self::OutOfMemory => f.write_str("cfroots.pem: out of memory"),
        }
    }
}

/// De certificaten uit een PEM-tekst, als aaneengeschakelde DER.
pub(crate) fn pem_to_der(pem: &str) -> Result<Vec<u8>, RootsError> {
    const BEGIN: &str = "-----BEGIN CERTIFICATE-----";
    const END: &str = "-----END CERTIFICATE-----";
    let mut out = Vec::new();
    let mut rest = pem;
    while let Some(i) = rest.find(BEGIN) {
        let body = rest.get(i + BEGIN.len()..).unwrap_or("");
        let j = body.find(END).ok_or(RootsError::Unterminated)?;
        let b64 = body.get(..j).unwrap_or("");
        let start = out.len();
        let room = b64.len() / 4 * 3 + 3;
        out.try_reserve(room).map_err(|_| RootsError::OutOfMemory)?;
        out.resize(start + room, 0);
        let n = b64::decode(b64.as_bytes(), out.get_mut(start..).unwrap_or(&mut []))
            .map_err(RootsError::Base64)?;
        out.truncate(start + n);
        rest = body.get(j + END.len()..).unwrap_or("");
    }
    Ok(out)
}

/// De CA's van de edge, één keer gelezen voor de levensduur van de app.
pub(crate) fn roots() -> Result<Roots<'static>, RootsError> {
    let der = pem_to_der(ROOTS_PEM)?;
    // Eén keer bij de start, en voor de levensduur van de app: elke dial
    // leent de wortels, niemand schrijft ze nog.
    let der: &'static [u8] = alloc::boxed::Box::leak(der.into_boxed_slice());
    Roots::from_concatenated_der(der).map_err(RootsError::Tls)
}

/// De willekeur van één verbindingstaak: een [`Rng`] van applib (het zaad
/// van de kern, gemengd met jitter) in de twee vormen die lean vraagt, de 96
/// bytes van een handshake en een `leanrand::Source` voor de spreiding.
pub(crate) struct Rand(pub(crate) Rng);

impl Rand {
    /// Een `leantls::Entropy` voor één handshake.
    fn entropy(&mut self) -> leantls::Entropy {
        leantls::Entropy::new(self.0.array())
    }
}

impl leanrand::Source for Rand {
    fn fill(&mut self, buf: &mut [u8]) {
        self.0.fill(buf);
    }
}

/// Kale TCP naar één vast adres; de host uit de URL is alleen de SNI-naam.
struct Tcp {
    /// Het adres van de edge.
    ip: [u8; 4],
    /// De executor, voor de termijnen van de verbinding.
    exec: &'static Exec,
}

impl Dial for Tcp {
    type Conn = TcpConn;

    async fn dial(&mut self, t: Target<'_>) -> leanhttp::Result<TcpConn> {
        let s = TcpStream::connect_timeout(self.ip, t.port, DIAL_TIMEOUT)
            .await
            .map_err(|_| leanhttp::Error::Connect)?;
        Ok(TcpConn::new(s, self.exec))
    }
}

/// Waarom een dial naar de edge niet lukte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DialError {
    /// TCP en TLS samen duurden langer dan [`DIAL_TIMEOUT`].
    Timeout,
    /// De TLS-laag, met de reden van leanhttps.
    Tls(leanhttps::Error),
    /// Iets onder TLS (de TCP-verbinding).
    Connect(leanhttp::Error),
}

impl fmt::Display for DialError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Timeout => write!(f, "no TLS session within {} s", DIAL_TIMEOUT.as_secs()),
            Self::Tls(e) => write!(f, "{e}"),
            Self::Connect(e) => write!(f, "tcp: {e}"),
        }
    }
}

/// Opent TCP naar `ip:7844` en doet de handshake met SNI
/// [`SERVER_NAME`], tegen de CA's op tijdstip `now_unix`.
pub(crate) async fn dial(
    exec: &'static Exec,
    ip: [u8; 4],
    roots: Roots<'static>,
    now_unix: u64,
    rand: &mut Rand,
) -> Result<EdgeIo, DialError> {
    let verifier = ChainVerifier::new(roots, now_unix);
    let mut tls = TlsDial::new(Tcp { ip, exec }, Trust::Chain(&verifier), || rand.entropy());
    let target = Target {
        https: true,
        host: SERVER_NAME,
        port: PORT,
    };
    let got = match select(exec.after(DIAL_TIMEOUT), tls.dial(target)).await {
        Either::Left(()) => return Err(DialError::Timeout),
        Either::Right(r) => r,
    };
    match got {
        Ok(c) => Ok(EdgeIo(c)),
        Err(e) => Err(tls
            .last_error()
            .map_or(DialError::Connect(e), DialError::Tls)),
    }
}

/// Een TLS-sessie met de edge in de vorm die leanh2 leest en schrijft.
///
/// leanh2 heeft eigen poll-traits (met `Pin` en `poll_close`), leanhttps
/// geeft die van leanhttp; dit is de naad, zonder buffer ertussen.
pub(crate) struct EdgeIo(TlsConn<TcpConn>);

impl leanh2::AsyncRead for EdgeIo {
    type Error = IoError;

    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, IoError>> {
        leanhttp::AsyncRead::poll_read(&mut self.get_mut().0, cx, buf)
    }
}

impl leanh2::AsyncWrite for EdgeIo {
    type Error = IoError;

    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, IoError>> {
        leanhttp::AsyncWrite::poll_write(&mut self.get_mut().0, cx, buf)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), IoError>> {
        self.get_mut().0.poll_close(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // De drie CA's uit cfroots.pem: ECC, RSA 2048 en origin-pull (RSA 4096),
    // en leantls leest ze alle drie.
    #[test]
    fn the_three_roots_parse() {
        let der = pem_to_der(ROOTS_PEM).unwrap();
        let roots = Roots::from_concatenated_der(&der).unwrap();
        assert_eq!(roots.len(), 3);
    }

    #[test]
    fn pem_refusals() {
        assert_eq!(
            pem_to_der("-----BEGIN CERTIFICATE-----\nAAAA").unwrap_err(),
            RootsError::Unterminated
        );
        assert!(matches!(
            pem_to_der("-----BEGIN CERTIFICATE-----\n!!\n-----END CERTIFICATE-----"),
            Err(RootsError::Base64(_))
        ));
        assert!(pem_to_der("# alleen commentaar").unwrap().is_empty());
    }
}
