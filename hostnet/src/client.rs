//! De HTTP(S)-client van de host: leanhttp over [`StdConn`], TLS via leanhttps.
//!
//! Bezit de wortels (één keer gelezen) en per verzoek een dialer. Elke hop
//! (ook na een redirect) zoekt zijn host op met de resolver van het OS,
//! opent TCP met een termijn, en kiest dan zelf: kaal voor `http://`, TLS
//! met ketenverificatie voor `https://`. leanhttp volgt redirects alleen
//! voor GET en HEAD en weigert `https` naar `http`.
//!
//! De datum van de ketentoets is de systeemklok. Op HopOS wacht de
//! downloader op SNTP; een host heeft een klok die het OS bijhoudt, en een
//! host met een verkeerde klok faalt hier luid op de datum van de keten.

use std::fmt;
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use leanhttp::{AsyncRead, AsyncWrite, Close, IoError, Target};
use leanhttps::{TlsConn, TlsDial};
use leantls::{ChainVerifier, Entropy, Roots, Trust};

use crate::conn::StdConn;
use crate::exec::block_on;

/// De vertrouwde wortels: de Mozilla-set (NSS `certdata.txt`) van juli 2026,
/// 119 certificaten als aaneengeschakelde DER.
///
/// Dezelfde bytes als de HopOS-bewoner (`agentd-hopos/roots.der`, gekopieerd
/// op 29-09-2026 uit lean v3.1.1, commit db67247); één bestand in de repo,
/// zodat host en bewoner niet uiteenlopen. Vervangen is een nieuwe kopie
/// daar, met de datum mee.
pub const ROOTS_DER: &[u8] = include_bytes!("../../agentd-hopos/roots.der");

/// Hoeveel van een foutbody bewaard wordt voor de melding: 4 KiB.
const ERROR_BODY: usize = 4 << 10;

/// De leesbuffer van een gestroomde download.
const STREAM_BUF: usize = 64 << 10;

/// Wat er mis kan gaan bij een verzoek.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// Opzoeken, verbinden of de TLS-handshake faalde; de zin zegt welke.
    Dial(String),
    /// leanhttp faalde (framing, termijn, reset).
    Http(leanhttp::Error),
    /// De server antwoordde met een foutstatus (alleen bij [`Http::stream`]).
    Status {
        /// De status.
        code: u16,
        /// Het begin van de body, hoogstens 4 KiB.
        body: String,
    },
    /// De ontvanger van een stroom faalde (schijf vol, ...).
    Sink(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Dial(why) => f.write_str(why),
            Self::Http(e) => write!(f, "http: {e}"),
            Self::Status { code, body } if body.is_empty() => write!(f, "status {code}"),
            Self::Status { code, body } => write!(f, "status {code}: {body}"),
            Self::Sink(why) => write!(f, "write: {why}"),
        }
    }
}

impl std::error::Error for Error {}

/// Het resultaat van deze crate.
pub type Result<T = (), E = Error> = core::result::Result<T, E>;

/// Eén verzoek.
#[derive(Clone, Copy, Debug)]
pub struct Call<'a> {
    /// De methode; leeg is GET.
    pub method: &'a str,
    /// De absolute URL.
    pub url: &'a str,
    /// Extra headers.
    pub headers: &'a [(&'a str, &'a str)],
    /// De body, als die er is.
    pub body: Option<&'a [u8]>,
    /// De termijn per fase (verbinden, kop, elke lees en schrijf).
    pub timeout: Duration,
}

impl<'a> Call<'a> {
    /// Een GET op `url` met termijn `timeout`.
    pub fn get(url: &'a str, timeout: Duration) -> Self {
        Self {
            method: "GET",
            url,
            headers: &[],
            body: None,
            timeout,
        }
    }
}

/// Een gebufferd antwoord.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Reply {
    /// De status.
    pub status: u16,
    /// De headers.
    pub headers: Vec<(String, String)>,
    /// De body.
    pub body: Vec<u8>,
}

impl Reply {
    /// Een header op naam (hoofdletterongevoelig).
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

/// Nu in Unix-seconden volgens de systeemklok (0 vóór 1970).
pub fn unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// 96 bytes uit de entropiebron van het OS (`/dev/urandom`).
///
/// Linux en macOS hebben hem allebei; zonder is er geen handshake, en dat is
/// een fout, geen zwakke sleutel.
pub fn entropy() -> std::io::Result<[u8; Entropy::LEN]> {
    let mut out = [0u8; Entropy::LEN];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut out)?;
    Ok(out)
}

/// Een verbinding van de client: kaal of in TLS.
#[expect(
    clippy::large_enum_variant,
    reason = "één verbinding per verzoek, en TLS is het gewone geval; boxen kost een allocatie per hop voor niets"
)]
pub enum HostConn {
    /// `http://`.
    Plain(StdConn<TcpStream>),
    /// `https://`.
    Tls(TlsConn<StdConn<TcpStream>>),
}

impl AsyncRead for HostConn {
    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize, IoError>> {
        match self {
            Self::Plain(c) => c.poll_read(cx, buf),
            Self::Tls(c) => c.poll_read(cx, buf),
        }
    }

    fn set_read_timeout(&mut self, t: Option<Duration>) -> Result<(), IoError> {
        match self {
            Self::Plain(c) => c.set_read_timeout(t),
            Self::Tls(c) => c.set_read_timeout(t),
        }
    }
}

impl AsyncWrite for HostConn {
    fn poll_write(&mut self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, IoError>> {
        match self {
            Self::Plain(c) => c.poll_write(cx, buf),
            Self::Tls(c) => c.poll_write(cx, buf),
        }
    }

    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), IoError>> {
        match self {
            Self::Plain(c) => c.poll_flush(cx),
            Self::Tls(c) => c.poll_flush(cx),
        }
    }

    fn set_write_timeout(&mut self, t: Option<Duration>) -> Result<(), IoError> {
        match self {
            Self::Plain(c) => c.set_write_timeout(t),
            Self::Tls(c) => c.set_write_timeout(t),
        }
    }
}

impl Close for HostConn {
    fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), IoError>> {
        match self {
            Self::Plain(c) => c.poll_close(cx),
            Self::Tls(c) => c.poll_close(cx),
        }
    }
}

/// Een dialer die één al geopende verbinding geeft: de TCP-kant onder
/// leanhttps, zodat die alleen de handshake doet.
struct Ready(Option<StdConn<TcpStream>>);

impl leanhttp::Dial for Ready {
    type Conn = StdConn<TcpStream>;

    async fn dial(&mut self, _target: Target<'_>) -> leanhttp::Result<Self::Conn> {
        self.0.take().ok_or(leanhttp::Error::Connect)
    }
}

/// De dialer van één verzoek; onthoudt waarom de laatste dial faalde.
pub(crate) struct Dialer<'t> {
    trust: Option<Trust<'t>>,
    timeout: Duration,
    why: Option<String>,
}

impl<'t> Dialer<'t> {
    pub(crate) fn new(trust: Option<Trust<'t>>, timeout: Duration) -> Self {
        Self {
            trust,
            timeout,
            why: None,
        }
    }

    /// Waarom de laatste dial faalde, en wist het.
    pub(crate) fn take_why(&mut self) -> Option<String> {
        self.why.take()
    }

    /// Eén hop: opzoeken, TCP, en TLS als de hop `https` is.
    pub(crate) async fn hop(&mut self, target: Target<'_>) -> Result<HostConn, String> {
        let host = target.host;
        if target.https && self.trust.is_none() {
            return Err(String::from(
                "the built-in root certificates did not parse; https refused",
            ));
        }
        let addrs = (host, target.port)
            .to_socket_addrs()
            .map_err(|e| format!("resolve {host}: {e}"))?;
        let mut last = format!("resolve {host}: no addresses");
        let mut stream = None;
        for a in addrs {
            match TcpStream::connect_timeout(&a, self.timeout) {
                Ok(s) => {
                    stream = Some(s);
                    break;
                }
                Err(e) => last = format!("connect {host} ({a}): {e}"),
            }
        }
        let stream = stream.ok_or(last)?;
        // Verzoeken zijn klein en praterig; Nagle zou elke kop laten wachten.
        let _ = stream.set_nodelay(true);
        let raw = StdConn::new(stream, Some(self.timeout));
        let Some(trust) = self.trust.filter(|_| target.https) else {
            return Ok(HostConn::Plain(raw));
        };
        let seed = entropy().map_err(|e| format!("tls {host}: no entropy: {e}"))?;
        let mut seed = Some(seed);
        let mut tls = TlsDial::new(Ready(Some(raw)), trust, move || {
            // De dialer vraagt één keer per dial, en Ready dialt één keer.
            Entropy::new(seed.take().unwrap_or([0u8; Entropy::LEN]))
        });
        match leanhttp::Dial::dial(&mut tls, target).await {
            Ok(c) => Ok(HostConn::Tls(c)),
            Err(e) => Err(match tls.last_error() {
                Some(why) => format!("tls {host}: {why}"),
                None => format!("tls {host}: {e}"),
            }),
        }
    }
}

impl leanhttp::Dial for Dialer<'_> {
    type Conn = HostConn;

    async fn dial(&mut self, target: Target<'_>) -> leanhttp::Result<HostConn> {
        match self.hop(target).await {
            Ok(c) => {
                self.why = None;
                Ok(c)
            }
            Err(why) => {
                self.why = Some(why);
                Err(leanhttp::Error::Connect)
            }
        }
    }

    /// Versleutelt als de hop `https` is; de keuze valt per hop.
    fn is_encrypted(&self) -> bool {
        true
    }
}

/// De HTTP(S)-client van de host.
#[derive(Clone, Copy, Debug)]
pub struct Http {
    /// De wortels; `None` als de ingebakken set niet las (dan weigert elke `https`).
    roots: Option<Roots<'static>>,
}

impl Default for Http {
    fn default() -> Self {
        Self::new()
    }
}

impl Http {
    /// Een client met de ingebakken wortels.
    pub fn new() -> Self {
        Self {
            roots: Roots::from_concatenated_der(ROOTS_DER).ok(),
        }
    }

    /// Hoeveel wortels de client vertrouwt (0: de set las niet).
    pub fn root_count(&self) -> usize {
        self.roots.map_or(0, |r| r.len())
    }

    /// De ketentoets van nu, als er wortels zijn.
    pub(crate) fn verifier(&self) -> Option<ChainVerifier<'static>> {
        self.roots.map(|r| ChainVerifier::new(r, unix_secs()))
    }

    /// Doet één verzoek en buffert de body tot `limit` bytes.
    ///
    /// Elke status is een antwoord, ook 4xx en 5xx; alleen transport en
    /// framing zijn fouten. GET en HEAD volgen redirects.
    pub fn request(&self, call: &Call<'_>, limit: usize) -> Result<Reply> {
        block_on(self.request_async(call, limit))
    }

    async fn request_async(&self, call: &Call<'_>, limit: usize) -> Result<Reply> {
        let verifier = self.verifier();
        let trust = verifier.as_ref().map(|v| Trust::Chain(v));
        let mut dial = Dialer::new(trust, call.timeout);
        let lc = leanhttp_call(call)?;
        let mut resp = leanhttp::fetch(&mut dial, lc)
            .await
            .map_err(|e| dial.take_why().map_or(Error::Http(e), Error::Dial))?;
        let body = resp.read_to_end(limit).await.map_err(Error::Http)?;
        let headers = resp
            .header
            .iter()
            .map(|(k, v)| (String::from(k), String::from(v)))
            .collect();
        let status = resp.status;
        // Geen pool: de verbinding gaat dicht (Drop), elk verzoek dialt vers.
        drop(resp);
        Ok(Reply {
            status,
            headers,
            body,
        })
    }

    /// Stroomt de body van een GET naar `sink`; geeft het aantal bytes.
    ///
    /// Alleen 200 is een download; elke andere status is [`Error::Status`]
    /// met het begin van de body. `progress` krijgt (gelezen, lengte) na elke hap.
    pub fn stream<W: Write>(
        &self,
        call: &Call<'_>,
        sink: &mut W,
        progress: &mut dyn FnMut(u64, Option<u64>),
    ) -> Result<u64> {
        block_on(self.stream_async(call, sink, progress))
    }

    async fn stream_async<W: Write>(
        &self,
        call: &Call<'_>,
        sink: &mut W,
        progress: &mut dyn FnMut(u64, Option<u64>),
    ) -> Result<u64> {
        let verifier = self.verifier();
        let trust = verifier.as_ref().map(|v| Trust::Chain(v));
        let mut dial = Dialer::new(trust, call.timeout);
        let lc = leanhttp_call(call)?;
        let mut resp = leanhttp::fetch(&mut dial, lc)
            .await
            .map_err(|e| dial.take_why().map_or(Error::Http(e), Error::Dial))?;
        if resp.status != 200 {
            let body = resp.read_to_end(ERROR_BODY).await.unwrap_or_default();
            return Err(Error::Status {
                code: resp.status,
                body: String::from_utf8_lossy(&body).trim().to_string(),
            });
        }
        let length = resp.length;
        let mut buf = vec![0u8; STREAM_BUF];
        let mut total: u64 = 0;
        loop {
            let n = resp.read(&mut buf).await.map_err(Error::Http)?;
            if n == 0 {
                return Ok(total);
            }
            let chunk = buf.get(..n).unwrap_or_default();
            sink.write_all(chunk)
                .map_err(|e| Error::Sink(e.to_string()))?;
            total = total.saturating_add(u64::try_from(n).unwrap_or(u64::MAX));
            progress(total, length);
        }
    }
}

/// Een [`Call`] als leanhttp-call.
fn leanhttp_call<'a>(call: &Call<'a>) -> Result<leanhttp::Call<'a>> {
    let mut header = leanhttp::Header::new();
    for (k, v) in call.headers {
        header.set(k, v).map_err(Error::Http)?;
    }
    Ok(leanhttp::Call {
        method: call.method,
        url: call.url,
        header,
        body: call.body,
        header_timeout: Some(call.timeout),
        ..leanhttp::Call::default()
    })
}
