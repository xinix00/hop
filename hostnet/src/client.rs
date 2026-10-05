//! De HTTP(S)-client van de host: leanhttp over [`StdConn`], TLS via `leanhttps::WebDial`.
//!
//! Bezit per verzoek een dialer: de kale std-dial ([`Tcp`]: opzoeken met de
//! resolver van het OS, TCP met een termijn, elke mislukking een eigen zin)
//! en daarboven `leanhttps::WebDial`: kaal voor `http://`, TLS met
//! ketenverificatie tegen `leantls::MOZILLA_ROOTS` voor `https://`, met SNI
//! per hop en verse entropie van het OS per handshake. leanhttp volgt
//! redirects alleen voor GET en HEAD en weigert `https` naar `http`.
//!
//! De datum van de ketentoets is de systeemklok. Op HopOS wacht de
//! downloader op SNTP; een host heeft een klok die het OS bijhoudt, en een
//! host met een verkeerde klok faalt hier luid op de datum van de keten.

use std::fmt;
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use leanhttp::{IoError, Target};
use leanhttps::{Link, WebDial};
use leantls::Entropy;

use crate::conn::StdConn;
use crate::exec::block_on;

/// Hoeveel van een foutbody bewaard wordt voor de melding: 4 KiB.
const ERROR_BODY: usize = 4 << 10;

/// De zin van een dial die geen tijd meer had binnen de totale grens.
const OUT_OF_TIME: &str = "the call ran out of its total time";

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

/// Een verbinding van de client: kaal of in TLS (`leanhttps::Link`).
pub type HostConn = Link<StdConn<TcpStream>>;

/// Wat de std-dial van de laatste hop zag: de host, en waarom hij faalde.
#[derive(Debug, Default)]
pub(crate) struct Trace {
    host: String,
    why: Option<String>,
}

/// De kale TCP-dial van de host: opzoeken met de resolver van het OS, TCP
/// met een termijn, binnen de totale grens van de aanroep.
pub(crate) struct Tcp<'t> {
    timeout: Duration,
    /// De totale grens van de aanroep; de verbinding krijgt hem mee.
    limit: Option<Instant>,
    /// Waar de zin van een mislukking heen gaat; `None` als niemand hem leest.
    trace: Option<&'t mut Trace>,
}

impl<'t> Tcp<'t> {
    /// Een dial met `timeout` per fase, binnen `limit`, die in `trace` schrijft.
    pub(crate) fn new(
        timeout: Duration,
        limit: Option<Instant>,
        trace: Option<&'t mut Trace>,
    ) -> Self {
        Self {
            timeout,
            limit,
            trace,
        }
    }

    /// De verbindtermijn: de fasetermijn, maar niet voorbij de grens.
    fn connect_timeout(&self) -> Option<Duration> {
        let Some(at) = self.limit else {
            return Some(self.timeout);
        };
        let left = at.saturating_duration_since(Instant::now());
        (!left.is_zero()).then(|| self.timeout.min(left))
    }

    /// Opzoeken en verbinden; de zin van een mislukking als `Err`.
    fn open(&self, host: &str, port: u16) -> Result<TcpStream, (leanhttp::Error, String)> {
        let connect = leanhttp::Error::Connect;
        let addrs = (host, port)
            .to_socket_addrs()
            .map_err(|e| (connect, format!("resolve {host}: {e}")))?;
        let mut last = format!("resolve {host}: no addresses");
        for a in addrs {
            let Some(t) = self.connect_timeout() else {
                let out = leanhttp::Error::Io(IoError::TimedOut);
                return Err((out, format!("connect {host}: {OUT_OF_TIME}")));
            };
            match TcpStream::connect_timeout(&a, t) {
                Ok(s) => return Ok(s),
                Err(e) => last = format!("connect {host} ({a}): {e}"),
            }
        }
        Err((connect, last))
    }
}

impl leanhttp::Dial for Tcp<'_> {
    type Conn = StdConn<TcpStream>;

    async fn dial(&mut self, target: Target<'_>) -> leanhttp::Result<Self::Conn> {
        let opened = self.open(target.host, target.port);
        if let Some(trace) = self.trace.as_deref_mut() {
            trace.host.clear();
            trace.host.push_str(target.host);
            trace.why = opened.as_ref().err().map(|(_, why)| why.clone());
        }
        let stream = opened.map_err(|(e, _)| e)?;
        // Verzoeken zijn klein en praterig; Nagle zou elke kop laten wachten.
        let _ = stream.set_nodelay(true);
        Ok(StdConn::new(stream, Some(self.timeout)).with_limit(self.limit))
    }
}

/// De webdialer van de host: [`Tcp`] eronder, TLS met de Mozilla-wortels
/// op de systeemklok en entropie van het OS.
pub(crate) type Web<'t> = WebDial<Tcp<'t>, fn() -> Option<u64>, fn() -> Option<Entropy>>;

/// Nu als wandklok voor de keten: de systeemklok.
#[expect(clippy::unnecessary_wraps, reason = "de vorm van de klok van WebDial")]
fn wall() -> Option<u64> {
    Some(unix_secs())
}

/// Verse entropie van het OS voor één handshake; zonder geen handshake.
fn os_entropy() -> Option<Entropy> {
    entropy().ok().map(Entropy::new)
}

/// Een webdialer over `tcp`.
pub(crate) fn web(tcp: Tcp<'_>) -> Web<'_> {
    WebDial::new(tcp, leantls::MOZILLA_ROOTS, wall, os_entropy)
}

/// Een mislukt verzoek als [`Error`]: de TLS-reden, of de stap van de
/// std-dial, of (zonder beide) de fout van leanhttp.
fn failure(e: leanhttp::Error, tls: Option<leanhttps::Error>, trace: &mut Trace) -> Error {
    match (tls, trace.why.take()) {
        (Some(t @ leanhttps::Error::ChainWithoutName), _) => Error::Dial(format!("tls: {t}")),
        (Some(t), _) => Error::Dial(format!("tls {}: {t}", trace.host)),
        (None, Some(why)) => Error::Dial(why),
        // `WebDial` faalt zo vóór de verbinding: geen entropie (of geen wortels).
        (None, None) if e == leanhttp::Error::Connect => Error::Dial(String::from(
            "no entropy from the OS (/dev/urandom); https refused",
        )),
        (None, None) => Error::Http(e),
    }
}

/// Opent `call` met redirects (GET en HEAD) en geeft het antwoord zodra de kop binnen is.
async fn send(call: &Call<'_>, until: Option<Instant>) -> Result<leanhttp::Response<HostConn>> {
    let lc = leanhttp_call(call)?;
    let mut trace = Trace::default();
    let mut dial = web(Tcp::new(call.timeout, until, Some(&mut trace)));
    let got = leanhttp::fetch(&mut dial, lc).await;
    let tls = dial.last_error();
    got.map_err(|e| failure(e, tls, &mut trace))
}

/// De HTTP(S)-client van de host.
#[derive(Clone, Copy, Debug, Default)]
pub struct Http;

impl Http {
    /// Een client.
    pub fn new() -> Self {
        Self
    }

    /// Doet één verzoek en buffert de body tot `limit` bytes.
    ///
    /// Elke status is een antwoord, ook 4xx en 5xx; alleen transport en
    /// framing zijn fouten. GET en HEAD volgen redirects.
    pub fn request(&self, call: &Call<'_>, limit: usize) -> Result<Reply> {
        block_on(self.request_async(call, limit, None))
    }

    /// Als [`Http::request`], maar de hele aanroep (verbinden, TLS, kop,
    /// body, elke redirect) is klaar vóór `until`, of hij faalt met een
    /// termijnfout.
    ///
    /// Waarom naast de fasetermijn van [`Call::timeout`]: die begint per
    /// fase opnieuw, dus een trage of druppelende server kan een aanroep
    /// een veelvoud ervan laten duren. Een lease-vernieuwing die dat doet,
    /// verliest de lease terwijl hij nog wacht.
    pub fn request_until(&self, call: &Call<'_>, limit: usize, until: Instant) -> Result<Reply> {
        block_on(self.request_async(call, limit, Some(until)))
    }

    async fn request_async(
        &self,
        call: &Call<'_>,
        limit: usize,
        until: Option<Instant>,
    ) -> Result<Reply> {
        let mut resp = send(call, until).await?;
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
        let mut resp = send(call, None).await?;
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

    /// Opent een verzoek en geeft het antwoord zodra de kop binnen is; de
    /// body leest de aanroeper zelf, hap voor hap ([`Open::read`]).
    ///
    /// Voor stromen die niet af zijn als het antwoord begint: een SSE-stroom
    /// of een log-tail door de proxy. [`Call::timeout`] geldt per lees als
    /// stiltetermijn; een stroom die langer zwijgt, is dood. Elke status is
    /// een antwoord; GET volgt redirects.
    pub fn open(&self, call: &Call<'_>) -> Result<Open> {
        block_on(async {
            let resp = send(call, None).await?;
            Ok(Open { resp })
        })
    }
}

/// Een antwoord waarvan de body nog op de verbinding staat ([`Http::open`]).
///
/// Bezit de verbinding; `Drop` sluit hem.
pub struct Open {
    resp: leanhttp::Response<HostConn>,
}

impl fmt::Debug for Open {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Open")
            .field("status", &self.resp.status)
            .finish_non_exhaustive()
    }
}

impl Open {
    /// De status.
    pub fn status(&self) -> u16 {
        self.resp.status
    }

    /// Een header op naam (hoofdletterongevoelig).
    pub fn header(&self, name: &str) -> Option<&str> {
        self.resp.header.get(name)
    }

    /// Leest de volgende hap van de body in `buf`; 0 is het einde.
    pub fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        block_on(self.resp.read(buf)).map_err(Error::Http)
    }

    /// Leest de rest van de body, hoogstens `limit` bytes.
    pub fn read_to_end(&mut self, limit: usize) -> Result<Vec<u8>> {
        block_on(self.resp.read_to_end(limit)).map_err(Error::Http)
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
