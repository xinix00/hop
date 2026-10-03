//! Artifacts ophalen over `http://` en `https://`, met echte certificaatketens.
//!
//! Een job wijst naar een ELF op een URL; op een echt net is dat meestal
//! een GitHub-release (`https://github.com/.../releases/download/...`), en
//! GitHub stuurt door naar `objects.githubusercontent.com`. Dit module
//! bezit de dialer van die download ([`ArtifactDial`]) en de downloader
//! ([`HttpImages`]):
//!
//! - elke hop (ook na een redirect) zoekt zijn host op met een
//!   [`Resolve`], opent TCP met een [`Connect`], en kiest dan zelf: kaal
//!   voor `http://`, TLS voor `https://`. leanhttp volgt de redirects en
//!   weigert `https` naar `http`;
//! - TLS is `leanhttps` met [`Trust::Chain`]: een [`ChainVerifier`] op de
//!   ingebakken Mozilla-wortels ([`ROOTS_DER`]) en de tijd van de
//!   [`Clock`]. Zonder vertrouwde tijd (SNTP lukte nog niet) weigert de
//!   dialer `https` met die reden, want een keten zonder datumtoets is
//!   geen keten;
//! - de willekeur van elke handshake komt uit de [`Pool`] van de
//!   downloader.
//!
//! Waarom de TCP-verbinding hier geopend wordt en niet in de dialer van
//! leanhttps: zo heeft elke mislukking (de naam, de verbinding, de
//! handshake, de keten) een eigen zin in de log van de taak, in plaats van
//! leanhttp's ene `Connect`.

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use core::future::Future;
use core::task::{Context, Poll};
use core::time::Duration;

use leanhttp::{AsyncRead, AsyncWrite, Close, IoError, Target};
use leanhttps::{TlsConn, TlsDial};
use leantls::{ChainVerifier, Roots, Trust};

use crate::entropy::Pool;
use crate::node::{Images, Sink};

/// De vertrouwde wortels: de Mozilla-set (NSS `certdata.txt`) van juli
/// 2026, 119 certificaten als aaneengeschakelde DER.
///
/// Gekopieerd op 29-09-2026 uit `lean/leantls/testdata/github/mozilla-roots.der`
/// (lean v3.1.1, commit db67247), waar leantls er de echte ketens van
/// github.com, api.github.com en objects.githubusercontent.com (augustus
/// 2026) tegen toetst. Vervangen is een nieuwe kopie van een nieuwere set,
/// met deze datum mee.
pub const ROOTS_DER: &[u8] = include_bytes!("../roots.der");

/// De leesbuffer van een download: één hap die de runner in brokken naar de kern stroomt.
pub const DOWNLOAD_BUF: usize = 64 << 10;

/// Zoekt een host op.
pub trait Resolve {
    /// Het IPv4-adres van `host`; een fout is een zin voor de log.
    fn resolve(&mut self, host: &str) -> impl Future<Output = Result<[u8; 4], String>>;
}

/// Opent TCP-verbindingen.
pub trait Connect {
    /// De verbinding.
    type Conn: leanhttp::Conn + Unpin;
    /// Verbindt met `ip:port`; een fout is een zin voor de log.
    fn connect(
        &mut self,
        ip: [u8; 4],
        port: u16,
    ) -> impl Future<Output = Result<Self::Conn, String>>;
}

/// De klok van de downloader.
pub trait Clock {
    /// Nu in Unix-seconden, alleen als de tijd vertrouwd is (SNTP lukte).
    fn trusted_unix_secs(&self) -> Option<u64>;
    /// De monotone klok (ns), voor de willekeur.
    fn mono_ns(&self) -> u64;
}

/// Een resolver voor namen die al een adres zijn; een echte naam faalt met
/// een zin die zegt waarom.
#[derive(Copy, Clone, Debug, Default)]
pub struct IpOnly;

impl Resolve for IpOnly {
    async fn resolve(&mut self, host: &str) -> Result<[u8; 4], String> {
        applib::appnet::parse_ip4(host)
            .ok_or_else(|| format!("{host}: no resolver, only IPv4 addresses"))
    }
}

/// Het schema van een artifact-URL.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Scheme {
    /// `http://`.
    Http,
    /// `https://`.
    Https,
}

/// Het schema van `url`, of waarom het geen artifact-URL is.
pub fn scheme_of(url: &str) -> Result<Scheme, String> {
    let (scheme, rest) = url
        .split_once("://")
        .ok_or_else(|| format!("{url:?} is not a URL"))?;
    let s = if scheme.eq_ignore_ascii_case("http") {
        Scheme::Http
    } else if scheme.eq_ignore_ascii_case("https") {
        Scheme::Https
    } else {
        return Err(format!(
            "{url:?}: scheme {scheme:?} is not supported (http or https)"
        ));
    };
    let host = rest.split(['/', '?', '#']).next().unwrap_or("");
    if host.is_empty() {
        return Err(format!("{url:?} has no host"));
    }
    Ok(s)
}

/// Een verbinding van een download: kaal of in TLS.
#[expect(
    clippy::large_enum_variant,
    reason = "één verbinding per download, en TLS is het gewone geval (GitHub); boxen kost een allocatie per hop voor niets"
)]
pub enum ArtConn<C> {
    /// `http://`.
    Plain(C),
    /// `https://`.
    Tls(TlsConn<C>),
}

impl<C: leanhttp::Conn + Unpin> AsyncRead for ArtConn<C> {
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

impl<C: leanhttp::Conn + Unpin> AsyncWrite for ArtConn<C> {
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

impl<C: leanhttp::Conn + Unpin> Close for ArtConn<C> {
    fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), IoError>> {
        match self {
            Self::Plain(c) => c.poll_close(cx),
            Self::Tls(c) => c.poll_close(cx),
        }
    }

    fn has_grown(&self) -> bool {
        match self {
            Self::Plain(c) => c.has_grown(),
            Self::Tls(c) => c.has_grown(),
        }
    }
}

/// Een dialer die één al geopende verbinding geeft: de TCP-kant onder
/// leanhttps, zodat die alleen de handshake doet.
struct Ready<C>(Option<C>);

impl<C: leanhttp::Conn> leanhttp::Dial for Ready<C> {
    type Conn = C;

    async fn dial(&mut self, _target: Target<'_>) -> leanhttp::Result<C> {
        self.0.take().ok_or(leanhttp::Error::Connect)
    }
}

/// De dialer van één download: opzoeken, TCP, en TLS als de hop `https`
/// is. Onthoudt waarom de laatste dial faalde.
pub struct ArtifactDial<'a, 't, C, R> {
    connect: &'a mut C,
    resolver: &'a mut R,
    /// `None` zonder vertrouwde tijd: dan weigert elke `https`-hop.
    trust: Option<Trust<'t>>,
    pool: &'a mut Pool,
    why: Option<String>,
}

impl<'a, 't, C: Connect, R: Resolve> ArtifactDial<'a, 't, C, R> {
    /// Een dialer; `trust` is `None` zonder vertrouwde tijd.
    pub fn new(
        connect: &'a mut C,
        resolver: &'a mut R,
        trust: Option<Trust<'t>>,
        pool: &'a mut Pool,
    ) -> Self {
        Self {
            connect,
            resolver,
            trust,
            pool,
            why: None,
        }
    }

    /// Waarom de laatste dial faalde, als hij faalde.
    pub fn why(&self) -> Option<&str> {
        self.why.as_deref()
    }

    /// Eén hop naar `target`, met de reden van een mislukking in `why`.
    async fn hop(&mut self, target: Target<'_>) -> Result<ArtConn<C::Conn>, String> {
        let host = target.host;
        if target.https {
            // Een keten toetst een naam; een kaal IP heeft er geen.
            if applib::appnet::parse_ip4(host).is_some() {
                return Err(format!(
                    "https to the bare address {host} cannot verify a certificate chain; use a host name"
                ));
            }
            if self.trust.is_none() {
                return Err(String::from(
                    "no trusted wall clock (SNTP has not succeeded), so certificate dates cannot be checked; https refused",
                ));
            }
        }
        let ip = self
            .resolver
            .resolve(host)
            .await
            .map_err(|e| format!("resolve {host}: {e}"))?;
        let [a, b, c, d] = ip;
        let raw = self
            .connect
            .connect(ip, target.port)
            .await
            .map_err(|e| format!("connect {host} ({a}.{b}.{c}.{d}:{}): {e}", target.port))?;
        let Some(trust) = self.trust.filter(|_| target.https) else {
            return Ok(ArtConn::Plain(raw));
        };
        let pool = &mut *self.pool;
        let mut tls = TlsDial::new(Ready(Some(raw)), trust, || pool.entropy());
        match leanhttp::Dial::dial(&mut tls, target).await {
            Ok(c) => Ok(ArtConn::Tls(c)),
            Err(e) => Err(match tls.last_error() {
                Some(why) => format!("tls {host}: {why}"),
                None => format!("tls {host}: {e}"),
            }),
        }
    }
}

impl<C: Connect, R: Resolve> leanhttp::Dial for ArtifactDial<'_, '_, C, R> {
    type Conn = ArtConn<C::Conn>;

    async fn dial(&mut self, target: Target<'_>) -> leanhttp::Result<Self::Conn> {
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

    /// Deze dialer versleutelt als de hop `https` is, dus leanhttp laat
    /// `https://` en een redirect ernaartoe door; de keuze valt per hop.
    fn is_encrypted(&self) -> bool {
        true
    }
}

/// De downloader van artifacts: `http://` en `https://`, en de bytes via de runner de kooi in.
///
/// De artifacts haalt hij in de downloadtaak ([`crate::download`]), naast
/// de eigenaar: de bytes gaan als berichten naar de node, die ze brok voor
/// brok de kern in stroomt. De node heeft er een tweede voor de kernbundel
/// van een flip.
pub struct HttpImages<C, R, K> {
    connect: C,
    resolver: R,
    clock: K,
    pool: Pool,
    /// De wortels, één keer gelezen; `None` als de ingebakken set niet las
    /// (dan weigert elke `https`, luid).
    roots: Option<Roots<'static>>,
}

impl<C: Connect, R: Resolve, K: Clock> HttpImages<C, R, K> {
    /// Een downloader met de ingebakken wortels.
    pub fn new(connect: C, resolver: R, clock: K, pool: Pool) -> Self {
        Self {
            connect,
            resolver,
            clock,
            pool,
            roots: Roots::from_concatenated_der(ROOTS_DER).ok(),
        }
    }

    /// Hoeveel wortels de downloader vertrouwt (0: de set las niet).
    pub fn root_count(&self) -> usize {
        self.roots.map_or(0, |r| r.len())
    }
}

impl<C: Connect, R: Resolve, K: Clock> Images for HttpImages<C, R, K> {
    async fn fetch<S: Sink>(&mut self, url: &str, sink: &mut S) -> Result<(), String> {
        let scheme = scheme_of(url).map_err(|e| format!("download: {e}"))?;
        // De tijd van deze download gaat mee in de willekeur.
        self.pool.stir(&self.clock.mono_ns().to_le_bytes());
        let now = self.clock.trusted_unix_secs();
        let verifier = self
            .roots
            .zip(now)
            .map(|(roots, now)| ChainVerifier::new(roots, now));
        if scheme == Scheme::Https && self.roots.is_none() {
            return Err(format!(
                "download {url}: the built-in root certificates did not parse; https refused"
            ));
        }
        let trust = verifier.as_ref().map(|v| Trust::Chain(v));
        let mut dial =
            ArtifactDial::new(&mut self.connect, &mut self.resolver, trust, &mut self.pool);
        // `get` eist 200 en een Content-Length: een image zonder lengte
        // kan de kern niet plaatsen.
        let got = leanhttp::get(&mut dial, url).await;
        let mut resp = got.map_err(|e| match dial.why() {
            Some(why) => format!("download {url}: {why}"),
            None => format!("download {url}: {e}"),
        })?;
        let len = resp
            .length
            .ok_or_else(|| format!("download {url}: no Content-Length"))?;
        sink.begin(len).await?;
        let mut buf = Vec::new();
        buf.try_reserve_exact(DOWNLOAD_BUF)
            .map_err(|_| String::from("download buffer: out of memory"))?;
        buf.resize(DOWNLOAD_BUF, 0);
        loop {
            let n = resp
                .read(&mut buf)
                .await
                .map_err(|e| format!("download {url}: {e}"))?;
            if n == 0 {
                return Ok(());
            }
            sink.chunk(buf.get(..n).unwrap_or_default()).await?;
        }
    }
}
