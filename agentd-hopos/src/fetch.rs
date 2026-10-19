//! Artifacts ophalen met de downloader van elke node ([`runner::artifact`]:
//! `http://`, `https://`, `s3://`), en de kale dial van de bewoner.
//!
//! Een job wijst naar een ELF op een URL; op een echt net is dat meestal
//! een GitHub-release (`https://github.com/.../releases/download/...`), en
//! GitHub stuurt door naar `objects.githubusercontent.com`. Dit module
//! bezit de downloader ([`HttpImages`]) en de platformkant onder elke
//! uitgaande verbinding van de bewoner ([`Tcp`]):
//!
//! - elke hop (ook na een redirect) zoekt zijn host op met een
//!   [`Resolve`] en opent TCP met een [`Connect`]: dat is [`Tcp`], de kale
//!   dial. Elke mislukking (de naam, de verbinding) krijgt een eigen zin in
//!   een [`Trace`], in plaats van leanhttp's ene `Connect`;
//! - daarboven `leanhttps::WebDial` (in [`crate::client::Client::web`]):
//!   kaal voor `http://`, TLS met ketenverificatie tegen
//!   `leantls::MOZILLA_ROOTS` op de vertrouwde wandklok voor `https://`,
//!   met SNI per hop. Zonder vertrouwde tijd (SNTP lukte nog niet) weigert
//!   hij `https` vóór er een verbinding opengaat, want een keten zonder
//!   datumtoets is geen keten. leanhttp volgt de redirects en weigert
//!   `https` naar `http`.
//!
//! Waarom de bewoner zijn eigen [`Tcp`] houdt en niet `applib::tcp::Dialer`
//! neemt: die geeft bij elke mislukking dezelfde `Connect`, en de log van
//! een taak zegt hier welke stap faalde (`resolve x: NXDOMAIN`,
//! `connect x (ip:poort): refused`); en de toetsen draaien de hele keten
//! tegen een nep-netstack ([`Connect`] en [`Resolve`] uit het geheugen).

use alloc::format;
use alloc::string::String;
use core::future::Future;

use leanhttp::Target;

use runner::artifact::{self, Fetch};
use types::Artifact;

use crate::client::Client;
use crate::node::{Images, Sink};

/// De leesbuffer van een download: één hap die de runner in brokken naar de kern stroomt.
pub const DOWNLOAD_BUF: usize = 64 << 10;

/// Hoe lang verbinden en de kop van een download mogen duren.
const HEADER_TIMEOUT: core::time::Duration = core::time::Duration::from_secs(60);

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

/// Wat de kale dial van de laatste hop zag: de host, en waarom hij faalde.
#[derive(Clone, Debug, Default)]
pub struct Trace {
    /// De host van de laatste hop die [`Tcp`] dialde (de naam van een TLS-fout).
    pub host: String,
    /// Waarom de laatste dial faalde; `None` als hij lukte.
    pub why: Option<String>,
}

/// De kale dial van de bewoner: opzoeken, dan TCP; de zin van een
/// mislukking in de [`Trace`].
pub struct Tcp<'a, C, R> {
    /// Opent de verbinding.
    pub connect: &'a mut C,
    /// Zoekt de host op.
    pub resolver: &'a mut R,
    /// Wat deze dial zag.
    pub trace: &'a mut Trace,
}

impl<C: Connect, R: Resolve> leanhttp::Dial for Tcp<'_, C, R> {
    type Conn = C::Conn;

    async fn dial(&mut self, target: Target<'_>) -> leanhttp::Result<C::Conn> {
        let host = target.host;
        self.trace.host.clear();
        self.trace.host.push_str(host);
        let ip = match self.resolver.resolve(host).await {
            Ok(ip) => ip,
            Err(e) => {
                self.trace.why = Some(format!("resolve {host}: {e}"));
                return Err(leanhttp::Error::Connect);
            }
        };
        let [a, b, c, d] = ip;
        match self.connect.connect(ip, target.port).await {
            Ok(conn) => {
                self.trace.why = None;
                Ok(conn)
            }
            Err(e) => {
                let port = target.port;
                self.trace.why = Some(format!("connect {host} ({a}.{b}.{c}.{d}:{port}): {e}"));
                Err(leanhttp::Error::Connect)
            }
        }
    }
}

/// De webdialer van de bewoner: `leanhttps::WebDial` over [`Tcp`], met de
/// vertrouwde wandklok als functie en `E` als bron van handshake-entropie.
pub type Web<'a, C, R, E> = leanhttps::WebDial<Tcp<'a, C, R>, fn() -> Option<u64>, E>;

/// De downloader van artifacts: [`runner::artifact`] over de dial van de
/// bewoner, dezelfde als op de host (`http://`, `https://`, `s3://`).
///
/// De artifacts haalt hij in de downloadtaak ([`crate::download`]), naast
/// de eigenaar: de bytes gaan als berichten naar de node, die ze brok voor
/// brok de kern in stroomt. De node heeft er een tweede voor de kernbundel
/// van een flip. Een download volgt redirects (GitHub naar
/// `objects.githubusercontent.com`), anders dan de aanroepen van de cluster.
pub struct HttpImages<C, R> {
    client: Client<C, R>,
    how: Fetch,
}

impl<C: Connect, R: Resolve> HttpImages<C, R> {
    /// Een downloader; `trusted_secs` is nu in Unix-seconden, alleen als de
    /// wandklok vertrouwd is (de keten van https), `wall_secs` de wandklok
    /// voor de handtekening van `s3://`, en `rng` de willekeur van de handshakes.
    pub fn new(
        connect: C,
        resolver: R,
        trusted_secs: fn() -> Option<u64>,
        wall_secs: fn() -> u64,
        rng: applib::rand::Rng,
    ) -> Self {
        Self {
            client: Client::new(connect, resolver, rng, trusted_secs),
            how: Fetch {
                timeout: HEADER_TIMEOUT,
                chunk: DOWNLOAD_BUF,
                now: Some(wall_secs),
            },
        }
    }
}

/// De kern plaatst een image op zijn maat: een download zonder lengte kan niet.
struct Sized<'s, S> {
    url: &'s str,
    sink: &'s mut S,
}

impl<S: Sink> artifact::Sink for Sized<'_, S> {
    async fn begin(&mut self, size: Option<u64>) -> Result<(), String> {
        let size = size.ok_or_else(|| format!("download {}: no Content-Length", self.url))?;
        self.sink.begin(size).await
    }

    async fn chunk(&mut self, bytes: &[u8]) -> Result<(), String> {
        self.sink.chunk(bytes).await
    }
}

impl<C: Connect, R: Resolve> Images for HttpImages<C, R> {
    async fn fetch<S: Sink>(&mut self, a: &Artifact, sink: &mut S) -> Result<(), String> {
        let url = a.url.as_str();
        let mut sized = Sized { url, sink };
        let mut dial = self.client.web();
        let got = artifact::fetch(&mut dial, &self.how, a, &mut sized).await;
        let tls = dial.last_error();
        drop(dial);
        got.map_err(|e| match e {
            // De sink zei het zelf al, met de URL erbij.
            artifact::Error::Sink(why) => why,
            artifact::Error::Net(e) => format!("download {url}: {}", self.client.reason(e, tls)),
            e => format!("download {url}: {e}"),
        })
    }
}
