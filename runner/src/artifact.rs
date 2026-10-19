//! Eén downloader voor het artifact van een taak, op elke node: `http://`,
//! `https://` en `s3://`, en de bytes naar een [`Sink`].
//!
//! Het platform levert alleen twee dingen: de verbinding (een
//! `leanhttp::Dial`: hostnet op de host, de slot-dial met `WebDial` op
//! HopOS) en de [`Sink`] (een bestand in de taakmap, of het slot van de kern
//! waar de bytes brok voor brok in stromen). Zo kan een node niets meer of
//! minder ophalen dan een andere (19-10: `s3://` werkte alleen op de host).
//!
//! - `http`/`https`: de koppen van het artifact, Basic-auth uit
//!   `username`/`password` (die wint van een eigen `Authorization`, zoals in
//!   Go), redirects gevolgd (GitHub naar `objects.githubusercontent.com`).
//! - `s3`: `s3://host/sleutel` met de sleutels in `auth` (zie [`s3_client`]);
//!   leans3 tekent het verzoek, en de [`Pump`] stuurt het zonder redirect en
//!   voert de body zelf aan de sink, zodat die zijn lengte vóór de bytes
//!   krijgt, net als bij http.
//!
//! Een fout draagt de fout van leanhttp ([`Error::Net`]): alleen het platform
//! weet welke stap van zijn dial faalde, en maakt er de zin van.

use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::fmt;
use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use core::time::Duration;

use leanhttp::{Call, Dial, Header};
use types::Artifact;

/// De regio als het artifact er geen noemt (Go: `us-east-1`).
const DEFAULT_REGION: &str = "us-east-1";

/// Zoveel van de body van een foutantwoord komt in de fout.
const ERROR_BODY: usize = 512;

/// Waar de bytes van een artifact heen gaan.
///
/// `-> impl Future` en niet `async fn`: de executor van HopOS eist geen
/// `Send`, en de lint `async_fn_in_trait` kan dat niet weten.
pub trait Sink {
    /// De lengte uit `Content-Length`, of `None` zonder (chunked). Een sink
    /// die een lengte eist (de kern plaatst een image op zijn maat), weigert
    /// hier.
    fn begin(&mut self, size: Option<u64>) -> impl Future<Output = Result<(), String>>;
    /// De volgende bytes.
    fn chunk(&mut self, bytes: &[u8]) -> impl Future<Output = Result<(), String>>;
}

/// Hoe er gedownload wordt; van het platform.
#[derive(Clone, Copy, Debug)]
pub struct Fetch {
    /// De termijn van verbinden en kop (de stilte daarna is van de verbinding).
    pub timeout: Duration,
    /// Hoeveel bytes er per brok naar de sink gaan.
    pub chunk: usize,
    /// Nu in Unix-seconden, voor de handtekening van S3; `None` laat leans3 kiezen.
    pub now: Option<fn() -> u64>,
}

/// Waarom een download niet lukte.
#[derive(Debug)]
pub enum Error {
    /// Het schema is geen `http`, `https` of `s3`.
    Scheme(String),
    /// De URL deugt niet voor zijn schema.
    Url(String),
    /// Een `s3://`-artifact zonder `access_key` en `secret_key` in `auth`.
    S3Credentials,
    /// Verbinden, TLS, framing of lezen faalde; het platform maakt er de zin van.
    Net(leanhttp::Error),
    /// De server antwoordde, maar niet met 200.
    Status {
        /// De statuscode.
        code: u16,
        /// Het begin van de body.
        body: String,
    },
    /// S3 weigerde, of de handtekening kon niet.
    S3(leans3::Error),
    /// Minder bytes dan `Content-Length` beloofde.
    Short {
        /// Wat er kwam.
        got: u64,
        /// Wat er beloofd was.
        want: u64,
    },
    /// De sink weigerde (geen lengte, de kern vol, de schijf vol).
    Sink(String),
    /// Geen geheugen voor de brokbuffer.
    OutOfMemory,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Scheme(s) => write!(f, "unsupported scheme {s:?} (http, https or s3)"),
            Self::Url(u) => write!(f, "{u}"),
            Self::S3Credentials => f.write_str("s3 needs access_key and secret_key in auth"),
            Self::Net(e) => write!(f, "{e}"),
            Self::Status { code, body } if body.is_empty() => write!(f, "status {code}"),
            Self::Status { code, body } => write!(f, "status {code}: {body}"),
            Self::S3(e) => write!(f, "s3: {e}"),
            Self::Short { got, want } => write!(f, "short body: {got} of {want} bytes"),
            Self::Sink(s) => f.write_str(s),
            Self::OutOfMemory => f.write_str("download buffer: out of memory"),
        }
    }
}

/// Het schema van een URL (`http`, `s3`, ...), of leeg zonder `://`.
pub fn scheme(url: &str) -> &str {
    url.split_once("://").map_or("", |(s, _)| s)
}

/// Toetst vóór er iets gebeurt of dit artifact ooit kan lukken: het schema.
pub fn check(a: &Artifact) -> Result<(), Error> {
    match scheme(&a.url) {
        "http" | "https" | "s3" => Ok(()),
        other => Err(Error::Scheme(other.to_string())),
    }
}

/// Haalt `a` op over `dial` en voert de bytes aan `sink`.
pub async fn fetch<D: Dial, S: Sink>(
    dial: &mut D,
    how: &Fetch,
    a: &Artifact,
    sink: &mut S,
) -> Result<(), Error> {
    check(a)?;
    if scheme(&a.url) == "s3" {
        let (mut client, key) = s3_client(a)?;
        if how.now.is_some() {
            client.now = how.now;
        }
        let mut pump = Pump {
            dial,
            how,
            sink,
            fault: None,
        };
        let got = client.get_to(&mut pump, &key, &mut Nothing).await;
        return match (got, pump.fault.take()) {
            (_, Some(fault)) => Err(fault),
            (Ok(_), None) => Ok(()),
            (Err(e), None) => Err(Error::S3(e)),
        };
    }
    let mut header = Header::new();
    for (k, v) in http_headers(a) {
        header
            .set(&k, &v)
            .map_err(|_| Error::Url(format!("{}: invalid header {k}", a.url)))?;
    }
    let call = Call {
        method: "GET",
        url: &a.url,
        header,
        header_timeout: Some(how.timeout),
        ..Call::default()
    };
    let mut resp = leanhttp::fetch(dial, call).await.map_err(Error::Net)?;
    if resp.status != 200 {
        let body = resp.read_to_end(ERROR_BODY).await.unwrap_or_default();
        return Err(Error::Status {
            code: resp.status,
            body: String::from_utf8_lossy(&body).into_owned(),
        });
    }
    pump(&mut resp, how.chunk, sink).await
}

/// De koppen van een http-download: die van het artifact, en Basic-auth uit
/// `username`/`password` als die er allebei zijn.
pub fn http_headers(a: &Artifact) -> Vec<(String, String)> {
    let mut headers: Vec<(String, String)> = a
        .headers
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect();
    let user = a.auth.get("username").map_or("", String::as_str);
    let pass = a.auth.get("password").map_or("", String::as_str);
    if !user.is_empty() && !pass.is_empty() {
        headers.retain(|(k, _)| !k.eq_ignore_ascii_case("authorization"));
        headers.push((
            "Authorization".to_string(),
            format!("Basic {}", base64(format!("{user}:{pass}").as_bytes())),
        ));
    }
    headers
}

/// Stroomt een 200-antwoord naar `sink`: eerst de lengte, dan de brokken.
async fn pump<C: leanhttp::Conn, S: Sink>(
    resp: &mut leanhttp::Response<C>,
    chunk: usize,
    sink: &mut S,
) -> Result<(), Error> {
    sink.begin(resp.length).await.map_err(Error::Sink)?;
    let mut buf = Vec::new();
    buf.try_reserve_exact(chunk.max(1))
        .map_err(|_| Error::OutOfMemory)?;
    buf.resize(chunk.max(1), 0);
    let mut got = 0u64;
    loop {
        let n = resp.read(&mut buf).await.map_err(Error::Net)?;
        if n == 0 {
            break;
        }
        got = got.saturating_add(u64::try_from(n).unwrap_or(u64::MAX));
        sink.chunk(buf.get(..n).unwrap_or_default())
            .await
            .map_err(Error::Sink)?;
    }
    match resp.length {
        Some(want) if got != want => Err(Error::Short { got, want }),
        _ => Ok(()),
    }
}

/// De leans3-client van een `s3://host/sleutel`-URL; geeft ook de sleutel.
///
/// Go tekende `https://<host>/<sleutel>` zelf, met `host` de hele
/// virtual-hosted naam (`bucket.endpoint`). leans3 wil bucket en endpoint
/// apart; zonder `endpoint` in `auth` is het eerste label van de host de
/// bucket en de rest het endpoint, zodat de URL over de draad dezelfde is
/// als in Go. Een host zonder punt is een kale bucket op AWS
/// (`s3.<regio>.amazonaws.com`). Met `endpoint` in `auth` is de host de
/// bucket, en `path_style = "true"` zet hem in het pad (MinIO).
pub fn s3_client(a: &Artifact) -> Result<(leans3::Client, String), Error> {
    let rest = a.url.strip_prefix("s3://").unwrap_or("");
    let (host, key) = rest.split_once('/').unwrap_or((rest, ""));
    if host.is_empty() || key.is_empty() {
        return Err(Error::Url(format!("{}: want s3://host/key", a.url)));
    }
    let auth = |k: &str| a.auth.get(k).cloned().unwrap_or_default();
    let (access, secret) = (auth("access_key"), auth("secret_key"));
    if access.is_empty() || secret.is_empty() {
        return Err(Error::S3Credentials);
    }
    let mut region = auth("region");
    if region.is_empty() {
        region = DEFAULT_REGION.to_string();
    }
    let endpoint = auth("endpoint");
    let (bucket, endpoint) = if !endpoint.is_empty() {
        let e = if endpoint.contains("://") {
            endpoint
        } else {
            format!("https://{endpoint}")
        };
        (host.to_string(), e)
    } else if let Some((bucket, base)) = host.split_once('.') {
        (bucket.to_string(), format!("https://{base}"))
    } else {
        (
            host.to_string(),
            format!("https://s3.{region}.amazonaws.com"),
        )
    };
    let client = leans3::Client {
        endpoint,
        bucket,
        region,
        access_key_id: access,
        secret_access_key: secret,
        session_token: auth("session_token"),
        path_style: auth("path_style") == "true",
        now: None,
    };
    Ok((client, key.to_string()))
}

/// Het transport van een S3-download: stuurt het getekende verzoek (zonder
/// redirect: de handtekening dekt host en pad) en voert een 200 meteen aan
/// de sink. leans3 ziet daarna een leeg antwoord met lengte 0.
struct Pump<'a, D, S> {
    dial: &'a mut D,
    how: &'a Fetch,
    sink: &'a mut S,
    /// De echte fout (dial, sink, lengte); leans3 kent alleen zijn eigen.
    fault: Option<Error>,
}

impl<D: Dial, S: Sink> leans3::Transport for Pump<'_, D, S> {
    type Response = Answer;

    async fn send(&mut self, request: leans3::Request<'_, '_>) -> Result<Answer, leans3::IoError> {
        let url = format!(
            "{}://{}{}",
            if request.https { "https" } else { "http" },
            request.host,
            request.target
        );
        let mut header = Header::new();
        for h in request.headers {
            header
                .set(h.name, &h.value)
                .map_err(|_| leans3::IoError::Other("invalid header"))?;
        }
        let call = Call {
            method: request.method,
            url: &url,
            header,
            header_timeout: Some(self.how.timeout),
            no_follow: true,
            ..Call::default()
        };
        let mut resp = match leanhttp::fetch(self.dial, call).await {
            Ok(resp) => resp,
            Err(e) => {
                self.fault = Some(Error::Net(e));
                return Err(leans3::IoError::Other("dial failed"));
            }
        };
        if resp.status != 200 {
            let body = resp.read_to_end(ERROR_BODY).await.unwrap_or_default();
            return Ok(Answer {
                status: resp.status,
                reason: resp.reason.clone(),
                body,
                at: 0,
            });
        }
        if let Err(e) = pump(&mut resp, self.how.chunk, self.sink).await {
            self.fault = Some(e);
            return Err(leans3::IoError::Other("download failed"));
        }
        Ok(Answer {
            status: 200,
            reason: String::new(),
            body: Vec::new(),
            at: 0,
        })
    }
}

/// Wat de [`Pump`] leans3 teruggeeft: een foutantwoord met zijn body, of
/// een lege 200 als de bytes al in de sink staan.
struct Answer {
    status: u16,
    reason: String,
    body: Vec<u8>,
    at: usize,
}

impl leans3::AsyncRead for Answer {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, leans3::IoError>> {
        let this = self.get_mut();
        let rest = this.body.get(this.at..).unwrap_or_default();
        let n = rest.len().min(buf.len());
        if let (Some(dst), Some(src)) = (buf.get_mut(..n), rest.get(..n)) {
            dst.copy_from_slice(src);
        }
        this.at += n;
        Poll::Ready(Ok(n))
    }
}

impl leans3::Response for Answer {
    fn status(&self) -> u16 {
        self.status
    }
    fn reason(&self) -> &str {
        &self.reason
    }
    fn header(&self, _: &str) -> Option<&str> {
        None
    }
    fn content_length(&self) -> Option<u64> {
        u64::try_from(self.body.len()).ok()
    }
}

/// De schrijver die leans3 krijgt: er komt niets, de bytes gingen al naar de sink.
struct Nothing;

impl leans3::AsyncWrite for Nothing {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, leans3::IoError>> {
        Poll::Ready(Ok(buf.len()))
    }
}

/// Standaard base64 (RFC 4648, met opvulling), voor de Basic-kop.
pub fn base64(data: &[u8]) -> String {
    const ABC: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b = [
            chunk.first().copied().unwrap_or(0),
            chunk.get(1).copied().unwrap_or(0),
            chunk.get(2).copied().unwrap_or(0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        for i in 0..4 {
            if i <= chunk.len() {
                let idx = usize::try_from((n >> (18 - 6 * i)) & 63).unwrap_or(0);
                out.push(char::from(ABC.get(idx).copied().unwrap_or(b'=')));
            } else {
                out.push('=');
            }
        }
    }
    out
}
