//! De uitgaande HTTP van de cluster: één verzoek over een verse verbinding, kaal of in TLS.
//!
//! De lease, de clusterstaat, de aanroepen bij de leader (register,
//! heartbeat, notify), de doorgifte van de leader naar zijn agents en die van
//! een agent naar de leader: allemaal één verzoek, één antwoord, en dan de
//! verbinding dicht. Dit module bezit per eigenaar (een taak) één [`Client`]:
//! de dialer van de artifacts ([`ArtifactDial`]: opzoeken, TCP, TLS met de
//! Mozilla-wortels als het `https` is) en een eigen [`Pool`] voor de
//! willekeur van de handshakes. Een verbinding leeft één verzoek; er is geen
//! pool van verbindingen, want de aanroepen zijn zeldzaam (een lease-renew per
//! tien seconden) en een herbruikte verbinding naar een node die intussen
//! herstartte is een fout die een verse nooit heeft.
//!
//! Termijnen: de dial heeft er een (de [`Connect`] van de binary), de kop
//! krijgt `timeout`, en de body loopt op de stilte-grens van [`Idle`]:
//! leanhttp zet de leestermijn voor de body op "geen", en een server die na
//! zijn kop zwijgt zou de taak anders voor altijd vasthouden.
//!
//! Wat hier niet staat: welke URL en welke koppen (de backends in
//! [`crate::hoplock`] en [`crate::s3`], de link in [`crate::link`], de
//! doorgifte in [`crate::forward`]).

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use core::task::{Context, Poll};
use core::time::Duration;

use leanhttp::{AsyncRead, AsyncWrite, Close, IoError};
use leantls::{ChainVerifier, Roots, Trust};

use crate::entropy::Pool;
use crate::fetch::{ArtConn, ArtifactDial, Connect, ROOTS_DER, Resolve};

/// Een verbinding die nooit zonder leestermijn leest.
///
/// leanhttp's client zet de leestermijn na de kop op `None` (een download van
/// een uur mag duren), maar een aanroep van de cluster is klein: wie na zijn
/// kop `idle` lang niets meer stuurt, is dood. Een gezette termijn gaat
/// ongewijzigd door.
pub struct Idle<C> {
    inner: C,
    idle: Duration,
}

impl<C> Idle<C> {
    /// `inner` met hoogstens `idle` stilte per lees.
    pub fn new(inner: C, idle: Duration) -> Self {
        Self { inner, idle }
    }
}

impl<C: AsyncRead> AsyncRead for Idle<C> {
    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize, IoError>> {
        self.inner.poll_read(cx, buf)
    }

    fn set_read_timeout(&mut self, t: Option<Duration>) -> Result<(), IoError> {
        self.inner.set_read_timeout(Some(t.unwrap_or(self.idle)))
    }
}

impl<C: AsyncWrite> AsyncWrite for Idle<C> {
    fn poll_write(&mut self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, IoError>> {
        self.inner.poll_write(cx, buf)
    }

    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), IoError>> {
        self.inner.poll_flush(cx)
    }

    fn set_write_timeout(&mut self, t: Option<Duration>) -> Result<(), IoError> {
        self.inner.set_write_timeout(Some(t.unwrap_or(self.idle)))
    }
}

impl<C: Close> Close for Idle<C> {
    fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), IoError>> {
        self.inner.poll_close(cx)
    }

    fn has_grown(&self) -> bool {
        self.inner.has_grown()
    }
}

/// Eén uitgaand verzoek.
#[derive(Clone, Copy, Debug)]
pub struct Req<'a> {
    /// De methode (`GET`, `PUT`, ...).
    pub method: &'a str,
    /// De absolute URL, `http://` of `https://`.
    pub url: &'a str,
    /// Extra koppen.
    pub headers: &'a [(&'a str, &'a str)],
    /// De body, als die er is.
    pub body: Option<&'a [u8]>,
    /// Hoe lang de kop van het antwoord mag uitblijven.
    pub timeout: Duration,
}

impl<'a> Req<'a> {
    /// Een `GET` op `url` zonder koppen.
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

/// Een gebufferd antwoord: status, de koppen die de cluster leest, en de body.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Reply {
    /// De status.
    pub status: u16,
    /// De `ETag`, als die er was en niet leeg is.
    pub etag: Option<String>,
    /// Het `Content-Type`, als dat er was.
    pub content_type: Option<String>,
    /// De body.
    pub body: Vec<u8>,
}

/// Een open antwoord: de body staat nog op de verbinding (een stroom).
pub type Open<C> = leanhttp::Response<ArtConn<C>>;

/// De HTTP-client van één eigenaar.
pub struct Client<C, R> {
    connect: C,
    resolver: R,
    pool: Pool,
    roots: Option<Roots<'static>>,
    /// Nu in Unix-seconden, alleen als de klok vertrouwd is: een keten zonder
    /// datumtoets is geen keten, dus zonder vertrouwde tijd geen `https`.
    trusted_secs: fn() -> Option<u64>,
}

impl<C: Connect, R: Resolve> Client<C, R> {
    /// Een client met de ingebakken wortels.
    pub fn new(connect: C, resolver: R, pool: Pool, trusted_secs: fn() -> Option<u64>) -> Self {
        Self {
            connect,
            resolver,
            pool,
            roots: Roots::from_concatenated_der(ROOTS_DER).ok(),
            trusted_secs,
        }
    }

    /// Opent een verzoek en geeft het antwoord met de body nog op de verbinding.
    ///
    /// Volgt geen redirect: een API-aanroep of een getekend S3-verzoek hoort
    /// er geen te krijgen, en een omleiding zou de koppen (de sleutel) naar
    /// een andere host sturen. Elke status is een antwoord; alleen transport
    /// en termijn zijn een fout, met de reden erin.
    pub async fn open(&mut self, req: Req<'_>) -> Result<Open<C::Conn>, String> {
        let mut header = leanhttp::Header::new();
        for (k, v) in req.headers {
            header
                .set(k, v)
                .map_err(|e| format!("{} {}: header {k}: {e}", req.method, redact(req.url)))?;
        }
        let verifier = self
            .roots
            .zip((self.trusted_secs)())
            .map(|(roots, now)| ChainVerifier::new(roots, now));
        let trust = verifier.as_ref().map(|v| Trust::Chain(v));
        let mut dial =
            ArtifactDial::new(&mut self.connect, &mut self.resolver, trust, &mut self.pool);
        let call = leanhttp::Call {
            method: req.method,
            url: req.url,
            header,
            body: req.body,
            header_timeout: Some(req.timeout),
            no_follow: true,
            ..leanhttp::Call::default()
        };
        let got = leanhttp::fetch(&mut dial, call).await;
        got.map_err(|e| match dial.why() {
            Some(why) => format!("{} {}: {why}", req.method, redact(req.url)),
            None => format!("{} {}: {e}", req.method, redact(req.url)),
        })
    }

    /// Eén verzoek met de body gebufferd, hoogstens `limit` bytes.
    pub async fn request(&mut self, req: Req<'_>, limit: usize) -> Result<Reply, String> {
        let mut resp = self.open(req).await?;
        let body = resp
            .read_to_end(limit)
            .await
            .map_err(|e| format!("{} {}: body: {e}", req.method, redact(req.url)))?;
        let etag = resp
            .header
            .get("ETag")
            .filter(|e| !e.is_empty())
            .map(String::from);
        let content_type = resp.header.get("Content-Type").map(String::from);
        let status = resp.status;
        // Dicht: de verbinding leeft één verzoek (zie de moduledoc).
        if let Some(mut conn) = resp.release().await {
            let _ = leanhttp::close(&mut conn).await;
        }
        Ok(Reply {
            status,
            etag,
            content_type,
            body,
        })
    }

    /// Mengt `sample` in de willekeur van de handshakes (een tijd, een tik).
    pub fn stir(&mut self, sample: &[u8]) {
        self.pool.stir(sample);
    }
}

/// Een URL zonder `gebruiker:wachtwoord@`, zodat een geheim in de URL niet in een logregel belandt.
pub fn redact(url: &str) -> String {
    let (scheme, rest) = match url.split_once("://") {
        Some((s, r)) => (Some(s), r),
        None => (None, url),
    };
    let end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let (authority, tail) = rest.split_at(end);
    let host = match authority.rsplit_once('@') {
        Some((_, host)) => host,
        None => authority,
    };
    match scheme {
        Some(s) => format!("{s}://{host}{tail}"),
        None => format!("{host}{tail}"),
    }
}

/// Het begin van een foutbody, als tekst en getrimd (hoogstens 256 bytes).
pub fn excerpt(body: &[u8]) -> String {
    let cut = body.get(..256).unwrap_or(body);
    String::from(String::from_utf8_lossy(cut).trim())
}
