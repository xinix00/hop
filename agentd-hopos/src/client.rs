//! De uitgaande HTTP van de cluster: één verzoek over een verse verbinding, kaal of in TLS.
//!
//! De lease, de clusterstaat, de aanroepen bij de leader (register,
//! heartbeat, notify), de doorgifte van de leader naar zijn agents en die van
//! een agent naar de leader: allemaal één verzoek, één antwoord, en dan de
//! verbinding dicht. Dit module bezit per eigenaar (een taak) één [`Client`]:
//! de kale dial van de bewoner ([`Tcp`]: opzoeken en TCP) met daarboven
//! `leanhttps::WebDial` (TLS met `leantls::MOZILLA_ROOTS` als het `https`
//! is), en een eigen `applib::rand::Rng` voor de willekeur van de
//! handshakes. Een verbinding leeft één verzoek; er is geen pool van
//! verbindingen, want de aanroepen zijn zeldzaam (een lease-renew per tien
//! seconden) en een herbruikte verbinding naar een node die intussen
//! herstartte is een fout die een verse nooit heeft.
//!
//! Termijnen: de dial heeft er een (de [`Connect`] van de binary), de
//! handshake die van `WebDial` (20 s), de kop krijgt `timeout`, en de body
//! loopt op de stilte-grens van [`Idle`]: leanhttp zet de leestermijn voor
//! de body op "geen", en een server die na zijn kop zwijgt zou de taak
//! anders voor altijd vasthouden.
//!
//! Wat hier niet staat: welke URL en welke koppen (de backends in
//! [`crate::hoplock`] en [`crate::s3`], de link in [`crate::link`], de
//! doorgifte in [`crate::forward`]).

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use core::task::{Context, Poll};
use core::time::Duration;

use applib::rand::Rng;
use leanhttp::{AsyncRead, AsyncWrite, Close, IoError};
use leanhttps::{Link, WebDial};
use leantls::Entropy;

use crate::fetch::{Connect, Resolve, Tcp, Trace, Web};

/// De grootste S3-body van [`Client::s3`]: één byte boven de grens van een
/// gebufferde GET, zodat leans3 zelf zegt dat een object te groot is.
const S3_BODY: usize = leans3::MAX_BUFFERED_GET as usize + 1;

/// De zin als `https` wacht op de klok.
const NO_CLOCK: &str = "no trusted wall clock (SNTP has not succeeded), so certificate dates cannot be checked; https refused";

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
pub type Open<C> = leanhttp::Response<Link<C>>;

/// De HTTP-client van één eigenaar.
pub struct Client<C, R> {
    connect: C,
    resolver: R,
    rng: Rng,
    /// Nu in Unix-seconden, alleen als de klok vertrouwd is: een keten zonder
    /// datumtoets is geen keten, dus zonder vertrouwde tijd geen `https`.
    trusted_secs: fn() -> Option<u64>,
    /// Wat de kale dial van het laatste verzoek zag.
    trace: Trace,
}

impl<C: Connect, R: Resolve> Client<C, R> {
    /// Een client over `connect` en `resolver`, met `rng` voor de handshakes.
    pub fn new(connect: C, resolver: R, rng: Rng, trusted_secs: fn() -> Option<u64>) -> Self {
        Self {
            connect,
            resolver,
            rng,
            trusted_secs,
            trace: Trace::default(),
        }
    }

    /// De webdialer van één verzoek: [`Tcp`] eronder, TLS met de
    /// Mozilla-wortels voor `https`, verse willekeur per handshake.
    ///
    /// Na een mislukking geeft [`Client::reason`] de zin, met
    /// `WebDial::last_error` van deze dialer erbij.
    pub fn web(&mut self) -> Web<'_, C, R, impl FnMut() -> Option<Entropy> + '_> {
        let rng = &mut self.rng;
        let tcp = Tcp {
            connect: &mut self.connect,
            resolver: &mut self.resolver,
            trace: &mut self.trace,
        };
        WebDial::new(tcp, leantls::MOZILLA_ROOTS, self.trusted_secs, move || {
            Some(Entropy::new(rng.array()))
        })
    }

    /// Het S3-transport van één verzoek: `leans3http::Http` over
    /// [`Client::web`], op `clock`.
    ///
    /// Eén poging binnen `budget` (verbinden, verzoek en hele body), en de
    /// kop ook binnen `budget`: een lease-aanroep heeft één budget, en de
    /// verkiezing is zelf de herkansing (elke tien seconden). De body hoogstens
    /// zo groot als leans3 een gebufferde GET toestaat, zodat een lease of
    /// snapshot nooit meer dan dat in de heap van Hop zet.
    pub fn s3<K: leans3http::Clock>(
        &mut self,
        clock: K,
        budget: Duration,
    ) -> leans3http::Http<impl leanhttp::Dial + '_, K> {
        let mut http = leans3http::Http::new(self.web(), clock);
        http.limits = leans3http::Limits {
            deadline: budget,
            header: budget,
            body: S3_BODY,
            attempts: 1,
        };
        http
    }

    /// Waarom de kale dial van het laatste verzoek faalde, en wist het.
    pub fn dial_why(&mut self) -> Option<String> {
        self.trace.why.take()
    }

    /// Waarom een verzoek over [`Client::web`] faalde, als zin voor de log:
    /// de TLS-reden (`tls`, uit `WebDial::last_error`), anders de stap van
    /// de kale dial, anders de klok, anders de fout van leanhttp.
    pub fn reason(&mut self, e: leanhttp::Error, tls: Option<leanhttps::Error>) -> String {
        let why = self.trace.why.take();
        match (tls, why) {
            (Some(leanhttps::Error::ChainWithoutName), _) => String::from(
                "https to a bare address cannot verify a certificate chain; use a host name",
            ),
            (Some(t), _) => format!("tls {}: {t}", self.trace.host),
            (None, Some(why)) => why,
            (None, None) if e == leanhttp::Error::Connect && (self.trusted_secs)().is_none() => {
                String::from(NO_CLOCK)
            }
            (None, None) => format!("{e}"),
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
        let call = leanhttp::Call {
            method: req.method,
            url: req.url,
            header,
            body: req.body,
            header_timeout: Some(req.timeout),
            no_follow: true,
            ..leanhttp::Call::default()
        };
        let mut dial = self.web();
        let got = leanhttp::fetch(&mut dial, call).await;
        let tls = dial.last_error();
        drop(dial);
        got.map_err(|e| {
            format!(
                "{} {}: {}",
                req.method,
                redact(req.url),
                self.reason(e, tls)
            )
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
