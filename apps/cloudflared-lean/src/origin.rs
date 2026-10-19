//! De poot naar de lokale dienst: een verzoek van de edge als HTTP/1.1.
//!
//! Bezit de vertaling van één HTTP/2-stream naar één verzoek met leanhttp
//! over een eigen `appnet::TcpStream`, en terug: de koppen in de bundel die
//! de edge eist ([`crate::edgeproto`]), de body als DATA-frames. Niet de
//! keuze van de dienst ([`crate::ingress`]) en niet de antwoorden die de
//! tunnel zelf maakt (`crate::tunnel`).
//!
//! De edge-kant is HTTP/2 en dit is HTTP/1.1: precies de vertaling die een
//! tunnel doet, multiplexen naar buiten, gewone verzoeken naar binnen. Eén
//! verbinding per verzoek, zoals de Go-voorganger na zijn pool-les: een
//! herbruikte verbinding naar een dienst die intussen herstartte, is een
//! fout die een verse nooit heeft, en de dial op het slot-LAN is goedkoop.

#![forbid(unsafe_code)]

use alloc::string::String;
use alloc::vec::Vec;
use core::fmt;
use core::fmt::Write as _;
use core::time::Duration;

use applib::appnet::{self, NetError, TcpStream};
use applib::rt::Exec;
use applib::tcp::TcpConn;
use leanh2::{Request, Response};

use crate::edgeproto::{self, Bundle, Source};

/// Het plafond voor een verzoekbody. Elke body wordt eerst gelezen en dan
/// als geheel verstuurd, nooit stromend.
///
/// Dat is een naad tussen twee lean-helften en hij kostte de Go-voorganger
/// een 417 (gemeten 19-08): leanhttp's client stuurt bij een gestroomde body
/// altijd `Expect: 100-continue`, en leanhttp's server (die de diensten in
/// het huis draaien) weigert elke `Expect`. Bufferen kan wel, want dan
/// schrijft de client een gewone `Content-Length`. 4 MiB, en een body die
/// er niet in past krijgt een luide 413 in plaats van een halve upload.
pub(crate) const MAX_BODY: usize = 4 << 20;

/// Hoe lang de tunnel op de antwoordkop van de dienst wacht. 95 seconden:
/// een verzoek mag zelf traag werk zijn (Matter-commissioneren via stulp
/// wacht tot een minuut, gemeten 20-08), en Cloudflare kapt een antwoord
/// rond 100 seconden zelf af (hun 524), dus meer is theater.
pub(crate) const HEADER_TIMEOUT: Duration = Duration::from_secs(95);

/// Hoe lang een dial naar de dienst mag duren; een dichte poort op het
/// slot-LAN antwoordt meteen met een reset, een dode host niet.
pub(crate) const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Het stuk body dat per keer van de dienst naar de edge gaat: één frame van
/// leanh2.
const CHUNK: usize = 16 << 10;

/// Waarom een verzoek niet bij de dienst aankwam, of halverwege stopte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Error {
    /// De dienst is geen `http://`-adres dat deze tunnel draagt.
    Service(&'static str),
    /// De naam van de dienst werd geen adres.
    Resolve(NetError),
    /// De dienst nam niet op.
    Connect(NetError),
    /// De verzoekbody past niet in [`MAX_BODY`].
    BodyTooLarge,
    /// De verzoekbody van de edge brak af.
    RequestBody(leanh2::Error),
    /// Het verzoek of het antwoord van de dienst faalde.
    Http(leanhttp::Error),
    /// Het antwoord naar de edge faalde; de koppen kunnen al weg zijn.
    Edge(leanh2::Error),
    /// De heap weigerde.
    OutOfMemory,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Service(why) => f.write_str(why),
            Self::Resolve(e) => write!(f, "resolving the service: {e}"),
            Self::Connect(e) => write!(f, "connecting to the service: {e}"),
            Self::BodyTooLarge => write!(f, "request body above {MAX_BODY} bytes"),
            Self::RequestBody(e) => write!(f, "reading the request body: {e}"),
            Self::Http(e) => write!(f, "origin: {e}"),
            Self::Edge(e) => write!(f, "writing to the edge: {e}"),
            Self::OutOfMemory => f.write_str("out of memory"),
        }
    }
}

/// Waar een dienst-URL heen wijst.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Target<'a> {
    /// De host: een naam of een IPv4-adres.
    pub(crate) host: &'a str,
    /// De poort (80 als hij ontbreekt).
    pub(crate) port: u16,
    /// Een padvoorvoegsel zonder slot-`/`: `http://h:8080/api` stuurt alles
    /// onder `/api`.
    pub(crate) prefix: &'a str,
}

/// Leest een dienst uit een ingress-regel.
pub(crate) fn target(service: &str) -> Result<Target<'_>, Error> {
    let Some(rest) = service
        .get(..7)
        .filter(|s| s.eq_ignore_ascii_case("http://"))
        .and_then(|_| service.get(7..))
    else {
        if service.len() >= 8
            && service
                .get(..8)
                .is_some_and(|s| s.eq_ignore_ascii_case("https://"))
        {
            // Een oorsprong achter TLS vraagt leanhttps plus een keuze over
            // certificaatverificatie (Cloudflare's standaard is "niet
            // verifiëren", en dat is geen keuze om hier stil te maken).
            return Err(Error::Service(
                "https origins are not carried yet; use http:// in the ingress rule",
            ));
        }
        return Err(Error::Service(
            "service is not an http:// address this tunnel carries",
        ));
    };
    let end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let (authority, tail) = rest.split_at(end);
    let prefix = tail
        .split(['?', '#'])
        .next()
        .unwrap_or("")
        .trim_end_matches('/');
    if authority.is_empty() || authority.contains('@') || authority.starts_with('[') {
        return Err(Error::Service(
            "service has no usable host (IPv6 and user info are not carried)",
        ));
    }
    let (host, port) = match authority.rsplit_once(':') {
        Some((h, p)) => (
            h,
            p.parse::<u16>()
                .ok()
                .filter(|&p| p != 0)
                .ok_or(Error::Service("service has an unusable port"))?,
        ),
        None => (authority, 80),
    };
    if host.is_empty() {
        return Err(Error::Service("service has no host"));
    }
    Ok(Target { host, port, prefix })
}

/// Of een verzoekkop bij onze verbinding met de edge hoort en niet bij het
/// verzoek aan de dienst (RFC 9110 §7.6.1, plus cloudflared's eigen koppen).
/// leanhttp bezit daarnaast zelf de framing (`Host`, `Content-Length`,
/// `Connection`, `Transfer-Encoding`, `Expect`) en weigert een aanroeper die
/// ze zet; die staan hier dus ook in.
pub(crate) fn is_hop_by_hop(name: &str) -> bool {
    HOP_BY_HOP.iter().any(|h| h.eq_ignore_ascii_case(name))
}

/// De koppen van [`is_hop_by_hop`].
const HOP_BY_HOP: [&str; 12] = [
    "connection",
    "keep-alive",
    "transfer-encoding",
    "upgrade",
    "proxy-connection",
    "te",
    "trailer",
    "host",
    "content-length",
    "expect",
    "cf-cloudflared-proxy-connection-upgrade",
    "cf-cloudflared-proxy-src",
];

/// De host van het verzoek zonder poort: die hoort niet bij een
/// ingress-hostname.
pub(crate) fn request_host<'a>(authority: &'a str, host_header: Option<&'a str>) -> &'a str {
    let host = if authority.is_empty() {
        host_header.unwrap_or("")
    } else {
        authority
    };
    match host.rfind(':') {
        Some(i) if !host.get(i..).is_some_and(|t| t.contains(']')) => host.get(..i).unwrap_or(host),
        _ => host,
    }
}

/// De URL voor leanhttp: de publieke hostnaam (zodat de `Host`-kop die van
/// de bezoeker is), dan het voorvoegsel van de dienst en het pad.
pub(crate) fn url(public_host: &str, t: &Target<'_>, path: &str) -> Result<String, Error> {
    let path = if path.is_empty() { "/" } else { path };
    let mut out = String::new();
    out.try_reserve_exact(7 + public_host.len() + 6 + t.prefix.len() + path.len())
        .map_err(|_| Error::OutOfMemory)?;
    out.push_str("http://");
    if public_host.is_empty() {
        write!(out, "{}:{}", t.host, t.port).map_err(|_| Error::OutOfMemory)?;
    } else {
        out.push_str(public_host);
    }
    out.push_str(t.prefix);
    out.push_str(path);
    Ok(out)
}

/// Of een lengte uit `content-length` boven het plafond ligt; een onleesbare
/// lengte heeft leanh2 al geweigerd.
fn announced_too_large(req: &Request<'_>) -> bool {
    req.get("content-length")
        .and_then(|v| v.parse::<u64>().ok())
        .is_some_and(|n| n > MAX_BODY as u64)
}

/// Leest de verzoekbody helemaal, tot [`MAX_BODY`]; `None` als er geen is.
async fn read_body(req: &mut Request<'_>) -> Result<Option<Vec<u8>>, Error> {
    if req.method == "GET" || req.method == "HEAD" {
        return Ok(None);
    }
    // Een aangekondigde lengte boven het plafond weigeren voordat er één
    // byte gelezen is: dat scheelt de bezoeker een upload naar een 413.
    if announced_too_large(req) {
        return Err(Error::BodyTooLarge);
    }
    let mut body = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let n = req
            .body
            .read(&mut chunk)
            .await
            .map_err(Error::RequestBody)?;
        if n == 0 {
            break;
        }
        if body.len() + n > MAX_BODY {
            return Err(Error::BodyTooLarge);
        }
        body.try_reserve(n).map_err(|_| Error::OutOfMemory)?;
        body.extend_from_slice(chunk.get(..n).unwrap_or(&[]));
    }
    Ok((!body.is_empty()).then_some(body))
}

/// De koppen voor de dienst: alles van de bezoeker behalve wat bij de edge
/// hoort, plus wie er echt aanklopt.
fn origin_header(req: &Request<'_>) -> leanhttp::Header {
    let mut h = leanhttp::Header::new();
    for (name, value) in &req.header {
        if is_hop_by_hop(name) {
            continue;
        }
        // Een veld dat leanhttp weigert (een waarde met controletekens),
        // gaat niet mee; de rest van het verzoek wel.
        let _ = h.append(name, value);
    }
    // De edge zet cf-connecting-ip: het enige eerlijke antwoord op "wie",
    // want ons slot-IP zegt niemand iets.
    if let Some(ip) = req.get("cf-connecting-ip") {
        let _ = h.set("x-forwarded-for", ip);
        let _ = h.set("x-forwarded-proto", "https");
    }
    h
}

/// Brengt één verzoek naar de dienst op `service` en schrijft het antwoord
/// naar de edge. Geeft de status van de dienst.
pub(crate) async fn proxy(
    exec: &'static Exec,
    service: &str,
    req: &mut Request<'_>,
    res: &mut Response<'_>,
) -> Result<u16, Error> {
    let t = target(service)?;
    let ip = appnet::resolve(t.host).await.map_err(Error::Resolve)?;
    let body = read_body(req).await?;
    let stream = TcpStream::connect_timeout(ip, t.port, CONNECT_TIMEOUT)
        .await
        .map_err(Error::Connect)?;
    let public = request_host(&req.authority, req.get("host"));
    let url = url(public, &t, &req.path)?;
    let call = leanhttp::Call {
        method: &req.method,
        url: &url,
        header: origin_header(req),
        body: body.as_deref(),
        header_timeout: Some(HEADER_TIMEOUT),
        // Een omleiding is voor de bezoeker, niet voor ons.
        no_follow: true,
        ..leanhttp::Call::default()
    };
    let mut resp = leanhttp::send(TcpConn::new(stream, exec), call)
        .await
        .map_err(Error::Http)?;

    // De koppen van de dienst gaan in de bundel, niet plat mee: de edge
    // negeert wat hij niet kent (zie edgeproto).
    let mut bundle = Bundle::new();
    for (name, value) in resp.header.iter() {
        if !is_hop_by_hop(name) {
            bundle.add(name, value);
        }
    }
    for c in &resp.set_cookie {
        bundle.add("set-cookie", c);
    }
    let bundle = bundle.finish().ok_or(Error::OutOfMemory)?;
    res.write_header(
        resp.status,
        &[
            (edgeproto::HEADER_USER, &bundle),
            (edgeproto::HEADER_META, Source::Origin.meta()),
        ],
    )
    .map_err(Error::Edge)?;
    drop(bundle);

    let mut chunk = [0u8; CHUNK];
    loop {
        let n = resp.read(&mut chunk).await.map_err(Error::Http)?;
        if n == 0 {
            break;
        }
        res.write(chunk.get(..n).unwrap_or(&[]))
            .await
            .map_err(Error::Edge)?;
    }
    let status = resp.status;
    // Dicht: één verbinding per verzoek (zie de moduledoc).
    if let Some(mut conn) = resp.release().await {
        let _ = leanhttp::close(&mut conn).await;
    }
    Ok(status)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn targets() {
        assert_eq!(
            target("http://10.100.0.2:7080").unwrap(),
            Target {
                host: "10.100.0.2",
                port: 7080,
                prefix: ""
            }
        );
        assert_eq!(
            target("HTTP://stulp.local").unwrap(),
            Target {
                host: "stulp.local",
                port: 80,
                prefix: ""
            }
        );
        assert_eq!(
            target("http://10.0.0.5:8080/api/").unwrap(),
            Target {
                host: "10.0.0.5",
                port: 8080,
                prefix: "/api"
            }
        );
        assert_eq!(target("http://h/x?q=1").unwrap().prefix, "/x");
        assert!(matches!(target("https://h"), Err(Error::Service(s)) if s.contains("https")));
        for bad in [
            "tcp://h:22",
            "http://",
            "http://h:0",
            "http://h:99999",
            "http://u@h",
            "http://[::1]:80",
            "unix:/x",
            "",
        ] {
            assert!(target(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn urls_carry_the_public_host() {
        let t = target("http://10.0.0.5:8080/api").unwrap();
        assert_eq!(
            url("stulp.example.nl", &t, "/v1?x=1").unwrap(),
            "http://stulp.example.nl/api/v1?x=1"
        );
        assert_eq!(url("", &t, "").unwrap(), "http://10.0.0.5:8080/api/");
    }

    #[test]
    fn hosts_lose_their_port() {
        assert_eq!(request_host("a.nl:443", None), "a.nl");
        assert_eq!(request_host("", Some("b.nl")), "b.nl");
        assert_eq!(request_host("[::1]", None), "[::1]");
        assert_eq!(request_host("", None), "");
    }

    #[test]
    fn hop_by_hop() {
        for h in [
            "connection",
            "Host",
            "content-length",
            "CF-Cloudflared-Proxy-Src",
            "te",
        ] {
            assert!(is_hop_by_hop(h), "{h}");
        }
        for h in ["cookie", "accept", "cf-connecting-ip", "authorization"] {
            assert!(!is_hop_by_hop(h), "{h}");
        }
    }
}
