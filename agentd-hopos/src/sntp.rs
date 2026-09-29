//! SNTP (RFC 4330): de wandklok van de node, gehaald bij een NTP-server.
//!
//! Hop is de enige bewoner die de klok van de kern mag zetten
//! (`SET_CLOCK`, een bevoegde op). De kern zelf heeft geen bron: zonder
//! Hop loopt hij op een vaste boottijd (QEMU) of op niets. En de klok is
//! wat TLS nodig heeft: een certificaat is geldig tussen twee data, dus een
//! node zonder echte tijd heeft geen echte ketenverificatie. Daarom hier,
//! bij start en elk uur, zoals de Go-kern (`hopnet.SyncTime` op
//! `pool.ntp.org`, OLD/metal/cmd/hopos/main.go).
//!
//! Dit module bezit het pakket en zijn strenge lezing ([`request`],
//! [`parse`]) en één synchronisatie ([`sync`]): naam opzoeken, vragen,
//! lezen. Het versturen doet een [`NtpLink`] (in de binary een UDP-socket
//! van applib, in de tests een nep-server), het opzoeken een [`Resolve`].
//!
//! Strenger dan Go: het antwoord moet van een server komen (mode 4), niet
//! "unsynchronized" zijn (LI 3, stratum 0 of boven 15), ons eigen
//! transmit-veld terugzeggen als originate (RFC 4330 §5: zo telt een
//! gespoofd antwoord zonder onze vraag niet), en na 2026 liggen. De tijd
//! wordt gecorrigeerd voor de helft van de rondreis, met de verwerkingstijd
//! van de server eraf.

use alloc::string::String;
use core::fmt;
use core::future::Future;

use crate::fetch::Resolve;

/// De NTP-poort.
pub const PORT: u16 = 123;

/// Een SNTP-pakket zonder extensies.
pub const PACKET: usize = 48;

/// De server van de node, zoals in Go.
pub const SERVER: &str = "pool.ntp.org";

/// Hoe vaak één synchronisatie vraagt voor ze opgeeft (Go: drie keer).
pub const ATTEMPTS: usize = 3;

/// Seconden van 1900 (NTP) tot 1970 (Unix).
const NTP_TO_UNIX: u64 = 2_208_988_800;

/// De vroegste tijd die een server mag zeggen: 2026-01-01T00:00:00Z. Een
/// kapotte of gespoofte server die 1970 of 2000 zegt, maakt elk certificaat
/// "nog niet geldig"; die fout hoort hier, met het getal, en niet later
/// als een raadselachtige TLS-weigering.
pub const MIN_UNIX_SECS: u64 = 1_767_225_600;

const NS: u64 = 1_000_000_000;

/// Eén UDP-uitwisseling met een NTP-server.
pub trait NtpLink {
    /// Stuurt `req` naar `server`:[`PORT`] en wacht (met een eigen termijn)
    /// op één antwoord van dat adres; geeft zijn lengte in `resp`.
    fn exchange(
        &mut self,
        server: [u8; 4],
        req: &[u8; PACKET],
        resp: &mut [u8],
    ) -> impl Future<Output = Result<usize, String>>;
}

/// Waarom er geen tijd kwam.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SntpError {
    /// De naam van de server werd geen adres.
    Resolve(String),
    /// Versturen of ontvangen faalde (ook: geen antwoord binnen de termijn).
    Link(String),
    /// Het antwoord is korter dan een pakket.
    Short(usize),
    /// Het antwoord komt niet van een server (mode).
    NotServer(u8),
    /// De server zegt zelf dat zijn klok niet klopt (LI 3 of stratum 16+).
    Unsynchronized,
    /// Kiss-o'-Death (stratum 0): de server wil dat we ophouden, met zijn
    /// code (`RATE`, `DENY`).
    Kiss([u8; 4]),
    /// Het originate-veld is niet ons transmit-veld: niet ons antwoord.
    Mismatch,
    /// De server zegt een tijd vóór [`MIN_UNIX_SECS`].
    TooEarly(u64),
}

impl fmt::Display for SntpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Resolve(e) => write!(f, "resolve: {e}"),
            Self::Link(e) => write!(f, "udp: {e}"),
            Self::Short(n) => write!(f, "answer of {n} bytes, need {PACKET}"),
            Self::NotServer(m) => write!(f, "answer in mode {m}, not a server"),
            Self::Unsynchronized => f.write_str("server says its clock is unsynchronized"),
            Self::Kiss(code) => write!(
                f,
                "kiss-o'-death {:?}",
                core::str::from_utf8(code).unwrap_or("????")
            ),
            Self::Mismatch => f.write_str("answer does not echo our request"),
            Self::TooEarly(s) => write!(f, "server time {s} s is before 2026, refused"),
        }
    }
}

/// Een gemeten tijd.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Sample {
    /// Unix-nanoseconden op het moment `at`.
    pub unix_ns: u64,
    /// De monotone klok van de app op dat moment (ns).
    pub at: u64,
    /// De rondreis zonder de verwerkingstijd van de server (ns).
    pub delay_ns: u64,
    /// Het stratum van de server.
    pub stratum: u8,
}

impl Sample {
    /// De Unix-tijd op monotone tijd `now` (na `at`).
    pub fn unix_at(&self, now: u64) -> u64 {
        self.unix_ns.saturating_add(now.saturating_sub(self.at))
    }
}

/// Een client-vraag (LI 0, versie 4, mode 3) met `nonce` als
/// transmit-veld; de server zegt het terug als originate.
pub fn request(nonce: u64) -> [u8; PACKET] {
    let mut p = [0u8; PACKET];
    p[0] = 0x23;
    p[40..48].copy_from_slice(&nonce.to_be_bytes());
    p
}

/// Een NTP-tijdstempel (32.32 sinds 1900) op `at` in `p`.
fn stamp(p: &[u8], at: usize) -> u64 {
    let mut b = [0u8; 8];
    if let Some(s) = p.get(at..at + 8) {
        b.copy_from_slice(s);
    }
    u64::from_be_bytes(b)
}

/// Een NTP-tijdstempel als Unix-nanoseconden; `None` vóór 1970.
fn unix_ns(ts: u64) -> Option<u64> {
    let secs = (ts >> 32).checked_sub(NTP_TO_UNIX)?;
    let frac = ((ts & 0xffff_ffff) * NS) >> 32;
    secs.checked_mul(NS)?.checked_add(frac)
}

/// Leest het antwoord `resp` op de vraag met `nonce`, verstuurd op
/// monotone tijd `sent` en ontvangen op `recv`.
pub fn parse(resp: &[u8], nonce: u64, sent: u64, recv: u64) -> Result<Sample, SntpError> {
    if resp.len() < PACKET {
        return Err(SntpError::Short(resp.len()));
    }
    let head = resp.first().copied().unwrap_or(0);
    let (li, mode) = (head >> 6, head & 7);
    if mode != 4 {
        return Err(SntpError::NotServer(mode));
    }
    let stratum = resp.get(1).copied().unwrap_or(0);
    if stratum == 0 {
        let mut code = [0u8; 4];
        code.copy_from_slice(resp.get(12..16).unwrap_or(&[0; 4]));
        return Err(SntpError::Kiss(code));
    }
    if li == 3 || stratum > 15 {
        return Err(SntpError::Unsynchronized);
    }
    if stamp(resp, 24) != nonce {
        return Err(SntpError::Mismatch);
    }
    let (t2, t3) = (stamp(resp, 32), stamp(resp, 40));
    if (t3 >> 32) < NTP_TO_UNIX + MIN_UNIX_SECS {
        return Err(SntpError::TooEarly((t3 >> 32).saturating_sub(NTP_TO_UNIX)));
    }
    let server_ns = unix_ns(t3).ok_or(SntpError::TooEarly(0))?;
    // De rondreis min wat de server zelf deed (T3 - T2); de helft daarvan
    // is de weg terug.
    let held = unix_ns(t3)
        .zip(unix_ns(t2))
        .map_or(0, |(a, b)| a.saturating_sub(b));
    let delay_ns = recv.saturating_sub(sent).saturating_sub(held);
    Ok(Sample {
        unix_ns: server_ns.saturating_add(delay_ns / 2),
        at: recv,
        delay_ns,
        stratum,
    })
}

/// Eén synchronisatie: `host` opzoeken en tot [`ATTEMPTS`] keer vragen.
///
/// `mono` is de monotone klok van de app; `nonce` geeft per vraag een
/// ander transmit-veld. Geeft de eerste goede meting, of de laatste fout.
pub async fn sync<R: Resolve, L: NtpLink>(
    resolver: &mut R,
    link: &mut L,
    host: &str,
    mono: impl Fn() -> u64,
    mut nonce: impl FnMut() -> u64,
) -> Result<Sample, SntpError> {
    let server = resolver.resolve(host).await.map_err(SntpError::Resolve)?;
    let mut last = SntpError::Link(String::from("no attempt"));
    for _ in 0..ATTEMPTS {
        let n = nonce();
        let req = request(n);
        let mut resp = [0u8; PACKET];
        let sent = mono();
        let got = match link.exchange(server, &req, &mut resp).await {
            Ok(len) => len,
            Err(e) => {
                last = SntpError::Link(e);
                continue;
            }
        };
        let recv = mono();
        match parse(resp.get(..got).unwrap_or_default(), n, sent, recv) {
            Ok(s) => return Ok(s),
            // Een server die ons wegstuurt, vragen we niet nog eens.
            Err(e @ SntpError::Kiss(_)) => return Err(e),
            Err(e) => last = e,
        }
    }
    Err(last)
}
