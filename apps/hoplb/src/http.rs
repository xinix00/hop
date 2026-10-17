//! HTTP/1.1 voor een proxy: koppen lezen en schrijven, de framing van een
//! body bepalen, en een body in begrensde happen doorzetten.
//!
//! Waarom niet de server en client van leanhttp: die zijn gebouwd voor wie
//! een verzoek zelf afhandelt (een body tot 1 MiB, een upload met een
//! bekende lengte, een antwoord dat de crate zelf framet). Een proxy moet
//! een upload van elke lengte doorgeven, chunked in beide richtingen, en een
//! stroom (SSE) per hap doorspoelen zonder hem te bufferen. Wat hier wel van
//! leanhttp komt, zijn de traits: [`AsyncRead`], [`AsyncWrite`], [`Close`].
//!
//! Dezelfde strengheid als leanhttp aan de leeskant (KAM: dicht falen):
//! regels eindigen op CRLF, een headernaam is een token, geen obs-fold,
//! geen controlebytes, `Content-Length` naast `Transfer-Encoding` is een
//! fout. Twee parsers die het oneens zijn over waar een bericht eindigt,
//! zijn een smokkelroute.
//!
//! Bezit per verbinding één leesbuffer van [`BUF_SIZE`]; de kop van een
//! bericht moet daarin passen.

use alloc::string::String;
use alloc::vec::Vec;
use core::fmt::Write as _;
use core::time::Duration;

use leanhttp::{AsyncRead, AsyncWrite, IoError};

use crate::error::{Error, Result, try_extend, try_push, try_string};

/// De leesbuffer per kant van een verbinding, en daarmee de grens van een
/// kop. Go's `http.Server` staat 1 MiB toe; 32 KiB draagt elke gemeten
/// browser met een volle cookie-jar, en een proxy met een vaste pool mag
/// niet per verbinding een megabyte vasthouden.
pub const BUF_SIZE: usize = 32 << 10;

/// De grootste hap van een body die in één keer doorgaat.
pub const PIECE: usize = 16 << 10;

/// De langste chunk-kop of trailerregel.
pub const MAX_LINE: usize = 8 << 10;

/// Een gebufferde lezer over een verbinding; de verbinding blijft bereikbaar
/// voor de schrijfkant.
///
/// # Invariants
///
/// `start <= end <= buf.len() == BUF_SIZE`; `buf[start..end]` is gelezen en
/// nog niet verbruikt.
pub struct Reader<C> {
    conn: C,
    buf: Vec<u8>,
    start: usize,
    end: usize,
}

impl<C> Reader<C> {
    /// Neemt `conn` over met een verse buffer.
    pub fn new(conn: C) -> Result<Self> {
        let mut buf = Vec::new();
        buf.try_reserve_exact(BUF_SIZE)
            .map_err(|_| Error::OutOfMemory { bytes: BUF_SIZE })?;
        buf.resize(BUF_SIZE, 0);
        // INVARIANT: lege buffer van precies BUF_SIZE.
        Ok(Self {
            conn,
            buf,
            start: 0,
            end: 0,
        })
    }

    /// De verbinding, om te schrijven of een termijn te zetten.
    pub fn get_mut(&mut self) -> &mut C {
        &mut self.conn
    }

    /// De gelezen, nog niet verbruikte bytes.
    pub fn buffered(&self) -> &[u8] {
        self.buf.get(self.start..self.end).unwrap_or(&[])
    }

    /// Verbruikt `n` gebufferde bytes (hoogstens wat er is).
    pub fn consume(&mut self, n: usize) {
        // INVARIANT: start blijft onder end; leeg begint weer vooraan.
        self.start = self.start.saturating_add(n).min(self.end);
        if self.start == self.end {
            self.start = 0;
            self.end = 0;
        }
    }
}

impl<C: AsyncRead> Reader<C> {
    /// Leest meer achter de buffer; 0 is einde van de stroom. Een volle
    /// buffer is [`Error::HeadTooLarge`]: wie hier leest, zoekt een regel of
    /// een kop die er niet in past.
    async fn fill(&mut self) -> Result<usize> {
        if self.end == self.buf.len() && self.start > 0 {
            self.buf.copy_within(self.start..self.end, 0);
            self.end -= self.start;
            self.start = 0;
        }
        let tail = self.buf.get_mut(self.end..).unwrap_or(&mut []);
        if tail.is_empty() {
            return Err(Error::HeadTooLarge { limit: BUF_SIZE });
        }
        let n = leanhttp::read(&mut self.conn, tail).await?;
        // INVARIANT: de verbinding schreef hoogstens tail.len() bytes.
        self.end = self.end.saturating_add(n).min(self.buf.len());
        Ok(n)
    }

    /// Of de andere kant wegging: één kijkje op de leeskant onder `probe`.
    ///
    /// Een termijn die verloopt is een lezer die er nog is; bytes die hij
    /// stuurde blijven in de buffer staan (een volgend verzoek op een
    /// keep-alive-verbinding gaat niet verloren); einde of een andere fout
    /// is een lezer die weg is. Een volle buffer is "aanwezig", want dan
    /// kan er niets gelezen worden zonder verlies.
    ///
    /// Waarom: een stroom van een backend naar een client (SSE, een lange
    /// download) merkt anders pas aan een mislukte schrijf dat de client
    /// weg is, en op de netstack van HopOS faalt zo'n schrijf lang niet.
    pub async fn peer_gone(&mut self, probe: Duration) -> bool
    where
        C: AsyncRead,
    {
        if self.end == self.buf.len() {
            return false;
        }
        if self.conn.set_read_timeout(Some(probe)).is_err() {
            return false;
        }
        let r = self.fill().await;
        let _ = self.conn.set_read_timeout(None);
        match r {
            Ok(0) => true,
            Ok(_) | Err(Error::Io(IoError::TimedOut) | Error::HeadTooLarge { .. }) => false,
            Err(_) => true,
        }
    }

    /// Zorgt dat een hele kop (tot en met de lege regel) gebufferd is en
    /// geeft zijn lengte.
    ///
    /// Niets gelezen en einde van de stroom is [`Error::Eof`] (een client
    /// die een keep-alive-verbinding sluit); een halve kop is
    /// [`Error::UnexpectedEof`].
    pub async fn read_head(&mut self) -> Result<usize> {
        let mut scanned = 0;
        loop {
            let have = self.buffered();
            if let Some(n) = head_end(have, scanned) {
                return Ok(n);
            }
            scanned = have.len().saturating_sub(3);
            if self.fill().await? == 0 {
                return Err(if self.buffered().is_empty() {
                    Error::Eof
                } else {
                    Error::UnexpectedEof
                });
            }
        }
    }

    /// Zorgt dat een regel tot en met `\n` gebufferd is en geeft zijn lengte
    /// met het regeleinde; langer dan `max` is een fout.
    pub async fn read_line(&mut self, max: usize) -> Result<usize> {
        let mut scanned = 0;
        loop {
            let have = self.buffered();
            if let Some(i) = have
                .get(scanned..)
                .and_then(|t| t.iter().position(|&b| b == b'\n'))
            {
                let n = scanned + i + 1;
                if n > max {
                    return Err(Error::HeadTooLarge { limit: max });
                }
                return Ok(n);
            }
            scanned = have.len();
            if scanned > max {
                return Err(Error::HeadTooLarge { limit: max });
            }
            if self.fill().await? == 0 {
                return Err(Error::UnexpectedEof);
            }
        }
    }

    /// Leest body-bytes: eerst uit de buffer, anders rechtstreeks van de
    /// verbinding. 0 is einde van de stroom.
    pub async fn read_some(&mut self, out: &mut [u8]) -> Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        let have = self.buffered();
        if have.is_empty() {
            return Ok(leanhttp::read(&mut self.conn, out).await?);
        }
        let n = have.len().min(out.len());
        out.get_mut(..n)
            .unwrap_or(&mut [])
            .copy_from_slice(have.get(..n).unwrap_or(&[]));
        self.consume(n);
        Ok(n)
    }
}

/// Waar de lege regel eindigt in `b`, zoekend vanaf `from`.
fn head_end(b: &[u8], from: usize) -> Option<usize> {
    b.get(from..)?
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|i| from + i + 4)
}

/// De velden van een kop, in volgorde; namen hoofdletterongevoelig.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Headers(pub Vec<(String, String)>);

impl Headers {
    /// De eerste waarde van `name`.
    pub fn get(&self, name: &str) -> Option<&str> {
        self.all(name).next()
    }

    /// Alle waarden van `name`, in volgorde.
    pub fn all<'a, 'n>(&'a self, name: &'n str) -> impl Iterator<Item = &'a str> + use<'a, 'n> {
        self.0
            .iter()
            .filter(move |(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    /// Of een van de waarden van `name` (een kommalijst) `token` noemt.
    pub fn has_token(&self, name: &str, token: &str) -> bool {
        self.all(name)
            .flat_map(|v| v.split(','))
            .any(|t| t.trim().eq_ignore_ascii_case(token))
    }

    /// Alle velden.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.0.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }
}

/// De kop van een verzoek.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestHead {
    /// De methode, hoofdlettergevoelig.
    pub method: String,
    /// Het doel zoals het op de regel stond.
    pub target: String,
    /// De kleine versie: 0 voor HTTP/1.0, 1 voor HTTP/1.1.
    pub minor: u8,
    /// De velden.
    pub headers: Headers,
}

/// De kop van een antwoord.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResponseHead {
    /// De kleine versie.
    pub minor: u8,
    /// De status.
    pub status: u16,
    /// De redentekst, zoals de backend hem stuurde.
    pub reason: String,
    /// De velden.
    pub headers: Headers,
}

/// Leest een verzoekkop (inclusief de lege regel).
pub fn parse_request(head: &[u8]) -> Result<RequestHead> {
    let (line, headers) = split_head(head, Error::BadRequest)?;
    let mut parts = line.split(' ');
    let (Some(method), Some(target), Some(version), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(Error::BadRequest("malformed request line"));
    };
    if method.is_empty() || !is_token(method) {
        return Err(Error::BadRequest("invalid method"));
    }
    if target.is_empty() || target.bytes().any(|b| b <= b' ' || b == 0x7f) {
        return Err(Error::BadRequest("invalid target"));
    }
    let minor = version_minor(version).ok_or(Error::Unsupported("http version"))?;
    Ok(RequestHead {
        method: try_string(method)?,
        target: try_string(target)?,
        minor,
        headers: parse_fields(headers, Error::BadRequest)?,
    })
}

/// Leest een antwoordkop (inclusief de lege regel).
pub fn parse_response(head: &[u8]) -> Result<ResponseHead> {
    let (line, headers) = split_head(head, Error::BadResponse)?;
    let (version, rest) = line
        .split_once(' ')
        .ok_or(Error::BadResponse("malformed status line"))?;
    let minor = version_minor(version).ok_or(Error::BadResponse("http version"))?;
    let (code, reason) = rest.split_once(' ').unwrap_or((rest, ""));
    let status = match code.as_bytes() {
        [a, b, c] if a.is_ascii_digit() && b.is_ascii_digit() && c.is_ascii_digit() => {
            u16::from(a - b'0') * 100 + u16::from(b - b'0') * 10 + u16::from(c - b'0')
        }
        _ => return Err(Error::BadResponse("status code")),
    };
    if !(100..=599).contains(&status) {
        return Err(Error::BadResponse("status code"));
    }
    Ok(ResponseHead {
        minor,
        status,
        reason: try_string(reason)?,
        headers: parse_fields(headers, Error::BadResponse)?,
    })
}

/// De eerste regel en de veldregels van een kop.
fn split_head(head: &[u8], bad: fn(&'static str) -> Error) -> Result<(&str, &str)> {
    let text = core::str::from_utf8(head).map_err(|_| bad("head is not utf-8"))?;
    let body = text
        .strip_suffix("\r\n\r\n")
        .ok_or(bad("head does not end in CRLF CRLF"))?;
    let (line, fields) = body.split_once("\r\n").unwrap_or((body, ""));
    if line.contains('\n') || line.contains('\r') {
        return Err(bad("bare LF or CR"));
    }
    Ok((line, fields))
}

fn parse_fields(fields: &str, bad: fn(&'static str) -> Error) -> Result<Headers> {
    let mut out = Vec::new();
    if fields.is_empty() {
        return Ok(Headers(out));
    }
    for line in fields.split("\r\n") {
        if line.starts_with([' ', '\t']) {
            return Err(bad("obsolete line folding"));
        }
        let (k, v) = line.split_once(':').ok_or(bad("header without colon"))?;
        if !is_token(k) {
            return Err(bad("invalid header name"));
        }
        let v = v.trim_matches([' ', '\t']);
        if v.bytes().any(|b| (b < b' ' && b != b'\t') || b == 0x7f) {
            return Err(bad("control byte in header"));
        }
        try_push(&mut out, (try_string(k)?, try_string(v)?))?;
    }
    Ok(Headers(out))
}

fn version_minor(v: &str) -> Option<u8> {
    match v {
        "HTTP/1.1" => Some(1),
        "HTTP/1.0" => Some(0),
        _ => None,
    }
}

/// Een token uit RFC 9110 §5.6.2.
pub(crate) fn is_token(s: &str) -> bool {
    !s.is_empty()
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b))
}

/// Hoe een body eindigt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Framing {
    /// Geen body.
    Empty,
    /// Precies zoveel bytes.
    Length(u64),
    /// Chunked.
    Chunked,
    /// Tot de verbinding sluit (alleen een antwoord).
    Eof,
}

/// De framing van een verzoek (RFC 9112 §6.3).
pub fn request_framing(h: &Headers) -> Result<Framing> {
    let te = h.get("Transfer-Encoding").is_some();
    let cl = content_length(h, Error::BadRequest)?;
    match (te, cl) {
        (true, Some(_)) => Err(Error::BadRequest(
            "both Content-Length and Transfer-Encoding",
        )),
        (true, None) if is_chunked(h) => Ok(Framing::Chunked),
        (true, None) => Err(Error::Unsupported("transfer coding")),
        (false, Some(0)) | (false, None) => Ok(Framing::Empty),
        (false, Some(n)) => Ok(Framing::Length(n)),
    }
}

/// De framing van een antwoord op een verzoek met `method`.
pub fn response_framing(method: &str, status: u16, h: &Headers) -> Result<Framing> {
    if method == "HEAD" || status < 200 || status == 204 || status == 304 {
        return Ok(Framing::Empty);
    }
    let te = h.get("Transfer-Encoding").is_some();
    let cl = content_length(h, Error::BadResponse)?;
    match (te, cl) {
        (true, Some(_)) => Err(Error::BadResponse(
            "both Content-Length and Transfer-Encoding",
        )),
        (true, None) if is_chunked(h) => Ok(Framing::Chunked),
        // Een andere codering als laatste: lezen tot de verbinding sluit.
        (true, None) => Ok(Framing::Eof),
        (false, Some(n)) => Ok(Framing::Length(n)),
        (false, None) => Ok(Framing::Eof),
    }
}

/// Is `chunked` de laatste (en enige) codering?
fn is_chunked(h: &Headers) -> bool {
    let mut codings = h.all("Transfer-Encoding").flat_map(|v| v.split(','));
    matches!(
        (codings.next().map(str::trim), codings.next()),
        (Some(c), None) if c.eq_ignore_ascii_case("chunked")
    )
}

/// `Content-Length`: alle waarden (ook in een lijst) moeten gelijk zijn.
fn content_length(h: &Headers, bad: fn(&'static str) -> Error) -> Result<Option<u64>> {
    let mut len = None;
    for v in h.all("Content-Length").flat_map(|v| v.split(',')) {
        let v = v.trim();
        if v.is_empty() || !v.bytes().all(|b| b.is_ascii_digit()) {
            return Err(bad("invalid Content-Length"));
        }
        let n: u64 = v.parse().map_err(|_| bad("invalid Content-Length"))?;
        if len.is_some_and(|l| l != n) {
            return Err(bad("conflicting Content-Length"));
        }
        len = Some(n);
    }
    Ok(len)
}

/// Welke kant faalde bij het doorzetten van een body.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fault {
    /// De bron (lezen, of een kapotte framing van de bron).
    Source(Error),
    /// Het doel (schrijven).
    Sink(Error),
}

/// Hoe vaak een kopie die zijn lezer in de gaten houdt ([`Copy::watch`])
/// tijdens stilte van de bron naar die lezer kijkt.
pub const WATCH_EVERY: Duration = Duration::from_secs(1);

/// Hoe een body doorgaat.
#[derive(Clone, Copy, Debug)]
pub struct Copy {
    /// De framing van de bron.
    pub framing: Framing,
    /// Chunked uitpakken in plaats van doorgeven (een HTTP/1.0-client).
    pub dechunk: bool,
    /// Na elke hap doorspoelen (een antwoord: SSE mag niet blijven hangen).
    pub flush: bool,
    /// De stiltetermijn per lees van de bron.
    pub read_idle: Option<Duration>,
    /// De termijn per schrijf naar het doel.
    pub write_timeout: Option<Duration>,
    /// Houd de lezer van het doel in de gaten: vóór elke hap, en elke
    /// [`WATCH_EVERY`] stilte van de bron, één kijkje van zo lang
    /// ([`Reader::peer_gone`]). Is hij weg, dan stopt de kopie met
    /// [`Fault::Sink`] en [`Error::Eof`]. `None` kijkt niet.
    pub watch: Option<Duration>,
}

/// Zet een body van `from` naar `to` door, in happen van hoogstens
/// `piece.len()`; het aantal bodybytes.
///
/// Chunked gaat ongewijzigd door (de chunk-koppen en de trailers ook), of
/// uitgepakt als `dechunk`; zo is een stroom van een backend op de draad
/// naar de client dezelfde stroom, hap voor hap. Het doel is een
/// [`Reader`], zodat de kopie zijn lezer kan zien weggaan ([`Copy::watch`]).
pub async fn copy_body<R, W>(
    from: &mut Reader<R>,
    to: &mut Reader<W>,
    how: Copy,
    piece: &mut [u8],
) -> core::result::Result<u64, Fault>
where
    R: AsyncRead,
    W: AsyncRead + AsyncWrite,
{
    match how.framing {
        Framing::Empty => Ok(0),
        Framing::Length(n) => copy_exact(from, to, n, &how, piece).await,
        Framing::Eof => copy_to_eof(from, to, &how, piece).await,
        Framing::Chunked => copy_chunked(from, to, &how, piece).await,
    }
}

/// Zet de stiltetermijn van de bron opnieuw, vóór elke lees: een termijn
/// van leanhttp is een deadline vanaf nu, en een lange upload mag duren
/// zolang hij stroomt.
fn arm_read<R: AsyncRead>(
    from: &mut Reader<R>,
    t: Option<Duration>,
) -> core::result::Result<(), Fault> {
    from.get_mut()
        .set_read_timeout(t)
        .map_err(|e| Fault::Source(e.into()))
}

/// De termijn van de volgende lees van de bron, en of er dan nog een
/// volgende komt: zonder [`Copy::watch`] de hele stilte in één keer, met
/// hooguit [`WATCH_EVERY`] zodat de kopie tussendoor naar zijn lezer kijkt.
fn slice(how: &Copy, waited: Duration) -> (Option<Duration>, bool) {
    if how.watch.is_none() {
        return (how.read_idle, false);
    }
    match how.read_idle {
        None => (Some(WATCH_EVERY), true),
        Some(idle) => {
            let left = idle.saturating_sub(waited);
            (Some(left.min(WATCH_EVERY)), left > WATCH_EVERY)
        }
    }
}

/// Eén hap van de bron in `buf`, met de lezer van `to` in de gaten.
async fn read_piece<R: AsyncRead, W: AsyncRead + AsyncWrite>(
    from: &mut Reader<R>,
    to: &mut Reader<W>,
    how: &Copy,
    buf: &mut [u8],
) -> core::result::Result<usize, Fault> {
    let mut waited = Duration::ZERO;
    loop {
        let (t, more) = slice(how, waited);
        arm_read(from, t)?;
        match from.read_some(buf).await {
            Err(Error::Io(IoError::TimedOut)) if more => {
                waited = waited.saturating_add(t.unwrap_or(WATCH_EVERY));
                look(to, how).await?;
            }
            r => return r.map_err(Fault::Source),
        }
    }
}

/// Eén regel van de bron (gebufferd, lengte terug), met de lezer van `to`
/// in de gaten: tussen twee chunks van een SSE-stroom wacht de kopie hier.
async fn read_line_watched<R: AsyncRead, W: AsyncRead + AsyncWrite>(
    from: &mut Reader<R>,
    to: &mut Reader<W>,
    how: &Copy,
) -> core::result::Result<usize, Fault> {
    let mut waited = Duration::ZERO;
    loop {
        let (t, more) = slice(how, waited);
        arm_read(from, t)?;
        match from.read_line(MAX_LINE).await {
            Err(Error::Io(IoError::TimedOut)) if more => {
                waited = waited.saturating_add(t.unwrap_or(WATCH_EVERY));
                look(to, how).await?;
            }
            r => return r.map_err(Fault::Source),
        }
    }
}

/// Kijkt of de lezer van `to` er nog is, als de kopie dat moet doen.
async fn look<W: AsyncRead + AsyncWrite>(
    to: &mut Reader<W>,
    how: &Copy,
) -> core::result::Result<(), Fault> {
    if let Some(probe) = how.watch
        && to.peer_gone(probe).await
    {
        return Err(Fault::Sink(Error::Eof));
    }
    Ok(())
}

async fn put<W: AsyncRead + AsyncWrite>(
    to: &mut Reader<W>,
    how: &Copy,
    bytes: &[u8],
) -> core::result::Result<(), Fault> {
    if bytes.is_empty() {
        return Ok(());
    }
    look(to, how).await?;
    to.get_mut()
        .set_write_timeout(how.write_timeout)
        .map_err(|e| Fault::Sink(e.into()))?;
    leanhttp::write_all(to.get_mut(), bytes)
        .await
        .map_err(|e| Fault::Sink(e.into()))
}

async fn spill<W: AsyncRead + AsyncWrite>(
    to: &mut Reader<W>,
    how: &Copy,
) -> core::result::Result<(), Fault> {
    if how.flush {
        leanhttp::flush(to.get_mut())
            .await
            .map_err(|e| Fault::Sink(e.into()))?;
    }
    Ok(())
}

async fn copy_exact<R: AsyncRead, W: AsyncRead + AsyncWrite>(
    from: &mut Reader<R>,
    to: &mut Reader<W>,
    n: u64,
    how: &Copy,
    piece: &mut [u8],
) -> core::result::Result<u64, Fault> {
    let mut left = n;
    while left > 0 {
        let want = piece.len().min(usize::try_from(left).unwrap_or(usize::MAX));
        let buf = piece.get_mut(..want).unwrap_or(&mut []);
        let got = read_piece(from, to, how, buf).await?;
        if got == 0 {
            return Err(Fault::Source(Error::UnexpectedEof));
        }
        put(to, how, buf.get(..got).unwrap_or(&[])).await?;
        spill(to, how).await?;
        left -= got as u64;
    }
    Ok(n)
}

async fn copy_to_eof<R: AsyncRead, W: AsyncRead + AsyncWrite>(
    from: &mut Reader<R>,
    to: &mut Reader<W>,
    how: &Copy,
    piece: &mut [u8],
) -> core::result::Result<u64, Fault> {
    let mut total = 0u64;
    loop {
        let got = read_piece(from, to, how, piece).await?;
        if got == 0 {
            return Ok(total);
        }
        put(to, how, piece.get(..got).unwrap_or(&[])).await?;
        spill(to, how).await?;
        total = total.saturating_add(got as u64);
    }
}

async fn copy_chunked<R: AsyncRead, W: AsyncRead + AsyncWrite>(
    from: &mut Reader<R>,
    to: &mut Reader<W>,
    how: &Copy,
    piece: &mut [u8],
) -> core::result::Result<u64, Fault> {
    let mut total = 0u64;
    loop {
        let n = read_line_watched(from, to, how).await?;
        let size = chunk_size(from.buffered().get(..n).unwrap_or(&[])).map_err(Fault::Source)?;
        if how.dechunk {
            from.consume(n);
        } else {
            pass_line(from, to, how, piece, n).await?;
        }
        if size == 0 {
            copy_trailers(from, to, how, piece).await?;
            spill(to, how).await?;
            return Ok(total);
        }
        let mut left = size;
        while left > 0 {
            let want = piece.len().min(usize::try_from(left).unwrap_or(usize::MAX));
            let buf = piece.get_mut(..want).unwrap_or(&mut []);
            let got = read_piece(from, to, how, buf).await?;
            if got == 0 {
                return Err(Fault::Source(Error::UnexpectedEof));
            }
            put(to, how, buf.get(..got).unwrap_or(&[])).await?;
            left -= got as u64;
        }
        total = total.saturating_add(size);
        // De CRLF na de data.
        let n = read_line_watched(from, to, how).await?;
        if from.buffered().get(..n) != Some(b"\r\n") {
            return Err(Fault::Source(Error::BadResponse(
                "chunk not followed by CRLF",
            )));
        }
        from.consume(n);
        if !how.dechunk {
            put(to, how, b"\r\n").await?;
        }
        spill(to, how).await?;
    }
}

/// De trailers na de nul-chunk, tot en met de lege regel.
async fn copy_trailers<R: AsyncRead, W: AsyncRead + AsyncWrite>(
    from: &mut Reader<R>,
    to: &mut Reader<W>,
    how: &Copy,
    piece: &mut [u8],
) -> core::result::Result<(), Fault> {
    let mut budget = BUF_SIZE;
    loop {
        let n = read_line_watched(from, to, how).await?;
        budget = budget
            .checked_sub(n)
            .ok_or(Fault::Source(Error::HeadTooLarge { limit: BUF_SIZE }))?;
        let line = from.buffered().get(..n).unwrap_or(&[]);
        let last = line == b"\r\n";
        if !line.ends_with(b"\r\n") {
            return Err(Fault::Source(Error::BadResponse("trailer not CRLF")));
        }
        if how.dechunk {
            from.consume(n);
        } else {
            pass_line(from, to, how, piece, n).await?;
        }
        if last {
            return Ok(());
        }
    }
}

/// Zet de eerste `n` gebufferde bytes van `from` ongewijzigd door, in
/// happen van `piece` (de regel leeft in de buffer van `from`, en `to` is
/// een andere verbinding).
async fn pass_line<R: AsyncRead, W: AsyncRead + AsyncWrite>(
    from: &mut Reader<R>,
    to: &mut Reader<W>,
    how: &Copy,
    piece: &mut [u8],
    n: usize,
) -> core::result::Result<(), Fault> {
    let mut rest = n;
    while rest > 0 {
        let src = from.buffered();
        let k = rest.min(piece.len()).min(src.len());
        if k == 0 {
            // Een lege hap of een regel die niet (meer) gebufferd is: dit
            // kan alleen een fout in deze module zijn, en dan dicht.
            return Err(Fault::Source(Error::UnexpectedEof));
        }
        let (Some(dst), Some(src)) = (piece.get_mut(..k), src.get(..k)) else {
            return Err(Fault::Source(Error::UnexpectedEof));
        };
        dst.copy_from_slice(src);
        from.consume(k);
        put(to, how, dst).await?;
        rest -= k;
    }
    Ok(())
}

/// De grootte uit een chunk-kop (`1a\r\n`, `1a;ext=x\r\n`).
fn chunk_size(line: &[u8]) -> Result<u64> {
    let line = line
        .strip_suffix(b"\r\n")
        .ok_or(Error::BadRequest("chunk size line not CRLF"))?;
    let hex = match line.iter().position(|&b| b == b';') {
        Some(i) => line.get(..i).unwrap_or(&[]),
        None => line,
    };
    if hex.is_empty() || hex.len() > 16 {
        return Err(Error::BadRequest("chunk size"));
    }
    let mut n: u64 = 0;
    for &b in hex {
        let d = match b {
            b'0'..=b'9' => b - b'0',
            b'a'..=b'f' => b - b'a' + 10,
            b'A'..=b'F' => b - b'A' + 10,
            _ => return Err(Error::BadRequest("chunk size")),
        };
        n = n
            .checked_mul(16)
            .and_then(|n| n.checked_add(u64::from(d)))
            .ok_or(Error::BadRequest("chunk size"))?;
    }
    Ok(n)
}

/// Een kop in opbouw: faalbaar, zonder `format!`.
pub(crate) struct HeadBuf(pub(crate) Vec<u8>);

impl HeadBuf {
    pub(crate) fn new() -> Result<Self> {
        let mut v = Vec::new();
        v.try_reserve(1024)
            .map_err(|_| Error::OutOfMemory { bytes: 1024 })?;
        Ok(Self(v))
    }

    pub(crate) fn push(&mut self, s: &str) -> Result {
        try_extend(&mut self.0, s.as_bytes())
    }

    pub(crate) fn field(&mut self, k: &str, v: &str) -> Result {
        self.push(k)?;
        self.push(": ")?;
        self.push(v)?;
        self.push("\r\n")
    }

    pub(crate) fn fmt(&mut self, args: core::fmt::Arguments<'_>) -> Result {
        let mut s = FmtVec(&mut self.0);
        s.write_fmt(args)
            .map_err(|_| Error::OutOfMemory { bytes: 0 })
    }
}

struct FmtVec<'a>(&'a mut Vec<u8>);

impl core::fmt::Write for FmtVec<'_> {
    fn write_str(&mut self, s: &str) -> core::fmt::Result {
        try_extend(self.0, s.as_bytes()).map_err(|_| core::fmt::Error)
    }
}

/// De standaard redentekst bij een status die de proxy zelf stuurt.
pub fn reason(status: u16) -> &'static str {
    match status {
        100 => "Continue",
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        405 => "Method Not Allowed",
        413 => "Request Entity Too Large",
        431 => "Request Header Fields Too Large",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        505 => "HTTP Version Not Supported",
        _ => "",
    }
}

#[cfg(test)]
mod tests;
