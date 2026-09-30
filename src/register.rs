//! De registratie bij de edge: het token, de drie berichten en het antwoord.
//!
//! Bezit wat er in `TUNNEL_TOKEN` zit ([`Token`]), het bouwen van de
//! Cap'n Proto-berichten van de control-stream en het lezen van het
//! antwoord. Sans-I/O: de bytes gaan heen en weer in `crate::tunnel`.
//!
//! De registratie is één Cap'n Proto-RPC over de control-stream:
//!
//! ```text
//! bootstrap (vraag 0)  ->  de RegistrationServer-capability
//! call (vraag 1)       ->  registerConnection(auth, tunnelId, connIndex, options)
//! return (antwoord 1)  ->  ConnectionResponse: details of een fout
//! ```
//!
//! De tweede aanroep wacht niet op het antwoord van de eerste: hij richt zich
//! op "het resultaat van vraag 0" (`promisedAnswer`). Dat is pipelining, en
//! zo doet cloudflared het zelf: het scheelt een rondgang en een
//! capability-tabel die we één keer zouden gebruiken.
//!
//! Alle getallen komen uit de gepinde cloudflared en capnproto2, niet uit een
//! gok (overgenomen uit de Go-voorganger, `internal/tunnel/register.go`):
//!
//! ```text
//! rpc.capnp        Message{data 1, ptr 1}, which op UInt16(0):
//!                  call 2, return 3, bootstrap 8
//!                  Bootstrap{data 1, ptr 1}: questionId UInt32(0)
//!                  Call{data 3, ptr 3}: questionId UInt32(0), methodId
//!                  UInt16(4), interfaceId UInt64(8), target ptr0, params ptr1
//!                  MessageTarget{data 1, ptr 1}: which UInt16(4),
//!                  promisedAnswer 1 op ptr0
//!                  PromisedAnswer{data 1, ptr 1}: questionId UInt32(0),
//!                  transform ptr0
//!                  Payload{data 0, ptr 2}: content ptr0, capTable ptr1
//!                  Return{data 2, ptr 1}: answerId UInt32(0), which
//!                  UInt16(6) (results 0, exception 1), results ptr0
//! tunnelrpc.capnp  RegistrationServer 0xf71695ec7fe85497,
//!                  registerConnection = methode 0
//!                  Params{data 1, ptr 3}: connIndex UInt8(0), auth ptr0,
//!                  tunnelId ptr1, options ptr2
//!                  TunnelAuth{data 0, ptr 2}: accountTag ptr0, secret ptr1
//!                  ClientInfo{data 0, ptr 4}: clientId ptr0, features ptr1,
//!                  version ptr2, arch ptr3
//!                  ConnectionOptions{data 1, ptr 2}: client ptr0,
//!                  originLocalIp ptr1, replaceExisting bit 0,
//!                  compressionQuality UInt8(1), numPreviousAttempts UInt8(2)
//!                  ConnectionResponse{data 1, ptr 1}: which UInt16(0)
//!                  (error 0, connectionDetails 1) op ptr0
//!                  ConnectionDetails: uuid ptr0, locationName ptr1,
//!                  remotelyManaged bit 0
//!                  ConnectionError: cause ptr0, retryAfter Int64(0),
//!                  shouldRetry bit 64
//! ```

#![forbid(unsafe_code)]

use alloc::string::String;
use alloc::vec::Vec;
use core::fmt;
use core::time::Duration;

use crate::b64;
use crate::capnp::{self, Builder, View};
use crate::json;

/// De interface-id van `RegistrationServer`.
const REGISTRATION_SERVER: u64 = 0xf716_95ec_7fe8_5497;
/// `registerConnection`.
const METHOD_REGISTER: u16 = 0;

/// `Message.which`: een aanroep.
const MSG_CALL: u16 = 2;
/// `Message.which`: een antwoord.
const MSG_RETURN: u16 = 3;
/// `Message.which`: de bootstrap.
const MSG_BOOTSTRAP: u16 = 8;

/// `Return.which`: resultaten.
const RETURN_RESULTS: u16 = 0;
/// `Return.which`: een uitzondering.
const RETURN_EXCEPTION: u16 = 1;

/// `MessageTarget.which`: het beloofde antwoord op een vraag.
const TARGET_PROMISED_ANSWER: u16 = 1;

/// De vraag van de registratie; de bootstrap is vraag 0.
const QUESTION_REGISTER: u32 = 1;

/// De grootste buffer die een registratiebericht vraagt: kop, structs, de
/// account-tag (32 tekens), het geheim (32 bytes), de id en de versie. Een
/// token met een veel groter geheim past niet en is dan een luide fout.
pub(crate) const MESSAGE_CAP: usize = 1024;

/// Wat er in `TUNNEL_TOKEN` zit: base64 van een JSON met het account, het
/// geheim en de tunnel-id.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct Token {
    /// Het Cloudflare-account (`a`).
    pub(crate) account_tag: String,
    /// Het tunnelgeheim (`s`, zelf base64).
    pub(crate) secret: Vec<u8>,
    /// De tunnel-id (`t`), de zestien bytes van de UUID.
    pub(crate) tunnel_id: [u8; 16],
}

impl fmt::Debug for Token {
    // Het geheim komt nooit in een logregel, ook niet via `{:?}`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Token")
            .field("account_tag", &self.account_tag)
            .field("secret", &"<redacted>")
            .field("tunnel_id", &Uuid(&self.tunnel_id))
            .finish()
    }
}

/// Waarom een token niet bruikbaar is. De tekst is wat iemand op zijn node
/// leest als hij de verkeerde waarde in de jobspec plakte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TokenError {
    /// Het token is geen base64.
    NotBase64(b64::Error),
    /// Het token is langer dan de tunnel aanneemt.
    TooLong,
    /// Het gedecodeerde token is geen JSON van de verwachte vorm.
    NotJson(json::Error),
    /// Een van `a`, `s` of `t` ontbreekt of is leeg.
    Missing,
    /// Het geheim is geen base64.
    SecretNotBase64(b64::Error),
    /// De tunnel-id is geen UUID.
    BadId,
}

impl fmt::Display for TokenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotBase64(e) => write!(f, "tunnel token is not base64: {e}"),
            Self::TooLong => f.write_str("tunnel token is longer than 4 KiB"),
            Self::NotJson(e) => write!(f, "tunnel token is not the expected JSON: {e}"),
            Self::Missing => f.write_str("tunnel token misses a, s or t"),
            Self::SecretNotBase64(e) => write!(f, "tunnel secret is not base64: {e}"),
            Self::BadId => f.write_str("tunnel id is not a UUID of 16 bytes"),
        }
    }
}

/// De langste token-JSON die de tunnel decodeert.
const TOKEN_CAP: usize = 4096;

impl Token {
    /// Ontleedt `TUNNEL_TOKEN`.
    pub(crate) fn parse(s: &str) -> Result<Self, TokenError> {
        let mut raw = [0u8; TOKEN_CAP];
        let n = b64::decode(s.trim().as_bytes(), &mut raw).map_err(|e| match e {
            b64::Error::Full => TokenError::TooLong,
            e => TokenError::NotBase64(e),
        })?;
        let raw = raw.get(..n).unwrap_or(&[]);
        let (mut a, mut sec, mut t) = (None, None, None);
        let mut r = json::Reader::new(raw);
        r.object(|r, k| {
            let slot = if k.is("a") {
                &mut a
            } else if k.is("s") {
                &mut sec
            } else if k.is("t") {
                &mut t
            } else {
                return r.skip();
            };
            *slot = Some(r.string()?.decode()?);
            Ok(())
        })
        .and_then(|()| r.end())
        .map_err(TokenError::NotJson)?;
        let (Some(a), Some(sec), Some(t)) = (a, sec, t) else {
            return Err(TokenError::Missing);
        };
        if a.is_empty() || sec.is_empty() || t.is_empty() {
            return Err(TokenError::Missing);
        }
        let mut secret = Vec::new();
        secret
            .try_reserve_exact(sec.len())
            .map_err(|_| TokenError::TooLong)?;
        secret.resize(sec.len(), 0);
        let n = b64::decode(sec.as_bytes(), &mut secret).map_err(TokenError::SecretNotBase64)?;
        secret.truncate(n);
        Ok(Self {
            account_tag: a,
            secret,
            tunnel_id: parse_uuid(&t).ok_or(TokenError::BadId)?,
        })
    }
}

/// Leest de streepjesvorm van een UUID naar zestien bytes.
fn parse_uuid(s: &str) -> Option<[u8; 16]> {
    let mut out = [0u8; 16];
    let mut n = 0usize;
    let mut hi: Option<u8> = None;
    for c in s.chars() {
        if c == '-' {
            continue;
        }
        let v = u8::try_from(c.to_digit(16)?).ok()?;
        match hi.take() {
            None => hi = Some(v),
            Some(h) => {
                *out.get_mut(n)? = (h << 4) | v;
                n += 1;
            }
        }
    }
    (hi.is_none() && n == 16).then_some(out)
}

/// Zestien bytes in de streepjesvorm, voor een logregel.
pub(crate) struct Uuid<'a>(pub(crate) &'a [u8]);

impl fmt::Display for Uuid<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, b) in self.0.iter().enumerate() {
            if matches!(i, 4 | 6 | 8 | 10) {
                f.write_str("-")?;
            }
            write!(f, "{b:02x}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for Uuid<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

/// Wat we over onszelf melden. Het dashboard toont versie en arch.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ClientInfo<'a> {
    /// De client-id; cloudflared en de Go-voorganger sturen de tunnel-id.
    pub(crate) client_id: &'a [u8],
    /// De namen waarmee cloudflared zegt wat hij kan. De tunnel meldt er
    /// bewust geen: elke naam is een belofte, en de edge stuurt dan verkeer
    /// waarvan we het pad niet hebben.
    pub(crate) features: &'a [&'a str],
    /// De versie.
    pub(crate) version: &'a str,
    /// De architectuur, als `hopos_arm64`.
    pub(crate) arch: &'a str,
}

/// De bootstrap: vraag 0, de `RegistrationServer`.
pub(crate) fn bootstrap(buf: &mut [u8]) -> capnp::Result<&[u8]> {
    let mut b = Builder::new(buf);
    let msg = b.root(1, 1);
    b.set_u16(msg, 0, MSG_BOOTSTRAP);
    let boot = b.new_struct(msg, 0, 1, 1);
    b.set_u32(boot, 0, 0);
    b.finish()
}

/// De kop van een aanroep op het beloofde antwoord van vraag 0; geeft de
/// payload-struct terug, waar de parameters in komen.
fn call_head(b: &mut Builder<'_>, question: u32, method: u16) -> capnp::Struct {
    let msg = b.root(1, 1);
    b.set_u16(msg, 0, MSG_CALL);
    let c = b.new_struct(msg, 0, 3, 3);
    b.set_u32(c, 0, question);
    b.set_u16(c, 4, method);
    b.set_u64(c, 8, REGISTRATION_SERVER);
    let target = b.new_struct(c, 0, 1, 1);
    b.set_u16(target, 4, TARGET_PROMISED_ANSWER);
    let answer = b.new_struct(target, 0, 1, 1);
    b.set_u32(answer, 0, 0);
    // transform: leeg is de capability zelf.
    b.set_empty_list(answer, 0);
    b.new_struct(c, 1, 0, 2)
}

/// `registerConnection(auth, tunnelId, connIndex, options)`, vraag 1.
pub(crate) fn register_call<'b>(
    buf: &'b mut [u8],
    tok: &Token,
    conn_index: u8,
    info: &ClientInfo<'_>,
    attempts: u8,
) -> capnp::Result<&'b [u8]> {
    let mut b = Builder::new(buf);
    let payload = call_head(&mut b, QUESTION_REGISTER, METHOD_REGISTER);
    let params = b.new_struct(payload, 0, 1, 3);
    b.set_u8(params, 0, conn_index);
    let auth = b.new_struct(params, 0, 0, 2);
    b.set_text(auth, 0, &tok.account_tag);
    b.set_data(auth, 1, &tok.secret);
    b.set_data(params, 1, &tok.tunnel_id);
    let options = b.new_struct(params, 2, 1, 2);
    let client = b.new_struct(options, 0, 0, 4);
    b.set_data(client, 0, info.client_id);
    b.set_text_list(client, 1, info.features);
    b.set_text(client, 2, info.version);
    b.set_text(client, 3, info.arch);
    // originLocalIp blijft leeg: de edge toont het alleen, en een slot-IP
    // zegt niemand iets.
    // replaceExisting: een wees van een vorige start moet wijken.
    b.set_bool(options, 0, true);
    // compressionQuality 0 is uit: de tunnel draagt al gzip.
    b.set_u8(options, 1, 0);
    b.set_u8(options, 2, attempts);
    b.finish()
}

/// Wat de edge terugmeldt bij een geslaagde registratie.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Details {
    /// De verbindings-id van de edge.
    pub(crate) uuid: [u8; 16],
    /// De luchthavencode van de colo, zoals `AMS`.
    pub(crate) location: Location,
    /// Of de tunnel zijn config van het dashboard krijgt.
    pub(crate) remotely_managed: bool,
}

/// Een korte naam (de colo), zonder heap.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct Location {
    /// De bytes.
    buf: [u8; 16],
    /// De lengte.
    len: u8,
}

impl Location {
    /// Neemt `s` over, afgekapt op zestien bytes (op een tekengrens).
    fn new(s: &str) -> Self {
        let mut end = s.len().min(16);
        while !s.is_char_boundary(end) {
            end -= 1;
        }
        let mut buf = [0u8; 16];
        if let (Some(dst), Some(src)) = (buf.get_mut(..end), s.as_bytes().get(..end)) {
            dst.copy_from_slice(src);
        }
        Self {
            buf,
            len: u8::try_from(end).unwrap_or(0),
        }
    }

    /// De naam.
    pub(crate) fn as_str(&self) -> &str {
        let raw = self.buf.get(..usize::from(self.len)).unwrap_or(&[]);
        core::str::from_utf8(raw).unwrap_or("")
    }
}

impl fmt::Debug for Location {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.as_str(), f)
    }
}

/// Een nette weigering van de edge: waarom, en of het zin heeft het opnieuw
/// te proberen.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Refusal {
    /// De reden, afgekapt.
    pub(crate) cause: String,
    /// Wanneer opnieuw.
    pub(crate) retry_after: Duration,
    /// Of opnieuw zin heeft; een ingetrokken token lost geen herhaling op.
    pub(crate) should_retry: bool,
}

/// Wat een boodschap van de edge voor de registratie betekent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Answer {
    /// Niet het antwoord op de registratie (de bootstrap-return, een finish).
    Other,
    /// Geregistreerd.
    Registered(Details),
    /// Geweigerd, met een reden.
    Refused(Refusal),
    /// Een RPC-uitzondering, met de reden.
    Exception(String),
}

/// De langste reden die de tunnel bewaart; genoeg voor een logregel.
const CAUSE_CAP: usize = 256;

/// Een reden van de edge als `String`, afgekapt op [`CAUSE_CAP`].
fn cause(s: &str) -> String {
    let mut end = s.len().min(CAUSE_CAP);
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    let mut out = String::new();
    if out.try_reserve_exact(end).is_ok() {
        out.push_str(s.get(..end).unwrap_or(""));
    }
    out
}

/// Leest één segment van de edge (zonder de segmenttabel).
pub(crate) fn read_answer(seg: &[u8]) -> capnp::Result<Answer> {
    let root = View::root(seg)?;
    if root.u16(0) != MSG_RETURN {
        return Ok(Answer::Other);
    }
    let ret = root.struct_at(0)?;
    if ret.u32(0) != QUESTION_REGISTER {
        return Ok(Answer::Other);
    }
    match ret.u16(6) {
        RETURN_EXCEPTION => {
            let exc = ret.struct_at(0)?;
            Ok(Answer::Exception(cause(exc.text_at(0).unwrap_or(""))))
        }
        RETURN_RESULTS => {
            // Payload.content is de results-struct van registerConnection;
            // die heeft één veld, de ConnectionResponse.
            let content = ret.struct_at(0)?.struct_at(0)?;
            connection_response(content.struct_at(0)?)
        }
        _ => Ok(Answer::Exception(cause(
            "edge answered with a return kind this client does not handle",
        ))),
    }
}

/// De union van `ConnectionResponse`: een fout of de details.
fn connection_response(resp: View<'_>) -> capnp::Result<Answer> {
    if resp.is_null() {
        return Ok(Answer::Exception(cause("edge answered without a result")));
    }
    match resp.u16(0) {
        0 => {
            let e = resp.struct_at(0)?;
            let after = u64::try_from(e.i64(0)).unwrap_or(0);
            Ok(Answer::Refused(Refusal {
                cause: cause(e.text_at(0)?),
                retry_after: Duration::from_nanos(after),
                should_retry: e.bool(64),
            }))
        }
        1 => {
            let d = resp.struct_at(0)?;
            let mut uuid = [0u8; 16];
            let raw = d.data_at(0)?;
            if let (Some(dst), Some(src)) = (
                uuid.get_mut(..raw.len().min(16)),
                raw.get(..raw.len().min(16)),
            ) {
                dst.copy_from_slice(src);
            }
            Ok(Answer::Registered(Details {
                uuid,
                location: Location::new(d.text_at(1)?),
                remotely_managed: d.bool(0),
            }))
        }
        _ => Ok(Answer::Exception(cause(
            "edge answered with an unknown result kind",
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Hex naar bytes, voor de vastgelegde berichten.
    fn unhex(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    /// Het segment uit een boodschap met één segment.
    fn seg(msg: &[u8]) -> &[u8] {
        &msg[8..]
    }

    // Het token uit de Go-toets (TestParseToken), en daarmee de bytes van de
    // Go-voorganger (scratch-run van internal/tunnel met dit token, 30-09).
    const TOKEN: &str = "eyJhIjoiOWMyYjY4MGRhNjBhNjU4OTI2YjNmZTViM2JmNWY4ZWUiLCJzIjoiQUFFQ0F3UUZCZ2NJQ1FvTERBME9EeEFSRWhNVUZSWVhHQmthR3h3ZEhoOD0iLCJ0IjoiODcyODc1YmItZTI3OS00YzY5LWE3NjctZjM1Mjg2ZWY5ZDVkIn0=";

    /// `Register(rw, tok, 2, {ClientID: id, Version: "3.0.0", Arch:
    /// "hopos_aarch64"}, 3)` in Go: de bootstrap plus de aanroep.
    const GO_REGISTER: &str = "000000000500000000000000010001000800000000000000000000000100010000000000000000000000000000000000000000002c00000000000000010001000200000000000000000000000300030001000000000000009754e87fec9516f700000000000000000800000001000100140000000000020000000000000000000000000001000000000000000100010000000000000000000100000006000000040000000100030000000000000000000200000000000000080000000000020031000000820000003400000001000200050000000a010000150000000201000039633262363830646136306136353839323662336665356233626635663865650000000000000000000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f872875bbe2794c69a767f35286ef9d5d0100030000000000040000000000040000000000000000000d0000008200000011000000060000000d000000320000000d00000072000000872875bbe2794c69a767f35286ef9d5d332e302e30000000686f706f735f61617263683634000000";

    /// Dezelfde aanroep met index 0, twee features, versie "v", arch "a".
    const GO_REGISTER_FEATURES: &str = "000000000500000000000000010001000800000000000000000000000100010000000000000000000000000000000000000000003100000000000000010001000200000000000000000000000300030001000000000000009754e87fec9516f700000000000000000800000001000100140000000000020000000000000000000000000001000000000000000100010000000000000000000100000006000000040000000100030000000000000000000000000000000000080000000000020031000000820000003400000001000200050000000a010000150000000201000039633262363830646136306136353839323662336665356233626635663865650000000000000000000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f872875bbe2794c69a767f35286ef9d5d0100000000000000040000000000040000000000000000000d00000082000000110000001600000025000000120000002500000012000000872875bbe2794c69a767f35286ef9d5d050000009a0000000d0000001200000073657269616c697a65645f68656164657273000000000000780000000000000076000000000000006100000000000000";

    /// Antwoorden zoals de Go-bouwer ze maakt: de bootstrap-return, de
    /// details (uuid deadbeef0102..0c, AMS, remotely managed), een
    /// weigering (tunnel deleted, 5 s, niet opnieuw) en een uitzondering.
    const GO_BOOT_RET: &str = "000000000800000000000000010001000300000000000000000000000200010000000000000000000000000000000000000000000000020000000000000000000000000000000000";
    const GO_DETAILS_RET: &str = "000000001100000000000000010001000300000000000000000000000200010001000000000000000000000000000000000000000000020004000000000001000000000000000000000000000100010001000000000000000000000001000200010000000000000005000000820000000900000022000000deadbeef0102030405060708090a0b0c414d530000000000";
    const GO_ERROR_RET: &str = "00000000100000000000000001000100030000000000000000000000020001000100000000000000000000000000000000000000000002000400000000000100000000000000000000000000010001000000000000000000000000000200010000f2052a010000000000000000000000010000007a00000074756e6e656c2064656c657465640000";
    const GO_EXC_RET: &str = "000000000a0000000000000001000100030000000000000000000000020001000100000000000100000000000000000000000000010001000000000000000000010000004a00000062616420617574680000000000000000";

    #[test]
    fn parse_token_like_go() {
        let tok = Token::parse(TOKEN).unwrap();
        assert_eq!(tok.account_tag, "9c2b680da60a658926b3fe5b3bf5f8ee");
        assert_eq!(tok.secret.len(), 32);
        assert_eq!(tok.secret[31], 31);
        assert_eq!(
            tok.tunnel_id,
            [
                0x87, 0x28, 0x75, 0xbb, 0xe2, 0x79, 0x4c, 0x69, 0xa7, 0x67, 0xf3, 0x52, 0x86, 0xef,
                0x9d, 0x5d
            ]
        );
        assert_eq!(
            alloc::format!("{}", Uuid(&tok.tunnel_id)),
            "872875bb-e279-4c69-a767-f35286ef9d5d"
        );
        assert!(
            !alloc::format!("{tok:?}").contains("AAEC"),
            "geen geheim in Debug"
        );
    }

    #[test]
    fn token_refusals() {
        fn b64(s: &str) -> String {
            let mut out = String::new();
            b64::encode_raw(s.as_bytes(), |c| out.push(char::from(c)));
            out
        }
        let id = "872875bb-e279-4c69-a767-f35286ef9d5d";
        assert!(matches!(
            Token::parse("dit-is-geen-base64!!"),
            Err(TokenError::NotBase64(_))
        ));
        assert!(matches!(
            Token::parse(&b64("hallo")),
            Err(TokenError::NotJson(_))
        ));
        assert_eq!(
            Token::parse(&b64(r#"{"a":"acc"}"#)),
            Err(TokenError::Missing)
        );
        assert!(matches!(
            Token::parse(&b64(&alloc::format!(
                r#"{{"a":"acc","s":"!!","t":"{id}"}}"#
            ))),
            Err(TokenError::SecretNotBase64(_))
        ));
        assert_eq!(
            Token::parse(&b64(r#"{"a":"acc","s":"eA==","t":"872875bb"}"#)),
            Err(TokenError::BadId)
        );
        assert_eq!(
            Token::parse(&b64(
                r#"{"a":"acc","s":"eA==","t":"zzzz75bb-e279-4c69-a767-f35286ef9d5d"}"#
            )),
            Err(TokenError::BadId)
        );
        // De URL-veilige vorm zonder opvulling werkt ook.
        let ok = b64(&alloc::format!(
            r#"{{"a":"acc","s":"Z2VoZWlt","t":"{id}"}}"#
        ))
        .replace('+', "-");
        assert_eq!(Token::parse(&ok).unwrap().secret, b"geheim");
    }

    #[test]
    fn bootstrap_and_register_match_go() {
        let tok = Token::parse(TOKEN).unwrap();
        let info = ClientInfo {
            client_id: &tok.tunnel_id,
            features: &[],
            version: "3.0.0",
            arch: "hopos_aarch64",
        };
        let mut out = Vec::new();
        let mut buf = [0u8; MESSAGE_CAP];
        out.extend_from_slice(bootstrap(&mut buf).unwrap());
        out.extend_from_slice(register_call(&mut buf, &tok, 2, &info, 3).unwrap());
        assert_eq!(out, unhex(GO_REGISTER));

        let info = ClientInfo {
            features: &["serialized_headers", "x"],
            version: "v",
            arch: "a",
            ..info
        };
        let mut out = Vec::new();
        out.extend_from_slice(bootstrap(&mut buf).unwrap());
        out.extend_from_slice(register_call(&mut buf, &tok, 0, &info, 0).unwrap());
        assert_eq!(out, unhex(GO_REGISTER_FEATURES));
    }

    #[test]
    fn answers_like_go() {
        assert_eq!(
            read_answer(seg(&unhex(GO_BOOT_RET))).unwrap(),
            Answer::Other
        );
        let Answer::Registered(d) = read_answer(seg(&unhex(GO_DETAILS_RET))).unwrap() else {
            panic!("geen details");
        };
        assert_eq!(
            alloc::format!("{}", Uuid(&d.uuid)),
            "deadbeef-0102-0304-0506-0708090a0b0c"
        );
        assert_eq!(d.location.as_str(), "AMS");
        assert!(d.remotely_managed);
        assert_eq!(
            read_answer(seg(&unhex(GO_ERROR_RET))).unwrap(),
            Answer::Refused(Refusal {
                cause: "tunnel deleted".into(),
                retry_after: Duration::from_secs(5),
                should_retry: false,
            })
        );
        assert_eq!(
            read_answer(seg(&unhex(GO_EXC_RET))).unwrap(),
            Answer::Exception("bad auth".into())
        );
    }

    #[test]
    fn answer_from_the_network_is_bounds_checked() {
        let mut msg = unhex(GO_DETAILS_RET);
        let n = msg.len();
        msg.truncate(n - 16);
        assert!(read_answer(seg(&msg)).is_err());
        assert!(read_answer(&[]).is_err());
    }
}
