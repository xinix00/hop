//! Het DNS-draadformaat (RFC 1035 §4), zo klein als hopdns het nodig heeft.
//!
//! Bezit: een vraag lezen ([`parse_query`]), een antwoord schrijven
//! ([`Reply`]: A, CNAME, SOA en de EDNS-pseudo-RR), en voor de zelftoets en
//! de tests een vraag bouwen ([`encode_query`]) en een antwoord lezen
//! ([`decode`]). Bezit niet: welke records een vraag krijgt (dat is
//! [`crate::server`]).
//!
//! Het leespad van de server alloceert niets: de vraag is een lening in het
//! datagram, de naam wordt in een kladblok op de stack in kleine letters
//! gezet ([`name_text`]), en het antwoord gaat rechtstreeks in de buffer van
//! de aanroeper. Een antwoordnaam is een verwijzing (0xC00C) naar de naam in
//! de vraag, dus een A-record kost 16 bytes.
//!
//! Streng waar het een server moet zijn: een datagram met de QR-bit (een
//! antwoord) krijgt niets terug, zodat twee servers elkaar niet eindeloos
//! antwoorden; een andere opcode dan QUERY is NOTIMP; QDCOUNT anders dan
//! één is FORMERR (RFC 9619); een verwijzing in de vraagnaam is FORMERR.

use alloc::string::String;
use alloc::vec::Vec;
use core::net::Ipv4Addr;

use crate::{Error, Result};

/// De kop van elke boodschap.
pub const HEADER_LEN: usize = 12;

/// De grootste boodschap over UDP zonder EDNS (RFC 1035 §4.2.1).
pub const UDP_MAX: usize = 512;

/// De grootste boodschap die hopdns met EDNS stuurt: de 1232 van DNS Flag
/// Day 2020, die over elk pad zonder fragmentatie past.
pub const EDNS_MAX: usize = 1232;

/// De langste naam op de draad (RFC 1035 §2.3.4).
pub const NAME_MAX: usize = 255;

/// De langste label.
pub const LABEL_MAX: usize = 63;

/// De omvang van één OPT-record zonder opties.
const OPT_LEN: usize = 11;

/// Verwijzing naar de naam in de vraag: die begint direct na de kop.
const QNAME_PTR: [u8; 2] = [0xC0, HEADER_LEN as u8];

/// Recordtypen die hopdns kent.
pub mod rtype {
    /// Een IPv4-adres.
    pub const A: u16 = 1;
    /// Een canonieke naam.
    pub const CNAME: u16 = 5;
    /// Het begin van een zone.
    pub const SOA: u16 = 6;
    /// Een IPv6-adres (hopdns heeft er geen: een lege NOERROR).
    pub const AAAA: u16 = 28;
    /// De EDNS-pseudo-RR (RFC 6891).
    pub const OPT: u16 = 41;
    /// Alles.
    pub const ANY: u16 = 255;
}

/// De klasse Internet.
pub const CLASS_IN: u16 = 1;

/// De klasse "elke".
pub const CLASS_ANY: u16 = 255;

/// De antwoordcode (RCODE, de onderste vier bits van de vlaggen).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Rcode {
    /// Geen fout.
    NoError = 0,
    /// De vraag leest niet.
    FormErr = 1,
    /// De server kon niet.
    ServFail = 2,
    /// De naam bestaat niet.
    NxDomain = 3,
    /// Die opcode doen we niet.
    NotImp = 4,
    /// Die vraag beantwoorden we niet (niet onze zone).
    Refused = 5,
}

impl Rcode {
    /// De code uit de vlaggen van een boodschap.
    #[must_use]
    pub const fn from_flags(flags: u16) -> u8 {
        (flags & 0x000F) as u8
    }
}

/// De QR-bit: dit is een antwoord.
const QR: u16 = 0x8000;
/// De AA-bit: gezaghebbend.
const AA: u16 = 0x0400;
/// De TC-bit: afgekapt.
const TC: u16 = 0x0200;
/// De RD-bit: recursie gevraagd (wij kopiëren hem).
const RD: u16 = 0x0100;
/// De CD-bit: controle uit (wij kopiëren hem).
const CD: u16 = 0x0010;

fn be16(b: &[u8], at: usize) -> Option<u16> {
    Some(u16::from_be_bytes([*b.get(at)?, *b.get(at + 1)?]))
}

fn be32(b: &[u8], at: usize) -> Option<u32> {
    Some(u32::from_be_bytes([
        *b.get(at)?,
        *b.get(at + 1)?,
        *b.get(at + 2)?,
        *b.get(at + 3)?,
    ]))
}

/// Een gelezen vraag: een lening in het datagram.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Query<'a> {
    /// Het id.
    pub id: u16,
    /// De vlaggen van de vraag.
    pub flags: u16,
    /// De vraag zoals hij op de draad stond: naam, type, klasse.
    pub question: &'a [u8],
    /// De naam op de draad, met de nul-label.
    pub qname: &'a [u8],
    /// Het type.
    pub qtype: u16,
    /// De klasse.
    pub qclass: u16,
    /// De UDP-maat die de vrager met EDNS aanbiedt, als hij EDNS sprak.
    pub edns: Option<u16>,
}

/// Waarom een datagram geen gewone vraag is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Bad {
    /// Niets terugsturen: te kort, of zelf een antwoord.
    Drop,
    /// Een kaal antwoord met deze code, zonder vraagsectie.
    Reply {
        /// Het id van de vraag.
        id: u16,
        /// De vlaggen van de vraag (opcode en RD gaan mee terug).
        flags: u16,
        /// De code.
        rcode: Rcode,
    },
}

/// Leest de naam op `at` zonder verwijzingen; geeft het einde.
fn skip_plain_name(msg: &[u8], mut at: usize) -> Option<usize> {
    let start = at;
    loop {
        let len = usize::from(*msg.get(at)?);
        if len == 0 {
            at += 1;
            return (at - start <= NAME_MAX).then_some(at);
        }
        if len > LABEL_MAX {
            return None; // Een verwijzing of een gereserveerde vorm.
        }
        at += 1 + len;
    }
}

/// Slaat een naam over die verwijzingen mag dragen; geeft het einde.
fn skip_name(msg: &[u8], mut at: usize) -> Option<usize> {
    loop {
        let len = *msg.get(at)?;
        match len {
            0 => return Some(at + 1),
            1..=63 => at += 1 + usize::from(len),
            0xC0..=0xFF => {
                msg.get(at + 1)?;
                return Some(at + 2);
            }
            _ => return None,
        }
    }
}

/// Leest één vraag uit `msg`.
pub fn parse_query(msg: &[u8]) -> core::result::Result<Query<'_>, Bad> {
    let (Some(id), Some(flags)) = (be16(msg, 0), be16(msg, 2)) else {
        return Err(Bad::Drop);
    };
    if msg.len() < HEADER_LEN || flags & QR != 0 {
        return Err(Bad::Drop);
    }
    let reply = |rcode| Bad::Reply { id, flags, rcode };
    if (flags >> 11) & 0xF != 0 {
        return Err(reply(Rcode::NotImp));
    }
    let counts = (be16(msg, 4), be16(msg, 6), be16(msg, 8), be16(msg, 10));
    let (Some(1), Some(an), Some(ns), Some(ar)) = counts else {
        return Err(reply(Rcode::FormErr));
    };
    let Some(name_end) = skip_plain_name(msg, HEADER_LEN) else {
        return Err(reply(Rcode::FormErr));
    };
    let (Some(qtype), Some(qclass)) = (be16(msg, name_end), be16(msg, name_end + 2)) else {
        return Err(reply(Rcode::FormErr));
    };
    let q_end = name_end + 4;
    let (Some(question), Some(qname)) = (msg.get(HEADER_LEN..q_end), msg.get(HEADER_LEN..name_end))
    else {
        return Err(reply(Rcode::FormErr));
    };
    let rrs = usize::from(an) + usize::from(ns) + usize::from(ar);
    Ok(Query {
        id,
        flags,
        question,
        qname,
        qtype,
        qclass,
        edns: find_opt(msg, q_end, rrs),
    })
}

/// Zoekt de OPT-record in de `count` records vanaf `at`: de UDP-maat.
/// Een kapotte staart telt als geen EDNS; de vraag zelf las al.
fn find_opt(msg: &[u8], mut at: usize, count: usize) -> Option<u16> {
    for _ in 0..count {
        let name_end = skip_name(msg, at)?;
        let rtype = be16(msg, name_end)?;
        let class = be16(msg, name_end + 2)?;
        let rdlen = usize::from(be16(msg, name_end + 8)?);
        if rtype == rtype::OPT {
            return Some(class);
        }
        at = name_end + 10 + rdlen;
    }
    None
}

/// De naam op de draad als tekst in kleine letters, zonder slotpunt, in
/// `buf`. `None` voor een naam die geen hostnaam kan zijn (een punt of een
/// stuurteken in een label): die is nooit van ons.
pub fn name_text<'b>(wire: &[u8], buf: &'b mut [u8; NAME_MAX]) -> Option<&'b str> {
    let mut at = 0;
    let mut n = 0;
    loop {
        let len = usize::from(*wire.get(at)?);
        if len == 0 {
            break;
        }
        let label = wire.get(at + 1..at + 1 + len)?;
        if n > 0 {
            *buf.get_mut(n)? = b'.';
            n += 1;
        }
        for &b in label {
            if !(0x21..=0x7E).contains(&b) || b == b'.' {
                return None;
            }
            *buf.get_mut(n)? = b.to_ascii_lowercase();
            n += 1;
        }
        at += 1 + len;
    }
    core::str::from_utf8(buf.get(..n)?).ok()
}

/// Schrijft `name` (tekst, met of zonder slotpunt) op de draad in `out`
/// vanaf `at`; geeft het einde.
fn put_name(out: &mut [u8], mut at: usize, name: &str) -> Result<usize> {
    let name = name.strip_suffix('.').unwrap_or(name);
    let start = at;
    if !name.is_empty() {
        for label in name.split('.') {
            let len = label.len();
            if len == 0 || len > LABEL_MAX {
                return Err(Error::BadName);
            }
            let dst = out.get_mut(at..at + 1 + len).ok_or(Error::NoRoom)?;
            dst[0] = len as u8;
            dst[1..].copy_from_slice(label.as_bytes());
            at += 1 + len;
        }
    }
    *out.get_mut(at).ok_or(Error::NoRoom)? = 0;
    at += 1;
    if at - start > NAME_MAX {
        return Err(Error::BadName);
    }
    Ok(at)
}

/// Een antwoord in opbouw, rechtstreeks in de buffer van de aanroeper.
///
/// # Invariants
///
/// `buf[..len]` is een geldige boodschap met de tellers in de kop gelijk
/// aan `an`, `ns` en `ar`, en `len <= limit <= buf.len()`.
#[derive(Debug)]
pub struct Reply<'b> {
    buf: &'b mut [u8],
    len: usize,
    limit: usize,
    an: u16,
    ns: u16,
    ar: u16,
    edns: Option<u16>,
}

impl<'b> Reply<'b> {
    /// Begint het antwoord op `q`: de kop (QR, AA, opcode en RD en CD van de
    /// vraag) en de vraag zelf. `limit` is de grootste boodschap die de
    /// vrager aanneemt; met EDNS blijft er plaats voor de OPT-record.
    pub fn start(buf: &'b mut [u8], q: &Query<'_>, limit: usize) -> Result<Self> {
        let limit = limit.min(buf.len());
        let mut r = Self::bare(buf, q.id, q.flags, Rcode::NoError, limit)?;
        let end = HEADER_LEN + q.question.len();
        if end + if q.edns.is_some() { OPT_LEN } else { 0 } > r.limit {
            return Err(Error::NoRoom);
        }
        r.buf
            .get_mut(HEADER_LEN..end)
            .ok_or(Error::NoRoom)?
            .copy_from_slice(q.question);
        r.len = end;
        r.set_count(4, 1);
        r.edns = q.edns;
        Ok(r)
    }

    /// Een antwoord zonder vraagsectie, alleen een code (FORMERR, NOTIMP).
    pub fn bare(
        buf: &'b mut [u8],
        id: u16,
        flags: u16,
        rcode: Rcode,
        limit: usize,
    ) -> Result<Self> {
        let limit = limit.min(buf.len());
        if limit < HEADER_LEN {
            return Err(Error::NoRoom);
        }
        let head = buf.get_mut(..HEADER_LEN).ok_or(Error::NoRoom)?;
        head.fill(0);
        head[..2].copy_from_slice(&id.to_be_bytes());
        let out = QR | AA | (flags & (0x7800 | RD | CD)) | rcode as u16;
        head[2..4].copy_from_slice(&out.to_be_bytes());
        Ok(Self {
            buf,
            len: HEADER_LEN,
            limit,
            an: 0,
            ns: 0,
            ar: 0,
            edns: None,
        })
    }

    fn set_count(&mut self, at: usize, n: u16) {
        if let Some(dst) = self.buf.get_mut(at..at + 2) {
            dst.copy_from_slice(&n.to_be_bytes());
        }
    }

    fn flags(&self) -> u16 {
        be16(self.buf, 2).unwrap_or(0)
    }

    fn set_flags(&mut self, f: u16) {
        self.set_count(2, f);
    }

    /// Zet de antwoordcode.
    pub fn set_rcode(&mut self, rcode: Rcode) {
        let f = (self.flags() & !0x000F) | rcode as u16;
        self.set_flags(f);
    }

    /// Zet de TC-bit: er paste niet alles in.
    pub fn set_truncated(&mut self) {
        let f = self.flags() | TC;
        self.set_flags(f);
    }

    /// De ruimte die records nog hebben, na de plaats voor de OPT-record.
    fn room(&self) -> usize {
        let reserve = if self.edns.is_some() { OPT_LEN } else { 0 };
        self.limit.saturating_sub(self.len + reserve)
    }

    /// Schrijft een recordkop (naam als verwijzing naar de vraag) en geeft
    /// de plek van de rdata.
    fn rr_head(&mut self, rtype: u16, ttl: u32, rdlen: usize) -> Result<usize> {
        if 12 + rdlen > self.room() {
            return Err(Error::NoRoom);
        }
        let at = self.len;
        let head = self.buf.get_mut(at..at + 12).ok_or(Error::NoRoom)?;
        head[..2].copy_from_slice(&QNAME_PTR);
        head[2..4].copy_from_slice(&rtype.to_be_bytes());
        head[4..6].copy_from_slice(&CLASS_IN.to_be_bytes());
        head[6..10].copy_from_slice(&ttl.to_be_bytes());
        let rdlen16 = u16::try_from(rdlen).map_err(|_| Error::NoRoom)?;
        head[10..12].copy_from_slice(&rdlen16.to_be_bytes());
        Ok(at + 12)
    }

    /// Een A-record voor de gevraagde naam. [`Error::NoRoom`] als hij niet
    /// meer past; de boodschap blijft dan zoals hij was.
    pub fn answer_a(&mut self, ip: Ipv4Addr, ttl: u32) -> Result {
        let at = self.rr_head(rtype::A, ttl, 4)?;
        self.buf
            .get_mut(at..at + 4)
            .ok_or(Error::NoRoom)?
            .copy_from_slice(&ip.octets());
        self.len = at + 4;
        self.an += 1;
        self.set_count(6, self.an);
        Ok(())
    }

    /// Een CNAME-record voor de gevraagde naam, naar `target`.
    pub fn answer_cname(&mut self, target: &str, ttl: u32) -> Result {
        let mut tmp = [0u8; NAME_MAX + 1];
        let n = put_name(&mut tmp, 0, target)?;
        let at = self.rr_head(rtype::CNAME, ttl, n)?;
        self.buf
            .get_mut(at..at + n)
            .ok_or(Error::NoRoom)?
            .copy_from_slice(tmp.get(..n).ok_or(Error::NoRoom)?);
        self.len = at + n;
        self.an += 1;
        self.set_count(6, self.an);
        Ok(())
    }

    /// De SOA-record van `zone` in de autoriteitssectie (RFC 2308): een
    /// lege of negatieve reactie wordt dan `ttl` seconden bewaard, niet de
    /// eigen standaard van de resolver. Alle tijden zijn `ttl`.
    pub fn authority_soa(&mut self, zone: &str, ttl: u32) -> Result {
        let mut tmp = [0u8; 3 * (NAME_MAX + 1) + 20];
        let mut n = put_name(&mut tmp, 0, zone)?;
        let rdata_at = n + 10;
        let mut at = rdata_at;
        at = put_soa_name(&mut tmp, at, "ns.", zone)?;
        at = put_soa_name(&mut tmp, at, "hostmaster.", zone)?;
        for v in [1, ttl, ttl, ttl, ttl] {
            tmp.get_mut(at..at + 4)
                .ok_or(Error::NoRoom)?
                .copy_from_slice(&u32::to_be_bytes(v));
            at += 4;
        }
        let rdlen = u16::try_from(at - rdata_at).map_err(|_| Error::NoRoom)?;
        let head = tmp.get_mut(n..n + 10).ok_or(Error::NoRoom)?;
        head[..2].copy_from_slice(&rtype::SOA.to_be_bytes());
        head[2..4].copy_from_slice(&CLASS_IN.to_be_bytes());
        head[4..8].copy_from_slice(&ttl.to_be_bytes());
        head[8..10].copy_from_slice(&rdlen.to_be_bytes());
        n = at;
        if n > self.room() {
            return Err(Error::NoRoom);
        }
        let start = self.len;
        self.buf
            .get_mut(start..start + n)
            .ok_or(Error::NoRoom)?
            .copy_from_slice(tmp.get(..n).ok_or(Error::NoRoom)?);
        self.len += n;
        self.ns += 1;
        self.set_count(8, self.ns);
        Ok(())
    }

    /// Het aantal antwoorden tot nu toe.
    #[must_use]
    pub fn answers(&self) -> u16 {
        self.an
    }

    /// Sluit af: de OPT-record als de vraag EDNS sprak (met onze maat), en
    /// de lengte van de boodschap.
    pub fn finish(mut self) -> usize {
        if self.edns.is_some() {
            let at = self.len;
            if let Some(opt) = self.buf.get_mut(at..at + OPT_LEN) {
                opt.fill(0);
                opt[1..3].copy_from_slice(&rtype::OPT.to_be_bytes());
                opt[3..5].copy_from_slice(&(EDNS_MAX as u16).to_be_bytes());
                self.len += OPT_LEN;
                self.ar += 1;
                self.set_count(10, self.ar);
            }
        }
        self.len
    }
}

/// `prefix` + `zone` als naam op de draad in `out` vanaf `at`.
fn put_soa_name(out: &mut [u8], at: usize, prefix: &str, zone: &str) -> Result<usize> {
    let label = prefix.strip_suffix('.').unwrap_or(prefix);
    let dst = out.get_mut(at..at + 1 + label.len()).ok_or(Error::NoRoom)?;
    dst[0] = u8::try_from(label.len()).map_err(|_| Error::BadName)?;
    dst[1..].copy_from_slice(label.as_bytes());
    put_name(out, at + 1 + label.len(), zone)
}

/// Bouwt een vraag naar `name` van type `qtype` in `out`; met `edns` een
/// OPT-record met die UDP-maat. Geeft de lengte.
pub fn encode_query(
    id: u16,
    name: &str,
    qtype: u16,
    edns: Option<u16>,
    out: &mut [u8],
) -> Result<usize> {
    let head = out.get_mut(..HEADER_LEN).ok_or(Error::NoRoom)?;
    head.fill(0);
    head[..2].copy_from_slice(&id.to_be_bytes());
    head[2..4].copy_from_slice(&RD.to_be_bytes());
    head[4..6].copy_from_slice(&1u16.to_be_bytes());
    let mut at = put_name(out, HEADER_LEN, name)?;
    let tail = out.get_mut(at..at + 4).ok_or(Error::NoRoom)?;
    tail[..2].copy_from_slice(&qtype.to_be_bytes());
    tail[2..].copy_from_slice(&CLASS_IN.to_be_bytes());
    at += 4;
    if let Some(size) = edns {
        let opt = out.get_mut(at..at + OPT_LEN).ok_or(Error::NoRoom)?;
        opt.fill(0);
        opt[1..3].copy_from_slice(&rtype::OPT.to_be_bytes());
        opt[3..5].copy_from_slice(&size.to_be_bytes());
        at += OPT_LEN;
        if let Some(ar) = out.get_mut(10..12) {
            ar.copy_from_slice(&1u16.to_be_bytes());
        }
    }
    Ok(at)
}

/// De gegevens van een gelezen record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Rdata {
    /// Een IPv4-adres.
    A(Ipv4Addr),
    /// Een doelnaam, als FQDN.
    Cname(String),
    /// Het begin van een zone.
    Soa {
        /// De primaire naamserver.
        mname: String,
        /// De beheerder.
        rname: String,
        /// Het serienummer.
        serial: u32,
        /// Verversen.
        refresh: u32,
        /// Opnieuw proberen.
        retry: u32,
        /// Verlopen.
        expire: u32,
        /// De negatieve TTL.
        minimum: u32,
    },
    /// Een ander type, met zijn nummer.
    Other(u16),
}

/// Een gelezen record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    /// De naam, als FQDN.
    pub name: String,
    /// Het type.
    pub rtype: u16,
    /// De TTL.
    pub ttl: u32,
    /// De gegevens.
    pub data: Rdata,
}

/// Een gelezen antwoord.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Message {
    /// Het id.
    pub id: u16,
    /// De vlaggen.
    pub flags: u16,
    /// De antwoordsectie.
    pub answers: Vec<Record>,
    /// De autoriteitssectie.
    pub authority: Vec<Record>,
    /// Het aantal records in de extra sectie.
    pub additional: u16,
}

impl Message {
    /// De antwoordcode.
    #[must_use]
    pub fn rcode(&self) -> u8 {
        Rcode::from_flags(self.flags)
    }

    /// Of het antwoord gezaghebbend is.
    #[must_use]
    pub fn is_authoritative(&self) -> bool {
        self.flags & AA != 0
    }

    /// Of het antwoord afgekapt is.
    #[must_use]
    pub fn is_truncated(&self) -> bool {
        self.flags & TC != 0
    }

    /// De A-adressen in de antwoordsectie.
    pub fn a_records(&self) -> impl Iterator<Item = Ipv4Addr> + '_ {
        self.answers.iter().filter_map(|r| match r.data {
            Rdata::A(ip) => Some(ip),
            _ => None,
        })
    }
}

/// Leest een naam met verwijzingen als FQDN; geeft de naam en het einde
/// op de plek zelf. Een verwijzing moet terug wijzen, dus een lus kan niet.
fn read_name(msg: &[u8], at: usize) -> Result<(String, usize)> {
    let mut name = String::new();
    let mut pos = at;
    let mut end = None;
    loop {
        let len = *msg.get(pos).ok_or(Error::Malformed)?;
        match len {
            0 => {
                if name.is_empty() {
                    name.try_reserve(1)?;
                    name.push('.');
                }
                return Ok((name, end.unwrap_or(pos + 1)));
            }
            1..=63 => {
                let label = msg
                    .get(pos + 1..pos + 1 + usize::from(len))
                    .ok_or(Error::Malformed)?;
                name.try_reserve(label.len() + 1)?;
                name.extend(label.iter().map(|&b| char::from(b)));
                name.push('.');
                if name.len() > NAME_MAX {
                    return Err(Error::Malformed);
                }
                pos += 1 + usize::from(len);
            }
            0xC0..=0xFF => {
                let target = usize::from(be16(msg, pos).ok_or(Error::Malformed)? & 0x3FFF);
                if target >= pos {
                    return Err(Error::Malformed);
                }
                end.get_or_insert(pos + 2);
                pos = target;
            }
            _ => return Err(Error::Malformed),
        }
    }
}

/// Leest één record op `at`; geeft het record en het einde.
fn read_record(msg: &[u8], at: usize) -> Result<(Record, usize)> {
    let (name, at) = read_name(msg, at)?;
    let rtype = be16(msg, at).ok_or(Error::Malformed)?;
    let ttl = be32(msg, at + 4).ok_or(Error::Malformed)?;
    let rdlen = usize::from(be16(msg, at + 8).ok_or(Error::Malformed)?);
    let rd = at + 10;
    let rdata = msg.get(rd..rd + rdlen).ok_or(Error::Malformed)?;
    let data = match rtype {
        rtype::A => {
            let o: [u8; 4] = rdata.try_into().map_err(|_| Error::Malformed)?;
            Rdata::A(Ipv4Addr::from(o))
        }
        rtype::CNAME => Rdata::Cname(read_name(msg, rd)?.0),
        rtype::SOA => {
            let (mname, p) = read_name(msg, rd)?;
            let (rname, p) = read_name(msg, p)?;
            let n = |i: usize| be32(msg, p + 4 * i).ok_or(Error::Malformed);
            Rdata::Soa {
                mname,
                rname,
                serial: n(0)?,
                refresh: n(1)?,
                retry: n(2)?,
                expire: n(3)?,
                minimum: n(4)?,
            }
        }
        other => Rdata::Other(other),
    };
    Ok((
        Record {
            name,
            rtype,
            ttl,
            data,
        },
        rd + rdlen,
    ))
}

/// Leest een antwoord: kop, vraag overslaan, antwoorden en autoriteit.
pub fn decode(msg: &[u8]) -> Result<Message> {
    let field = |at| be16(msg, at).ok_or(Error::Malformed);
    let mut m = Message {
        id: field(0)?,
        flags: field(2)?,
        additional: field(10)?,
        ..Message::default()
    };
    let (qd, an, ns) = (field(4)?, field(6)?, field(8)?);
    let mut at = HEADER_LEN;
    for _ in 0..qd {
        at = skip_name(msg, at).ok_or(Error::Malformed)? + 4;
    }
    for (count, out) in [(an, &mut m.answers), (ns, &mut m.authority)] {
        for _ in 0..count {
            let (rec, next) = read_record(msg, at)?;
            out.try_reserve(1)?;
            out.push(rec);
            at = next;
        }
    }
    Ok(m)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_query_round_trips_through_the_parser() {
        let mut q = [0u8; 512];
        let n = encode_query(0xBEEF, "MyApp.Hop.Local.", rtype::A, None, &mut q).unwrap();
        let query = parse_query(&q[..n]).unwrap();
        assert_eq!(query.id, 0xBEEF);
        assert_eq!(query.qtype, rtype::A);
        assert_eq!(query.qclass, CLASS_IN);
        assert_eq!(query.edns, None);
        let mut buf = [0u8; NAME_MAX];
        assert_eq!(name_text(query.qname, &mut buf), Some("myapp.hop.local"));
        let n = encode_query(1, "x.hop.local", rtype::A, Some(4096), &mut q).unwrap();
        assert_eq!(parse_query(&q[..n]).unwrap().edns, Some(4096));
    }

    #[test]
    fn a_reply_carries_the_question_and_records() {
        let mut q = [0u8; 512];
        let n = encode_query(7, "web.hop.local", rtype::A, None, &mut q).unwrap();
        let query = parse_query(&q[..n]).unwrap();
        let mut out = [0u8; 512];
        let mut r = Reply::start(&mut out, &query, UDP_MAX).unwrap();
        r.answer_a(Ipv4Addr::new(10, 0, 0, 1), 5).unwrap();
        r.answer_cname("target.example.com", 5).unwrap();
        r.authority_soa("hop.local", 5).unwrap();
        let len = r.finish();
        let m = decode(&out[..len]).unwrap();
        assert_eq!(m.id, 7);
        assert!(m.is_authoritative());
        assert_eq!(m.rcode(), 0);
        assert_eq!(m.answers[0].name, "web.hop.local.");
        assert_eq!(m.answers[0].ttl, 5);
        assert_eq!(m.answers[0].data, Rdata::A(Ipv4Addr::new(10, 0, 0, 1)));
        assert_eq!(
            m.answers[1].data,
            Rdata::Cname(String::from("target.example.com."))
        );
        let Rdata::Soa {
            mname,
            rname,
            minimum,
            ..
        } = &m.authority[0].data
        else {
            panic!("no SOA");
        };
        assert_eq!(m.authority[0].name, "hop.local.");
        assert_eq!(mname, "ns.hop.local.");
        assert_eq!(rname, "hostmaster.hop.local.");
        assert_eq!(*minimum, 5);
    }

    #[test]
    fn the_server_side_is_strict() {
        // Te kort, of zelf een antwoord: niets terug.
        assert_eq!(parse_query(&[0; 5]), Err(Bad::Drop));
        let mut q = [0u8; 512];
        let n = encode_query(9, "a.hop.local", rtype::A, None, &mut q).unwrap();
        let mut resp = q;
        resp[2] |= 0x80;
        assert_eq!(parse_query(&resp[..n]), Err(Bad::Drop));
        // Een andere opcode: NOTIMP.
        let mut notify = q;
        notify[2] = 0x20; // opcode 4
        assert!(matches!(
            parse_query(&notify[..n]),
            Err(Bad::Reply {
                rcode: Rcode::NotImp,
                ..
            })
        ));
        // Twee vragen: FORMERR.
        let mut two = q;
        two[5] = 2;
        assert!(matches!(
            parse_query(&two[..n]),
            Err(Bad::Reply {
                rcode: Rcode::FormErr,
                ..
            })
        ));
        // Een afgekapte vraag: FORMERR.
        assert!(matches!(
            parse_query(&q[..n - 2]),
            Err(Bad::Reply {
                rcode: Rcode::FormErr,
                ..
            })
        ));
        // Een verwijzing in de vraagnaam: FORMERR.
        let mut ptr = q;
        ptr[12] = 0xC0;
        assert!(matches!(
            parse_query(&ptr[..n]),
            Err(Bad::Reply {
                rcode: Rcode::FormErr,
                ..
            })
        ));
    }

    #[test]
    fn names_that_are_never_ours() {
        let mut buf = [0u8; NAME_MAX];
        assert_eq!(name_text(b"\x03a.b\x00", &mut buf), None);
        assert_eq!(name_text(b"\x02a\x01\x00", &mut buf), None);
        assert_eq!(name_text(b"\x00", &mut buf), Some(""));
        let mut out = [0u8; 300];
        assert_eq!(put_name(&mut out, 0, "a..b"), Err(Error::BadName));
        let long = "a".repeat(64);
        assert_eq!(put_name(&mut out, 0, &long), Err(Error::BadName));
    }

    #[test]
    fn a_full_reply_refuses_the_next_record() {
        let mut q = [0u8; 512];
        let n = encode_query(1, "big.hop.local", rtype::A, None, &mut q).unwrap();
        let query = parse_query(&q[..n]).unwrap();
        let mut out = [0u8; 1500];
        let mut r = Reply::start(&mut out, &query, UDP_MAX).unwrap();
        let mut fitted = 0;
        while r.answer_a(Ipv4Addr::new(10, 0, 0, 1), 5).is_ok() {
            fitted += 1;
        }
        // 512 min kop (12) en vraag (15 + 4), gedeeld door 16.
        assert_eq!(fitted, (512 - 12 - 19) / 16);
        let len = r.finish();
        assert!(len <= UDP_MAX);
        assert_eq!(decode(&out[..len]).unwrap().answers.len(), fitted);
    }

    #[test]
    fn edns_gets_an_opt_back() {
        let mut q = [0u8; 512];
        let n = encode_query(1, "e.hop.local", rtype::A, Some(1400), &mut q).unwrap();
        let query = parse_query(&q[..n]).unwrap();
        let mut out = [0u8; 1500];
        let r = Reply::start(&mut out, &query, EDNS_MAX).unwrap();
        let len = r.finish();
        let m = decode(&out[..len]).unwrap();
        assert_eq!(m.additional, 1);
        assert_eq!(be16(&out, len - 11 + 1), Some(rtype::OPT));
        assert_eq!(be16(&out, len - 11 + 3), Some(EDNS_MAX as u16));
    }
}
