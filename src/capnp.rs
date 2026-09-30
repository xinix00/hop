//! Precies zoveel Cap'n Proto als de tunnel-registratie vraagt.
//!
//! Bezit één segment bouwen in een buffer van de aanroeper, één segment
//! lezen met een grenscontrole op elke pointer, en de stream-omhulling
//! eromheen (de segmenttabel). Geen schema-compiler, geen capability-tabel,
//! geen arena's, geen heap: de berichtvormen zijn vast (drie stuks, zie
//! [`crate::register`]), en dan is de draadvorm eenvoudig genoeg om
//! rechtstreeks te schrijven:
//!
//! - alles is little-endian, alles in woorden van acht bytes;
//! - een struct is: data-woorden, dan pointer-woorden;
//! - een struct-pointer is `offset << 2 | 0`, met daarna data- en
//!   pointer-woorden;
//! - een lijst-pointer is `offset << 2 | 1`, met elementtype en aantal;
//! - offsets zijn relatief, geteld vanaf het woord ná de pointer.
//!
//! De bouwer legt structs en lijsten neer in de volgorde van aanroepen,
//! precies zoals de Go-voorganger (`internal/capnp`), zodat dezelfde
//! aanroepen dezelfde bytes geven; de toetsen in [`crate::register`] leggen
//! dat vast tegen bytes die de Go-code maakte.

#![forbid(unsafe_code)]

use core::fmt;

/// De grootste boodschap die de tunnel van de edge aanneemt, in woorden.
/// Een registratie-antwoord is tientallen bytes; dit plafond (512 KiB, als
/// in Go) houdt een edge die onzin stuurt uit het geheugen van het slot.
pub(crate) const MAX_MESSAGE_WORDS: u64 = 1 << 16;

/// Het meeste aantal segmenten in één boodschap dat de tunnel overslaat.
pub(crate) const MAX_SEGMENTS: u32 = 512;

/// Elementtype 2: één byte per element (Text en Data).
const ELEM_BYTE: u64 = 2;
/// Elementtype 6: één pointer per element (List(Text)).
const ELEM_POINTER: u64 = 6;

/// Waarom bouwen of lezen faalde.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Error {
    /// De buffer van de bouwer is vol; zoveel bytes waren nodig.
    Full {
        /// De benodigde lengte.
        need: usize,
    },
    /// Een pointer-index buiten de struct (een fout in de bouwer zelf).
    PointerIndex,
    /// Een pointer die buiten het segment wijst.
    OutOfBounds,
    /// Een ander pointertype dan verwacht (0 struct, 1 lijst).
    PointerType {
        /// Het gevonden type.
        kind: u8,
    },
    /// Een lijst die geen byte-lijst is.
    ElementType {
        /// Het gevonden elementtype.
        kind: u8,
    },
    /// Tekst die geen UTF-8 is.
    Utf8,
    /// Een boodschap van zoveel woorden of segmenten weigert de lezer.
    TooLarge {
        /// Het aantal woorden.
        words: u64,
    },
    /// Een lege boodschap.
    Empty,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full { need } => write!(f, "capnp: message needs {need} bytes"),
            Self::PointerIndex => f.write_str("capnp: pointer index outside the struct"),
            Self::OutOfBounds => f.write_str("capnp: pointer beyond the segment"),
            Self::PointerType { kind } => write!(f, "capnp: unexpected pointer type {kind}"),
            Self::ElementType { kind } => {
                write!(f, "capnp: expected a byte list, got element type {kind}")
            }
            Self::Utf8 => f.write_str("capnp: text is not UTF-8"),
            Self::TooLarge { words } => write!(f, "capnp: message of {words} words refused"),
            Self::Empty => f.write_str("capnp: empty message"),
        }
    }
}

/// Het resultaat van deze module.
pub(crate) type Result<T = (), E = Error> = core::result::Result<T, E>;

/// Een struct in aanbouw: waar zijn data begint en hoe groot hij is.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Struct {
    /// Byte-offset van het eerste data-woord, vanaf het begin van het segment.
    data: usize,
    /// Aantal data-woorden.
    data_words: u16,
    /// Aantal pointer-woorden.
    ptr_words: u16,
}

/// Bouwt één boodschap in een buffer van de aanroeper: acht bytes
/// segmenttabel, dan het segment.
///
/// Een fout (buffer vol, verkeerde index) blijft staan tot [`Builder::finish`]
/// hem teruggeeft; zo leest de bouw als de berichtvorm, zonder `?` per veld,
/// en toch zonder paniek.
pub(crate) struct Builder<'b> {
    /// De buffer; de eerste acht bytes zijn voor de segmenttabel.
    buf: &'b mut [u8],
    /// Hoeveel bytes van het segment in gebruik zijn.
    len: usize,
    /// De eerste fout, als die er is.
    err: Option<Error>,
}

/// De lengte van de segmenttabel voor één segment.
const TABLE: usize = 8;

impl<'b> Builder<'b> {
    /// Een lege bouwer over `buf`.
    pub(crate) fn new(buf: &'b mut [u8]) -> Self {
        Self {
            buf,
            len: 0,
            err: None,
        }
    }

    /// Onthoudt de eerste fout.
    fn fail(&mut self, e: Error) {
        self.err.get_or_insert(e);
    }

    /// Reserveert `bytes` (een veelvoud van acht) aan nullen en geeft de
    /// offset, of `None` als de buffer vol is.
    fn alloc(&mut self, bytes: usize) -> Option<usize> {
        let start = self.len;
        let end = start.checked_add(bytes)?;
        match self.buf.get_mut(TABLE + start..TABLE + end) {
            Some(w) => {
                w.fill(0);
                self.len = end;
                Some(start)
            }
            None => {
                self.fail(Error::Full { need: TABLE + end });
                None
            }
        }
    }

    /// Schrijft woord `v` op segment-offset `at`.
    fn put(&mut self, at: usize, v: u64) {
        match self.buf.get_mut(TABLE + at..TABLE + at + 8) {
            Some(w) => w.copy_from_slice(&v.to_le_bytes()),
            None => self.fail(Error::OutOfBounds),
        }
    }

    /// De relatieve offset in woorden van een pointer op `at` naar `target`,
    /// al op zijn plek (`<< 2`) in de onderste 32 bits.
    fn offset(at: usize, target: usize) -> u64 {
        // Offsets zijn in dit segment altijd vooruit (de bouwer legt kinderen
        // achter hun ouder), dus niet negatief.
        let words = (target.saturating_sub(at + 8) / 8) as u64;
        (words << 2) & 0xffff_fffc
    }

    /// Een struct-pointer op `at` naar `s`.
    fn struct_ptr(&mut self, at: usize, s: Struct) {
        let p = Self::offset(at, s.data)
            | (u64::from(s.data_words) << 32)
            | (u64::from(s.ptr_words) << 48);
        self.put(at, p);
    }

    /// Een lijst-pointer op `at` naar `target`.
    fn list_ptr(&mut self, at: usize, target: usize, elem: u64, count: usize) {
        let p = 1 | Self::offset(at, target) | (elem << 32) | ((count as u64) << 35);
        self.put(at, p);
    }

    /// Reserveert een struct.
    fn new_bare(&mut self, data_words: u16, ptr_words: u16) -> Struct {
        let bytes = (usize::from(data_words) + usize::from(ptr_words)) * 8;
        let data = self.alloc(bytes).unwrap_or(0);
        Struct {
            data,
            data_words,
            ptr_words,
        }
    }

    /// Zet de wortel-struct neer; de wortel-pointer staat in woord 0.
    pub(crate) fn root(&mut self, data_words: u16, ptr_words: u16) -> Struct {
        self.alloc(8);
        let s = self.new_bare(data_words, ptr_words);
        self.struct_ptr(0, s);
        s
    }

    /// De segment-offset van pointer-slot `i` van `s`.
    fn ptr_at(&mut self, s: Struct, i: u16) -> Option<usize> {
        if i >= s.ptr_words {
            self.fail(Error::PointerIndex);
            return None;
        }
        Some(s.data + usize::from(s.data_words) * 8 + usize::from(i) * 8)
    }

    /// Zet een nieuwe struct neer in pointer-slot `i` van `s`.
    pub(crate) fn new_struct(
        &mut self,
        s: Struct,
        i: u16,
        data_words: u16,
        ptr_words: u16,
    ) -> Struct {
        let child = self.new_bare(data_words, ptr_words);
        if let Some(at) = self.ptr_at(s, i) {
            self.struct_ptr(at, child);
        }
        child
    }

    /// Schrijft `v` op byte-offset `off` in de data-sectie van `s`.
    fn set_bytes(&mut self, s: Struct, off: usize, v: &[u8]) {
        if off + v.len() > usize::from(s.data_words) * 8 {
            self.fail(Error::OutOfBounds);
            return;
        }
        match self
            .buf
            .get_mut(TABLE + s.data + off..TABLE + s.data + off + v.len())
        {
            Some(w) => w.copy_from_slice(v),
            None => self.fail(Error::OutOfBounds),
        }
    }

    /// Een `UInt8` op byte-offset `off`.
    pub(crate) fn set_u8(&mut self, s: Struct, off: usize, v: u8) {
        self.set_bytes(s, off, &[v]);
    }

    /// Een `UInt16` op byte-offset `off`.
    pub(crate) fn set_u16(&mut self, s: Struct, off: usize, v: u16) {
        self.set_bytes(s, off, &v.to_le_bytes());
    }

    /// Een `UInt32` op byte-offset `off`.
    pub(crate) fn set_u32(&mut self, s: Struct, off: usize, v: u32) {
        self.set_bytes(s, off, &v.to_le_bytes());
    }

    /// Een `UInt64` op byte-offset `off` (de interface-id's zijn 64-bits
    /// hashes).
    pub(crate) fn set_u64(&mut self, s: Struct, off: usize, v: u64) {
        self.set_bytes(s, off, &v.to_le_bytes());
    }

    /// Een `Bool` op bit-offset `bit`; `false` is de standaard en schrijft
    /// niets.
    pub(crate) fn set_bool(&mut self, s: Struct, bit: usize, v: bool) {
        if !v {
            return;
        }
        let at = TABLE + s.data + bit / 8;
        if bit / 8 >= usize::from(s.data_words) * 8 {
            self.fail(Error::OutOfBounds);
            return;
        }
        match self.buf.get_mut(at) {
            Some(b) => *b |= 1 << (bit % 8),
            None => self.fail(Error::OutOfBounds),
        }
    }

    /// Legt `raw` plus `nul` nul-bytes neer als byte-lijst, op woorden
    /// afgerond, en geeft (offset, elementen).
    fn bytes(&mut self, raw: &[u8], nul: usize) -> Option<(usize, usize)> {
        let n = raw.len() + nul;
        let start = self.alloc(n.div_ceil(8) * 8)?;
        let dst = self.buf.get_mut(TABLE + start..TABLE + start + raw.len())?;
        dst.copy_from_slice(raw);
        Some((start, n))
    }

    /// Een `Text`-veld (UTF-8 met afsluitende nul) in pointer-slot `i`.
    pub(crate) fn set_text(&mut self, s: Struct, i: u16, v: &str) {
        self.set_list_bytes(s, i, v.as_bytes(), 1);
    }

    /// Een `Data`-veld (kale bytes) in pointer-slot `i`.
    pub(crate) fn set_data(&mut self, s: Struct, i: u16, v: &[u8]) {
        self.set_list_bytes(s, i, v, 0);
    }

    /// De gedeelde weg van Text en Data.
    fn set_list_bytes(&mut self, s: Struct, i: u16, v: &[u8], nul: usize) {
        let Some(at) = self.ptr_at(s, i) else {
            return;
        };
        if let Some((start, n)) = self.bytes(v, nul) {
            self.list_ptr(at, start, ELEM_BYTE, n);
        }
    }

    /// Een `List(Text)` in pointer-slot `i`: een lijst van pointers, elk
    /// naar een eigen tekst. `ClientInfo.features` heeft die vorm.
    pub(crate) fn set_text_list(&mut self, s: Struct, i: u16, values: &[&str]) {
        let Some(at) = self.ptr_at(s, i) else {
            return;
        };
        let Some(start) = self.alloc(values.len() * 8) else {
            return;
        };
        self.list_ptr(at, start, ELEM_POINTER, values.len());
        for (n, v) in values.iter().enumerate() {
            if let Some((text, len)) = self.bytes(v.as_bytes(), 1) {
                self.list_ptr(start + n * 8, text, ELEM_BYTE, len);
            }
        }
    }

    /// Een lege pointer-lijst in pointer-slot `i`: de `transform` van een
    /// `PromisedAnswer` die de capability zelf bedoelt.
    pub(crate) fn set_empty_list(&mut self, s: Struct, i: u16) {
        if let Some(at) = self.ptr_at(s, i) {
            self.put(at, 1 | (ELEM_POINTER << 32));
        }
    }

    /// Zet de segmenttabel en geeft de hele boodschap: tabel plus segment.
    pub(crate) fn finish(self) -> Result<&'b [u8]> {
        if let Some(e) = self.err {
            return Err(e);
        }
        let words = u32::try_from(self.len / 8).map_err(|_| Error::TooLarge {
            words: (self.len / 8) as u64,
        })?;
        let head = self
            .buf
            .get_mut(..TABLE)
            .ok_or(Error::Full { need: TABLE })?;
        head[..4].copy_from_slice(&0u32.to_le_bytes()); // één segment (aantal min één)
        head[4..].copy_from_slice(&words.to_le_bytes());
        self.buf.get(..TABLE + self.len).ok_or(Error::OutOfBounds)
    }
}

/// Hoeveel segmenten een boodschap heeft, uit de eerste vier bytes van zijn
/// tabel.
pub(crate) fn segment_count(first: [u8; 4]) -> Result<u32> {
    let n = u32::from_le_bytes(first).wrapping_add(1);
    if n == 0 || n > MAX_SEGMENTS {
        return Err(Error::TooLarge {
            words: u64::from(n),
        });
    }
    Ok(n)
}

/// Hoeveel bytes van de tabel er na de eerste vier nog komen voor `n`
/// segmenten: vier per segment, en de tabel eindigt op een woordgrens.
pub(crate) fn table_rest(n: u32) -> usize {
    let table = (4 + 4 * n as usize).div_ceil(8) * 8;
    table - 4
}

/// De totale lengte van de segmenten in bytes, uit de rest van de tabel
/// (de maten, elk vier bytes).
pub(crate) fn body_len(rest: &[u8], n: u32) -> Result<usize> {
    let mut words: u64 = 0;
    for i in 0..n as usize {
        let b = rest.get(i * 4..i * 4 + 4).ok_or(Error::OutOfBounds)?;
        let mut w = [0u8; 4];
        w.copy_from_slice(b);
        words += u64::from(u32::from_le_bytes(w));
    }
    if words == 0 {
        return Err(Error::Empty);
    }
    if words > MAX_MESSAGE_WORDS {
        return Err(Error::TooLarge { words });
    }
    usize::try_from(words * 8).map_err(|_| Error::TooLarge { words })
}

/// Een gelezen struct.
#[derive(Debug, Clone, Copy)]
pub(crate) struct View<'a> {
    /// Het segment.
    seg: &'a [u8],
    /// Byte-offset van het eerste data-woord.
    data: usize,
    /// Aantal data-woorden.
    data_words: usize,
    /// Aantal pointer-woorden.
    ptr_words: usize,
    /// Een lege pointer: elk veld leest als zijn standaard.
    null: bool,
}

/// Leest woord `at` van `seg`.
fn word(seg: &[u8], at: usize) -> Result<u64> {
    let b = seg.get(at..at + 8).ok_or(Error::OutOfBounds)?;
    let mut w = [0u8; 8];
    w.copy_from_slice(b);
    Ok(u64::from_le_bytes(w))
}

/// Het doel van een pointer op `at`: `at + 8 + offset * 8`, of een fout als
/// dat buiten het segment valt.
fn target(at: usize, p: u64) -> Result<usize> {
    let off = i64::from((p as u32 as i32) >> 2);
    let base = i64::try_from(at + 8).map_err(|_| Error::OutOfBounds)?;
    usize::try_from(base + off * 8).map_err(|_| Error::OutOfBounds)
}

impl<'a> View<'a> {
    /// De wortel-struct van `seg` (de pointer in woord 0).
    pub(crate) fn root(seg: &'a [u8]) -> Result<Self> {
        Self::at(seg, 0)
    }

    /// Een lege struct.
    fn empty(seg: &'a [u8]) -> Self {
        Self {
            seg,
            data: 0,
            data_words: 0,
            ptr_words: 0,
            null: true,
        }
    }

    /// Volgt de struct-pointer op byte-offset `at`.
    fn at(seg: &'a [u8], at: usize) -> Result<Self> {
        let p = word(seg, at)?;
        if p == 0 {
            return Ok(Self::empty(seg));
        }
        if p & 3 != 0 {
            return Err(Error::PointerType {
                kind: (p & 3) as u8,
            });
        }
        let data_words = usize::from((p >> 32) as u16);
        let ptr_words = usize::from((p >> 48) as u16);
        let data = target(at, p)?;
        if data + (data_words + ptr_words) * 8 > seg.len() {
            return Err(Error::OutOfBounds);
        }
        Ok(Self {
            seg,
            data,
            data_words,
            ptr_words,
            null: false,
        })
    }

    /// Of de pointer leeg was; in Cap'n Proto de standaard, geen fout.
    pub(crate) fn is_null(&self) -> bool {
        self.null
    }

    /// Bytes op `off` in de data-sectie, of `None` voorbij het einde (een
    /// veld dat een oudere afzender nog niet had: dat leest als nul).
    fn field<const N: usize>(&self, off: usize) -> [u8; N] {
        let mut out = [0u8; N];
        if !self.null
            && off + N <= self.data_words * 8
            && let Some(b) = self.seg.get(self.data + off..self.data + off + N)
        {
            out.copy_from_slice(b);
        }
        out
    }

    /// Een `UInt16` op byte-offset `off`.
    pub(crate) fn u16(&self, off: usize) -> u16 {
        u16::from_le_bytes(self.field(off))
    }

    /// Een `UInt32` op byte-offset `off`.
    pub(crate) fn u32(&self, off: usize) -> u32 {
        u32::from_le_bytes(self.field(off))
    }

    /// Een `Int64` op byte-offset `off`.
    pub(crate) fn i64(&self, off: usize) -> i64 {
        i64::from_le_bytes(self.field(off))
    }

    /// Een `Bool` op bit-offset `bit`.
    pub(crate) fn bool(&self, bit: usize) -> bool {
        let [b] = self.field::<1>(bit / 8);
        b & (1 << (bit % 8)) != 0
    }

    /// De byte-offset van pointer-slot `i`, of `None` voorbij de struct.
    fn ptr(&self, i: usize) -> Option<usize> {
        (!self.null && i < self.ptr_words).then(|| self.data + self.data_words * 8 + i * 8)
    }

    /// Volgt pointer-slot `i` naar een struct.
    pub(crate) fn struct_at(&self, i: usize) -> Result<View<'a>> {
        match self.ptr(i) {
            Some(at) => Self::at(self.seg, at),
            None => Ok(Self::empty(self.seg)),
        }
    }

    /// Een `Data`-veld uit pointer-slot `i`.
    pub(crate) fn data_at(&self, i: usize) -> Result<&'a [u8]> {
        let Some(at) = self.ptr(i) else {
            return Ok(&[]);
        };
        let p = word(self.seg, at)?;
        if p == 0 {
            return Ok(&[]);
        }
        if p & 3 != 1 {
            return Err(Error::PointerType {
                kind: (p & 3) as u8,
            });
        }
        let elem = ((p >> 32) & 7) as u8;
        if u64::from(elem) != ELEM_BYTE {
            return Err(Error::ElementType { kind: elem });
        }
        let n = usize::try_from(p >> 35).map_err(|_| Error::OutOfBounds)?;
        let start = target(at, p)?;
        self.seg
            .get(start..start.checked_add(n).ok_or(Error::OutOfBounds)?)
            .ok_or(Error::OutOfBounds)
    }

    /// Een `Text`-veld uit pointer-slot `i`, zonder de afsluitende nul.
    pub(crate) fn text_at(&self, i: usize) -> Result<&'a str> {
        let raw = self.data_at(i)?;
        let raw = raw.strip_suffix(&[0]).unwrap_or(raw);
        core::str::from_utf8(raw).map_err(|_| Error::Utf8)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Het segment uit een boodschap met één segment.
    fn segment(msg: &[u8]) -> &[u8] {
        assert_eq!(segment_count(msg[..4].try_into().unwrap()).unwrap(), 1);
        let n = body_len(&msg[4..8], 1).unwrap();
        assert_eq!(msg.len(), 8 + n);
        &msg[8..]
    }

    // De rondgang uit de Go-toets: elk veld op de plek waar de gegenereerde
    // cloudflared-code hem verwacht.
    #[test]
    fn round_trip() {
        let mut buf = [0u8; 512];
        let mut b = Builder::new(&mut buf);
        let root = b.root(1, 3);
        b.set_u8(root, 0, 7);
        b.set_u16(root, 2, 0xbeef);
        b.set_u32(root, 4, 0x1234_5678);
        let auth = b.new_struct(root, 0, 0, 2);
        b.set_text(auth, 0, "9c2b680da60a658926b3fe5b3bf5f8ee");
        b.set_data(auth, 1, &[1, 2, 3, 4, 5]);
        b.set_data(root, 1, &[0xde, 0xad, 0xbe, 0xef]);
        let opts = b.new_struct(root, 2, 1, 2);
        let client = b.new_struct(opts, 0, 0, 4);
        b.set_data(client, 0, b"id");
        b.set_text_list(client, 1, &["eerste", "tweede", "derde"]);
        b.set_text(client, 2, "v0.1.0");
        b.set_text(client, 3, "hopos_riscv64");
        b.set_bool(opts, 0, true);
        b.set_u8(opts, 1, 3);
        b.set_u8(opts, 2, 42);
        let msg = b.finish().unwrap();

        let got = View::root(segment(msg)).unwrap();
        assert_eq!(got.u16(2), 0xbeef);
        assert_eq!(got.u32(4), 0x1234_5678);
        let a = got.struct_at(0).unwrap();
        assert_eq!(a.text_at(0).unwrap(), "9c2b680da60a658926b3fe5b3bf5f8ee");
        assert_eq!(a.data_at(1).unwrap(), [1, 2, 3, 4, 5]);
        assert_eq!(got.data_at(1).unwrap(), [0xde, 0xad, 0xbe, 0xef]);
        let o = got.struct_at(2).unwrap();
        assert!(o.bool(0));
        // compressionQuality op byte 1, numPreviousAttempts op byte 2.
        assert_eq!(o.u32(0) >> 8 & 0xff, 3);
        assert_eq!(o.u32(0) >> 16 & 0xff, 42);
        let c = o.struct_at(0).unwrap();
        assert_eq!(c.text_at(2).unwrap(), "v0.1.0");
        assert_eq!(c.text_at(3).unwrap(), "hopos_riscv64");
    }

    // Een lege pointer is de standaard, geen fout.
    #[test]
    fn null_pointers_are_defaults() {
        let mut buf = [0u8; 64];
        let mut b = Builder::new(&mut buf);
        let root = b.root(1, 3);
        b.set_u8(root, 0, 1);
        let msg = b.finish().unwrap();
        let got = View::root(segment(msg)).unwrap();
        let child = got.struct_at(1).unwrap();
        assert!(child.is_null());
        assert_eq!(child.u32(0), 0);
        assert_eq!(got.text_at(2).unwrap(), "");
    }

    // Buiten de data-sectie lezen geeft nul: zo leest een nieuwer schema
    // een ouder bericht.
    #[test]
    fn short_struct_reads_zero() {
        let mut buf = [0u8; 64];
        let mut b = Builder::new(&mut buf);
        let root = b.root(1, 1);
        b.set_u32(root, 0, 5);
        let msg = b.finish().unwrap();
        let got = View::root(segment(msg)).unwrap();
        assert_eq!(got.i64(8), 0);
        assert!(!got.bool(200));
    }

    // Een pointer die buiten het segment wijst, is een fout en geen lees in
    // andermans geheugen.
    #[test]
    fn refuses_pointer_outside_segment() {
        let seg = ((100u64 << 2) | (1u64 << 32)).to_le_bytes();
        assert_eq!(View::root(&seg).unwrap_err(), Error::OutOfBounds);
        // Een negatieve offset voor het begin.
        let seg = (u64::from(((-4i32) << 2) as u32) | (1u64 << 32)).to_le_bytes();
        assert_eq!(View::root(&seg).unwrap_err(), Error::OutOfBounds);
        // Een far-pointer (type 2) kent deze lezer niet.
        assert_eq!(
            View::root(&2u64.to_le_bytes()).unwrap_err(),
            Error::PointerType { kind: 2 }
        );
    }

    // De stream-vorm: aantal segmenten min één, dan de maat in woorden.
    #[test]
    fn stream_framing() {
        let mut buf = [0u8; 64];
        let mut b = Builder::new(&mut buf);
        b.root(2, 0);
        let msg = b.finish().unwrap();
        assert_eq!(&msg[..8], &[0, 0, 0, 0, 3, 0, 0, 0]);
        assert_eq!(msg.len(), 32);
        // Twee segmenten: de tabel is 4 + 8 bytes, afgerond op 16.
        let n = segment_count([1, 0, 0, 0]).unwrap();
        assert_eq!(n, 2);
        assert_eq!(table_rest(n), 12);
        assert_eq!(
            body_len(&[1, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0], n).unwrap(),
            24
        );
        assert_eq!(table_rest(1), 4);
        assert!(segment_count([0xff, 0xff, 0xff, 0xff]).is_err());
        assert!(body_len(&[0, 0, 0, 0], 1).is_err());
        assert!(body_len(&[0, 0, 2, 0], 1).is_err(), "boven het plafond");
    }

    // Een volle buffer is een fout bij het afronden, geen paniek.
    #[test]
    fn full_buffer_is_an_error() {
        let mut buf = [0u8; 32];
        let mut b = Builder::new(&mut buf);
        let root = b.root(1, 1);
        b.set_text(root, 0, "dit past niet meer in de buffer");
        assert!(matches!(b.finish(), Err(Error::Full { .. })));
        let mut buf = [0u8; 64];
        let mut b = Builder::new(&mut buf);
        let root = b.root(1, 1);
        b.new_struct(root, 3, 0, 0);
        assert_eq!(b.finish().unwrap_err(), Error::PointerIndex);
    }
}
