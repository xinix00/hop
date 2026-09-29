//! Inflate (RFC 1951) en gzip (RFC 1952) als `Read`, met begrensd geheugen.
//!
//! Bezit de bitlezer over een bron, het venster van 32 KiB en de
//! Huffman-tabellen van het lopende blok. Bezit de bron niet langer dan de
//! lezer leeft, en schrijft nergens heen: wie leest, krijgt bytes.
//!
//! Waarom zelf: Go deed dit met `compress/flate` uit zijn stdlib, Rust heeft
//! er geen, en een crate van buiten komt er niet in (handboek §8). De vorm is
//! die van zlib's `puff.c` (Mark Adler): canonieke codes, bit voor bit
//! gedecodeerd. Dat is trager dan een tabel-decoder (gemeten 29-09-2026: in
//! de orde van tientallen MB/s in release), en dat is ruim genoeg voor een
//! artifact dat één keer per start binnenkomt. Het geheugen is vast: het
//! venster (32 KiB), een leesbuffer (8 KiB) en twee tabellen van een paar
//! honderd `u16`, wat de invoer ook zegt.

use std::io::{self, Read};

/// Het venster van deflate: afstanden reiken hoogstens 32 KiB terug.
const WSIZE: usize = 32 << 10;

/// De leesbuffer van de bitlezer.
const INBUF: usize = 8 << 10;

/// De langste code die deflate kent.
const MAXBITS: usize = 15;

/// Het aantal literal/length-symbolen (286 geldig, 288 in de vaste tabel).
const MAXLCODES: usize = 288;

/// Het aantal afstandssymbolen (30 geldig, 32 in de vaste tabel).
const MAXDCODES: usize = 32;

/// Basislengtes van symbool 257..285.
const LEN_BASE: [u16; 29] = [
    3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 15, 17, 19, 23, 27, 31, 35, 43, 51, 59, 67, 83, 99, 115, 131,
    163, 195, 227, 258,
];

/// Extra bits van symbool 257..285.
const LEN_EXTRA: [u8; 29] = [
    0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 0,
];

/// Basisafstanden van symbool 0..29.
const DIST_BASE: [u16; 30] = [
    1, 2, 3, 4, 5, 7, 9, 13, 17, 25, 33, 49, 65, 97, 129, 193, 257, 385, 513, 769, 1025, 1537,
    2049, 3073, 4097, 6145, 8193, 12289, 16385, 24577,
];

/// Extra bits van afstandssymbool 0..29.
const DIST_EXTRA: [u8; 30] = [
    0, 0, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10, 11, 11, 12, 12, 13,
    13,
];

/// De volgorde van de codelengte-codes in een dynamisch blok.
const CL_ORDER: [usize; 19] = [
    16, 17, 18, 0, 8, 7, 9, 6, 10, 5, 11, 4, 12, 3, 13, 2, 14, 1, 15,
];

/// Een kapotte stroom als `io::Error`; de tekst zegt welke regel geschonden is.
fn corrupt(why: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, why)
}

/// Leest bits LSB-eerst uit een bytebron, zoals deflate ze legt.
pub(crate) struct BitReader<R> {
    src: R,
    buf: Vec<u8>,
    pos: usize,
    len: usize,
    bits: u64,
    nbits: u32,
}

impl<R: Read> BitReader<R> {
    /// Een lezer over `src`.
    pub(crate) fn new(src: R) -> Self {
        Self {
            src,
            buf: vec![0u8; INBUF],
            pos: 0,
            len: 0,
            bits: 0,
            nbits: 0,
        }
    }

    /// De volgende byte van de bron, of `None` aan het einde.
    fn next_byte(&mut self) -> io::Result<Option<u8>> {
        if self.pos >= self.len {
            self.len = loop {
                match self.src.read(&mut self.buf) {
                    Ok(n) => break n,
                    Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                    Err(e) => return Err(e),
                }
            };
            self.pos = 0;
            if self.len == 0 {
                return Ok(None);
            }
        }
        let b = self.buf.get(self.pos).copied();
        self.pos += 1;
        Ok(b)
    }

    /// `n` bits (hoogstens 32), LSB-eerst.
    pub(crate) fn bits(&mut self, n: u32) -> io::Result<u32> {
        while self.nbits < n {
            let b = self
                .next_byte()?
                .ok_or_else(|| corrupt("unexpected end of stream"))?;
            self.bits |= u64::from(b) << self.nbits;
            self.nbits += 8;
        }
        let v = self.bits & ((1u64 << n) - 1);
        self.bits >>= n;
        self.nbits -= n;
        // Past: n <= 32, dus v < 2^32.
        Ok(u32::try_from(v).unwrap_or(0))
    }

    /// Gooit de bits tot de volgende bytegrens weg.
    pub(crate) fn align(&mut self) {
        let drop = self.nbits % 8;
        self.bits >>= drop;
        self.nbits -= drop;
    }

    /// Of de bron op is (na [`BitReader::align`]).
    pub(crate) fn at_end(&mut self) -> io::Result<bool> {
        if self.nbits > 0 {
            return Ok(false);
        }
        match self.next_byte()? {
            None => Ok(true),
            Some(b) => {
                self.bits = u64::from(b);
                self.nbits = 8;
                Ok(false)
            }
        }
    }
}

/// Een canonieke Huffman-code: aantal codes per lengte en de symbolen op volgorde.
struct Huffman {
    count: [u16; MAXBITS + 1],
    symbol: Vec<u16>,
}

impl Huffman {
    /// Bouwt de code uit `lengths`; een overvolle set is een kapotte stroom.
    ///
    /// Een onvolledige set mag (puff.c doet het ook): een blok met één
    /// afstandscode is geldig, en een code die niet bestaat faalt bij het
    /// decoderen.
    fn new(lengths: &[u8]) -> io::Result<Self> {
        let mut count = [0u16; MAXBITS + 1];
        for &l in lengths {
            if let Some(c) = count.get_mut(usize::from(l)) {
                *c += 1;
            }
        }
        let mut left: i32 = 1;
        for &c in count.iter().skip(1) {
            left = left * 2 - i32::from(c);
            if left < 0 {
                return Err(corrupt("over-subscribed huffman code"));
            }
        }
        let mut offs = [0u16; MAXBITS + 1];
        for len in 1..MAXBITS {
            let (o, c) = (offs.get(len).copied(), count.get(len).copied());
            if let (Some(o), Some(c), Some(next)) = (o, c, offs.get_mut(len + 1)) {
                *next = o + c;
            }
        }
        let mut symbol = vec![0u16; lengths.len()];
        for (sym, &l) in lengths.iter().enumerate() {
            if l == 0 {
                continue;
            }
            let Some(o) = offs.get_mut(usize::from(l)) else {
                continue;
            };
            if let Some(slot) = symbol.get_mut(usize::from(*o)) {
                *slot = u16::try_from(sym).unwrap_or(0);
            }
            *o += 1;
        }
        Ok(Self { count, symbol })
    }

    /// Decodeert één symbool, bit voor bit (puff.c, `decode`).
    fn decode<R: Read>(&self, br: &mut BitReader<R>) -> io::Result<u16> {
        let (mut code, mut first, mut index) = (0i32, 0i32, 0i32);
        for &count in self.count.iter().skip(1) {
            code |= i32::try_from(br.bits(1)?).unwrap_or(0);
            let count = i32::from(count);
            if code - count < first {
                let at = usize::try_from(index + (code - first)).unwrap_or(usize::MAX);
                return self
                    .symbol
                    .get(at)
                    .copied()
                    .ok_or_else(|| corrupt("huffman symbol out of range"));
            }
            index += count;
            first += count;
            first <<= 1;
            code <<= 1;
        }
        Err(corrupt("invalid huffman code"))
    }
}

/// Waar de decoder staat.
enum State {
    /// Vóór de kop van het volgende blok.
    Header,
    /// In een ongecomprimeerd blok met nog zoveel bytes.
    Stored(u16),
    /// In een blok met codes.
    Codes,
    /// Het laatste blok is klaar.
    Done,
}

/// Een deflate-decoder over een [`BitReader`].
pub(crate) struct Inflate {
    state: State,
    last: bool,
    window: Vec<u8>,
    wpos: usize,
    /// Hoeveel bytes er ooit uitkwamen (verzadigd); een afstand verder terug is kapot.
    total: usize,
    copy_len: usize,
    copy_dist: usize,
    lit: Huffman,
    dist: Huffman,
}

impl Inflate {
    /// Een decoder aan het begin van een stroom.
    pub(crate) fn new() -> Self {
        Self {
            state: State::Header,
            last: false,
            window: vec![0u8; WSIZE],
            wpos: 0,
            total: 0,
            copy_len: 0,
            copy_dist: 0,
            lit: Huffman {
                count: [0; MAXBITS + 1],
                symbol: Vec::new(),
            },
            dist: Huffman {
                count: [0; MAXBITS + 1],
                symbol: Vec::new(),
            },
        }
    }

    /// Of het laatste blok gelezen is.
    pub(crate) fn is_done(&self) -> bool {
        matches!(self.state, State::Done) && self.copy_len == 0
    }

    /// Zet `b` in het venster en in `out`.
    fn emit(&mut self, b: u8, out: &mut [u8], n: &mut usize) {
        if let Some(w) = self.window.get_mut(self.wpos) {
            *w = b;
        }
        self.wpos = (self.wpos + 1) % WSIZE;
        self.total = self.total.saturating_add(1);
        if let Some(o) = out.get_mut(*n) {
            *o = b;
        }
        *n += 1;
    }

    /// Decodeert tot `out` vol is of de stroom klaar is; geeft het aantal bytes.
    pub(crate) fn read<R: Read>(
        &mut self,
        br: &mut BitReader<R>,
        out: &mut [u8],
    ) -> io::Result<usize> {
        let mut n = 0;
        while n < out.len() {
            if self.copy_len > 0 {
                let at = (self.wpos + WSIZE - self.copy_dist) % WSIZE;
                let b = self.window.get(at).copied().unwrap_or(0);
                self.emit(b, out, &mut n);
                self.copy_len -= 1;
                continue;
            }
            match self.state {
                State::Done => break,
                State::Header => self.header(br)?,
                State::Stored(0) => self.state = State::Header,
                State::Stored(left) => {
                    let b = u8::try_from(br.bits(8)?).unwrap_or(0);
                    self.emit(b, out, &mut n);
                    self.state = State::Stored(left - 1);
                }
                State::Codes => self.symbol(br, out, &mut n)?,
            }
        }
        Ok(n)
    }

    /// Leest één symbool van een codeblok: een byte, een kopie of het einde.
    fn symbol<R: Read>(
        &mut self,
        br: &mut BitReader<R>,
        out: &mut [u8],
        n: &mut usize,
    ) -> io::Result<()> {
        let sym = self.lit.decode(br)?;
        if sym < 256 {
            self.emit(u8::try_from(sym).unwrap_or(0), out, n);
            return Ok(());
        }
        if sym == 256 {
            self.state = State::Header;
            return Ok(());
        }
        let i = usize::from(sym - 257);
        let (Some(&base), Some(&extra)) = (LEN_BASE.get(i), LEN_EXTRA.get(i)) else {
            return Err(corrupt("invalid length symbol"));
        };
        let len = usize::from(base) + usize::try_from(br.bits(u32::from(extra))?).unwrap_or(0);
        let d = usize::from(self.dist.decode(br)?);
        let (Some(&base), Some(&extra)) = (DIST_BASE.get(d), DIST_EXTRA.get(d)) else {
            return Err(corrupt("invalid distance symbol"));
        };
        let dist = usize::from(base) + usize::try_from(br.bits(u32::from(extra))?).unwrap_or(0);
        if dist > self.total {
            return Err(corrupt("distance too far back"));
        }
        self.copy_len = len;
        self.copy_dist = dist;
        Ok(())
    }

    /// Leest de kop van het volgende blok, of zet `Done` na het laatste.
    fn header<R: Read>(&mut self, br: &mut BitReader<R>) -> io::Result<()> {
        if self.last {
            self.state = State::Done;
            return Ok(());
        }
        self.last = br.bits(1)? == 1;
        match br.bits(2)? {
            0 => {
                br.align();
                let len = br.bits(16)?;
                let nlen = br.bits(16)?;
                if len != !nlen & 0xffff {
                    return Err(corrupt("stored block length does not match its complement"));
                }
                self.state = State::Stored(u16::try_from(len).unwrap_or(0));
            }
            1 => {
                self.fixed()?;
                self.state = State::Codes;
            }
            2 => {
                self.dynamic(br)?;
                self.state = State::Codes;
            }
            _ => return Err(corrupt("invalid block type")),
        }
        Ok(())
    }

    /// De vaste tabellen van blok-type 1.
    fn fixed(&mut self) -> io::Result<()> {
        let mut lengths = [0u8; MAXLCODES];
        for (sym, l) in lengths.iter_mut().enumerate() {
            *l = match sym {
                0..=143 => 8,
                144..=255 => 9,
                256..=279 => 7,
                _ => 8,
            };
        }
        self.lit = Huffman::new(&lengths)?;
        self.dist = Huffman::new(&[5u8; MAXDCODES])?;
        Ok(())
    }

    /// De tabellen van een dynamisch blok (type 2).
    fn dynamic<R: Read>(&mut self, br: &mut BitReader<R>) -> io::Result<()> {
        let nlen = usize::try_from(br.bits(5)?).unwrap_or(0) + 257;
        let ndist = usize::try_from(br.bits(5)?).unwrap_or(0) + 1;
        let ncode = usize::try_from(br.bits(4)?).unwrap_or(0) + 4;
        if nlen > 286 || ndist > 30 {
            return Err(corrupt("too many length or distance codes"));
        }
        let mut cl = [0u8; 19];
        for &i in CL_ORDER.iter().take(ncode) {
            if let Some(l) = cl.get_mut(i) {
                *l = u8::try_from(br.bits(3)?).unwrap_or(0);
            }
        }
        let clcode = Huffman::new(&cl)?;
        let mut lengths = [0u8; MAXLCODES + MAXDCODES];
        let total = nlen + ndist;
        let mut i = 0;
        while i < total {
            let sym = clcode.decode(br)?;
            if sym < 16 {
                if let Some(l) = lengths.get_mut(i) {
                    *l = u8::try_from(sym).unwrap_or(0);
                }
                i += 1;
                continue;
            }
            let (value, repeat) = match sym {
                16 => {
                    let prev = i
                        .checked_sub(1)
                        .and_then(|p| lengths.get(p).copied())
                        .ok_or_else(|| corrupt("repeat with no previous length"))?;
                    (prev, 3 + br.bits(2)?)
                }
                17 => (0, 3 + br.bits(3)?),
                _ => (0, 11 + br.bits(7)?),
            };
            let repeat = usize::try_from(repeat).unwrap_or(usize::MAX);
            if i + repeat > total {
                return Err(corrupt("too many code lengths"));
            }
            for l in lengths.iter_mut().skip(i).take(repeat) {
                *l = value;
            }
            i += repeat;
        }
        if lengths.get(256).copied().unwrap_or(0) == 0 {
            return Err(corrupt("no end-of-block code"));
        }
        self.lit = Huffman::new(lengths.get(..nlen).unwrap_or(&[]))?;
        self.dist = Huffman::new(lengths.get(nlen..total).unwrap_or(&[]))?;
        Ok(())
    }
}

/// CRC-32 (IEEE, gereflecteerd), zoals gzip en zip hem eisen.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Crc32(u32);

impl Crc32 {
    /// Een lege som.
    pub(crate) fn new() -> Self {
        Self(!0)
    }

    /// Telt `data` mee. Bitsgewijs: geen tabel van 1 KiB voor een pad dat
    /// hooguit een paar keer per start loopt.
    pub(crate) fn update(&mut self, data: &[u8]) {
        let mut c = self.0;
        for &b in data {
            c ^= u32::from(b);
            for _ in 0..8 {
                c = if c & 1 == 1 {
                    (c >> 1) ^ 0xedb8_8320
                } else {
                    c >> 1
                };
            }
        }
        self.0 = c;
    }

    /// De som.
    pub(crate) fn sum(self) -> u32 {
        !self.0
    }
}

/// Een gzip-stroom (één of meer leden achter elkaar) als `Read`.
pub(crate) struct Gzip<R> {
    br: BitReader<R>,
    inflate: Inflate,
    crc: Crc32,
    size: u32,
    /// Of er een lid open is; `false` na de laatste trailer.
    open: bool,
}

impl<R: Read> Gzip<R> {
    /// Leest de kop van het eerste lid.
    pub(crate) fn new(src: R) -> io::Result<Self> {
        let mut g = Self {
            br: BitReader::new(src),
            inflate: Inflate::new(),
            crc: Crc32::new(),
            size: 0,
            open: false,
        };
        g.member_header()?;
        Ok(g)
    }

    /// Eén byte van de kop.
    fn byte(&mut self) -> io::Result<u8> {
        Ok(u8::try_from(self.br.bits(8)?).unwrap_or(0))
    }

    /// Leest een nul-afgesloten veld (naam, commentaar) over.
    fn skip_cstr(&mut self) -> io::Result<()> {
        while self.byte()? != 0 {}
        Ok(())
    }

    /// De kop van een lid (RFC 1952 §2.3).
    fn member_header(&mut self) -> io::Result<()> {
        if self.byte()? != 0x1f || self.byte()? != 0x8b {
            return Err(corrupt("gzip: invalid header"));
        }
        if self.byte()? != 8 {
            return Err(corrupt("gzip: unknown compression method"));
        }
        let flags = self.byte()?;
        // MTIME (4), XFL, OS.
        for _ in 0..6 {
            self.byte()?;
        }
        if flags & 0x04 != 0 {
            let xlen = self.br.bits(16)?;
            for _ in 0..xlen {
                self.byte()?;
            }
        }
        if flags & 0x08 != 0 {
            self.skip_cstr()?;
        }
        if flags & 0x10 != 0 {
            self.skip_cstr()?;
        }
        if flags & 0x02 != 0 {
            self.br.bits(16)?;
        }
        self.inflate = Inflate::new();
        self.crc = Crc32::new();
        self.size = 0;
        self.open = true;
        Ok(())
    }

    /// Toetst de trailer van het lid en opent een volgend lid als dat er is.
    fn member_end(&mut self) -> io::Result<()> {
        self.br.align();
        let crc = self.br.bits(32)?;
        let size = self.br.bits(32)?;
        if crc != self.crc.sum() {
            return Err(corrupt("gzip: checksum mismatch"));
        }
        if size != self.size {
            return Err(corrupt("gzip: length mismatch"));
        }
        self.open = false;
        if !self.br.at_end()? {
            self.member_header()?;
        }
        Ok(())
    }
}

impl<R: Read> Read for Gzip<R> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        while self.open {
            let n = self.inflate.read(&mut self.br, out)?;
            if n > 0 {
                let got = out.get(..n).unwrap_or(&[]);
                self.crc.update(got);
                // ISIZE is de lengte modulo 2^32 (RFC 1952).
                self.size = self.size.wrapping_add(u32::try_from(n).unwrap_or(0));
                return Ok(n);
            }
            if out.is_empty() {
                return Ok(0);
            }
            if !self.inflate.is_done() {
                return Err(corrupt("inflate stalled before the end of the stream"));
            }
            self.member_end()?;
        }
        Ok(0)
    }
}

/// Een rauwe deflate-stroom (zip, methode 8) als `Read`.
pub(crate) struct Deflate<R> {
    br: BitReader<R>,
    inflate: Inflate,
}

impl<R: Read> Deflate<R> {
    /// Een decoder over `src`.
    pub(crate) fn new(src: R) -> Self {
        Self {
            br: BitReader::new(src),
            inflate: Inflate::new(),
        }
    }
}

impl<R: Read> Read for Deflate<R> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        self.inflate.read(&mut self.br, out)
    }
}
