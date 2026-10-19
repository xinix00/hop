//! Het pad van een ingress-regel: een reguliere expressie, geen voorvoegsel.
//!
//! Bezit het compileren van zo'n pad naar een klein programma en het zoeken
//! ermee (Go's `regexp.MatchString`: ergens in het pad, tenzij `^` of `$` het
//! vastpinnen). Cloudflare leest het pad met Go's RE2; dit is de deelverzameling
//! die een ingress-pad in de praktijk gebruikt, in lineaire tijd (een
//! Pike-machine: alle draden tegelijk, dus geen terugkrabbelen dat een
//! bezoeker met een lang pad kan opblazen).
//!
//! Wel: letters, `.`, klassen (`[a-z]`, `[^/]`, `\d \w \s` en hun
//! tegendelen), groepen (`(...)`, `(?:...)`), `|`, `* + ?`, `{n}`, `{n,}`,
//! `{n,m}` (ook luie varianten: voor ja of nee maakt dat niets uit), `^`,
//! `$`, `\A`, `\z` en de vlag `(?i)` vooraan. Niet: terugverwijzingen,
//! vooruitkijken, `\b`, `(?m)`, benoemde groepen. Wat niet kan, weigert de
//! compiler met een reden; dan houdt de tunnel zijn oude tabel en hoort de
//! edge waarom (zie [`crate::ingress`]).
//!
//! Byte-georiënteerd: `.` is één byte en geen teken. Een pad is ASCII (wat
//! erbuiten valt, is procent-gecodeerd), dus dat verschil ziet een ingress-
//! pad niet.

#![forbid(unsafe_code)]

use alloc::string::String;
use alloc::vec::Vec;
use core::fmt;

/// Het grootste programma. Een ingress-pad is een handvol tekens; 512
/// instructies laat `{n,m}` ruimte en houdt de werkruimte van [`Regex::is_match`]
/// op de stack klein (vier bytes per instructie per lijst).
pub(crate) const MAX_PROG: usize = 512;

/// De grootste herhaling in `{n,m}`. RE2 staat 1000 toe; een pad heeft
/// minder nodig en het programma groeit met elke herhaling.
const MAX_REPEAT: u32 = 64;

/// De diepste nesting van groepen.
const MAX_NEST: u32 = 32;

/// Waarom een patroon niet compileert, met de byte-offset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Error {
    /// Een haakje dat niet sluit of niet opent.
    Paren {
        /// De byte-offset.
        at: usize,
    },
    /// Een klasse die niet sluit, of een omgekeerd bereik.
    Class {
        /// De byte-offset.
        at: usize,
    },
    /// Een herhaling zonder iets om te herhalen, of met een kapotte `{}`.
    Repeat {
        /// De byte-offset.
        at: usize,
    },
    /// Een constructie die deze compiler niet kent.
    Unsupported {
        /// De byte-offset.
        at: usize,
    },
    /// Het programma wordt groter dan [`MAX_PROG`].
    TooLarge,
    /// De heap weigerde.
    OutOfMemory,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Paren { at } => write!(f, "unbalanced parenthesis at byte {at}"),
            Self::Class { at } => write!(f, "broken character class at byte {at}"),
            Self::Repeat { at } => write!(f, "broken repetition at byte {at}"),
            Self::Unsupported { at } => {
                write!(f, "unsupported regular expression syntax at byte {at}")
            }
            Self::TooLarge => write!(f, "regular expression larger than {MAX_PROG} steps"),
            Self::OutOfMemory => f.write_str("regular expression: out of memory"),
        }
    }
}

/// Het resultaat van deze module.
type Result<T = (), E = Error> = core::result::Result<T, E>;

/// Een verzameling bytes, als 256 bits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct Set([u64; 4]);

impl Set {
    fn add(&mut self, b: u8) {
        if let Some(w) = self.0.get_mut(usize::from(b >> 6)) {
            *w |= 1 << (b & 63);
        }
    }

    fn add_range(&mut self, lo: u8, hi: u8) {
        for b in lo..=hi {
            self.add(b);
        }
    }

    fn has(&self, b: u8) -> bool {
        self.0
            .get(usize::from(b >> 6))
            .is_some_and(|w| w & (1 << (b & 63)) != 0)
    }

    fn invert(&mut self) {
        for w in &mut self.0 {
            *w = !*w;
        }
    }

    fn union(&mut self, o: &Set) {
        for (a, b) in self.0.iter_mut().zip(o.0.iter()) {
            *a |= b;
        }
    }

    /// Voegt de andere kast van elke ASCII-letter toe.
    fn fold(&mut self) {
        for b in b'a'..=b'z' {
            if self.has(b) || self.has(b.to_ascii_uppercase()) {
                self.add(b);
                self.add(b.to_ascii_uppercase());
            }
        }
    }
}

/// Eén knoop van de ontleding; kinderen zijn indexen in de arena.
#[derive(Debug, Clone)]
enum Node {
    /// Niets (een lege tak).
    Empty,
    /// Eén byte uit een verzameling.
    Set(Set),
    /// Het begin van de tekst.
    Begin,
    /// Het einde van de tekst.
    End,
    /// Na elkaar.
    Concat(Vec<usize>),
    /// Een van de takken.
    Alt(Vec<usize>),
    /// Herhaald: minstens `min`, hooguit `max` (`None` is onbegrensd).
    Repeat {
        node: usize,
        min: u32,
        max: Option<u32>,
    },
}

/// Eén instructie van het programma.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Inst {
    /// Eén byte uit de verzameling, dan verder.
    Set(Set),
    /// Beide wegen.
    Split(u16, u16),
    /// Spring.
    Jmp(u16),
    /// Alleen op positie 0.
    Begin,
    /// Alleen aan het einde.
    End,
    /// Gevonden.
    Match,
}

/// Duwt `v` op `vec`, of weigert als de heap dat doet.
fn push<T>(vec: &mut Vec<T>, v: T) -> Result {
    vec.try_reserve(1).map_err(|_| Error::OutOfMemory)?;
    vec.push(v);
    Ok(())
}

/// De ontleder: recursive descent over de bytes van het patroon.
struct Parser<'a> {
    /// Het patroon.
    s: &'a [u8],
    /// De volgende byte.
    i: usize,
    /// De knopen.
    nodes: Vec<Node>,
    /// `(?i)`: letters in beide kasten.
    fold: bool,
    /// Hoe diep de groepen nu zijn.
    depth: u32,
}

impl Parser<'_> {
    /// Zet een knoop in de arena en geeft zijn index.
    fn node(&mut self, n: Node) -> Result<usize> {
        push(&mut self.nodes, n)?;
        Ok(self.nodes.len() - 1)
    }

    fn peek(&self) -> Option<u8> {
        self.s.get(self.i).copied()
    }

    /// `alt := concat ('|' concat)*`
    fn alt(&mut self) -> Result<usize> {
        let first = self.concat()?;
        if self.peek() != Some(b'|') {
            return Ok(first);
        }
        let mut arms = Vec::new();
        push(&mut arms, first)?;
        while self.peek() == Some(b'|') {
            self.i += 1;
            let next = self.concat()?;
            push(&mut arms, next)?;
        }
        self.node(Node::Alt(arms))
    }

    /// `concat := repeat*`, tot `|`, `)` of het einde.
    fn concat(&mut self) -> Result<usize> {
        let mut items = Vec::new();
        while let Some(c) = self.peek() {
            if c == b'|' || c == b')' {
                break;
            }
            let atom = self.atom()?;
            let item = self.quantifiers(atom)?;
            push(&mut items, item)?;
        }
        match items.len() {
            0 => self.node(Node::Empty),
            1 => Ok(items.first().copied().unwrap_or(0)),
            _ => self.node(Node::Concat(items)),
        }
    }

    /// De herhalingen achter een atoom.
    fn quantifiers(&mut self, mut atom: usize) -> Result<usize> {
        loop {
            let at = self.i;
            let (min, max) = match self.peek() {
                Some(b'*') => (0, None),
                Some(b'+') => (1, None),
                Some(b'?') => (0, Some(1)),
                // `braces` leest een geldige herhaling tot na de `}`.
                Some(b'{') => match self.braces()? {
                    Some(r) => r,
                    // Een `{` die geen herhaling is, is in RE2 een letter.
                    None => return Ok(atom),
                },
                _ => return Ok(atom),
            };
            if self.i == at {
                self.i += 1;
            }
            if matches!(self.nodes.get(atom), Some(Node::Begin | Node::End)) {
                return Err(Error::Repeat { at });
            }
            // Lui of gulzig: voor een ja of nee hetzelfde.
            if self.peek() == Some(b'?') {
                self.i += 1;
            }
            atom = self.node(Node::Repeat {
                node: atom,
                min,
                max,
            })?;
        }
    }

    /// `{n}`, `{n,}` of `{n,m}`; `None` als het geen herhaling is (dan blijft
    /// de lezer staan). Een geldige herhaling is gelezen tot na de `}`.
    fn braces(&mut self) -> Result<Option<(u32, Option<u32>)>> {
        let start = self.i;
        let mut j = self.i + 1;
        let num = |j: &mut usize, s: &[u8]| -> Option<u32> {
            let from = *j;
            let mut v: u32 = 0;
            while let Some(&c @ b'0'..=b'9') = s.get(*j) {
                v = v.saturating_mul(10).saturating_add(u32::from(c - b'0'));
                *j += 1;
            }
            (*j > from).then_some(v)
        };
        let Some(min) = num(&mut j, self.s) else {
            return Ok(None);
        };
        let max = match self.s.get(j) {
            Some(b'}') => Some(min),
            Some(b',') => {
                j += 1;
                let m = num(&mut j, self.s);
                if self.s.get(j) != Some(&b'}') {
                    return Ok(None);
                }
                m
            }
            _ => return Ok(None),
        };
        if min > MAX_REPEAT || max.is_some_and(|m| m > MAX_REPEAT || m < min) {
            return Err(Error::Repeat { at: start });
        }
        self.i = j + 1;
        Ok(Some((min, max)))
    }

    /// Eén atoom.
    fn atom(&mut self) -> Result<usize> {
        let at = self.i;
        let Some(c) = self.peek() else {
            return self.node(Node::Empty);
        };
        self.i += 1;
        match c {
            b'(' => self.group(at),
            b'[' => {
                let set = self.class(at)?;
                self.node(Node::Set(set))
            }
            b'.' => {
                let mut set = Set::default();
                set.add(b'\n');
                set.invert();
                self.node(Node::Set(set))
            }
            b'^' => self.node(Node::Begin),
            b'$' => self.node(Node::End),
            b'\\' => self.escape(at),
            b'*' | b'+' | b'?' => Err(Error::Repeat { at }),
            b')' => Err(Error::Paren { at }),
            c => {
                let set = self.literal(c);
                self.node(Node::Set(set))
            }
        }
    }

    /// Een groep, na de `(`.
    fn group(&mut self, at: usize) -> Result<usize> {
        if self.peek() == Some(b'?') {
            match self.s.get(self.i + 1..self.i + 3) {
                Some(b"i)") if at == 0 => {
                    self.fold = true;
                    self.i += 3;
                    return self.node(Node::Empty);
                }
                Some([b':', _]) => self.i += 2,
                _ => return Err(Error::Unsupported { at }),
            }
        }
        self.depth += 1;
        if self.depth > MAX_NEST {
            return Err(Error::Unsupported { at });
        }
        let inner = self.alt()?;
        if self.peek() != Some(b')') {
            return Err(Error::Paren { at });
        }
        self.i += 1;
        self.depth -= 1;
        Ok(inner)
    }

    /// Een letter, met de andere kast onder `(?i)`.
    fn literal(&self, c: u8) -> Set {
        let mut set = Set::default();
        set.add(c);
        if self.fold {
            set.fold();
        }
        set
    }

    /// De klasse van `\d`, `\w`, `\s` en hun tegendelen.
    fn perl(c: u8) -> Option<Set> {
        let mut set = Set::default();
        match c.to_ascii_lowercase() {
            b'd' => set.add_range(b'0', b'9'),
            b'w' => {
                set.add_range(b'0', b'9');
                set.add_range(b'a', b'z');
                set.add_range(b'A', b'Z');
                set.add(b'_');
            }
            b's' => {
                for b in [b' ', b'\t', b'\n', b'\r', b'\x0c', b'\x0b'] {
                    set.add(b);
                }
            }
            _ => return None,
        }
        if c.is_ascii_uppercase() {
            set.invert();
        }
        Some(set)
    }

    /// Een escape als letter binnen of buiten een klasse.
    fn escaped_byte(c: u8) -> Option<u8> {
        match c {
            b'n' => Some(b'\n'),
            b't' => Some(b'\t'),
            b'r' => Some(b'\r'),
            b'f' => Some(b'\x0c'),
            b'v' => Some(b'\x0b'),
            c if c.is_ascii_punctuation() => Some(c),
            _ => None,
        }
    }

    /// Een escape, na de `\`.
    fn escape(&mut self, at: usize) -> Result<usize> {
        let c = self.peek().ok_or(Error::Unsupported { at })?;
        self.i += 1;
        if let Some(set) = Self::perl(c) {
            return self.node(Node::Set(set));
        }
        match c {
            b'A' => self.node(Node::Begin),
            b'z' => self.node(Node::End),
            c => match Self::escaped_byte(c) {
                Some(b) => {
                    let set = self.literal(b);
                    self.node(Node::Set(set))
                }
                None => Err(Error::Unsupported { at }),
            },
        }
    }

    /// Een klasse, na de `[`.
    fn class(&mut self, at: usize) -> Result<Set> {
        let mut set = Set::default();
        let negate = self.peek() == Some(b'^');
        if negate {
            self.i += 1;
        }
        let mut first = true;
        loop {
            let c = self.peek().ok_or(Error::Class { at })?;
            self.i += 1;
            if c == b']' && !first {
                break;
            }
            first = false;
            let lo = if c == b'\\' {
                let e = self.peek().ok_or(Error::Class { at })?;
                self.i += 1;
                if let Some(p) = Self::perl(e) {
                    set.union(&p);
                    continue;
                }
                Self::escaped_byte(e).ok_or(Error::Unsupported { at: self.i - 2 })?
            } else if c == b'[' && self.peek() == Some(b':') {
                // POSIX-klassen ([:alpha:]) kent deze compiler niet.
                return Err(Error::Unsupported { at: self.i - 1 });
            } else {
                c
            };
            let range =
                self.peek() == Some(b'-') && self.s.get(self.i + 1).is_some_and(|&n| n != b']');
            if !range {
                set.add(lo);
                continue;
            }
            self.i += 1;
            let mut hi = self.peek().ok_or(Error::Class { at })?;
            self.i += 1;
            if hi == b'\\' {
                let e = self.peek().ok_or(Error::Class { at })?;
                self.i += 1;
                hi = Self::escaped_byte(e).ok_or(Error::Class { at })?;
            }
            if hi < lo {
                return Err(Error::Class { at });
            }
            set.add_range(lo, hi);
        }
        if self.fold {
            set.fold();
        }
        if negate {
            set.invert();
        }
        Ok(set)
    }
}

/// De codegenerator: van knopen naar instructies.
struct Codegen<'a> {
    /// De knopen van de ontleder.
    nodes: &'a [Node],
    /// Het programma.
    prog: Vec<Inst>,
}

impl Codegen<'_> {
    /// Het adres van de volgende instructie.
    fn pc(&self) -> Result<u16> {
        u16::try_from(self.prog.len())
            .ok()
            .filter(|&n| usize::from(n) < MAX_PROG)
            .ok_or(Error::TooLarge)
    }

    /// Voegt een instructie toe en geeft zijn adres.
    fn emit(&mut self, i: Inst) -> Result<u16> {
        let pc = self.pc()?;
        push(&mut self.prog, i)?;
        Ok(pc)
    }

    /// Zet een instructie op `pc` na (een sprong die nu pas zijn doel kent).
    fn patch(&mut self, pc: u16, i: Inst) {
        if let Some(slot) = self.prog.get_mut(usize::from(pc)) {
            *slot = i;
        }
    }

    /// De code van knoop `n`.
    fn emit_node(&mut self, n: usize) -> Result {
        let Some(node) = self.nodes.get(n) else {
            return Err(Error::TooLarge);
        };
        match node {
            Node::Empty => Ok(()),
            Node::Set(s) => self.emit(Inst::Set(*s)).map(|_| ()),
            Node::Begin => self.emit(Inst::Begin).map(|_| ()),
            Node::End => self.emit(Inst::End).map(|_| ()),
            Node::Concat(items) => {
                for &i in items {
                    self.emit_node(i)?;
                }
                Ok(())
            }
            Node::Alt(arms) => self.alt(arms),
            &Node::Repeat { node, min, max } => self.repeat(node, min, max),
        }
    }

    /// `a|b|c`: een split per tak behalve de laatste, elke tak springt naar
    /// het einde.
    fn alt(&mut self, arms: &[usize]) -> Result {
        let mut jumps: [u16; 64] = [0; 64];
        let mut nj = 0usize;
        let last = arms.len().saturating_sub(1);
        for (k, &arm) in arms.iter().enumerate() {
            if k == last {
                self.emit_node(arm)?;
                break;
            }
            let split = self.emit(Inst::Split(0, 0))?;
            self.emit_node(arm)?;
            let j = self.emit(Inst::Jmp(0))?;
            *jumps.get_mut(nj).ok_or(Error::TooLarge)? = j;
            nj += 1;
            let next = self.pc()?;
            self.patch(split, Inst::Split(split + 1, next));
        }
        let end = self.pc()?;
        for &j in jumps.get(..nj).unwrap_or(&[]) {
            self.patch(j, Inst::Jmp(end));
        }
        Ok(())
    }

    /// `x{min,max}`: `min` kopieën, dan `x*` of `max - min` keer `x?`.
    fn repeat(&mut self, node: usize, min: u32, max: Option<u32>) -> Result {
        for _ in 0..min {
            self.emit_node(node)?;
        }
        match max {
            None => {
                let top = self.emit(Inst::Split(0, 0))?;
                self.emit_node(node)?;
                self.emit(Inst::Jmp(top))?;
                let end = self.pc()?;
                self.patch(top, Inst::Split(top + 1, end));
            }
            Some(max) => {
                let mut splits: [u16; MAX_REPEAT as usize] = [0; MAX_REPEAT as usize];
                let n = usize::try_from(max - min).unwrap_or(0);
                for slot in splits.iter_mut().take(n) {
                    *slot = self.emit(Inst::Split(0, 0))?;
                    self.emit_node(node)?;
                }
                let end = self.pc()?;
                for &s in splits.iter().take(n) {
                    self.patch(s, Inst::Split(s + 1, end));
                }
            }
        }
        Ok(())
    }
}

/// Een gecompileerd pad.
#[derive(Clone)]
pub(crate) struct Regex {
    /// Het programma.
    prog: Vec<Inst>,
    /// Het patroon zoals het binnenkwam, voor een logregel.
    src: String,
}

impl fmt::Debug for Regex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Regex({:?})", self.src)
    }
}

impl Regex {
    /// Compileert `pattern`.
    pub(crate) fn new(pattern: &str) -> Result<Self> {
        let mut p = Parser {
            s: pattern.as_bytes(),
            i: 0,
            nodes: Vec::new(),
            fold: false,
            depth: 0,
        };
        let root = p.alt()?;
        if p.i != p.s.len() {
            return Err(Error::Paren { at: p.i });
        }
        let mut g = Codegen {
            nodes: &p.nodes,
            prog: Vec::new(),
        };
        g.emit_node(root)?;
        g.emit(Inst::Match)?;
        let mut src = String::new();
        src.try_reserve_exact(pattern.len())
            .map_err(|_| Error::OutOfMemory)?;
        src.push_str(pattern);
        Ok(Self { prog: g.prog, src })
    }

    /// Het patroon.
    pub(crate) fn as_str(&self) -> &str {
        &self.src
    }

    /// Of het patroon ergens in `s` past. Lineair in `s` maal het programma,
    /// zonder heap: de draadlijsten staan op de stack.
    pub(crate) fn is_match(&self, s: &str) -> bool {
        let s = s.as_bytes();
        let mut cur = Threads::new();
        let mut next = Threads::new();
        for pos in 0..=s.len() {
            // Een nieuwe draad op elke positie: zoeken, niet vastpinnen.
            if self.add(&mut cur, 0, pos, s.len()) {
                return true;
            }
            let byte = s.get(pos).copied();
            next.clear();
            for k in 0..cur.len {
                let pc = cur.pcs.get(k).copied().unwrap_or(0);
                if let (Some(Inst::Set(set)), Some(b)) = (self.prog.get(usize::from(pc)), byte)
                    && set.has(b)
                    && self.add(&mut next, pc + 1, pos + 1, s.len())
                {
                    return true;
                }
            }
            core::mem::swap(&mut cur, &mut next);
            if byte.is_none() {
                break;
            }
        }
        false
    }

    /// Voegt de draad op `pc` toe met zijn epsilon-sluiting; `true` als die
    /// de `Match` bereikt.
    fn add(&self, list: &mut Threads, pc: u16, pos: usize, len: usize) -> bool {
        let mut stack = [0u16; MAX_PROG];
        let mut sp = 0usize;
        let push = |stack: &mut [u16; MAX_PROG], sp: &mut usize, pc: u16| {
            if let Some(slot) = stack.get_mut(*sp) {
                *slot = pc;
                *sp += 1;
            }
        };
        push(&mut stack, &mut sp, pc);
        while sp > 0 {
            sp -= 1;
            let pc = stack.get(sp).copied().unwrap_or(0);
            if !list.insert(pc) {
                continue;
            }
            match self.prog.get(usize::from(pc)) {
                Some(Inst::Match) => return true,
                Some(Inst::Jmp(t)) => push(&mut stack, &mut sp, *t),
                Some(Inst::Split(a, b)) => {
                    // Eerst b op de stapel, zodat a eerst bekeken wordt.
                    push(&mut stack, &mut sp, *b);
                    push(&mut stack, &mut sp, *a);
                }
                Some(Inst::Begin) if pos == 0 => push(&mut stack, &mut sp, pc + 1),
                Some(Inst::End) if pos == len => push(&mut stack, &mut sp, pc + 1),
                _ => {}
            }
        }
        false
    }
}

/// Een draadlijst van de Pike-machine: een ijle verzameling van adressen.
struct Threads {
    /// De adressen in volgorde van toevoegen.
    pcs: [u16; MAX_PROG],
    /// Hoeveel.
    len: usize,
    /// Of een adres al in de lijst staat.
    on: [bool; MAX_PROG],
}

impl Threads {
    fn new() -> Self {
        Self {
            pcs: [0; MAX_PROG],
            len: 0,
            on: [false; MAX_PROG],
        }
    }

    fn clear(&mut self) {
        for k in 0..self.len {
            if let Some(&pc) = self.pcs.get(k)
                && let Some(o) = self.on.get_mut(usize::from(pc))
            {
                *o = false;
            }
        }
        self.len = 0;
    }

    /// Voegt `pc` toe; `false` als hij er al stond.
    fn insert(&mut self, pc: u16) -> bool {
        let Some(o) = self.on.get_mut(usize::from(pc)) else {
            return false;
        };
        if *o {
            return false;
        }
        *o = true;
        if let Some(slot) = self.pcs.get_mut(self.len) {
            *slot = pc;
            self.len += 1;
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(p: &str, s: &str) -> bool {
        Regex::new(p).unwrap().is_match(s)
    }

    // De paden uit de Go-toetsen van internal/ingress.
    #[test]
    fn go_ingress_paths() {
        assert!(m("^/api", "/api/v1/devices"));
        assert!(!m("^/api", "/"));
        assert!(!m("^/api", "/x/api"));
        assert!(m(r"\.(jpg|png)$", "/camera/voordeur.jpg"));
        assert!(m(r"\.(jpg|png)$", "/logo.png"));
        assert!(!m(r"\.(jpg|png)$", "/jpg/index.html"));
        assert!(Regex::new("(ongeldig").is_err());
    }

    #[test]
    fn search_is_unanchored_like_match_string() {
        assert!(m("api", "/x/api/y"));
        assert!(m("", "/"));
        assert!(m("a*", "xyz"));
        assert!(m("^$", ""));
        assert!(!m("^$", "/"));
        assert!(m(r"\Ahealth\z", "health"));
    }

    #[test]
    fn classes_and_repeats() {
        assert!(m(r"^/v[0-9]+/", "/v12/x"));
        assert!(!m(r"^/v[0-9]+/", "/v/x"));
        assert!(m(r"^/[^/]+$", "/file"));
        assert!(!m(r"^/[^/]+$", "/dir/file"));
        assert!(m(r"^/\d{4}-\d{2}$", "/2026-09"));
        assert!(!m(r"^/\d{4}-\d{2}$", "/26-09"));
        assert!(m(r"^a{2,}$", "aaaa"));
        assert!(!m(r"^a{2,3}$", "aaaa"));
        assert!(m(r"^a{2,3}$", "aaa"));
        assert!(m(r"^(?:ab)+$", "ababab"));
        assert!(m(r"^\w+\s\S$", "ab_9 x"));
        assert!(m(r"^[a\-z]$", "-"));
        assert!(m(r"^[-a]$", "-"));
        assert!(m(r"^[\d.]+$", "1.2.3"));
        assert!(m(r"^x{$", "x{"), "een losse accolade is een letter");
        assert!(m(r"^a.*?b$", "axxb"));
    }

    #[test]
    fn case_folding_flag() {
        assert!(m(r"(?i)^/API$", "/api"));
        assert!(m(r"(?i)^/[a-c]$", "/B"));
        assert!(!m(r"^/API$", "/api"));
    }

    #[test]
    fn empty_loops_terminate() {
        assert!(m(r"^(a*)*$", "aaaa"));
        assert!(!m(r"^(a*)*$", "aaab"));
        assert!(m(r"^(|a)+$", "aa"));
    }

    // Een klassiek geval van exponentieel terugkrabbelen is hier lineair.
    #[test]
    fn no_catastrophic_backtracking() {
        let s: String = core::iter::repeat_n('a', 5000).collect();
        assert!(!m(r"^(a+)+b$", &s));
    }

    #[test]
    fn refusals() {
        for (p, e) in [
            ("a)", Error::Paren { at: 1 }),
            ("(a", Error::Paren { at: 0 }),
            ("[a", Error::Class { at: 0 }),
            ("[z-a]", Error::Class { at: 0 }),
            ("*a", Error::Repeat { at: 0 }),
            ("^*", Error::Repeat { at: 1 }),
            (r"\1", Error::Unsupported { at: 0 }),
            (r"\bx", Error::Unsupported { at: 0 }),
            ("(?=a)", Error::Unsupported { at: 0 }),
            ("(?P<n>a)", Error::Unsupported { at: 0 }),
            ("a(?i)b", Error::Unsupported { at: 1 }),
            ("a{3,1}", Error::Repeat { at: 1 }),
            ("a{100}", Error::Repeat { at: 1 }),
            ("[[:alpha:]]", Error::Unsupported { at: 1 }),
        ] {
            assert_eq!(Regex::new(p).unwrap_err(), e, "{p}");
        }
        let big: String = core::iter::repeat_n("(a{64})", 10).collect();
        assert_eq!(Regex::new(&big).unwrap_err(), Error::TooLarge);
    }
}
