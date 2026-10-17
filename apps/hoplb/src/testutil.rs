//! Gereedschap voor de tests: de meetregel van een benchmark, een
//! `block_on` zonder wekker, en een verbinding in het geheugen.

use alloc::rc::Rc;
use alloc::string::String;
use alloc::vec::Vec;
use core::cell::RefCell;
use core::future::Future;
use core::pin::pin;
use core::task::{Context, Poll, Waker};
use std::time::Instant;

use core::time::Duration;
use leanhttp::{AsyncRead, AsyncWrite, Close, IoError};

/// Draait `f` `n` keer, drukt de meetregel af en geeft ns per keer.
///
/// De regel heeft de vorm `bench <naam>: <ns> ns/op (<n> ops)`, zodat de
/// gate hem kan greppen. De getallen komen uit een test-build (zonder
/// optimalisatie, tenzij `cargo test --release`); de lat in de test is
/// daarom ruim, de meetregel is het getal.
pub(crate) fn bench(name: &str, n: usize, mut f: impl FnMut(usize)) -> f64 {
    for i in 0..n / 100 {
        f(i);
    }
    let start = Instant::now();
    for i in 0..n {
        f(i);
    }
    let ns = start.elapsed().as_nanos() as f64 / n as f64;
    std::println!("\nbench {name}: {ns:.1} ns/op ({n} ops)");
    ns
}

/// Hetzelfde als [`bench`]; de naam zegt dat het een schaalreeks is.
pub(crate) fn ns_per_op(name: &str, n: usize, f: impl FnMut(usize)) -> f64 {
    bench(name, n, f)
}

/// Pollt `fut` tot hij klaar is. De verbindingen hieronder zijn nooit
/// `Pending`, dus dat is één ronde.
pub(crate) fn block_on<F: Future>(fut: F) -> F::Output {
    let mut fut = pin!(fut);
    let mut cx = Context::from_waker(Waker::noop());
    loop {
        if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
            return v;
        }
    }
}

/// Wat er op een [`Pipe`] gebeurde, in volgorde.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Op {
    /// Een lees die `n` bytes gaf.
    Read(usize),
    /// Een schrijf van deze bytes.
    Write(Vec<u8>),
    /// Een flush.
    Flush,
    /// Sluiten.
    Close,
}

/// De staat achter een [`Pipe`].
#[derive(Default)]
pub(crate) struct Inner {
    /// De happen die de andere kant stuurt; elke lees geeft hoogstens één hap.
    pub(crate) input: Vec<Vec<u8>>,
    pos: usize,
    /// Wat erheen geschreven is.
    pub(crate) output: Vec<u8>,
    /// Alles in volgorde.
    pub(crate) ops: Vec<Op>,
    /// Gesloten?
    pub(crate) closed: bool,
    /// Faalt de volgende lees als de happen op zijn (in plaats van EOF)?
    pub(crate) reset_at_end: bool,
    /// Blijft de andere kant na zijn happen aanwezig? Dan verloopt een lees
    /// met termijn (`TimedOut`) in plaats van EOF te geven: een client die
    /// op zijn antwoord wacht.
    pub(crate) linger: bool,
    /// Of er een leestermijn gezet is (de sondering van een kopie).
    pub(crate) timed: bool,
}

/// Een verbinding in het geheugen: leest uit een vast script van happen,
/// onthoudt alles wat erheen ging. Een kloon is een tweede handvat op
/// dezelfde verbinding, zodat de test hem na afloop kan lezen.
#[derive(Clone, Default)]
pub(crate) struct Pipe(pub(crate) Rc<RefCell<Inner>>);

impl Pipe {
    /// Een pijp die `chunks` laat lezen en dan EOF geeft.
    pub(crate) fn new(chunks: &[&[u8]]) -> Self {
        Self(Rc::new(RefCell::new(Inner {
            input: chunks.iter().map(|c| c.to_vec()).collect(),
            ..Inner::default()
        })))
    }

    /// Wat erheen geschreven is, als tekst.
    pub(crate) fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.borrow().output).into_owned()
    }

    /// Alles wat er gebeurde.
    pub(crate) fn ops(&self) -> Vec<Op> {
        self.0.borrow().ops.clone()
    }

    /// Gesloten?
    pub(crate) fn is_closed(&self) -> bool {
        self.0.borrow().closed
    }

    /// Een client die na zijn happen blijft wachten op zijn antwoord: een
    /// lees met termijn verloopt dan, in plaats van EOF.
    pub(crate) fn linger(&self) {
        self.0.borrow_mut().linger = true;
    }
}

impl AsyncRead for Pipe {
    fn poll_read(&mut self, _cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize, IoError>> {
        let mut p = self.0.borrow_mut();
        if p.closed {
            return Poll::Ready(Err(IoError::Closed));
        }
        let pos = p.pos;
        let Some(chunk) = p.input.first() else {
            if p.reset_at_end {
                return Poll::Ready(Err(IoError::Reset));
            }
            if p.linger && p.timed {
                return Poll::Ready(Err(IoError::TimedOut));
            }
            p.ops.push(Op::Read(0));
            return Poll::Ready(Ok(0));
        };
        let rest = &chunk[pos..];
        let n = rest.len().min(buf.len());
        buf[..n].copy_from_slice(&rest[..n]);
        let done = pos + n == chunk.len();
        p.pos += n;
        if done {
            p.input.remove(0);
            p.pos = 0;
        }
        p.ops.push(Op::Read(n));
        Poll::Ready(Ok(n))
    }

    fn set_read_timeout(&mut self, t: Option<Duration>) -> Result<(), IoError> {
        self.0.borrow_mut().timed = t.is_some();
        Ok(())
    }
}

impl AsyncWrite for Pipe {
    fn poll_write(&mut self, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, IoError>> {
        let mut p = self.0.borrow_mut();
        if p.closed {
            return Poll::Ready(Err(IoError::Closed));
        }
        p.output.extend_from_slice(buf);
        p.ops.push(Op::Write(buf.to_vec()));
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), IoError>> {
        self.0.borrow_mut().ops.push(Op::Flush);
        Poll::Ready(Ok(()))
    }
}

impl Close for Pipe {
    fn poll_close(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), IoError>> {
        let mut p = self.0.borrow_mut();
        p.closed = true;
        p.ops.push(Op::Close);
        Poll::Ready(Ok(()))
    }
}
