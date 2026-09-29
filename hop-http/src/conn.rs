//! Een `applib::appnet::TcpStream` als verbinding voor leanhttp.
//!
//! Bezit de stroom en zijn twee termijnen. De stroom zelf heeft alleen
//! async methodes (`read`, `write`); leanhttp vraagt poll-methodes. De brug
//! is eerlijk goedkoop: elke poll maakt de future van één `read` of `write`
//! en pollt hem één keer. Bij `WouldBlock` zet die future de waker van deze
//! taak op het handvat in de stack en geeft `Pending`; wegvallen kost niets,
//! want hij hield niets vast buiten die registratie.
//!
//! De termijnen van de server (KAM: verzoekkop, body, schrijven) lopen op
//! het timerwiel van de executor van de app-core: een verbinding die zwijgt
//! wordt zo na zijn termijn gewekt en gesloten, en houdt geen taak uit de
//! vaste pool vast.

use alloc::boxed::Box;
use core::future::Future;
use core::pin::{Pin, pin};
use core::task::{Context, Poll};
use core::time::Duration;

use applib::appnet::{NetError, StackError, TcpStream};
use applib::rt::Exec;
use leanhttp::{AsyncRead, AsyncWrite, Close, IoError};

/// Een wekker op het timerwiel: de future van `Exec::until`.
type Alarm = Pin<Box<dyn Future<Output = ()>>>;

/// Eén richting: de deadline en de wekker die erbij hoort.
#[derive(Default)]
struct Deadline {
    at: Option<u64>,
    alarm: Option<Alarm>,
}

impl Deadline {
    /// Zet de termijn op `d` vanaf `now`; `None` wist hem.
    fn set(&mut self, exec: &'static Exec, d: Option<Duration>) {
        self.at = d.map(|d| {
            let ns = u64::try_from(d.as_nanos()).unwrap_or(u64::MAX);
            exec.now().saturating_add(ns)
        });
        self.alarm = self.at.map(|at| -> Alarm { Box::pin(exec.until(at)) });
    }

    /// `true` als de termijn verstreken is; anders staat de wekker op de waker van `cx`.
    fn expired(&mut self, exec: &'static Exec, cx: &mut Context<'_>) -> bool {
        let Some(at) = self.at else {
            return false;
        };
        if exec.now() >= at {
            return true;
        }
        if let Some(a) = self.alarm.as_mut() {
            // De wekker registreert de waker van deze taak in het wiel; is hij
            // net afgelopen, dan is de termijn ook verstreken.
            return a.as_mut().poll(cx).is_ready();
        }
        false
    }
}

/// Een TCP-verbinding van de app als leanhttp-verbinding.
pub struct TcpConn {
    stream: Option<TcpStream>,
    exec: &'static Exec,
    read: Deadline,
    write: Deadline,
    cap: Option<Duration>,
}

impl TcpConn {
    /// Neemt `stream` over; de termijnen lopen op `exec`.
    pub fn new(stream: TcpStream, exec: &'static Exec) -> Self {
        Self {
            stream: Some(stream),
            exec,
            read: Deadline::default(),
            write: Deadline::default(),
            cap: None,
        }
    }

    /// Kapt elke leestermijn die de server zet af op `cap`.
    ///
    /// Voor een poort die zijn verbindingen één voor één bedient: de
    /// keep-alive-stilte van leanhttp (60 s) zou daar elke volgende client
    /// een minuut laten wachten. Een termijn `None` (de server leest dan
    /// niet) blijft `None`.
    #[must_use]
    pub fn with_read_cap(mut self, cap: Duration) -> Self {
        self.cap = Some(cap);
        self
    }
}

/// Een netfout als fout van de verbinding.
fn io_error(e: NetError) -> IoError {
    match e {
        NetError::Timeout => IoError::TimedOut,
        NetError::Stack(StackError::Reset) => IoError::Reset,
        NetError::Stack(StackError::Closed | StackError::TcpClosed | StackError::StackClosed) => {
            IoError::Closed
        }
        NetError::Stack(StackError::DeadlineExceeded) => IoError::TimedOut,
        _ => IoError::Other,
    }
}

impl AsyncRead for TcpConn {
    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize, IoError>> {
        let Some(s) = self.stream.as_mut() else {
            return Poll::Ready(Err(IoError::Closed));
        };
        if let Poll::Ready(r) = pin!(s.read(buf)).poll(cx) {
            return Poll::Ready(r.map_err(io_error));
        }
        if self.read.expired(self.exec, cx) {
            return Poll::Ready(Err(IoError::TimedOut));
        }
        Poll::Pending
    }

    fn set_read_timeout(&mut self, timeout: Option<Duration>) -> Result<(), IoError> {
        let timeout = match (timeout, self.cap) {
            (Some(t), Some(c)) => Some(t.min(c)),
            (t, _) => t,
        };
        self.read.set(self.exec, timeout);
        Ok(())
    }
}

impl AsyncWrite for TcpConn {
    fn poll_write(&mut self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, IoError>> {
        let Some(s) = self.stream.as_mut() else {
            return Poll::Ready(Err(IoError::Closed));
        };
        if let Poll::Ready(r) = pin!(s.write(buf)).poll(cx) {
            return Poll::Ready(r.map_err(io_error));
        }
        if self.write.expired(self.exec, cx) {
            return Poll::Ready(Err(IoError::TimedOut));
        }
        Poll::Pending
    }

    fn set_write_timeout(&mut self, timeout: Option<Duration>) -> Result<(), IoError> {
        self.write.set(self.exec, timeout);
        Ok(())
    }
}

impl Close for TcpConn {
    fn poll_close(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), IoError>> {
        // Synchroon: FIN na de gebufferde data, de pomp stuurt hem. Twee keer
        // sluiten is één keer sluiten.
        match self.stream.take() {
            Some(s) => Poll::Ready(s.close().map_err(io_error)),
            None => Poll::Ready(Ok(())),
        }
    }
}
