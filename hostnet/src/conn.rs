//! Een blokkerende std-socket als leanhttp-verbinding.
//!
//! Bezit de socket en zijn twee deadlines. leanhttp zet een termijn als
//! "vanaf nu plus zoveel" (Go's `SetReadDeadline`); hier wordt dat een
//! deadline, en vóór elke lees of schrijf gaat de resterende tijd als
//! socket-termijn naar de kernel. Zonder termijn (`None`) geldt de standaard
//! van de verbinding per lees of schrijf, als stiltetermijn: een grote
//! download mag lang duren zolang hij blijft stromen, en een vergeten
//! termijn houdt nooit een thread voor altijd vast.

use std::io::{self, Read, Write};
use std::net::{Shutdown, TcpStream};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use leanhttp::{AsyncRead, AsyncWrite, Close, IoError};

/// De kortste termijn die een socket aanneemt: std weigert nul.
const MIN_TIMEOUT: Duration = Duration::from_millis(1);

/// Wat een socket moet kunnen: lezen, schrijven, termijnen en sluiten.
pub trait Socket: Read + Write {
    /// Zet de leestermijn van de kernel.
    fn set_read(&self, t: Option<Duration>) -> io::Result<()>;
    /// Zet de schrijftermijn van de kernel.
    fn set_write(&self, t: Option<Duration>) -> io::Result<()>;
    /// Sluit beide richtingen.
    fn shut(&self) -> io::Result<()>;
}

impl Socket for TcpStream {
    fn set_read(&self, t: Option<Duration>) -> io::Result<()> {
        self.set_read_timeout(t)
    }
    fn set_write(&self, t: Option<Duration>) -> io::Result<()> {
        self.set_write_timeout(t)
    }
    fn shut(&self) -> io::Result<()> {
        self.shutdown(Shutdown::Both)
    }
}

#[cfg(unix)]
impl Socket for std::os::unix::net::UnixStream {
    fn set_read(&self, t: Option<Duration>) -> io::Result<()> {
        self.set_read_timeout(t)
    }
    fn set_write(&self, t: Option<Duration>) -> io::Result<()> {
        self.set_write_timeout(t)
    }
    fn shut(&self) -> io::Result<()> {
        self.shutdown(Shutdown::Both)
    }
}

/// Een blokkerende socket als leanhttp-verbinding.
pub struct StdConn<S> {
    sock: Option<S>,
    /// De termijn als leanhttp er geen zet; `None` is echt geen termijn.
    default: Option<Duration>,
    read_at: Option<Instant>,
    write_at: Option<Instant>,
}

impl<S: Socket> StdConn<S> {
    /// Neemt `sock` over; `default` geldt zolang leanhttp geen termijn zet.
    pub fn new(sock: S, default: Option<Duration>) -> Self {
        Self {
            sock: Some(sock),
            default,
            read_at: None,
            write_at: None,
        }
    }

    /// De socket, zolang de verbinding open is.
    pub fn get_ref(&self) -> Option<&S> {
        self.sock.as_ref()
    }
}

/// De resterende tijd tot `at`, of `Err(TimedOut)` als hij voorbij is;
/// zonder deadline de stiltetermijn `idle`.
fn remaining(at: Option<Instant>, idle: Option<Duration>) -> Result<Option<Duration>, IoError> {
    match at {
        None => Ok(idle),
        Some(at) => {
            let left = at.saturating_duration_since(Instant::now());
            if left.is_zero() {
                Err(IoError::TimedOut)
            } else {
                Ok(Some(left.max(MIN_TIMEOUT)))
            }
        }
    }
}

/// Een std-fout als verbindingsfout.
fn io_error(e: &io::Error) -> IoError {
    match e.kind() {
        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut => IoError::TimedOut,
        io::ErrorKind::ConnectionReset | io::ErrorKind::ConnectionAborted => IoError::Reset,
        io::ErrorKind::BrokenPipe | io::ErrorKind::NotConnected => IoError::Closed,
        _ => IoError::Other,
    }
}

/// Een termijn van leanhttp als deadline; `None` is geen deadline.
fn deadline(t: Option<Duration>) -> Option<Instant> {
    t.map(|d| Instant::now() + d)
}

impl<S: Socket> AsyncRead for StdConn<S> {
    fn poll_read(&mut self, _cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize, IoError>> {
        let Some(sock) = self.sock.as_mut() else {
            return Poll::Ready(Err(IoError::Closed));
        };
        let left = match remaining(self.read_at, self.default) {
            Ok(l) => l,
            Err(e) => return Poll::Ready(Err(e)),
        };
        if sock.set_read(left).is_err() {
            return Poll::Ready(Err(IoError::Other));
        }
        loop {
            match sock.read(buf) {
                Ok(n) => return Poll::Ready(Ok(n)),
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Poll::Ready(Err(io_error(&e))),
            }
        }
    }

    fn set_read_timeout(&mut self, timeout: Option<Duration>) -> Result<(), IoError> {
        self.read_at = deadline(timeout);
        Ok(())
    }
}

impl<S: Socket> AsyncWrite for StdConn<S> {
    fn poll_write(&mut self, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, IoError>> {
        let Some(sock) = self.sock.as_mut() else {
            return Poll::Ready(Err(IoError::Closed));
        };
        let left = match remaining(self.write_at, self.default) {
            Ok(l) => l,
            Err(e) => return Poll::Ready(Err(e)),
        };
        if sock.set_write(left).is_err() {
            return Poll::Ready(Err(IoError::Other));
        }
        loop {
            match sock.write(buf) {
                Ok(n) => return Poll::Ready(Ok(n)),
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Poll::Ready(Err(io_error(&e))),
            }
        }
    }

    fn poll_flush(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), IoError>> {
        match self.sock.as_mut() {
            Some(s) => Poll::Ready(s.flush().map_err(|e| io_error(&e))),
            None => Poll::Ready(Err(IoError::Closed)),
        }
    }

    fn set_write_timeout(&mut self, timeout: Option<Duration>) -> Result<(), IoError> {
        self.write_at = deadline(timeout);
        Ok(())
    }
}

impl<S: Socket> Close for StdConn<S> {
    fn poll_close(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), IoError>> {
        // Twee keer sluiten is één keer sluiten; Drop van de socket sluit ook.
        match self.sock.take() {
            Some(s) => {
                // Een peer die al weg is, is geen fout bij het sluiten.
                let _ = s.shut();
                Poll::Ready(Ok(()))
            }
            None => Poll::Ready(Ok(())),
        }
    }
}
