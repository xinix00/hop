//! `hop-gui`: het Hop-dashboard op een host, `hop-gui -listen :3000`.
//!
//! Een vaste pool van [`WORKERS`] threads, elk met een kloon van de
//! listener en één verbinding tegelijk: leanhttp over std-sockets (hop's
//! `hostnet`), per verzoek [`hop_gui::serve`]. Dezelfde vorm als de
//! HTTP-servers van `agentd` (handboek §2: een verbinding is een taak uit
//! een vaste pool). Er is geen gedeelde staat: de bestanden zijn
//! ingebakken en alleen-lezen, dus elke thread bezit wat hij aanraakt.
//!
//! Het dashboard praat vanuit de browser direct met een agent van Hop; die
//! agent moet vanaf de browser bereikbaar zijn en stuurt de CORS-koppen
//! (zie de README).
//!
//! Marker op stderr: `HOPGUI_UP listen=<adres> files=<n> bytes=<n>`.

#![forbid(unsafe_code)]

use std::net::{TcpListener, TcpStream};
use std::process::ExitCode;
use std::task::{Context, Poll};
use std::time::Duration;

use hostnet::{StdConn, block_on};
use leanhttp::{AsyncRead, AsyncWrite, Close, Exchange, IoError};

/// Verbindingsthreads. Een browser opent er tot zes naar één host; acht
/// dekt een paar lezers tegelijk.
const WORKERS: usize = 8;

/// De langste stilte op een verbinding: de keep-alive van leanhttp (60 s)
/// zou een thread uit de pool een minuut vasthouden voor een browser die
/// niets meer vraagt.
const READ_CAP: Duration = Duration::from_secs(5);

/// Het adres zonder `-listen`.
const DEFAULT_LISTEN: &str = ":3000";

const USAGE: &str = "usage: hop-gui [-listen [host]:port]   (default :3000)";

/// Een std-socket met een plafond op elke leestermijn.
struct Capped(StdConn<TcpStream>);

impl AsyncRead for Capped {
    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize, IoError>> {
        self.0.poll_read(cx, buf)
    }

    fn set_read_timeout(&mut self, t: Option<Duration>) -> Result<(), IoError> {
        let t = Some(t.map_or(READ_CAP, |t| t.min(READ_CAP)));
        self.0.set_read_timeout(t)
    }
}

impl AsyncWrite for Capped {
    fn poll_write(&mut self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, IoError>> {
        self.0.poll_write(cx, buf)
    }

    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), IoError>> {
        self.0.poll_flush(cx)
    }

    fn set_write_timeout(&mut self, t: Option<Duration>) -> Result<(), IoError> {
        self.0.set_write_timeout(t)
    }
}

impl Close for Capped {
    fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), IoError>> {
        self.0.poll_close(cx)
    }
}

/// Het luisteradres uit de argumenten: `-listen X`, `--listen X`,
/// `-listen=X`; `:3000` is elke interface (zoals Go).
fn listen_addr(args: &[String]) -> Result<String, String> {
    let mut addr = String::from(DEFAULT_LISTEN);
    let mut it = args.iter();
    while let Some(a) = it.next() {
        let flag = a.trim_start_matches('-');
        if flag == "listen" {
            addr = it.next().ok_or("-listen needs an address")?.clone();
        } else if let Some(v) = flag.strip_prefix("listen=") {
            addr = String::from(v);
        } else if flag == "h" || flag == "help" {
            return Err(String::new());
        } else {
            return Err(format!("unknown argument {a:?}"));
        }
    }
    Ok(if addr.starts_with(':') {
        format!("0.0.0.0{addr}")
    } else {
        addr
    })
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let addr = match listen_addr(&args) {
        Ok(a) => a,
        Err(why) => {
            if !why.is_empty() {
                eprintln!("hop-gui: {why}");
            }
            eprintln!("{USAGE}");
            return ExitCode::from(2);
        }
    };
    let listener = match TcpListener::bind(&addr) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("hop-gui: cannot listen on {addr}: {e} HOPGUI_LISTEN_FAIL");
            return ExitCode::FAILURE;
        }
    };
    let bound = listener
        .local_addr()
        .map_or_else(|_| addr.clone(), |a| a.to_string());
    let mut threads = Vec::new();
    for i in 0..WORKERS {
        let l = match listener.try_clone() {
            Ok(l) => l,
            Err(e) => {
                eprintln!("hop-gui: listener clone {i}: {e} HOPGUI_LISTEN_FAIL");
                return ExitCode::FAILURE;
            }
        };
        match std::thread::Builder::new()
            .name(format!("http-{i}"))
            .spawn(move || worker(&l))
        {
            Ok(t) => threads.push(t),
            Err(e) => {
                eprintln!("hop-gui: worker {i}: {e} HOPGUI_LISTEN_FAIL");
                return ExitCode::FAILURE;
            }
        }
    }
    eprintln!(
        "hop-gui: serving the Hop dashboard on http://{bound}/ with {WORKERS} workers HOPGUI_UP listen={bound} files={} bytes={}",
        hop_gui::FILES.len(),
        hop_gui::total_bytes()
    );
    for t in threads {
        let _ = t.join();
    }
    ExitCode::SUCCESS
}

/// Eén verbindingsthread: accepteren, leanhttp, [`hop_gui::serve`], opnieuw.
fn worker(l: &TcpListener) {
    loop {
        let stream = match l.accept() {
            Ok((s, _)) => s,
            Err(e) => {
                eprintln!("hop-gui: accept: {e}");
                std::thread::sleep(Duration::from_millis(100));
                continue;
            }
        };
        let _ = stream.set_nodelay(true);
        let conn = Capped(StdConn::new(stream, Some(READ_CAP)));
        // Een verbinding die eindigt met een termijn of een reset is een
        // browser die wegging; geen logregel waard.
        let _ = block_on(leanhttp::serve(
            conn,
            async |ex: &mut Exchange<'_, Capped>| hop_gui::serve(ex).await,
        ));
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use std::io::{Read, Write};

    fn args(a: &[&str]) -> Vec<String> {
        a.iter().map(|s| String::from(*s)).collect()
    }

    #[test]
    fn the_listen_flag_like_go() {
        assert_eq!(listen_addr(&args(&[])).unwrap(), "0.0.0.0:3000");
        assert_eq!(
            listen_addr(&args(&["-listen", ":8081"])).unwrap(),
            "0.0.0.0:8081"
        );
        assert_eq!(
            listen_addr(&args(&["--listen=127.0.0.1:9"])).unwrap(),
            "127.0.0.1:9"
        );
        assert!(listen_addr(&args(&["-listen"])).is_err());
        assert!(listen_addr(&args(&["-port", "1"])).is_err());
    }

    #[test]
    fn a_real_socket_gets_the_index() {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap();
        std::thread::spawn(move || worker(&l));
        let mut s = TcpStream::connect(addr).unwrap();
        s.write_all(b"GET / HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n")
            .unwrap();
        let mut got = Vec::new();
        s.read_to_end(&mut got).unwrap();
        let text = String::from_utf8_lossy(&got);
        assert!(text.starts_with("HTTP/1.1 200"), "{text}");
        assert!(text.contains("text/html"), "{text}");
        let index = hop_gui::find("/").unwrap();
        assert!(got.ends_with(index.body));
    }
}
