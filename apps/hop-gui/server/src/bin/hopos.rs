//! `hop-gui-hopos`: het Hop-dashboard als app in een slot van HopOS.
//!
//! Hop plaatst deze ELF met een jobspec (`"driver":"hop"`, een artifact,
//! `"ports":{"http":80}`); de kern zet poort 80 van de uplink door naar het
//! slot (DNAT), en Hop geeft de poort mee in `ER_PORT_HTTP`. Eén
//! HTTP-server (leanhttp over `applib::tcp::TcpConn`) serveert de
//! ingebakken bestanden met [`hop_gui::serve`], dezelfde handler als de
//! host-binary.
//!
//! De vorm (handboek §2), die van `apps/welcome` in HopOS: één taak
//! accepteert en geeft elke verbinding als waarde aan een vrije werker uit
//! een vaste pool van [`WORKERS`]; een werker bezit zijn verbinding tot hij
//! sluit. Geen taak per verbinding.
//!
//! Marker: `HOPOS_HOPGUI_UP files=<n> bytes=<n> port=<n>` als de listener
//! staat. Per verzoek niets: een dashboard dat elke tien seconden ververst,
//! hoort de console niet te vullen.

#![cfg_attr(target_os = "none", no_std, no_main)]

extern crate alloc;

use alloc::vec::Vec;
use applib::appnet::{self, TcpListener, TcpStream};
use applib::rt::Exec;
use applib::tcp::TcpConn;
use applib::{App, EXEC, log};
use core::cell::Cell;
use core::time::Duration;
use leanhttp::Exchange;
use sync::Local;
use sync::spsc::{Channel, Receiver, Sender};

applib::main!(hop_gui_app);

/// Op de host bestaat dit image niet: daar is dit een lege binary, zodat
/// clippy de rest kan lezen.
#[cfg(not(target_os = "none"))]
fn main() {}

/// De poort zonder `ER_PORT_HTTP` (een image buiten Hop om).
const DEFAULT_PORT: u16 = 80;

/// De werkers: zoveel verbindingen tegelijk. Een browser opent er tot zes
/// naar één host bij de eerste lading (html, twee scripts, de stijlen, het
/// font); vier dekt dat, de rest wacht kort op de eerste die vrijkomt.
const WORKERS: usize = 4;

/// De langste stilte op een keep-alive-verbinding: met een vaste pool houdt
/// een stille browser anders een werker een minuut vast.
const READ_CAP: Duration = Duration::from_secs(5);

/// Hoe vaak de acceptor kijkt of er een werker vrij is, als ze alle vier
/// bezig zijn. Een koud pad, dus pollen is eenvoudiger dan een bel.
const BUSY_POLL: Duration = Duration::from_millis(5);

/// De rij naar elke werker: één verbinding tegelijk.
static QUEUES: Local<[Channel<TcpStream, 1>; WORKERS]> =
    Local::new([const { Channel::new() }; WORKERS]);

/// Welke werker een verbinding heeft. De acceptor zet de vlag bij de
/// overdracht, de werker wist hem als de verbinding dicht is; beide op de
/// executor van deze core, nooit over een `.await` geleend.
static BUSY: Local<[Cell<bool>; WORKERS]> = Local::new([const { Cell::new(false) }; WORKERS]);

#[expect(
    clippy::expect_used,
    reason = "de start van de bin: zonder netstack of poort is er niets te serveren, en een luide paniek met reden is het goede einde"
)]
async fn hop_gui_app(app: &'static App) {
    let exec: &'static Exec = EXEC.get();
    let port = port_of(app.env("ER_PORT_HTTP"));
    let net = appnet::up(app).expect("hop-gui: network stack");
    let listener = TcpListener::bind(port).expect("hop-gui: listen on ER_PORT_HTTP");
    let mut senders: Vec<Sender<'static, TcpStream, 1>> = Vec::new();
    senders
        .try_reserve_exact(WORKERS)
        .expect("hop-gui: worker table");
    for (i, q) in QUEUES.get().iter().enumerate() {
        let (tx, rx) = q.split().expect("hop-gui: queue split once");
        senders.push(tx);
        exec.spawn(worker(i, rx, exec))
            .expect("hop-gui: spawn worker");
    }
    let [a, b, c, d] = net.ip();
    log!(
        "hop-gui: serving the Hop dashboard on {a}.{b}.{c}.{d}:{port}, slot {}, {WORKERS} workers HOPOS_HOPGUI_UP files={} bytes={} port={port}",
        app.slot(),
        hop_gui::FILES.len(),
        hop_gui::total_bytes()
    );
    accept(listener, &mut senders, exec).await;
}

/// De poort uit `ER_PORT_HTTP`, of [`DEFAULT_PORT`] zonder of bij onzin
/// (luid: een jobspec die iets anders bedoelde, moet dat kunnen zien).
fn port_of(env: Option<&str>) -> u16 {
    match env.map(str::parse::<u16>) {
        None => DEFAULT_PORT,
        Some(Ok(p)) if p != 0 => p,
        Some(_) => {
            log!(
                "hop-gui: ER_PORT_HTTP={env:?} is not a port, using {DEFAULT_PORT} HOPOS_HOPGUI_PORT"
            );
            DEFAULT_PORT
        }
    }
}

/// De acceptor: elke verbinding naar de eerste vrije werker.
async fn accept(
    listener: TcpListener,
    senders: &mut [Sender<'static, TcpStream, 1>],
    exec: &'static Exec,
) {
    loop {
        let mut stream = match listener.accept().await {
            Ok(s) => s,
            Err(e) => {
                log!("hop-gui: accept: {e} HOPOS_HOPGUI_ACCEPT");
                exec.after(Duration::from_millis(100)).await;
                continue;
            }
        };
        while let Some(back) = hand_off(stream, senders) {
            stream = back;
            exec.after(BUSY_POLL).await;
        }
    }
}

/// Geeft `stream` aan een vrije werker; alle werkers bezig is `Some` terug.
fn hand_off(stream: TcpStream, senders: &mut [Sender<'static, TcpStream, 1>]) -> Option<TcpStream> {
    let busy = BUSY.get();
    let free = senders
        .iter_mut()
        .zip(busy.iter())
        .find(|(tx, b)| !b.get() && tx.free() > 0);
    match free {
        Some((tx, b)) => match tx.try_send(stream) {
            Ok(()) => {
                b.set(true);
                None
            }
            Err(sync::Full(back)) => Some(back),
        },
        None => Some(stream),
    }
}

/// Eén werker: wacht op een verbinding, bedient hem met leanhttp tot hij
/// sluit, en meldt zich weer vrij.
async fn worker(i: usize, mut rx: Receiver<'static, TcpStream, 1>, exec: &'static Exec) {
    loop {
        let stream = rx.recv().await;
        let conn = TcpConn::new(stream, exec).with_read_cap(READ_CAP);
        // Een verbinding die eindigt met een termijn of een reset is een
        // browser die wegging; dat is geen logregel waard.
        let _ = leanhttp::serve(conn, async |ex: &mut Exchange<'_, TcpConn>| {
            hop_gui::serve(ex).await
        })
        .await;
        if let Some(b) = BUSY.get().get(i) {
            b.set(false);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_port_comes_from_the_env_or_is_80() {
        assert_eq!(port_of(Some("8081")), 8081);
        assert_eq!(port_of(None), 80);
        assert_eq!(port_of(Some("0")), 80);
    }
}
