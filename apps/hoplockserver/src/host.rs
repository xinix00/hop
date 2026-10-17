//! De host-vorm (feature `std`): de bestanden-store en de server op std-sockets.
//!
//! Bezit twee soorten threads, elk met één eigenaar (handboek §1; de vorm
//! van `agentd/src/http.rs` in hop):
//!
//! - **de store-thread** bezit de [`FileStore`]. Hij leest berichten uit
//!   één rij, voert elke [`Op`] uit en stuurt de uitkomst terug. Omdat hij
//!   de enige is die de bestanden aanraakt, is elke compare-and-swap
//!   lineariseerbaar zonder slot: Go's `sync.Mutex` om de store is hier de
//!   vorm van de thread geworden.
//! - **een vaste pool verbindingsthreads** ([`WORKERS`]), elk met een kloon
//!   van de listener en één verbinding tegelijk, leanhttp over een
//!   [`StdConn`]. Een verzoek dat de store nodig heeft, gaat als bericht
//!   naar de store-thread; de werker wacht op zijn eigen antwoordkanaal.
//!
//! Een schrijf is crashbestendig: een tijdelijk bestand naast het echte,
//! `fsync`, een atomische rename en een `fsync` van de map (Go deed de rename
//! zonder de map te syncen). Een gedode server laat dus nooit een half
//! lease-bestand achter, en een geslaagde PUT staat na een stroomuitval nog
//! op de schijf. De vorm op de schijf is die van Go: één kaal bestand per
//! sleutel onder `-data`, zodat een bestaande Go-datamap zo blijft werken.
//!
//! Wat hier niet is: een nette stop op SIGTERM (std heeft geen signaal-API,
//! zie `agentd`). Een gedode server verliest niets: elke geslaagde schrijf
//! staat al op de schijf, een halve bestaat niet.

use std::fs::{self, File, OpenOptions};
use std::io::{self, ErrorKind, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::string::{String, ToString};
use std::sync::mpsc::{self, Receiver, Sender, SyncSender};
use std::task::{Context, Poll};
use std::time::Duration;
use std::vec::Vec;
use std::{eprintln, format};

use hostnet::{StdConn, block_on};
use leanhttp::{AsyncRead, AsyncWrite, Close, Exchange, IoError};

use crate::etag::Etag;
use crate::http::Unanswered;
use crate::key::Key;
use crate::server::{Done, Op, Server, run};
use crate::store::{Condition, Error, Object, Result, Store, check_delete, check_put};

/// Verbindingsthreads. Een lease wordt elke paar seconden per node gelezen
/// of vernieuwd, elk verzoek duurt milliseconden en Hop's client opent per
/// verzoek een verse verbinding: zestien tegelijk is ruim voor een cluster
/// van honderd nodes.
pub const WORKERS: usize = 16;

/// De langste stilte op een verbinding voordat de thread hem sluit: Go's
/// `ReadHeaderTimeout` (5 s). De keep-alive van leanhttp (60 s) zou anders
/// een thread uit de pool een minuut vasthouden.
pub const READ_CAP: Duration = Duration::from_secs(5);

/// Hoe lang een werker op de store-thread wacht. Een operatie is één
/// lees en hoogstens één schrijf van 1 MiB met twee fsyncs.
pub const STORE_TIMEOUT: Duration = Duration::from_secs(30);

/// Het voorvoegsel van een tijdelijk bestand, zoals Go's `CreateTemp(".write-*")`.
const TMP_PREFIX: &str = ".write-";

/// De store op de schijf: één bestand per sleutel onder de datamap.
#[derive(Debug)]
pub struct FileStore {
    dir: PathBuf,
    /// Het volgnummer van het volgende tijdelijke bestand.
    seq: u64,
}

impl FileStore {
    /// Opent de store op `dir`; de map wordt gemaakt als hij er niet is (Go: `store.New`).
    pub fn open(dir: impl Into<PathBuf>) -> io::Result<FileStore> {
        let dir = dir.into();
        fs::create_dir_all(&dir)?;
        Ok(FileStore { dir, seq: 0 })
    }

    /// De datamap.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Telt de sleutels: elk gewoon bestand in de boom, zonder de tijdelijke.
    pub fn count_keys(&self) -> io::Result<u64> {
        let mut keys = 0u64;
        let mut dirs = std::vec![self.dir.clone()];
        while let Some(d) = dirs.pop() {
            for entry in fs::read_dir(&d)? {
                let entry = entry?;
                let ty = entry.file_type()?;
                if ty.is_dir() {
                    dirs.push(entry.path());
                } else if ty.is_file()
                    && !entry.file_name().to_string_lossy().starts_with(TMP_PREFIX)
                {
                    keys = keys.saturating_add(1);
                }
            }
        }
        Ok(keys)
    }

    fn path(&self, key: &Key) -> PathBuf {
        self.dir.join(key.as_str())
    }

    /// Wat er nu onder `path` staat; `None` als het er niet is.
    fn read(path: &Path) -> io::Result<Option<Vec<u8>>> {
        match fs::read(path) {
            Ok(b) => Ok(Some(b)),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Schrijft `body` crashbestendig op `path`: tmp, fsync, rename, fsync van de map.
    fn write_atomic(&mut self, path: &Path, body: &[u8]) -> io::Result<()> {
        let dir = path.parent().unwrap_or(&self.dir).to_path_buf();
        let (tmp, mut f) = self.create_tmp(&dir)?;
        let written = f
            .write_all(body)
            .and_then(|()| f.sync_all())
            .and_then(|()| {
                drop(f);
                fs::rename(&tmp, path)
            });
        if let Err(e) = written {
            // Na een geslaagde rename is er niets meer; na een fout ruimen we op.
            let _ = fs::remove_file(&tmp);
            return Err(e);
        }
        sync_dir(&dir)
    }

    /// Een vers tijdelijk bestand in `dir`, 0600, dat nooit een bestaand overschrijft.
    fn create_tmp(&mut self, dir: &Path) -> io::Result<(PathBuf, File)> {
        loop {
            self.seq = self.seq.wrapping_add(1);
            let name = format!("{TMP_PREFIX}{}-{}", std::process::id(), self.seq);
            let tmp = dir.join(name);
            match OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&tmp)
            {
                Ok(f) => return Ok((tmp, f)),
                // Een sleutel met precies deze naam: het volgende nummer.
                Err(e) if e.kind() == ErrorKind::AlreadyExists => {}
                Err(e) => return Err(e),
            }
        }
    }
}

/// Synct map `dir`, zodat een rename of een verwijdering een crash overleeft.
fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

/// Een I/O-fout als store-fout, met stap en sleutel.
fn io_err(op: &'static str, key: &Key, e: &io::Error) -> Error {
    Error::Io {
        op,
        key: key.to_string(),
        why: e.to_string(),
    }
}

impl Store for FileStore {
    async fn get(&mut self, key: &Key) -> Result<Object> {
        match Self::read(&self.path(key)) {
            Ok(Some(body)) => Ok(Object {
                etag: Etag::of(&body),
                body,
            }),
            Ok(None) => Err(Error::NotFound),
            Err(e) => Err(io_err("read", key, &e)),
        }
    }

    async fn put_if(&mut self, key: &Key, body: Vec<u8>, cond: &Condition) -> Result<Etag> {
        let path = self.path(key);
        if let Some(parent) = path.parent().filter(|p| *p != self.dir) {
            fs::create_dir_all(parent).map_err(|e| io_err("mkdir", key, &e))?;
        }
        // Go: een leesfout telt als "bestaat niet" voor de voorwaarde, en
        // wordt daarna pas een fout.
        let existing = Self::read(&path);
        let current = existing
            .as_ref()
            .ok()
            .and_then(|b| b.as_deref())
            .map(Etag::of);
        check_put(current.as_ref(), cond)?;
        if let Err(e) = existing {
            return Err(io_err("read", key, &e));
        }
        self.write_atomic(&path, &body)
            .map_err(|e| io_err("write", key, &e))?;
        Ok(Etag::of(&body))
    }

    async fn delete(&mut self, key: &Key, cond: &Condition) -> Result {
        let path = self.path(key);
        let body = match Self::read(&path) {
            Ok(Some(b)) => b,
            Ok(None) => return Err(Error::NotFound),
            Err(e) => return Err(io_err("read", key, &e)),
        };
        check_delete(&Etag::of(&body), cond)?;
        fs::remove_file(&path).map_err(|e| io_err("remove", key, &e))?;
        // Naar beste kunnen: de verwijdering zelf is al gebeurd.
        if let Some(parent) = path.parent() {
            let _ = sync_dir(parent);
        }
        Ok(())
    }
}

/// Een bericht aan de store-thread: de operatie en waar het antwoord heen gaat.
struct Ask {
    op: Op,
    reply: SyncSender<Done>,
}

/// De store-thread: de enige eigenaar van de store.
fn store_loop<S: Store>(mut store: S, rx: &Receiver<Ask>) {
    // Stopt als de laatste werker weg is (alle zenders gesloten).
    while let Ok(ask) = rx.recv() {
        let done = block_on(run(&mut store, ask.op));
        // Een werker die niet meer wacht (zijn termijn verliep), mist niets
        // dat nog te redden is: de operatie is gedaan of niet.
        let _ = ask.reply.send(done);
    }
}

/// Wat elke verbindingsthread meekrijgt.
struct Worker {
    server: Server,
    store: Sender<Ask>,
}

impl Worker {
    /// Stuurt `op` naar de store-thread en wacht op de uitkomst.
    fn ask(&self, op: Op) -> std::result::Result<Done, Unanswered> {
        let (tx, rx) = mpsc::sync_channel(1);
        self.store
            .send(Ask { op, reply: tx })
            .map_err(|_| Unanswered::Gone)?;
        rx.recv_timeout(STORE_TIMEOUT).map_err(|_| Unanswered::Late)
    }
}

/// Een std-socket met een plafond op elke leestermijn ([`READ_CAP`]).
struct Capped(StdConn<TcpStream>);

impl AsyncRead for Capped {
    fn poll_read(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::result::Result<usize, IoError>> {
        self.0.poll_read(cx, buf)
    }

    fn set_read_timeout(&mut self, t: Option<Duration>) -> std::result::Result<(), IoError> {
        let t = Some(t.map_or(READ_CAP, |t| t.min(READ_CAP)));
        self.0.set_read_timeout(t)
    }
}

impl AsyncWrite for Capped {
    fn poll_write(
        &mut self,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::result::Result<usize, IoError>> {
        self.0.poll_write(cx, buf)
    }

    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<std::result::Result<(), IoError>> {
        self.0.poll_flush(cx)
    }

    fn set_write_timeout(&mut self, t: Option<Duration>) -> std::result::Result<(), IoError> {
        self.0.set_write_timeout(t)
    }
}

impl Close for Capped {
    fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<std::result::Result<(), IoError>> {
        self.0.poll_close(cx)
    }
}

/// Start de store-thread met `store` en [`WORKERS`] verbindingsthreads op `listener`.
///
/// Keert meteen terug; de threads lopen zolang het proces loopt.
pub fn start<S: Store + Send + 'static>(
    listener: &TcpListener,
    store: S,
    server: &Server,
) -> io::Result<()> {
    let (tx, rx) = mpsc::channel::<Ask>();
    std::thread::Builder::new()
        .name(String::from("store"))
        .spawn(move || store_loop(store, &rx))?;
    for i in 0..WORKERS {
        let l = listener.try_clone()?;
        let w = Worker {
            server: server.clone(),
            store: tx.clone(),
        };
        std::thread::Builder::new()
            .name(format!("http-{i}"))
            .spawn(move || worker(&l, &w))?;
    }
    Ok(())
}

fn worker(l: &TcpListener, w: &Worker) {
    loop {
        let stream = match l.accept() {
            Ok((s, _)) => s,
            Err(e) => {
                eprintln!("hoplockserver: accept: {e} HOPLOCK_ACCEPT");
                std::thread::sleep(Duration::from_millis(100));
                continue;
            }
        };
        let _ = stream.set_nodelay(true);
        let conn = Capped(StdConn::new(stream, Some(READ_CAP)));
        // Een verbinding die eindigt met een termijn of een reset is een
        // client die wegging; geen logregel waard.
        let _ = block_on(leanhttp::serve(
            conn,
            async |ex: &mut Exchange<'_, Capped>| serve_one(ex, w).await,
        ));
    }
}

/// Eén verzoek op de verbinding van deze werker.
async fn serve_one(ex: &mut Exchange<'_, Capped>, w: &Worker) -> leanhttp::Result {
    // De werker blokkeert zijn eigen thread op het antwoord van de store:
    // deze thread is van deze ene verbinding, wachten is hier het werk.
    crate::http::exchange(ex, &w.server, |op| core::future::ready(w.ask(op))).await
}

/// Bindt `listen` zoals Go's `net.Listen("tcp", listen)`: `:8090` is elke
/// interface (IPv6 en IPv4 samen, of alleen IPv4 als de host geen IPv6 heeft).
pub fn bind(listen: &str) -> io::Result<TcpListener> {
    if let Some(port) = listen.strip_prefix(':') {
        let port: u16 = port.parse().map_err(|_| {
            io::Error::new(ErrorKind::InvalidInput, format!("bad port in {listen:?}"))
        })?;
        return TcpListener::bind(SocketAddr::from(([0u16; 8], port)))
            .or_else(|_| TcpListener::bind(SocketAddr::from(([0u8; 4], port))));
    }
    let addrs: Vec<SocketAddr> = listen.to_socket_addrs()?.collect();
    TcpListener::bind(&addrs[..])
}

/// De vlaggen van de host-bin, met Go's standaarden.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Flags {
    /// `-listen`: het adres (standaard `:8090`).
    pub listen: String,
    /// `-data`: de datamap (standaard `./data`).
    pub data: String,
    /// `-api-key`: de verplichte `X-API-Key`; leeg is zonder authenticatie.
    pub api_key: String,
}

impl Default for Flags {
    fn default() -> Flags {
        Flags {
            listen: format!(":{}", crate::DEFAULT_PORT),
            data: String::from("./data"),
            api_key: String::new(),
        }
    }
}

/// Waarom de vlaggen niet te lezen waren.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FlagError {
    /// `-h` of `-help`: de gebruiksregel, en klaar.
    Help,
    /// Een vlag die niet bestaat.
    Unknown(String),
    /// Een vlag zonder waarde.
    Missing(String),
    /// Een los argument (Go's `flag` stopt daar; deze server kent er geen).
    Stray(String),
}

impl core::fmt::Display for FlagError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Help => f.write_str("help requested"),
            Self::Unknown(n) => write!(f, "flag provided but not defined: -{n}"),
            Self::Missing(n) => write!(f, "flag needs an argument: -{n}"),
            Self::Stray(a) => write!(f, "unexpected argument {a:?}"),
        }
    }
}

/// De gebruikstekst, zoals Go's `flag.PrintDefaults`.
pub const USAGE: &str = "Usage of hoplockserver:
  -api-key string
    \tRequired X-API-Key header (empty = no auth)
  -data string
    \tDirectory to store lease files in (default \"./data\")
  -listen string
    \tAddress to listen on (default \":8090\")";

/// Leest de vlaggen zoals Go's `flag`: `-x v`, `-x=v`, `--x v` en `--x=v`;
/// `--` sluit af. `HOPLOCK_API_KEY` geldt als `-api-key` leeg is (`env`).
pub fn parse_flags(
    args: &[String],
    env_key: Option<&str>,
) -> std::result::Result<Flags, FlagError> {
    let mut f = Flags::default();
    let mut it = args.iter();
    while let Some(arg) = it.next() {
        if arg == "--" {
            if let Some(a) = it.next() {
                return Err(FlagError::Stray(a.clone()));
            }
            break;
        }
        let Some(flag) = arg.strip_prefix("--").or_else(|| arg.strip_prefix('-')) else {
            return Err(FlagError::Stray(arg.clone()));
        };
        let (name, inline) = match flag.split_once('=') {
            Some((n, v)) => (n, Some(v.to_string())),
            None => (flag, None),
        };
        let slot = match name {
            "h" | "help" => return Err(FlagError::Help),
            "listen" => &mut f.listen,
            "data" => &mut f.data,
            "api-key" => &mut f.api_key,
            other => return Err(FlagError::Unknown(other.to_string())),
        };
        *slot = match inline {
            Some(v) => v,
            None => it
                .next()
                .cloned()
                .ok_or_else(|| FlagError::Missing(name.to_string()))?,
        };
    }
    if f.api_key.is_empty()
        && let Some(k) = env_key.filter(|k| !k.is_empty())
    {
        f.api_key = k.to_string();
    }
    Ok(f)
}

/// Go's `authLabel`: `off`, of `on (N chars)`.
#[must_use]
pub fn auth_label(server: &Server) -> String {
    if server.has_auth() {
        format!("on ({} chars)", server.key_len())
    } else {
        String::from("off")
    }
}

#[cfg(test)]
mod tests;
