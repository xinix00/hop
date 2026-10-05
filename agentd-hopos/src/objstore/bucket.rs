//! De S3-kant van de object-store van de apps: leans3 over leanhttp(s), met de bytes rechtstreeks tussen de verbinding en de kern.
//!
//! Een pull stroomt de GET-body in happen van [`super::CHUNK`] naar
//! `STORE_WRITE`, een push leest het bestand met `STORE_READ` in de PUT-body
//! (met de hash van de eerste pass in de handtekening). Hop houdt zo nooit
//! meer dan één hap vast, hoe groot het object ook is (Go: `GetObjectTo`,
//! `PutObjectFrom`).
//!
//! De brug: leans3 praat poll-gebaseerd ([`leans3::AsyncWrite`],
//! [`leans3::AsyncRead`]) en de kern async. [`KernSink`] en [`KernSource`]
//! houden daarom de call naar de kern als future vast tussen twee polls; de
//! verbinding met de kern (`&mut S`) zit IN die future en komt met het
//! antwoord terug. Eén eigenaar, geen slot.
//!
//! Het transport ([`Transport`]) is één verzoek over een verse verbinding
//! van de webdialer van een [`Client`] (opzoeken, TCP, en TLS met
//! `leantls::MOZILLA_ROOTS` als het endpoint `https` is), en het antwoord
//! houdt zijn body op die verbinding. Een `http`-endpoint mag (de nep-S3 van
//! de QEMU-toets heeft geen certificaat), luid bij de start: getekend, maar
//! onderweg leesbaar.
//!
//! Waarom niet `leans3http::Http` zoals de lease ([`crate::s3`]): die leest
//! elke antwoordbody eerst helemaal in het geheugen (tot `Limits::body`,
//! 32 MiB) voordat leans3 hem ziet. Een pull van een app-object is precies
//! de stroom die dat niet mag: een object groter dan de grens faalt dan, en
//! een kleiner zet zijn hele maat in de heap van Hop in plaats van één hap
//! van [`super::CHUNK`]. Tot leans3http een antwoord met de body op de
//! verbinding kan geven, blijft dit transport; het draagt ook push, list en
//! delete, zodat één bucket één transport en één beleid heeft.

use alloc::boxed::Box;
use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use core::future::Future;
use core::pin::{Pin, pin};
use core::task::{Context, Poll};
use core::time::Duration;

use leanhttp::Target;
use leanhttps::Link;
use leans3::{DeleteOptions, IoError, PutOptions};
use runner::{StoreTask, SysError, SystemApi};

use crate::client::Client;
use crate::fetch::{Connect, Resolve};

use super::{Bucket, CHUNK};

/// Hoe lang de kop van een antwoord mag uitblijven: een bucket die niet
/// antwoordt, houdt de app niet een kwartier vast (Go: de per-op-timeout
/// van `store.go` was 10 minuten voor een transfer; hier is dat de termijn
/// van de kern op de hele call, en dit de termijn op het zwijgen).
const HEADER_TIMEOUT: Duration = Duration::from_secs(30);

/// De instellingen van de bucket (de `HOPOS_S3_*` van de env).
#[derive(Clone, Debug)]
pub struct Config {
    /// Het endpoint, `https://...` (of `http://...`, luid).
    pub endpoint: String,
    /// De bucket.
    pub bucket: String,
    /// De regio; leeg wordt `us-east-1`.
    pub region: String,
    /// Het sleutel-id.
    pub key: String,
    /// Het geheim.
    pub secret: String,
    /// Path-style adressen (MinIO en de meeste niet-AWS-opslag).
    pub path_style: bool,
}

/// De bucket van de apps op S3.
pub struct S3Bucket<C, R> {
    client: leans3::Client,
    /// De verbindingen: de webdialer, de willekeur en de vertrouwde klok.
    http: Client<C, R>,
}

impl<C: Connect, R: Resolve> S3Bucket<C, R> {
    /// Een bucket; `wall` tekent (Unix-seconden), `trusted` dateert de keten.
    pub fn new(
        cfg: &Config,
        connect: C,
        resolver: R,
        rng: applib::rand::Rng,
        wall: fn() -> u64,
        trusted: fn() -> Option<u64>,
    ) -> Self {
        let region = if cfg.region.is_empty() {
            // Een lege regio tekent niet; MinIO en R2 nemen elke naam.
            String::from("us-east-1")
        } else {
            cfg.region.clone()
        };
        Self {
            client: leans3::Client {
                endpoint: cfg.endpoint.clone(),
                bucket: cfg.bucket.clone(),
                region,
                access_key_id: cfg.key.clone(),
                secret_access_key: cfg.secret.clone(),
                session_token: String::new(),
                path_style: cfg.path_style,
                now: Some(wall),
            },
            http: Client::new(connect, resolver, rng, trusted),
        }
    }

    /// Is het endpoint kale http?
    pub fn is_plain_http(&self) -> bool {
        self.client
            .endpoint
            .get(..7)
            .is_some_and(|s| s.eq_ignore_ascii_case("http://"))
    }
}

/// Doet `$body` met een transport `t` over de client van de bucket, en
/// geeft de uitkomst met de reden van een mislukte dial.
macro_rules! with_transport {
    ($self:ident, $t:ident, $body:expr) => {{
        let mut $t = Transport {
            http: &mut $self.http,
            why: None,
        };
        let r = $body;
        (r, $t.why.take())
    }};
}

/// De tekst van een leans3-fout, met de reden van de dial erbij.
fn say(e: &leans3::Error, why: Option<String>) -> String {
    match why {
        Some(w) => format!("{e} ({w})"),
        None => format!("{e}"),
    }
}

impl<C: Connect, R: Resolve> Bucket for S3Bucket<C, R> {
    async fn pull<S: SystemApi>(
        &mut self,
        key: &str,
        sys: &mut S,
        task: &StoreTask,
    ) -> Result<Option<u64>, String> {
        let client = self.client.clone();
        let mut sink = KernSink::new(sys, task)?;
        let (got, why) = with_transport!(self, t, client.get_to(&mut t, key, &mut sink).await);
        let n = match got {
            Ok((n, _)) => n,
            // Niets geschreven: het lokale bestand is onaangeraakt (Go).
            Err(leans3::Error::NotFound) => return Ok(None),
            Err(leans3::Error::Sink { .. }) => {
                return Err(sink
                    .why()
                    .unwrap_or_else(|| String::from("kernel write failed")));
            }
            Err(e) => return Err(say(&e, why)),
        };
        // De staart, en bij een leeg object de vervanging zelf: een schrijf
        // van nul bytes op offset 0 kort het bestand in (en maakt het aan).
        sink.finish().await.map_err(|e| format!("{e}"))?;
        Ok(Some(n))
    }

    async fn push<S: SystemApi>(
        &mut self,
        key: &str,
        sys: &mut S,
        task: &StoreTask,
        size: u64,
        sha256: &str,
    ) -> Result<(), String> {
        let client = self.client.clone();
        let mut source = KernSource::new(sys, task, size)?;
        let (got, why) = with_transport!(
            self,
            t,
            client
                .put_from(
                    &mut t,
                    key,
                    &mut source,
                    size,
                    sha256,
                    &PutOptions::default()
                )
                .await
        );
        match got {
            Ok(_) => Ok(()),
            Err(e) => Err(match source.why() {
                Some(w) => format!("{e} ({w})"),
                None => say(&e, why),
            }),
        }
    }

    async fn list(&mut self, prefix: &str, max: usize) -> Result<(Vec<String>, bool), String> {
        let client = self.client.clone();
        let (got, why) = with_transport!(self, t, client.list(&mut t, prefix, max).await);
        got.map_err(|e| say(&e, why))
    }

    async fn delete(&mut self, key: &str) -> Result<(), String> {
        let client = self.client.clone();
        let (got, why) = with_transport!(
            self,
            t,
            client.delete(&mut t, key, &DeleteOptions::default()).await
        );
        match got {
            Ok(()) | Err(leans3::Error::NotFound) => Ok(()),
            Err(e) => Err(say(&e, why)),
        }
    }
}

/// `leans3::Transport` over één verse verbinding per verzoek, met de body op de verbinding.
pub struct Transport<'a, C, R> {
    http: &'a mut Client<C, R>,
    /// Waarom de laatste dial faalde (leans3 kent alleen een vaste zin).
    why: Option<String>,
}

/// Splitst `host[:poort]` met de standaardpoort van het schema.
fn split_host(host: &str, https: bool) -> (&str, u16) {
    let default = if https { 443 } else { 80 };
    match host.rsplit_once(':') {
        Some((h, p)) if !h.contains(':') => (h, p.parse().unwrap_or(default)),
        _ => (host, default),
    }
}

/// Een fout van leanhttp in de taal van leans3.
fn io_error(e: &leanhttp::Error) -> IoError {
    match e {
        leanhttp::Error::Io(leanhttp::IoError::TimedOut) => IoError::TimedOut,
        leanhttp::Error::Io(leanhttp::IoError::Closed) => IoError::Closed,
        leanhttp::Error::UnexpectedEof | leanhttp::Error::Eof => IoError::UnexpectedEof,
        _ => IoError::Other("http failed"),
    }
}

/// De body van een PUT als leanhttp-stroom.
struct Body<'b>(&'b mut (dyn leans3::AsyncRead + Unpin));

impl leanhttp::AsyncRead for Body<'_> {
    fn poll_read(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, leanhttp::IoError>> {
        Pin::new(&mut *self.0)
            .poll_read(cx, buf)
            .map(|r| r.map_err(|_| leanhttp::IoError::Other))
    }
}

impl<C: Connect, R: Resolve> leans3::Transport for Transport<'_, C, R> {
    type Response = Response<C::Conn>;

    async fn send(&mut self, req: leans3::Request<'_, '_>) -> Result<Self::Response, IoError> {
        let scheme = if req.https { "https" } else { "http" };
        let url = format!("{scheme}://{}{}", req.host, req.target);
        let mut header = leanhttp::Header::new();
        for h in req.headers {
            header
                .set(h.name, &h.value)
                .map_err(|_| IoError::Other("invalid header"))?;
        }
        let (body, mut stream, body_len) = match req.body {
            leans3::Body::None => (None, None, 0),
            leans3::Body::Bytes(b) => (Some(b), None, 0),
            leans3::Body::Stream { source, len } => (None, Some(Body(source)), len),
        };
        let (host, port) = split_host(req.host, req.https);
        let mut dial = self.http.web();
        let conn = leanhttp::Dial::dial(
            &mut dial,
            Target {
                https: req.https,
                host,
                port,
            },
        )
        .await;
        let tls = dial.last_error();
        drop(dial);
        let conn = match conn {
            Ok(c) => c,
            Err(e) => {
                self.why = Some(self.http.reason(e, tls));
                return Err(IoError::Other("dial failed"));
            }
        };
        let call = leanhttp::Call {
            method: req.method,
            url: &url,
            header,
            body,
            body_reader: stream.as_mut().map(|s| s as &mut dyn leanhttp::AsyncRead),
            body_len,
            header_timeout: Some(HEADER_TIMEOUT),
            // Een getekend verzoek volgt nooit een redirect.
            no_follow: true,
        };
        let inner = leanhttp::send(conn, call).await.map_err(|e| io_error(&e))?;
        Ok(Response { inner })
    }
}

/// Het antwoord: status, koppen, en de body op de verbinding.
pub struct Response<C> {
    inner: leanhttp::Response<Link<C>>,
}

impl<C: leanhttp::Conn + Unpin> leans3::AsyncRead for Response<C> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, IoError>> {
        // Een verse future per poll: de brug van applib kopieert pas als er
        // bytes zijn, dus wegvallen na `Pending` verliest niets.
        let this = self.get_mut();
        match pin!(this.inner.read(buf)).poll(cx) {
            Poll::Ready(r) => Poll::Ready(r.map_err(|e| io_error(&e))),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<C: leanhttp::Conn + Unpin> leans3::Response for Response<C> {
    fn status(&self) -> u16 {
        self.inner.status
    }

    fn reason(&self) -> &str {
        &self.inner.reason
    }

    fn header(&self, name: &str) -> Option<&str> {
        self.inner.header.get(name)
    }

    fn content_length(&self) -> Option<u64> {
        self.inner.length
    }
}

/// Een call naar de kern die tussen twee polls openstaat. Hij bezit de
/// verbinding met de kern en de buffer, en geeft ze met de uitkomst terug.
type KernCall<'a, S, T> =
    Pin<Box<dyn Future<Output = (&'a mut S, Vec<u8>, Result<T, SysError>)> + 'a>>;

/// Waar een brug staat.
enum Leg<'a, S, T> {
    /// De verbinding met de kern is hier.
    Idle(&'a mut S),
    /// Een call loopt; de verbinding zit erin.
    Busy(KernCall<'a, S, T>),
    /// Stuk: de kern weigerde (de reden staat in `why`).
    Broken,
}

/// Een lege buffer van [`CHUNK`] bytes.
fn chunk_buf(cap: usize) -> Result<Vec<u8>, String> {
    let mut v = Vec::new();
    v.try_reserve_exact(cap)
        .map_err(|_| String::from("store buffer: out of memory"))?;
    Ok(v)
}

/// De GET-body naar het bestand van een pull, in happen van [`CHUNK`].
pub struct KernSink<'a, S> {
    leg: Leg<'a, S, ()>,
    task: &'a StoreTask,
    buf: Vec<u8>,
    /// De offset van de eerste byte in `buf`.
    off: u64,
    why: Option<String>,
}

impl<'a, S: SystemApi> KernSink<'a, S> {
    /// Een sink naar het bestand van `task`.
    pub fn new(sys: &'a mut S, task: &'a StoreTask) -> Result<Self, String> {
        Ok(Self {
            leg: Leg::Idle(sys),
            task,
            buf: chunk_buf(CHUNK)?,
            off: 0,
            why: None,
        })
    }

    /// Waarom de kern een schrijf weigerde, als dat zo was.
    pub fn why(&self) -> Option<String> {
        self.why.clone()
    }

    /// Start de schrijf van de volle buffer.
    fn flush(&mut self) {
        let Leg::Idle(sys) = core::mem::replace(&mut self.leg, Leg::Broken) else {
            return;
        };
        let buf = core::mem::take(&mut self.buf);
        let (task, off) = (self.task, self.off);
        self.off += buf.len() as u64;
        self.leg = Leg::Busy(Box::pin(async move {
            let r = sys.store_write(task, off, &buf).await;
            (sys, buf, r)
        }));
    }

    /// Wacht de lopende schrijf af. `Err` als de kern weigerde.
    fn poll_idle(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), IoError>> {
        let Leg::Busy(f) = &mut self.leg else {
            return Poll::Ready(match self.leg {
                Leg::Broken => Err(IoError::Other("kernel refused the write")),
                _ => Ok(()),
            });
        };
        let Poll::Ready((sys, mut buf, r)) = f.as_mut().poll(cx) else {
            return Poll::Pending;
        };
        buf.clear();
        self.buf = buf;
        match r {
            Ok(()) => {
                self.leg = Leg::Idle(sys);
                Poll::Ready(Ok(()))
            }
            Err(e) => {
                self.why = Some(format!("kernel write: {e}"));
                self.leg = Leg::Broken;
                Poll::Ready(Err(IoError::Other("kernel refused the write")))
            }
        }
    }

    /// Schrijft wat er nog in de buffer staat; bij een leeg object de schrijf
    /// van nul bytes op offset 0 (de vervanging).
    pub async fn finish(mut self) -> Result<(), SysError> {
        core::future::poll_fn(|cx| self.poll_idle(cx))
            .await
            .map_err(|_| SysError::Refused(self.why.clone().unwrap_or_default()))?;
        if self.buf.is_empty() && self.off > 0 {
            return Ok(());
        }
        let Leg::Idle(sys) = self.leg else {
            return Err(SysError::Refused(String::from("store sink broken")));
        };
        sys.store_write(self.task, self.off, &self.buf).await
    }
}

impl<S: SystemApi> leans3::AsyncWrite for KernSink<'_, S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<Result<usize, IoError>> {
        let this = self.get_mut();
        loop {
            match this.poll_idle(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Ready(Ok(())) => {}
            }
            let room = CHUNK.saturating_sub(this.buf.len());
            if room > 0 {
                let n = room.min(data.len());
                this.buf
                    .extend_from_slice(data.get(..n).unwrap_or_default());
                return Poll::Ready(Ok(n));
            }
            this.flush();
        }
    }
}

/// Het bestand van een push als PUT-body, in happen van [`CHUNK`].
pub struct KernSource<'a, S> {
    leg: Leg<'a, S, (u64, usize)>,
    task: &'a StoreTask,
    buf: Vec<u8>,
    /// Hoeveel van `buf` al gelezen is.
    at: usize,
    /// De offset van de volgende hap uit de kern.
    off: u64,
    size: u64,
    why: Option<String>,
}

impl<'a, S: SystemApi> KernSource<'a, S> {
    /// Een bron van precies `size` bytes uit het bestand van `task`.
    pub fn new(sys: &'a mut S, task: &'a StoreTask, size: u64) -> Result<Self, String> {
        let cap = usize::try_from(size).unwrap_or(usize::MAX).min(CHUNK);
        Ok(Self {
            leg: Leg::Idle(sys),
            task,
            buf: chunk_buf(cap)?,
            at: 0,
            off: 0,
            size,
            why: None,
        })
    }

    /// Waarom de kern een lees weigerde, als dat zo was.
    pub fn why(&self) -> Option<String> {
        self.why.clone()
    }
}

impl<S: SystemApi> leans3::AsyncRead for KernSource<'_, S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        out: &mut [u8],
    ) -> Poll<Result<usize, IoError>> {
        let this = self.get_mut();
        loop {
            if this.at < this.buf.len() {
                let src = this.buf.get(this.at..).unwrap_or_default();
                let n = src.len().min(out.len());
                if let (Some(d), Some(s)) = (out.get_mut(..n), src.get(..n)) {
                    d.copy_from_slice(s);
                }
                this.at += n;
                return Poll::Ready(Ok(n));
            }
            match core::mem::replace(&mut this.leg, Leg::Broken) {
                Leg::Broken => return Poll::Ready(Err(IoError::Other("kernel refused the read"))),
                Leg::Idle(sys) => {
                    if this.off >= this.size {
                        this.leg = Leg::Idle(sys);
                        return Poll::Ready(Ok(0));
                    }
                    let mut buf = core::mem::take(&mut this.buf);
                    let want = usize::try_from(this.size - this.off)
                        .unwrap_or(usize::MAX)
                        .min(CHUNK);
                    buf.resize(want, 0);
                    let (task, off) = (this.task, this.off);
                    this.leg = Leg::Busy(Box::pin(async move {
                        let r = sys.store_read(task, off, &mut buf).await;
                        (sys, buf, r)
                    }));
                }
                Leg::Busy(mut f) => {
                    let Poll::Ready((sys, mut buf, r)) = f.as_mut().poll(cx) else {
                        this.leg = Leg::Busy(f);
                        return Poll::Pending;
                    };
                    match r {
                        Ok((_, n)) if n > 0 => {
                            buf.truncate(n);
                            this.buf = buf;
                            this.at = 0;
                            this.off += n as u64;
                            this.leg = Leg::Idle(sys);
                        }
                        Ok(_) => {
                            this.why = Some(format!(
                                "{} shrank during the upload ({} of {} bytes)",
                                this.task.path, this.off, this.size
                            ));
                            return Poll::Ready(Err(IoError::UnexpectedEof));
                        }
                        Err(e) => {
                            this.why = Some(format!("kernel read: {e}"));
                            return Poll::Ready(Err(IoError::Other("kernel refused the read")));
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hopos_runner::KernSys;
    use hopos_runner::fake::{FakeKern, FakeStore};
    use runner::{Slot, StoreOp};

    fn bl<F: Future>(f: F) -> F::Output {
        let mut f = pin!(f);
        let mut cx = Context::from_waker(core::task::Waker::noop());
        for _ in 0..100_000 {
            if let Poll::Ready(v) = f.as_mut().poll(&mut cx) {
                return v;
            }
        }
        panic!("hangt");
    }

    #[test]
    fn split_host_ports() {
        assert_eq!(split_host("s3.example.com", true), ("s3.example.com", 443));
        assert_eq!(split_host("10.0.2.2:9000", false), ("10.0.2.2", 9000));
        assert_eq!(split_host("host", false), ("host", 80));
    }

    /// Een S3 in RAM achter het leans3-transport: GET geeft het object in
    /// happen van 1000 bytes (zoals een verbinding), PUT leest de gestroomde
    /// body helemaal.
    #[derive(Default)]
    struct RamS3 {
        object: Vec<u8>,
        put: Vec<u8>,
    }

    struct RamResp {
        body: Vec<u8>,
        at: usize,
        len: u64,
    }

    impl leans3::AsyncRead for RamResp {
        fn poll_read(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buf: &mut [u8],
        ) -> Poll<Result<usize, IoError>> {
            let this = self.get_mut();
            let n = buf.len().min(1000).min(this.body.len() - this.at);
            buf[..n].copy_from_slice(&this.body[this.at..this.at + n]);
            this.at += n;
            Poll::Ready(Ok(n))
        }
    }

    impl leans3::Response for RamResp {
        fn status(&self) -> u16 {
            200
        }
        fn reason(&self) -> &str {
            "OK"
        }
        fn header(&self, _: &str) -> Option<&str> {
            None
        }
        fn content_length(&self) -> Option<u64> {
            Some(self.len)
        }
    }

    impl leans3::Transport for &mut RamS3 {
        type Response = RamResp;
        async fn send(&mut self, req: leans3::Request<'_, '_>) -> Result<RamResp, IoError> {
            if let leans3::Body::Stream { source, len } = req.body {
                let mut buf = [0u8; 4096];
                while (self.put.len() as u64) < len {
                    let n =
                        core::future::poll_fn(|cx| Pin::new(&mut *source).poll_read(cx, &mut buf))
                            .await?;
                    if n == 0 {
                        break;
                    }
                    self.put.extend_from_slice(&buf[..n]);
                }
                return Ok(RamResp {
                    body: Vec::new(),
                    at: 0,
                    len: 0,
                });
            }
            Ok(RamResp {
                body: self.object.clone(),
                at: 0,
                len: self.object.len() as u64,
            })
        }
    }

    fn task(op: StoreOp, path: &str) -> StoreTask {
        StoreTask {
            ticket: 1,
            slot: Slot(2),
            op,
            job: String::from("demo"),
            key: String::from("/big.bin"),
            path: String::from(path),
        }
    }

    fn client() -> leans3::Client {
        leans3::Client {
            endpoint: String::from("http://10.0.2.2:9000"),
            bucket: String::from("hop"),
            region: String::from("us-east-1"),
            access_key_id: String::from("k"),
            secret_access_key: String::from("s"),
            path_style: true,
            now: Some(|| 1_759_000_000),
            ..leans3::Client::default()
        }
    }

    /// Een object groter dan twee happen stroomt als drie schrijven de kern
    /// in, en een bestand van die maat als gestroomde PUT eruit: de brug
    /// tussen de polls van leans3 en de calls naar de kern houdt de
    /// verbinding in de lopende call.
    #[test]
    fn big_objects_stream_through_the_bridges_in_chunks() {
        let big: Vec<u8> = (0..(2 * CHUNK + 1234)).map(|i| (i % 251) as u8).collect();
        let k = FakeKern::new(4);
        let mut sys = KernSys::new(k.client(), 4);
        let mut s3 = RamS3 {
            object: big.clone(),
            ..RamS3::default()
        };
        // Pull: het ticket moet bij de kern "genomen" zijn.
        k.queue_store(FakeStore {
            slot: 2,
            op: abi::hopabi::OP_STORE_PULL,
            job: String::from("demo"),
            key: String::from("/big.bin"),
            path: String::from("/big.bin"),
        });
        bl(sys.next_store(0)).unwrap().unwrap();
        let t = task(StoreOp::Pull, "/big.bin");
        let mut sink = KernSink::new(&mut sys, &t).unwrap();
        let (n, _) = bl(client().get_to(&mut &mut s3, "apps/c/demo/big.bin", &mut sink)).unwrap();
        bl(sink.finish()).unwrap();
        assert_eq!(n, big.len() as u64);
        assert_eq!(k.0.borrow().files["/big.bin"], big);
        let writes = k.0.borrow().ops.iter().filter(|&&o| o == 0x49).count();
        assert_eq!(writes, 3, "two full chunks and the tail");
        // Push van hetzelfde bestand.
        k.queue_store(FakeStore {
            slot: 2,
            op: abi::hopabi::OP_STORE_PUSH,
            job: String::from("demo"),
            key: String::from("/big.bin"),
            path: String::from("/big.bin"),
        });
        let t = bl(sys.next_store(0)).unwrap().unwrap();
        let mut src = KernSource::new(&mut sys, &t, big.len() as u64).unwrap();
        bl(client().put_from(
            &mut &mut s3,
            "apps/c/demo/big.bin",
            &mut src,
            big.len() as u64,
            "00",
            &PutOptions::default(),
        ))
        .unwrap();
        drop(src);
        assert_eq!(s3.put, big);
        // Een leeg object: de schrijf van nul bytes op 0 vervangt.
        k.0.borrow_mut()
            .files
            .insert(String::from("/e"), b"oud".to_vec());
        k.queue_store(FakeStore {
            slot: 2,
            op: abi::hopabi::OP_STORE_PULL,
            job: String::from("demo"),
            key: String::from("/e"),
            path: String::from("/e"),
        });
        let t = bl(sys.next_store(0)).unwrap().unwrap();
        let mut empty = RamS3::default();
        let mut sink = KernSink::new(&mut sys, &t).unwrap();
        bl(client().get_to(&mut &mut empty, "apps/c/demo/e", &mut sink)).unwrap();
        bl(sink.finish()).unwrap();
        assert!(k.0.borrow().files["/e"].is_empty());
    }
}
