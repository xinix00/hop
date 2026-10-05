//! Het S3-transport van de host: `leans3http` voor de lease en de staat, en één gestroomd transport voor downloads.
//!
//! [`s3_http`] is het transport van één backend-aanroep (`store`): leans3http
//! over de webdialer van de host, één poging per verzoek, alles binnen de
//! totale grens van de aanroep (de std-verbinding draagt hem, zie
//! [`StdConn::with_limit`](crate::StdConn::with_limit)).
//!
//! [`S3Transport`] blijft voor `get_to` van grote objecten (een artifact van
//! de runner): leans3http leest elke antwoordbody eerst helemaal in het
//! geheugen (tot `Limits::body`, 32 MiB), en een download van honderden MB
//! moet hap voor hap naar de schijf. Dit transport geeft het antwoord met
//! de body nog op de verbinding. Een getekend verzoek volgt nooit een
//! redirect (de handtekening dekt host en pad), daarom `leanhttp::send` op
//! een zelf gedialde verbinding en geen `fetch`.

use std::future::{Future, poll_fn};
use std::pin::{Pin, pin};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use leanhttp::Target;
use leans3::IoError;

use crate::client::{HostConn, Tcp, web};

/// De klok van leans3http op de host: monotoon vanaf het maken van het transport.
#[derive(Clone, Copy, Debug)]
struct HostClock {
    origin: Instant,
}

impl Default for HostClock {
    fn default() -> Self {
        Self {
            origin: Instant::now(),
        }
    }
}

impl leans3http::Clock for HostClock {
    fn now(&self) -> Duration {
        self.origin.elapsed()
    }

    /// Een wekker die [`crate::block_on`] pollt; de verbindingen blokkeren,
    /// dus de termijn van een poging dragen hun sockets.
    fn sleep(&self, d: Duration) -> impl Future<Output = ()> {
        let at = Instant::now() + d;
        poll_fn(move |_| {
            if Instant::now() >= at {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
    }
}

/// Een transport met `timeout` per fase en alles klaar vóór `until`.
///
/// Eén poging per verzoek: een lease-schrijf die na een 412 nog een HEAD en
/// een tweede PUT doet, deelt één budget, en de verkiezing is zelf de
/// herkansing. De body hoogstens zo groot als leans3 een gebufferde GET
/// toestaat.
pub fn s3_http(timeout: Duration, until: Instant) -> impl leans3::Transport {
    let mut http = leans3http::Http::new(
        web(Tcp::new(timeout, Some(until), None)),
        HostClock::default(),
    );
    http.limits = leans3http::Limits {
        deadline: timeout,
        header: timeout,
        body: usize::try_from(leans3::MAX_BUFFERED_GET)
            .unwrap_or(usize::MAX)
            .saturating_add(1),
        attempts: 1,
    };
    http
}

/// `leans3::Transport` met de body op de verbinding, voor gestroomde GET's.
#[derive(Clone, Copy, Debug)]
pub struct S3Transport {
    timeout: Duration,
}

impl S3Transport {
    /// Een transport met termijn `timeout` per fase.
    pub fn new(timeout: Duration) -> Self {
        Self { timeout }
    }
}

/// Het antwoord: status, koppen, en de body op de verbinding.
pub struct S3Response {
    inner: leanhttp::Response<HostConn>,
}

impl std::fmt::Debug for S3Response {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Response")
            .field("status", &self.inner.status)
            .finish_non_exhaustive()
    }
}

/// Een fout van leanhttp in de taal van leans3.
fn io_error(e: &leanhttp::Error) -> IoError {
    match e {
        leanhttp::Error::Io(leanhttp::IoError::TimedOut) => IoError::TimedOut,
        leanhttp::Error::Io(leanhttp::IoError::Closed) => IoError::Closed,
        leanhttp::Error::UnexpectedEof | leanhttp::Error::Eof => IoError::UnexpectedEof,
        leanhttp::Error::Connect => IoError::Other("connect failed"),
        _ => IoError::Other("http failed"),
    }
}

/// Splitst `host[:poort]` met de standaardpoort van het schema.
fn split_host(host: &str, https: bool) -> (&str, u16) {
    let default = if https { 443 } else { 80 };
    match host.rsplit_once(':') {
        Some((h, p)) if !h.contains(':') => match p.parse() {
            Ok(port) => (h, port),
            Err(_) => (host, default),
        },
        _ => (host, default),
    }
}

impl leans3::Transport for S3Transport {
    type Response = S3Response;

    async fn send(&mut self, req: leans3::Request<'_, '_>) -> Result<S3Response, IoError> {
        let body = match req.body {
            leans3::Body::None => None,
            leans3::Body::Bytes(b) => Some(b),
            // Dit transport is voor gestroomde GET's; een gestroomde PUT
            // heeft geen klant (de kleine objecten gaan over [`s3_http`]).
            leans3::Body::Stream { .. } => {
                return Err(IoError::Other("hostnet: streamed PUT is not supported"));
            }
        };
        let scheme = if req.https { "https" } else { "http" };
        let url = format!("{scheme}://{}{}", req.host, req.target);
        let mut header = leanhttp::Header::new();
        for h in req.headers {
            header
                .set(h.name, &h.value)
                .map_err(|_| IoError::Other("invalid header"))?;
        }
        let (host, port) = split_host(req.host, req.https);
        let mut dial = web(Tcp::new(self.timeout, None, None));
        let target = Target {
            https: req.https,
            host,
            port,
        };
        let conn = leanhttp::Dial::dial(&mut dial, target)
            .await
            .map_err(|e| io_error(&e))?;
        let call = leanhttp::Call {
            method: req.method,
            url: &url,
            header,
            body,
            header_timeout: Some(self.timeout),
            no_follow: true,
            ..leanhttp::Call::default()
        };
        let inner = leanhttp::send(conn, call).await.map_err(|e| io_error(&e))?;
        Ok(S3Response { inner })
    }
}

impl leans3::AsyncRead for S3Response {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, IoError>> {
        // De verbinding blokkeert en is dus in één poll klaar; de future houdt
        // buiten die ene poll niets vast.
        let this = self.get_mut();
        match pin!(this.inner.read(buf)).poll(cx) {
            Poll::Ready(r) => Poll::Ready(r.map_err(|e| io_error(&e))),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl leans3::Response for S3Response {
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

#[cfg(test)]
mod tests {
    use super::split_host;

    #[test]
    fn split_host_ports() {
        assert_eq!(split_host("s3.example.com", true), ("s3.example.com", 443));
        assert_eq!(split_host("minio:9000", false), ("minio", 9000));
        assert_eq!(split_host("127.0.0.1:9000", false), ("127.0.0.1", 9000));
        assert_eq!(split_host("host", false), ("host", 80));
    }
}
