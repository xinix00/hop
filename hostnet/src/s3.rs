//! Het leans3-transport van de host: één getekend verzoek over een verse verbinding.
//!
//! Bezit per verzoek één verbinding en geeft die met het antwoord mee: de
//! body wordt uit dezelfde socket gelezen. Een getekend verzoek volgt nooit
//! een redirect (de handtekening dekt host en pad), daarom `leanhttp::send`
//! op een zelf gedialde verbinding en geen `fetch`.

use std::future::Future;
use std::pin::{Pin, pin};
use std::task::{Context, Poll};
use std::time::Duration;

use leanhttp::Target;
use leans3::IoError;
use leantls::Trust;

use crate::client::{Dialer, HostConn, Http};

/// `leans3::Transport` over de host-client.
#[derive(Clone, Copy, Debug)]
pub struct S3Transport<'h> {
    http: &'h Http,
    timeout: Duration,
}

impl<'h> S3Transport<'h> {
    /// Een transport over `http` met termijn `timeout` per fase.
    pub fn new(http: &'h Http, timeout: Duration) -> Self {
        Self { http, timeout }
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

impl leans3::Transport for S3Transport<'_> {
    type Response = S3Response;

    async fn send(&mut self, req: leans3::Request<'_, '_>) -> Result<S3Response, IoError> {
        let body = match req.body {
            leans3::Body::None => None,
            leans3::Body::Bytes(b) => Some(b),
            // De gebruikers van deze crate schrijven alleen kleine objecten
            // (lease, snapshot); een gestroomde PUT heeft nog geen klant.
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
        let verifier = self.http.verifier();
        let trust = verifier.as_ref().map(|v| Trust::Chain(v));
        let mut dial = Dialer::new(trust, self.timeout);
        let conn = dial
            .hop(Target {
                https: req.https,
                host,
                port,
            })
            .await
            .map_err(|_| IoError::Other("dial failed"))?;
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
