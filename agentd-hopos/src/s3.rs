//! De lease en de clusterstaat in een S3-compatibele bucket, async: leans3 over leans3http over `WebDial`.
//!
//! Hetzelfde CAS-protocol als `store::S3Lease` op de host: een PUT met
//! `If-None-Match: *` maakt alleen aan als er niets is, een PUT of DELETE met
//! `If-Match: <etag>` slaagt alleen als de opgeslagen ETag nog die is, en 412
//! (of 409) betekent dat iemand anders de lease houdt. Met de twee
//! herkansingen van de host: de kale ETag na een 412 op een geciteerde
//! (Hetzner, Ceph), en de overname van een "ghost" (GET 404, HEAD 200) die
//! een lease-TTL onveranderd bleef (Bunny Storage, 08-09-2026).
//!
//! Het transport is `leans3http::Http` over de webdialer van de [`Client`]
//! van de eigenaar-taak ([`Client::s3`]): TLS met de Mozilla-wortels zoals de
//! artifacts, nooit een redirect (de handtekening dekt host en pad), één
//! poging per verzoek binnen het budget van de aanroep. Tekenen vraagt een
//! wandklok: zonder SNTP (of de RTC van QEMU) weigert S3 de handtekening, en
//! dat staat dan in de fout.

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use core::future::Future;
use core::time::Duration;

use discovery::{LeaseState, wire};
use leans3::{DeleteOptions, PutOptions};

use crate::client::{Client, redact};
use crate::env::S3Config;
use crate::fetch::{Connect, Resolve};
use crate::lock::{LeaseBackend, StateBackend};

/// Het inhoudstype van lease en snapshot.
const JSON: &str = "application/json";

/// De klok van leans3http in de bewoner: de monotone klok van applib en
/// het timerwiel van de app-core.
#[derive(Clone, Copy, Debug, Default)]
pub struct ExecClock;

impl leans3http::Clock for ExecClock {
    fn now(&self) -> Duration {
        Duration::from_nanos(applib::clock::now_ns())
    }

    fn sleep(&self, d: Duration) -> impl Future<Output = ()> {
        applib::EXEC.get().after(d)
    }
}

/// Een leans3-client uit de S3-sectie, tekenend met `wall_secs`.
fn client_for(s3: &S3Config, wall_secs: fn() -> u64) -> leans3::Client {
    leans3::Client {
        endpoint: s3.endpoint.clone(),
        bucket: s3.bucket.clone(),
        region: if s3.region.is_empty() {
            // Een lege regio tekent niet; MinIO en R2 nemen elke naam aan,
            // AWS wil de echte en zegt dat dan luid in zijn 400.
            String::from("us-east-1")
        } else {
            s3.region.clone()
        },
        access_key_id: s3.key.clone(),
        secret_access_key: s3.secret.clone(),
        session_token: String::new(),
        path_style: s3.path_style,
        now: Some(wall_secs),
    }
}

/// De tekst van een leans3-fout, met de stap van de kale dial erbij als die faalde.
fn say<C: Connect, R: Resolve>(client: &mut Client<C, R>, e: &leans3::Error) -> String {
    match client.dial_why() {
        Some(why) => format!("{e} ({why})"),
        None => format!("{e}"),
    }
}

/// Haalt één paar omringende dubbele aanhalingstekens van een ETag.
fn strip_quotes(etag: &str) -> Option<&str> {
    wire::strip_quotes(etag)
}

/// De leader-lease in een S3-bucket.
///
/// # Invariants
///
/// `ghost` is `None` of de ETag van een object dat GET ontkende en HEAD
/// toegaf, met het moment (s) waarop die ETag voor het eerst zo gezien is.
pub struct S3Lease<C, R> {
    s3: leans3::Client,
    client: Client<C, R>,
    key: String,
    timeout: Duration,
    takeover_after_ms: u64,
    wall_secs: fn() -> u64,
    ghost: Option<(String, u64)>,
    last_error: Option<String>,
}

impl<C: Connect, R: Resolve> S3Lease<C, R> {
    /// Een lease op `key` in de bucket van `s3`; `takeover_after_ms` is de lease-TTL (0: geen ghost-overname).
    pub fn new(
        client: Client<C, R>,
        s3: &S3Config,
        key: &str,
        timeout: Duration,
        takeover_after_ms: u64,
        wall_secs: fn() -> u64,
    ) -> Self {
        Self {
            s3: client_for(s3, wall_secs),
            client,
            key: String::from(key),
            timeout,
            takeover_after_ms,
            wall_secs,
            ghost: None,
            last_error: None,
        }
    }

    /// De leans3-client, een transport over onze client, en de sleutel:
    /// drie leningen van verschillende velden tegelijk.
    fn parts(&mut self) -> (&leans3::Client, impl leans3::Transport + '_, &str) {
        let t = self.client.s3(ExecClock, self.timeout);
        (&self.s3, t, &self.key)
    }

    /// Bewaart de oorzaak en geeft het antwoord dat `discovery` kent.
    fn unreachable(&mut self, why: String) -> discovery::Error {
        self.last_error = Some(why);
        discovery::Error::Unreachable
    }

    fn s3_error(&mut self, op: &str, e: &leans3::Error) -> discovery::Error {
        let why = format!("s3 {op} {}: {}", self.key, say(&mut self.client, e));
        self.unreachable(why)
    }

    /// Eén voorwaardelijke PUT: aanmaken bij een lege `prev`, anders `If-Match`.
    async fn put(&mut self, prev: &str, body: &[u8]) -> discovery::Result<String> {
        let opt = if prev.is_empty() {
            PutOptions {
                content_type: JSON,
                if_none_match: "*",
                ..PutOptions::default()
            }
        } else {
            PutOptions {
                content_type: JSON,
                if_match: prev,
                ..PutOptions::default()
            }
        };
        let (s3, mut t, key) = self.parts();
        let result = s3.put(&mut t, key, body, &opt).await;
        drop(t);
        match result {
            Ok(Some(etag)) if !etag.is_empty() => Ok(etag),
            Ok(_) => {
                let why = format!("s3 PUT {}: response has no ETag", self.key);
                Err(self.unreachable(why))
            }
            Err(leans3::Error::PreconditionFailed) => Err(discovery::Error::LeaseHeld),
            Err(e) => Err(self.s3_error("PUT", &e)),
        }
    }

    /// Een PUT op `prev`, en bij 412 nog één keer met de kale ETag.
    async fn put_unquoting(&mut self, prev: &str, body: &[u8]) -> discovery::Result<String> {
        let first = self.put(prev, body).await;
        match (first, strip_quotes(prev)) {
            (Err(discovery::Error::LeaseHeld), Some(bare)) => self.put(bare, body).await,
            (r, _) => r,
        }
    }

    /// De ETag van een ghost die de overnametermijn onveranderd bleef.
    async fn ghost_handle(&mut self) -> Option<String> {
        if self.takeover_after_ms == 0 {
            return None;
        }
        let (s3, mut t, key) = self.parts();
        let head = s3.head(&mut t, key).await;
        drop(t);
        let etag = match head {
            Ok(e) if !e.is_empty() => e,
            _ => return None,
        };
        let now = (self.wall_secs)().saturating_mul(1000);
        match &self.ghost {
            Some((seen, since)) if *seen == etag => {
                let age = now.saturating_sub(*since);
                (age >= self.takeover_after_ms).then_some(etag)
            }
            _ => {
                self.ghost = Some((etag, now));
                None
            }
        }
    }

    /// Eén voorwaardelijke DELETE op `handle`.
    async fn remove(&mut self, handle: &str) -> discovery::Result {
        let opt = DeleteOptions { if_match: handle };
        let (s3, mut t, key) = self.parts();
        let got = s3.delete(&mut t, key, &opt).await;
        drop(t);
        match got {
            Ok(()) => Ok(()),
            Err(leans3::Error::NotFound) => Err(discovery::Error::NoLease),
            Err(leans3::Error::PreconditionFailed) => Err(discovery::Error::LeaseHeld),
            Err(e) => Err(self.s3_error("DELETE", &e)),
        }
    }
}

impl<C: Connect, R: Resolve> LeaseBackend for S3Lease<C, R> {
    async fn read(&mut self) -> discovery::Result<(LeaseState, String)> {
        let (s3, mut t, key) = self.parts();
        let got = s3.get(&mut t, key).await;
        drop(t);
        let (body, etag) = match got {
            Ok(r) => r,
            Err(leans3::Error::NotFound) => {
                self.last_error = None;
                return Err(discovery::Error::NoLease);
            }
            Err(e) => return Err(self.s3_error("GET", &e)),
        };
        let state = match wire::decode(&body) {
            Ok(s) => s,
            Err(why) => {
                let why = format!("lease {}: {why}", self.key);
                return Err(self.unreachable(why));
            }
        };
        let Some(etag) = etag.filter(|e| !e.is_empty()) else {
            let why = format!("s3 GET {}: response has no ETag", self.key);
            return Err(self.unreachable(why));
        };
        self.ghost = None;
        self.last_error = None;
        Ok((state, etag))
    }

    async fn write(&mut self, prev: &str, state: &LeaseState) -> discovery::Result<String> {
        let Ok(body) = wire::encode(state) else {
            let why = format!("lease {}: out of memory while encoding", self.key);
            return Err(self.unreachable(why));
        };
        let mut result = self.put_unquoting(prev, &body).await;
        if result == Err(discovery::Error::LeaseHeld)
            && prev.is_empty()
            && let Some(handle) = self.ghost_handle().await
        {
            // De aanmaak werd geweigerd terwijl de lees "geen lease" zei, en
            // HEAD toont een object dat een TTL stilstond: van niemand.
            result = self.put_unquoting(&handle, &body).await;
        }
        if result.is_ok() {
            self.ghost = None;
            self.last_error = None;
        }
        result
    }

    async fn delete(&mut self, handle: &str) -> discovery::Result {
        if handle.is_empty() {
            return Err(discovery::Error::LeaseHeld);
        }
        let first = self.remove(handle).await;
        let result = match (first, strip_quotes(handle)) {
            (Err(discovery::Error::LeaseHeld), Some(bare)) => self.remove(bare).await,
            (r, _) => r,
        };
        if result.is_ok() {
            self.last_error = None;
        }
        result
    }

    fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }
}

/// De clusterstaat als object `state/<cluster>` in de bucket van de lease.
pub struct S3State<C, R> {
    s3: leans3::Client,
    client: Client<C, R>,
    key: String,
    timeout: Duration,
}

impl<C: Connect, R: Resolve> S3State<C, R> {
    /// De staat op `key` in de bucket van `s3`.
    pub fn new(
        client: Client<C, R>,
        s3: &S3Config,
        key: &str,
        timeout: Duration,
        wall_secs: fn() -> u64,
    ) -> Self {
        Self {
            s3: client_for(s3, wall_secs),
            client,
            key: String::from(key),
            timeout,
        }
    }

    /// De leans3-client, een transport over onze client, en de sleutel:
    /// drie leningen van verschillende velden tegelijk.
    fn parts(&mut self) -> (&leans3::Client, impl leans3::Transport + '_, &str) {
        let t = self.client.s3(ExecClock, self.timeout);
        (&self.s3, t, &self.key)
    }
}

impl<C: Connect, R: Resolve> StateBackend for S3State<C, R> {
    async fn save(&mut self, snapshot: &[u8]) -> Result<(), String> {
        let opt = PutOptions {
            content_type: JSON,
            ..PutOptions::default()
        };
        let (s3, mut t, key) = self.parts();
        let got = s3.put(&mut t, key, snapshot, &opt).await;
        drop(t);
        got.map(|_| ())
            .map_err(|e| format!("s3 PUT {}: {}", self.key, say(&mut self.client, &e)))
    }

    async fn load(&mut self) -> Result<Option<Vec<u8>>, String> {
        let (s3, mut t, key) = self.parts();
        let got = s3.get(&mut t, key).await;
        drop(t);
        match got {
            Ok((data, _)) => Ok(Some(data)),
            Err(leans3::Error::NotFound) => Ok(None),
            Err(e) => Err(format!(
                "s3 GET {}: {}",
                self.key,
                say(&mut self.client, &e)
            )),
        }
    }

    fn describe(&self) -> String {
        format!(
            "s3 {}/{}/{}",
            redact(&self.s3.endpoint),
            self.s3.bucket,
            self.key
        )
    }
}
