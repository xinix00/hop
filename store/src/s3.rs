//! De lease en de clusterstaat in een S3-compatibele bucket (Go: `hoplock/s3`).
//!
//! Bezit per backend één leans3-client (configuratie, geen verbinding), de
//! host-client die de verbindingen maakt, en voor de lease de laatst geziene
//! "ghost". De verbinding zelf leeft één verzoek: [`hostnet::S3Transport`]
//! dialt vers en tekent met de systeemklok.
//!
//! Het CAS-protocol: een PUT met `If-None-Match: *` maakt alleen aan als er
//! niets is, een PUT of DELETE met `If-Match: <etag>` slaagt alleen als de
//! opgeslagen ETag nog die is. 412, of de 409 die sommige providers bij een
//! race geven, betekent: iemand anders houdt de lease. De ETag is de handle.
//! AWS rolde dit eind 2024 uit; een compatibele provider moet de voorwaarden
//! eren en sterke read-after-write op de lease-sleutel geven.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use config::S3LockConfig;
use discovery::{Backend, LeaseState};
use hostnet::{Http, S3Transport, block_on, unix_secs};
use leans3::{DeleteOptions, PutOptions};

use crate::{Error, Result, StateStore, redact_url, state_key, wire};

/// Het inhoudstype van lease en snapshot.
const JSON: &str = "application/json";

/// Een leans3-client uit de S3-sectie, tekenend met de systeemklok.
fn client_for(s3: &S3LockConfig) -> leans3::Client {
    leans3::Client {
        endpoint: s3.endpoint.clone(),
        bucket: s3.bucket.clone(),
        region: s3.region.clone(),
        access_key_id: s3.access_key_id.clone(),
        secret_access_key: s3.secret_access_key.clone(),
        session_token: s3.session_token.clone(),
        path_style: s3.use_path_style,
        now: Some(unix_secs),
    }
}

/// Nu in milliseconden sinds 1970 volgens de systeemklok.
fn wall_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// De leader-lease in een S3-bucket.
///
/// # Invariants
///
/// `ghost` is `None` of de ETag van een object dat GET ontkende en HEAD
/// toegaf, met het moment (ms) waarop die ETag voor het eerst zo gezien is.
#[derive(Debug)]
pub struct S3Lease {
    client: leans3::Client,
    http: Http,
    key: String,
    timeout: Duration,
    takeover_after_ms: u64,
    ghost: Option<(String, u64)>,
    last_error: Option<Error>,
    /// Een stilstaande klok voor de ghost-tests; `None` is de systeemklok.
    frozen_ms: Option<u64>,
}

impl S3Lease {
    /// Een lease op `key` in de bucket van `s3`, met `timeout` per verzoek.
    ///
    /// `takeover_after_ms` is hoe lang een ghost onveranderd moet blijven
    /// voordat we hem overnemen: zet hem op de lease-TTL; 0 zet de overname
    /// uit. Een ghost is een object dat GET ontkent (404), HEAD toegeeft (200
    /// met ETag) en een voorwaardelijke aanmaak weigert (412): gezien op
    /// Bunny Storage na een DELETE die met 204 bevestigd was (08-09-2026).
    /// Zonder overname loopt elke claim eeuwig 404, 412 en leidt niemand. Een
    /// levende eigenaar vernieuwt ruim binnen zijn TTL en elke vernieuwing
    /// verandert de ETag, dus een ETag die een TTL stilstaat is van niemand.
    pub fn new(s3: &S3LockConfig, key: &str, timeout: Duration, takeover_after_ms: u64) -> Self {
        Self {
            client: client_for(s3),
            http: Http::new(),
            key: key.to_string(),
            timeout,
            takeover_after_ms,
            ghost: None,
            last_error: None,
            frozen_ms: None,
        }
    }

    /// De sleutel van het lease-object.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// De laatste fout achter een `Unreachable`; gewist door een aanroep die slaagt.
    pub fn last_error(&self) -> Option<&Error> {
        self.last_error.as_ref()
    }

    /// Zet een stilstaande klok voor de ghost-timing.
    #[cfg(test)]
    pub(crate) fn set_now_ms(&mut self, ms: u64) {
        self.frozen_ms = Some(ms);
    }

    fn now_ms(&self) -> u64 {
        self.frozen_ms.unwrap_or_else(wall_ms)
    }

    /// Bewaart de oorzaak en geeft het antwoord dat `discovery` kent.
    fn unreachable(&mut self, e: Error) -> discovery::Error {
        self.last_error = Some(e);
        discovery::Error::Unreachable
    }

    fn s3_error(&self, op: &'static str, source: leans3::Error) -> Error {
        Error::S3 {
            op,
            key: self.key.clone(),
            source,
        }
    }

    /// Eén voorwaardelijke PUT: aanmaken bij een lege `prev`, anders `If-Match`.
    fn put(&mut self, prev: &str, body: &[u8], until: Instant) -> discovery::Result<String> {
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
        let mut t = S3Transport::new(&self.http, self.timeout).until(until);
        let result = block_on(self.client.put(&mut t, &self.key, body, &opt));
        match result {
            Ok(Some(etag)) if !etag.is_empty() => Ok(etag),
            Ok(_) => {
                let e = Error::MissingEtag {
                    op: "PUT",
                    key: self.key.clone(),
                };
                Err(self.unreachable(e))
            }
            Err(leans3::Error::PreconditionFailed) => Err(discovery::Error::LeaseHeld),
            Err(e) => {
                let e = self.s3_error("PUT", e);
                Err(self.unreachable(e))
            }
        }
    }

    /// Een PUT op `prev`, en bij 412 nog één keer met de kale ETag.
    ///
    /// Hetzner en Ceph geven een geciteerde ETag maar vergelijken `If-Match`
    /// met de kale waarde; zonder deze tweede poging faalde daar elke renew.
    /// AWS, R2 en MinIO slagen de eerste keer en merken er niets van.
    fn put_unquoting(
        &mut self,
        prev: &str,
        body: &[u8],
        until: Instant,
    ) -> discovery::Result<String> {
        let first = self.put(prev, body, until);
        match (first, wire::strip_quotes(prev)) {
            (Err(discovery::Error::LeaseHeld), Some(bare)) => self.put(bare, body, until),
            (r, _) => r,
        }
    }

    /// De ETag van een ghost die [`Self::new`]'s termijn onveranderd bleef.
    ///
    /// De eerste waarneming start alleen de klok. Een 404 op HEAD is een
    /// echte race (iemand maakte hem net aan), geen ghost.
    fn ghost_handle(&mut self, until: Instant) -> Option<String> {
        if self.takeover_after_ms == 0 {
            return None;
        }
        let mut t = S3Transport::new(&self.http, self.timeout).until(until);
        let etag = match block_on(self.client.head(&mut t, &self.key)) {
            Ok(e) if !e.is_empty() => e,
            _ => return None,
        };
        let now = self.now_ms();
        match &self.ghost {
            Some((seen, since)) if *seen == etag => {
                let age = now.saturating_sub(*since);
                if age < self.takeover_after_ms {
                    return None;
                }
                eprintln!(
                    "store: taking over ghost lease {} (ETag {etag} unchanged for {age} ms)",
                    self.key
                );
                Some(etag)
            }
            _ => {
                eprintln!(
                    "store: lease {} is a ghost (GET 404, HEAD {etag}); taking over if unchanged after {} ms",
                    self.key, self.takeover_after_ms
                );
                self.ghost = Some((etag, now));
                None
            }
        }
    }

    /// Eén voorwaardelijke DELETE op `handle`.
    fn remove(&mut self, handle: &str, until: Instant) -> discovery::Result {
        let mut t = S3Transport::new(&self.http, self.timeout).until(until);
        let opt = DeleteOptions { if_match: handle };
        match block_on(self.client.delete(&mut t, &self.key, &opt)) {
            Ok(()) => Ok(()),
            Err(leans3::Error::NotFound) => Err(discovery::Error::NoLease),
            Err(leans3::Error::PreconditionFailed) => Err(discovery::Error::LeaseHeld),
            Err(e) => {
                let e = self.s3_error("DELETE", e);
                Err(self.unreachable(e))
            }
        }
    }
}

/// Het einde van één backend-aanroep die nu begint: `timeout` is het
/// budget van de hele aanroep, niet van elke fase erin.
///
/// Waarom: `discovery::backend_timeout_for` rekent een derde van de lease
/// per aanroep, zodat er na een trage aanroep nog twee tikken over zijn.
/// Met alleen een fasetermijn kon één schrijf (PUT, 412, HEAD, tweede PUT,
/// elk met een verbinding, een kop en een body) een veelvoud daarvan
/// duren, en verliep de lease terwijl de vernieuwing nog wachtte.
pub(crate) fn deadline(timeout: Duration) -> Instant {
    Instant::now() + timeout
}

impl Backend for S3Lease {
    fn read(&mut self) -> discovery::Result<(LeaseState, String)> {
        let mut t = S3Transport::new(&self.http, self.timeout).until(deadline(self.timeout));
        let result = block_on(self.client.get(&mut t, &self.key));
        let (body, etag) = match result {
            Ok(r) => r,
            Err(leans3::Error::NotFound) => {
                self.last_error = None;
                return Err(discovery::Error::NoLease);
            }
            Err(e) => {
                let e = self.s3_error("GET", e);
                return Err(self.unreachable(e));
            }
        };
        let state = match wire::decode(&body) {
            Ok(s) => s,
            Err(why) => {
                let e = Error::BadLease {
                    key: self.key.clone(),
                    why,
                };
                return Err(self.unreachable(e));
            }
        };
        let Some(etag) = etag.filter(|e| !e.is_empty()) else {
            let e = Error::MissingEtag {
                op: "GET",
                key: self.key.clone(),
            };
            return Err(self.unreachable(e));
        };
        self.ghost = None;
        self.last_error = None;
        Ok((state, etag))
    }

    fn write(&mut self, prev: &str, state: &LeaseState) -> discovery::Result<String> {
        let body = match wire::encode(state) {
            Ok(b) => b,
            Err(_) => {
                let e = Error::BadLease {
                    key: self.key.clone(),
                    why: "out of memory while encoding",
                };
                return Err(self.unreachable(e));
            }
        };
        let until = deadline(self.timeout);
        let mut result = self.put_unquoting(prev, &body, until);
        if result == Err(discovery::Error::LeaseHeld) && prev.is_empty() {
            // De aanmaak werd geweigerd terwijl de lees "geen lease" zei: een
            // echte race (een volgende lees laat de winnaar zien) of een
            // ghost. HEAD beslist.
            if let Some(handle) = self.ghost_handle(until) {
                result = self.put_unquoting(&handle, &body, until);
            }
        }
        if result.is_ok() {
            self.ghost = None;
            self.last_error = None;
        }
        result
    }

    fn delete(&mut self, handle: &str) -> discovery::Result {
        if handle.is_empty() {
            return Err(discovery::Error::LeaseHeld);
        }
        let until = deadline(self.timeout);
        let first = self.remove(handle, until);
        let result = match (first, wire::strip_quotes(handle)) {
            (Err(discovery::Error::LeaseHeld), Some(bare)) => self.remove(bare, until),
            (r, _) => r,
        };
        if result.is_ok() {
            self.last_error = None;
        }
        result
    }
}

/// De clusterstaat als object `state/<cluster>` in de bucket van de lease.
///
/// Zelfde endpoint, sleutels en tekenaar als de lease: geen tweede
/// configuratie.
#[derive(Debug)]
pub struct S3StateStore {
    client: leans3::Client,
    http: Http,
    key: String,
    timeout: Duration,
}

impl S3StateStore {
    /// De staat van `cluster` in de bucket van `s3`, met `timeout` per verzoek.
    pub fn new(s3: &S3LockConfig, cluster: &str, timeout: Duration) -> Self {
        Self {
            client: client_for(s3),
            http: Http::new(),
            key: state_key(cluster),
            timeout,
        }
    }

    fn s3_error(&self, op: &'static str, source: leans3::Error) -> Error {
        Error::S3 {
            op,
            key: self.key.clone(),
            source,
        }
    }
}

impl StateStore for S3StateStore {
    fn save(&mut self, snapshot: &[u8]) -> Result {
        let mut t = S3Transport::new(&self.http, self.timeout).until(deadline(self.timeout));
        let opt = PutOptions {
            content_type: JSON,
            ..PutOptions::default()
        };
        block_on(self.client.put(&mut t, &self.key, snapshot, &opt))
            .map(|_| ())
            .map_err(|e| self.s3_error("PUT", e))
    }

    fn load(&mut self) -> Result<Option<Vec<u8>>> {
        let mut t = S3Transport::new(&self.http, self.timeout).until(deadline(self.timeout));
        match block_on(self.client.get(&mut t, &self.key)) {
            Ok((data, _)) => Ok(Some(data)),
            Err(leans3::Error::NotFound) => Ok(None),
            Err(e) => Err(self.s3_error("GET", e)),
        }
    }

    fn describe(&self) -> String {
        format!(
            "s3 {}/{}/{}",
            redact_url(&self.client.endpoint),
            self.client.bucket,
            self.key
        )
    }
}
