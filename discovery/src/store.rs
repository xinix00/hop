//! Welke opslag de gecommitte clusterstaat krijgt.
//!
//! Bezit alleen de keuze, niet de opslag zelf: S3, hoplockserver en het
//! lokale bestand zijn I/O en wonen bij de aanroeper.

/// De soort opslag voor de gecommitte clusterstaat.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StateStoreKind {
    /// Het object `state/<cluster>` in dezelfde bucket als de lease.
    S3,
    /// Het object `state/<cluster>` op dezelfde hoplockserver als de lease.
    HoplockServer,
    /// Een lokaal bestand (tmp, fsync, rename): standalone en in-memory lock.
    File,
}

/// Kiest de opslag voor de clusterstaat; de ENIGE poort, zodat daemon en HopOS niet uiteenlopen.
///
/// De backend die de LEASE houdt, houdt ook de STAAT: één bron van waarheid.
/// Een bruikbare S3-sectie wint (lease en staat in dezelfde bucket), anders een
/// hoplockserver-URL, anders (standalone, `mem`) het lokale bestand.
pub fn state_store_for(
    standalone: bool,
    lock_type: &str,
    lock_url: &str,
    s3_endpoint: &str,
    s3_bucket: &str,
) -> StateStoreKind {
    if standalone {
        return StateStoreKind::File;
    }
    if !s3_bucket.is_empty() && !s3_endpoint.is_empty() {
        return StateStoreKind::S3;
    }
    if (lock_type.is_empty() || lock_type == "hoplockserver") && !lock_url.is_empty() {
        return StateStoreKind::HoplockServer;
    }
    StateStoreKind::File
}
