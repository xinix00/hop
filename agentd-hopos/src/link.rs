//! De link-taak: de aanroepen van deze agent bij de leader op een andere node.
//!
//! Register, heartbeat en notify, zoals de link-thread van de daemon
//! (`agentd/src/link.rs`): een ondertekende `POST` naar de leader-poort van
//! die node over het LAN, en het antwoord als [`Mail::Link`] terug naar de
//! verkiezing in de eigenaar. Een eigen taak, want een register laat de
//! leader reconcilen, en die kan daarbij deze node om een `/run` vragen:
//! wachtte de eigenaar zelf op het antwoord, dan wachtte hij op zichzelf.

use alloc::string::String;
use alloc::vec::Vec;
use core::time::Duration;

use agent::LinkError;

use crate::client::{Client, Req};
use crate::fetch::{Connect, Resolve};
use crate::mail::{Inbox, LinkJob, LinkQueue, Mail, Nap, deliver};

/// Hoe lang een register of heartbeat mag duren (Go: 5 s).
pub const LINK_TIMEOUT: Duration = Duration::from_secs(5);

/// De grootste antwoordbody die de link leest; de leader zegt hooguit een foutzin.
const LINK_BODY: usize = 64 << 10;

/// Een ondertekende `POST` met JSON-body; de status, of de reden van een transportfout.
pub async fn post<C: Connect, R: Resolve>(
    client: &mut Client<C, R>,
    key: &[u8],
    url: &str,
    body: &str,
    timeout: Duration,
) -> Result<u16, String> {
    signed(
        client,
        key,
        "POST",
        url,
        Some(body.as_bytes()),
        timeout,
        LINK_BODY,
    )
    .await
    .map(|(status, _)| status)
}

/// Een ondertekend verzoek (`X-Hop-Auth` met de clustersleutel); status en body.
pub async fn signed<C: Connect, R: Resolve>(
    client: &mut Client<C, R>,
    key: &[u8],
    method: &str,
    url: &str,
    body: Option<&[u8]>,
    timeout: Duration,
    limit: usize,
) -> Result<(u16, Vec<u8>), String> {
    let sig = auth::sign_call(key, method, url, body.unwrap_or_default())
        .map(|s| String::from_utf8_lossy(&s).into_owned());
    let mut headers: Vec<(&str, &str)> = Vec::new();
    if let Some(s) = &sig {
        headers.push((auth::AUTH_HEADER, s));
    }
    if body.is_some() {
        headers.push(("Content-Type", "application/json"));
    }
    let req = Req {
        method,
        url,
        headers: &headers,
        body,
        timeout,
    };
    let r = client.request(req, limit).await?;
    Ok((r.status, r.body))
}

/// De link-taak: bezit `client`, voert de aanroepen uit `jobs` uit.
///
/// Een mislukte register of heartbeat komt één keer op de log, en opnieuw
/// pas als de reden verandert (een leader die een uur weg is, is één regel).
pub async fn link_task<C: Connect, R: Resolve, N: Nap>(
    mut client: Client<C, R>,
    key: Vec<u8>,
    jobs: &'static LinkQueue,
    inbox: &'static Inbox,
    nap: N,
) {
    let mut said = String::new();
    loop {
        match jobs.recv().await {
            LinkJob::Election { req, url, body } => {
                let got = post(&mut client, &key, &url, &body, LINK_TIMEOUT).await;
                let result = match &got {
                    Ok(200..=299) => Ok(()),
                    Ok(404) => Err(LinkError::NotRegistered),
                    _ => Err(LinkError::Failed),
                };
                let why = match got {
                    Ok(200..=299) => None,
                    Ok(status) => Some(alloc::format!("POST {url}: status {status}")),
                    Err(e) => Some(e),
                };
                match why {
                    Some(w) if w != said => {
                        let line = alloc::format!("hop: leader link: {w} HOP_LINK_FAIL");
                        said = w;
                        deliver(inbox, Mail::Note(line), &nap).await;
                    }
                    Some(_) => {}
                    None => said.clear(),
                }
                deliver(inbox, Mail::Link { req, result }, &nap).await;
            }
            LinkJob::Notify { url, body } => {
                // Een melding die niet aankomt, ziet de leader bij zijn
                // volgende reconcile alsnog; niets om op te wachten.
                let _ = post(&mut client, &key, &url, &body, LINK_TIMEOUT).await;
            }
        }
    }
}
