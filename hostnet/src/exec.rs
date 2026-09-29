//! De executor van één thread: een future pollen tot hij klaar is.
//!
//! Bezit niets dan de lus. Alle verbindingen van deze crate blokkeren in hun
//! poll en zijn dus meteen `Ready`; een future die toch `Pending` geeft
//! (een verbinding van buiten deze crate) krijgt een korte slaap en een
//! nieuwe poll. Dat is een spin met rem, geen reactor, en dat is bewust: zie
//! de crate-doc.

use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

/// Hoe lang een onverwachte `Pending` de thread laat rusten voor de volgende poll.
const PENDING_NAP: Duration = Duration::from_millis(1);

/// Draait `fut` op deze thread tot hij klaar is.
pub fn block_on<F: Future>(fut: F) -> F::Output {
    let mut fut = pin!(fut);
    // De waker doet niets: er is niemand om te wekken, deze thread pollt zelf.
    let mut cx = Context::from_waker(Waker::noop());
    loop {
        if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
            return v;
        }
        std::thread::sleep(PENDING_NAP);
    }
}
