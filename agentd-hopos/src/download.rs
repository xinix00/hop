//! De downloadtaak: haalt de artifacts op naast de eigenaar, en geeft de bytes als berichten.
//!
//! Tot 3.0.7 liep de download in de eigenaar-taak: de core ging terug bij
//! elke `.await`, maar de staat van de node wachtte, en daarmee elk verzoek
//! in de bus, ook `GET /v1/status` (03-10: een `POST /v1/jobs` gaf na 10 tot
//! 30 s niets terug terwijl de job wel landde; 30 MB over 100 Mbit is al
//! ruim 2 s, en TLS op een klein board kost meer). Nu bezit deze taak de
//! downloader en zijn verbinding, zoals de werker van `agentd` op de host
//! (`prep`): de eigenaar geeft een [`Order`], en krijgt de bytes terug als
//! [`Piece`]s door een rij van [`PIECES`] plaatsen. De runner en de
//! system-verbinding blijven van de eigenaar: hij stroomt elke brok zelf de
//! kern in, en handelt tussen twee brokken door de bus af. Een verzoek wacht
//! zo hoogstens op één brok naar de kern.
//!
//! Eén download tegelijk, in de volgorde van de starts, zoals voorheen; de
//! eigenaar geeft de volgende opdracht pas als de vorige klaar is.
//!
//! Afbreken: de eigenaar zet in `wanted` het nummer van de opdracht die hij
//! nog wil (0 is geen). Een opdracht met een ander nummer slaat deze taak
//! over, en een lopende download stopt bij zijn volgende brok; wat er dan
//! nog in de rij ligt, gooit de eigenaar weg op het nummer. Eén atomic met
//! één schrijver, geen protocol (handboek §1.3).

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use core::sync::atomic::{AtomicU64, Ordering::Relaxed};

use sync::mpsc::Mailbox;
use sync::spsc::{Channel, Receiver, Sender};

use crate::node::{Images, Sink};

/// Hoeveel brokken er tussen de downloadtaak en de eigenaar onderweg zijn.
///
/// Twee: terwijl de eigenaar er een de kern in stroomt, leest de taak de
/// volgende van het net. Meer kost alleen geheugen (een brok is tot
/// [`crate::fetch::DOWNLOAD_BUF`] bytes).
pub const PIECES: usize = 2;

/// Hoeveel opdrachten er in de rij naar de downloadtaak passen.
///
/// Er is er hoogstens één levend; de rest is afgebroken voordat de taak hem
/// pakte, en wordt bij het pakken meteen overgeslagen.
pub const ORDERS: usize = 4;

/// Een download voor de downloadtaak.
#[derive(Debug, PartialEq, Eq)]
pub struct Order {
    /// Het nummer, vanaf 1; elk [`Piece`] draagt het terug.
    pub seq: u64,
    /// De URL van het artifact.
    pub url: String,
}

/// Een stap van een download, van de downloadtaak naar de eigenaar.
#[derive(Debug, PartialEq, Eq)]
pub enum Piece {
    /// De lengte van het image (de `Content-Length`).
    Begin(u64),
    /// De volgende bytes, als waarde (handboek §1.2).
    Bytes(Vec<u8>),
    /// De download is klaar, of waarom niet.
    End(Result<(), String>),
}

/// De rij met opdrachten naar de downloadtaak.
pub type Orders = Mailbox<Order, ORDERS>;
/// De rij met brokken naar de eigenaar.
pub type Pieces = Channel<(u64, Piece), PIECES>;
/// De zendkant van [`Pieces`], van de downloadtaak.
pub type PieceTx<'a> = Sender<'a, (u64, Piece), PIECES>;
/// De ontvangkant van [`Pieces`], van de eigenaar.
pub type PieceRx<'a> = Receiver<'a, (u64, Piece), PIECES>;

/// De downloadtaak: pakt elke opdracht uit `orders` en geeft zijn brokken aan `tx`.
pub async fn download_task<I: Images>(
    mut images: I,
    orders: &Orders,
    mut tx: PieceTx<'_>,
    wanted: &AtomicU64,
) {
    loop {
        let order = orders.recv().await;
        download(&mut images, &order, &mut tx, wanted).await;
    }
}

/// Eén opdracht: haal op en geef de brokken door, met het einde erachter.
///
/// Een opdracht die de eigenaar niet meer wil, wordt niet begonnen; een
/// download die hij halverwege opgeeft, stopt bij de volgende brok en meldt
/// geen einde meer.
pub async fn download<I: Images>(
    images: &mut I,
    order: &Order,
    tx: &mut PieceTx<'_>,
    wanted: &AtomicU64,
) {
    if wanted.load(Relaxed) != order.seq {
        return;
    }
    let mut hand = Hand {
        seq: order.seq,
        tx,
        wanted,
    };
    let r = images.fetch(&order.url, &mut hand).await;
    if hand.is_wanted() {
        hand.tx.send((order.seq, Piece::End(r))).await;
    }
}

/// De [`Sink`] van de downloadtaak: elke stap als [`Piece`] in de rij.
struct Hand<'t, 'a> {
    seq: u64,
    tx: &'t mut PieceTx<'a>,
    wanted: &'t AtomicU64,
}

impl Hand<'_, '_> {
    fn is_wanted(&self) -> bool {
        self.wanted.load(Relaxed) == self.seq
    }

    /// Geeft `p` aan de eigenaar, en wacht als de rij vol is.
    async fn give(&mut self, p: Piece) -> Result<(), String> {
        if !self.is_wanted() {
            return Err(String::from("download cancelled"));
        }
        self.tx.send((self.seq, p)).await;
        Ok(())
    }
}

impl Sink for Hand<'_, '_> {
    async fn begin(&mut self, size: u64) -> Result<(), String> {
        self.give(Piece::Begin(size)).await
    }

    async fn chunk(&mut self, bytes: &[u8]) -> Result<(), String> {
        let mut v = Vec::new();
        v.try_reserve_exact(bytes.len())
            .map_err(|_| format!("download: no memory for a piece of {} bytes", bytes.len()))?;
        v.extend_from_slice(bytes);
        self.give(Piece::Bytes(v)).await
    }
}
