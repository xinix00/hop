//! Het contract tussen Hop en de HopOS-kern: de bevoegde system-API.
//!
//! Op HopOS is Hop de eerste bewoner, een app met één bevoegdheid die geen
//! andere app heeft: sloten starten en stoppen (PORT.md §6, beslissing 1). De
//! kern bezit het mechanisme (partities, cores, stage-2, de kooi); Hop bezit
//! het beleid. Deze module is de rand: de kern implementeert [`SystemApi`],
//! de [`crate::HopRunner`] gebruikt hem.

use alloc::collections::BTreeMap;
use alloc::string::String;
use core::fmt;

/// Een kooi op de node (een slot-index, 1 of hoger); geen core-nummer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Slot(pub u32);

/// De app-toestand op de controlepagina van een slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SlotApp {
    /// Niemand thuis.
    Empty,
    /// De app start.
    Booting,
    /// De app draait.
    Ready,
    /// De app is gestopt; zie [`SlotStatus::exit_code`].
    Exited,
}

/// Een momentopname van één slot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SlotStatus {
    /// Of de core(s) van dit slot aan staan.
    pub core_on: bool,
    /// De app-toestand.
    pub app: SlotApp,
    /// De exitcode als de app gestopt is.
    pub exit_code: u64,
    /// Het werkelijke geheugengebruik dat de app meldt; 0 = nog niet gemeld.
    pub mem_sys: u64,
    /// CPU als percentage van de EIGEN cores (0 tot 100); `None` zolang er geen meetvenster is.
    pub cpu_pct: Option<u8>,
    /// Vector + 1 van een stage-2-fout of harde kill; 0 = geen fout.
    pub fault_vec: u64,
    /// ESR bij een fout.
    pub fault_esr: u64,
    /// FAR bij een fout.
    pub fault_far: u64,
    /// De eigen regel van de node over de kooi, of leeg.
    ///
    /// Diagnose, geen invoer voor de toestandsmachine. Hij reist over het
    /// netwerk omdat de seriële console op 115200 bytes verliest, en een
    /// gehavend hex-getal is erger dan geen.
    pub cage: String,
}

impl SlotStatus {
    /// Een leeg slot.
    pub fn empty() -> Self {
        Self {
            core_on: false,
            app: SlotApp::Empty,
            exit_code: 0,
            mem_sys: 0,
            cpu_pct: None,
            fault_vec: 0,
            fault_esr: 0,
            fault_far: 0,
            cage: String::new(),
        }
    }
}

/// Alles wat een start in één fase nodig heeft.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StartSpec {
    /// De maat van het image in bytes; verplicht: de plaatsing valideert ertegen.
    pub image_size: u64,
    /// De partitiemaat (de `memory_limit` van de job).
    pub mem_limit: u64,
    /// Optionele fysieke core-klasse ("big", "mid", "small").
    pub core_class: String,
    /// SMP-cores voor de app zelf (1 of meer).
    pub cores: u32,
    /// Leeg = eigen core(s); anders de naam van een coöperatieve pool.
    pub sharegroup: String,
    /// De poolgrootte in hele cores (alleen met een sharegroup).
    pub pool_cores: u32,
    /// De env van de app, `ER_*` inbegrepen.
    pub env: BTreeMap<String, String>,
    /// Gedeeld pad naar lokaal pad.
    pub mounts: BTreeMap<String, String>,
    /// Gepubliceerde poorten, naam naar poort.
    pub ports: BTreeMap<String, u16>,
    /// De jobnaam: de namespace van de app in de object-store.
    pub job: String,
}

/// Een fout van de kern.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SysError {
    /// De kern kan de kooi niet plaatsen: geen vrije core, pool past niet.
    NoCapacity(String),
    /// De kern weigert om een andere reden.
    Refused(String),
    /// De stop is niet bevestigd binnen de termijn.
    NotConfirmed,
}

impl fmt::Display for SysError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SysError::NoCapacity(why) => write!(f, "no free core to place the cage: {why}"),
            SysError::Refused(why) => write!(f, "refused: {why}"),
            SysError::NotConfirmed => f.write_str("stop not confirmed"),
        }
    }
}

/// De bevoegde system-API van de HopOS-kern, zoals Hop hem ziet.
///
/// Geen methode blokkeert. Een start is één fase: [`SystemApi::start_slot`]
/// reserveert partitie en cores voor een image van bekende maat, daarna
/// stroomt het image met [`SystemApi::stream_image`] RECHTSTREEKS de
/// partitie in (elke byte landt op het adres waar hij gaat draaien), en na
/// de laatste byte start de kern de app. Faalt een start, dan ruimt de kern
/// zijn eigen reserveringen op; de aanroeper ruimt alleen zijn boekhouding op.
pub trait SystemApi {
    /// Het aantal bruikbare app-cores: de enige capaciteit waar Hop tegen plant.
    fn num_cores(&self) -> u32;

    /// Reserveert slot `slot` voor een image van `spec.image_size` bytes.
    fn start_slot(&mut self, slot: Slot, spec: &StartSpec) -> Result<(), SysError>;

    /// Schrijft de volgende bytes van het image; na de laatste byte start de app.
    ///
    /// Te veel bytes is een fout. De stroom is afgebroken als de aanroeper
    /// [`SystemApi::stop_slot`] roept voordat alle bytes er zijn.
    fn stream_image(&mut self, slot: Slot, chunk: &[u8]) -> Result<(), SysError>;

    /// Stopt het slot (killvlag, na `timeout_ms` de stage-2-intrekking) en geeft het vrij.
    ///
    /// Ook voor een half gestroomd slot: dan breekt de kern de stroom af. `Ok`
    /// betekent dat de kern de vrijgave op zich neemt; tot de core uit is meldt
    /// [`SystemApi::slot_status`] hem nog als aan, en wordt hij niet
    /// hergebruikt. Een fout betekent: niet bevestigd.
    fn stop_slot(&mut self, slot: Slot, timeout_ms: u64) -> Result<(), SysError>;

    /// De toestand van een slot.
    fn slot_status(&self, slot: Slot) -> SlotStatus;

    /// Haalt de volgende logregel van de app (hop-ABI outbox) in `buf`, zonder regeleinde.
    ///
    /// Geeft de lengte, of `None` als er niets klaarstaat. Een regel langer dan
    /// `buf` wordt afgekapt.
    fn next_log_line(&mut self, slot: Slot, buf: &mut [u8]) -> Option<usize>;

    /// De grootste partitie die de node nu nog in één stuk kan plaatsen; `None` als hij het niet weet.
    ///
    /// Een som is de verkeerde vraag: een pool van meerdere regio's kan 60 MB
    /// vrij hebben zonder 36 MB in één stuk (GEMETEN 19-08 op een LicheeRV).
    fn pool_largest(&self) -> Option<u64> {
        None
    }
}
