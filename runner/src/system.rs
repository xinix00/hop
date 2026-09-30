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
use core::future::Future;

use crate::store::{StoreStatus, StoreTask};

/// De weigering van een kern (of nep) zonder store-rij.
pub const NO_STORE_QUEUE: &str = "no object store on this node (the kernel has no store queue)";

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

/// De toestand van een slot in het grootboek van de kern
/// (`abi::systemapi::SlotState`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SlotState {
    /// Geen eigenaar.
    Empty,
    /// Gereserveerd; het image stroomt.
    Streaming,
    /// Gedispatcht.
    Running,
    /// Beëindiging onbevestigd; de kern hergebruikt het slot niet.
    Quarantined,
}

/// Een momentopname van één slot (`SLOT_STATUS`, abi `SlotInfo`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SlotStatus {
    /// De toestand in het grootboek van de kern.
    pub state: SlotState,
    /// Of de core(s) van dit slot aan staan.
    pub core_on: bool,
    /// De app-toestand.
    pub app: SlotApp,
    /// De exitcode als de app gestopt is.
    pub exit_code: u64,
    /// De heartbeat-teller van de app.
    pub heartbeat: u64,
    /// Het werkelijke geheugengebruik dat de app meldt; 0 = nog niet gemeld.
    ///
    /// Nog niet in `SLOT_STATUS`: de adapter laat hem 0.
    pub mem_sys: u64,
    /// CPU als percentage van de EIGEN cores (0 tot 100); `None` zolang er geen meetvenster is.
    ///
    /// Nog niet in `SLOT_STATUS`: de adapter laat hem `None`.
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
            state: SlotState::Empty,
            core_on: false,
            app: SlotApp::Empty,
            exit_code: 0,
            heartbeat: 0,
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
///
/// Op de draad (`START_SLOT`, abi `StartReq`) gaan `mem_limit`,
/// `image_size`, `cores`, `pool_cores`, `core_class`, `sharegroup`, de env
/// als `key=val\n`-blob, de poorten (elk nummer één keer; de kern zet ze
/// van de uplink door naar het slot en trekt ze bij de stop in) en `job`.
/// `mounts` gaan mee sinds de ABI van HopOS alpha.11: de kern zet ze in de
/// mount-tabel van de levensduur (gedeelde map onder het lokale pad).
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

/// Hoe een brok image viel (abi `StreamState`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Streamed {
    /// De kern wacht op meer bytes.
    More,
    /// Dit was de laatste byte: de kern plaatste het image en startte de app.
    Placed,
    /// Dit was de laatste byte, maar plaatsen of starten faalde. De kern
    /// ruimde zijn reserveringen zelf op (of hield het slot in quarantaine
    /// bij een onbekende dispatch-uitkomst); de aanroeper ruimt alleen zijn
    /// boekhouding op.
    Failed(SysError),
}

/// De bevoegde system-API van de HopOS-kern, zoals Hop hem ziet.
///
/// Eén-op-één de ops van `abi::systemapi::PrivOp` (0x40 tot en met 0x45;
/// `FLIP`, 0x46, komt hier met de kern-flip); een adapter bouwt en leest de
/// frames. Een start
/// is één fase: [`SystemApi::start_slot`] reserveert partitie en cores voor
/// een image van bekende maat en de KERN kiest het slot; daarna stroomt het
/// image met [`SystemApi::stream_image`] RECHTSTREEKS de partitie in (elke
/// byte landt op het adres waar hij gaat draaien), en na de laatste byte
/// plaatst de kern het en start hij de app. Faalt een start, dan ruimt de
/// kern zijn eigen reserveringen op; de aanroeper ruimt alleen zijn
/// boekhouding op.
///
/// # Asynchroon
///
/// Elke op die de kern raakt, geeft een future. Op HopOS is een op een frame
/// over een TCP-verbinding naar de kern, en die verbinding schuift alleen op
/// als de pomp-taak van de netstack draait, op dezelfde executor. Een
/// synchrone trait dwong de aanroeper om binnen zijn eigen poll rondes van
/// de executor te draaien, en de executor roept zichzelf nooit aan
/// (handboek §4; het kostte een verloren RX-pomp, gemeten 29-09 op QEMU).
/// Nu geeft de eigenaar-taak bij elke wachtende call gewoon de core terug.
///
/// De vorm is `-> impl Future` en niet `async fn`: dezelfde betekenis, maar
/// zonder de lint `async_fn_in_trait`, die waarschuwt dat de aanroeper geen
/// `Send` kan eisen. Dat hoeft hier ook niet: taken verhuizen nooit van core
/// (handboek §4), dus de executor eist geen `Send`. Een implementatie mag
/// gewoon `async fn` schrijven.
///
/// De trait is daarmee niet object-safe (`dyn SystemApi` bestaat niet); de
/// runner is generiek over hem ([`crate::HopRunner<S>`]), en dat was hij al:
/// één kern per node, dus één monomorfe runner en geen vtable.
///
/// [`SystemApi::num_cores`] en [`SystemApi::pool_largest`] blijven
/// synchroon: ze gaan niet over de draad (de cores komen uit de env van het
/// slot) en worden gelezen in synchrone paden (de plaatsing van de leader).
pub trait SystemApi {
    /// Het aantal bruikbare app-cores: de enige capaciteit waar Hop tegen plant.
    fn num_cores(&self) -> u32;

    /// `START_SLOT`: reserveert een slot voor een image van
    /// `spec.image_size` bytes; de kern kiest het slot en geeft het terug.
    fn start_slot(&mut self, spec: &StartSpec) -> impl Future<Output = Result<Slot, SysError>>;

    /// `STREAM_IMAGE`: schrijft de volgende bytes van het image.
    ///
    /// Na de laatste byte zegt het antwoord [`Streamed::Placed`] of
    /// [`Streamed::Failed`]. Een `Err` is een geweigerde brok (te veel bytes,
    /// een onbekend slot); ook dan is de stroom afgebroken en ruimde de kern
    /// op.
    fn stream_image(
        &mut self,
        slot: Slot,
        chunk: &[u8],
    ) -> impl Future<Output = Result<Streamed, SysError>>;

    /// `STOP_SLOT`: stopt het slot (killvlag, na `timeout_ms` de
    /// stage-2-intrekking) en geeft het vrij.
    ///
    /// Ook voor een half gestroomd slot: dan breekt de kern de stroom af. `Ok`
    /// betekent dat de kern de vrijgave op zich neemt; tot de core uit is meldt
    /// [`SystemApi::slot_status`] hem nog als aan, en wordt hij niet
    /// hergebruikt. Een fout betekent: niet bevestigd (quarantaine).
    fn stop_slot(
        &mut self,
        slot: Slot,
        timeout_ms: u64,
    ) -> impl Future<Output = Result<(), SysError>>;

    /// `SLOT_STATUS`: de toestand van een slot.
    ///
    /// `&mut self`: op HopOS is dit een call over de verbinding met de kern
    /// (applib's system-client), en die verbinding is van één eigenaar.
    fn slot_status(&mut self, slot: Slot) -> impl Future<Output = SlotStatus>;

    /// `NEXT_LOG`: haalt de volgende logregel van de app in `buf`, zonder
    /// regeleinde.
    ///
    /// Geeft de lengte, of `None` als er niets klaarstaat. Een regel langer dan
    /// `buf` wordt afgekapt. De kern bewaart per slot een korte ring; wie te
    /// laat komt, mist de oudste regels.
    fn next_log_line(&mut self, slot: Slot, buf: &mut [u8]) -> impl Future<Output = Option<usize>>;

    /// `SET_CLOCK`: zet de klok van de node (Unix-nanoseconden).
    fn set_clock(&mut self, unix_ns: u64) -> impl Future<Output = Result<(), SysError>>;

    /// `NEXT_STORE`: de volgende store-opdracht van een app, of `None` als
    /// er binnen `wait_ms` niets kwam (de kern kapt de wacht af op 5 s).
    ///
    /// De standaard is een kern zonder store-rij: een luide weigering. De
    /// store-kant heeft een eigen verbinding nodig (hij wacht lang), dus hij
    /// hoort niet bij de eigenaar-taak maar bij een eigen taak met een eigen
    /// implementatie.
    fn next_store(
        &mut self,
        wait_ms: u64,
    ) -> impl Future<Output = Result<Option<StoreTask>, SysError>> {
        let _ = wait_ms;
        async { Err(SysError::Refused(String::from(NO_STORE_QUEUE))) }
    }

    /// `STORE_READ`: een stuk van het bestand van `task` (een push) vanaf
    /// `off` in `dst`; geeft de maat van het hele bestand en het aantal
    /// gelezen bytes.
    fn store_read(
        &mut self,
        task: &StoreTask,
        off: u64,
        dst: &mut [u8],
    ) -> impl Future<Output = Result<(u64, usize), SysError>> {
        let _ = (task, off, dst);
        async { Err(SysError::Refused(String::from(NO_STORE_QUEUE))) }
    }

    /// `STORE_WRITE`: `data` op `off` in het bestand van `task` (een pull);
    /// op offset 0 kort de kern het bestand eerst in tot nul (vervangend).
    fn store_write(
        &mut self,
        task: &StoreTask,
        off: u64,
        data: &[u8],
    ) -> impl Future<Output = Result<(), SysError>> {
        let _ = (task, off, data);
        async { Err(SysError::Refused(String::from(NO_STORE_QUEUE))) }
    }

    /// `STORE_DONE`: de uitkomst van opdracht `ticket` naar de wachtende
    /// app: de maat (pull, push: bytes; list: het aantal namen) en de namen
    /// (list) of de fouttekst. Een fout betekent meestal dat de app al weg
    /// is; de opdracht is dan weg.
    fn store_done(
        &mut self,
        ticket: u64,
        status: StoreStatus,
        size: u64,
        payload: &[u8],
    ) -> impl Future<Output = Result<(), SysError>> {
        let _ = (ticket, status, size, payload);
        async { Err(SysError::Refused(String::from(NO_STORE_QUEUE))) }
    }

    /// De grootste partitie die de node nu nog in één stuk kan plaatsen; `None` als hij het niet weet.
    ///
    /// Een som is de verkeerde vraag: een pool van meerdere regio's kan 60 MB
    /// vrij hebben zonder 36 MB in één stuk (GEMETEN 19-08 op een LicheeRV).
    fn pool_largest(&self) -> Option<u64> {
        None
    }
}
