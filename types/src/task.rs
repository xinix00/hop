//! De taak (een draaiende instantie van een job) en de agent (een node die taken draait).

use alloc::string::String;
use alloc::vec::Vec;

use crate::de::{self, ObjectBuilder};
use crate::json::{self, Number, Value};
use crate::{Error, Map, Name, Result, Time, TryClone};

/// De staat van een taak.
///
/// Elke staat telt mee voor capaciteit: aanwezigheid is de maat, nooit de
/// staat. Een bewust gestopte taak bestaat niet meer; er is geen `stopped`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TaskState {
    /// Aangenomen, capaciteit gereserveerd, wacht op een downloadbeurt.
    ///
    /// Deze en [`TaskState::Downloading`] maken de startfase zichtbaar: een
    /// taak heette vanaf zijn geboorte "running", en op HopOS kan de download
    /// minuten duren (07-08: tien minuten "running, 0% cpu" terwijl er niets
    /// draaide).
    Queued,
    /// De bytes stromen binnen; voortgang in `downloaded`/`image_size`.
    Downloading,
    /// De app draait echt.
    #[default]
    Running,
    /// Wordt gestopt; daarna verdwijnt het record.
    Stopping,
    /// Gecrasht, OOM, te vaak herstart.
    Failed,
    /// Een systeembewoner van de node: de kern (slot 0) of Hop zelf.
    ///
    /// Alleen in het antwoord van `/v1/tasks`, gemaakt uit de heartbeat
    /// ([`Agent::system_tasks`]); nooit in de boeken van een agent of de
    /// leader, dus nooit geplaatst, herstart, gestopt of geteld.
    System,
}

impl TaskState {
    /// De naam op de draad.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Downloading => "downloading",
            Self::Running => "running",
            Self::Stopping => "stopping",
            Self::Failed => "failed",
            Self::System => "system",
        }
    }

    /// Leest de naam op de draad.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "queued" => Some(Self::Queued),
            "downloading" => Some(Self::Downloading),
            "running" => Some(Self::Running),
            "stopping" => Some(Self::Stopping),
            "failed" => Some(Self::Failed),
            "system" => Some(Self::System),
            _ => None,
        }
    }
}

/// Een draaiende instantie van een job.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Task {
    /// Uniek, en nieuw bij elke herstart.
    pub id: String,
    /// De job waar deze taak bij hoort (jobs hebben geen apart id).
    pub job_name: String,
    /// De runner die deze taak beheert (`"exec"`, `"docker"`, `"hop"`).
    pub driver: String,
    /// Het Docker-image (alleen voor Docker).
    pub image: String,
    /// Poortnaam naar hostpoort.
    pub ports: Map<u16>,
    /// Proces-id (Docker: 0; HopOS: de index van het primaire slot).
    pub pid: i64,
    /// De staat.
    pub state: TaskState,
    /// Wanneer de taak startte.
    pub started_at: Time,
    /// Hoe vaak herstart.
    pub restart_count: i64,
    /// De laatste crash (drijft het herstartvenster).
    pub last_failed_at: Time,
    /// Wanneer de volgende herstartpoging loopt; nul als hij draait of opgaf.
    pub next_restart_at: Time,
    /// Uit de job gekopieerd, voor de capaciteitsboekhouding.
    pub cpu_shares: i64,
    /// Uit de job gekopieerd, voor de capaciteitsboekhouding.
    pub memory_limit: u64,
    /// Actueel CPU-gebruik, gemeten door de agent.
    pub cpu_percent: f64,
    /// Het aantal cores waar `cpu_percent` op slaat (de cores van het slot);
    /// 0 is onbekend.
    pub cores: u64,
    /// De logische core waarop de taak draait (de primaire core van zijn
    /// slot); 0 is de OS-core en geldig, dus onbekend is `None`.
    pub core: Option<u64>,
    /// Actueel geheugengebruik, gemeten door de agent.
    pub mem_percent: f64,
    /// Bytes binnen tijdens `downloading`.
    pub downloaded: u64,
    /// Totale image-maat tijdens `downloading`.
    pub image_size: u64,
}

impl Task {
    /// Leest een taak uit een JSON-waarde (laks: onbekende sleutels tellen niet).
    pub fn from_value(v: &Value) -> Result<Self> {
        let obj = de::object(v, "task")?;
        let mut t = Self::default();
        for (k, v) in obj.iter() {
            if v.is_null() {
                continue;
            }
            match k {
                "id" => t.id = de::string(v, k)?,
                "job_name" => t.job_name = de::string(v, k)?,
                "driver" => t.driver = de::string(v, k)?,
                "image" => t.image = de::string(v, k)?,
                "ports" => {
                    let obj = de::object(v, k)?;
                    for (name, p) in obj.iter() {
                        let n = de::uint(p, k)?;
                        let port = u16::try_from(n).map_err(|_| Error::OutOfRange {
                            field: Name::new(k),
                        })?;
                        t.ports.insert(crate::try_string(name)?, port)?;
                    }
                }
                "pid" => t.pid = de::int(v, k)?,
                "state" => {
                    let s = v.as_str().unwrap_or_default();
                    t.state = TaskState::parse(s).ok_or(Error::Invalid {
                        field: Name::new(k),
                        why: "unknown task state",
                    })?;
                }
                "started_at" => t.started_at = time(v, k)?,
                "restart_count" => t.restart_count = de::int(v, k)?,
                "last_failed_at" => t.last_failed_at = time(v, k)?,
                "next_restart_at" => t.next_restart_at = time(v, k)?,
                "cpu_shares" => t.cpu_shares = de::int(v, k)?,
                "memory_limit" => t.memory_limit = de::uint(v, k)?,
                "cpu_percent" => t.cpu_percent = de::float(v, k)?,
                "cores" => t.cores = de::uint(v, k)?,
                "core" => t.core = Some(de::uint(v, k)?),
                "mem_percent" => t.mem_percent = de::float(v, k)?,
                "downloaded_bytes" => t.downloaded = de::uint(v, k)?,
                "image_size_bytes" => t.image_size = de::uint(v, k)?,
                _ => {}
            }
        }
        Ok(t)
    }

    /// De taak als JSON-waarde, veld voor veld zoals Go hem schreef.
    pub fn to_value(&self) -> Result<Value> {
        let mut o = ObjectBuilder::new();
        o.str("id", &self.id)?;
        o.str("job_name", &self.job_name)?;
        o.str("driver", &self.driver)?;
        o.str_opt("image", &self.image)?;
        let mut p = ObjectBuilder::new();
        for (k, v) in self.ports.iter() {
            p.field(k, Value::uint(u64::from(*v)))?;
        }
        o.field("ports", p.build())?;
        o.field("pid", Value::int(self.pid))?;
        o.str("state", self.state.as_str())?;
        // Go's omitempty heeft geen effect op time.Time: alle drie staan er altijd.
        o.field("started_at", time_value(self.started_at)?)?;
        o.field("restart_count", Value::int(self.restart_count))?;
        o.field("last_failed_at", time_value(self.last_failed_at)?)?;
        o.field("next_restart_at", time_value(self.next_restart_at)?)?;
        o.int_opt("cpu_shares", self.cpu_shares)?;
        o.uint_opt("memory_limit", self.memory_limit)?;
        o.field(
            "cpu_percent",
            Value::Number(Number::Float(self.cpu_percent)),
        )?;
        o.uint_opt("cores", self.cores)?;
        if let Some(c) = self.core {
            o.field("core", Value::uint(c))?;
        }
        o.field(
            "mem_percent",
            Value::Number(Number::Float(self.mem_percent)),
        )?;
        o.uint_opt("downloaded_bytes", self.downloaded)?;
        o.uint_opt("image_size_bytes", self.image_size)?;
        Ok(o.build())
    }
}

/// Het slot van de kern: [`Agent::system_tasks`] zet het als pid van `kern`.
pub const KERN_SLOT: i64 = 0;
/// Het slot dat de kern Hop geeft: de pid van `hop` in [`Agent::system_tasks`].
pub const HOP_SLOT: i64 = 1;

/// Het gebruik van een systeembewoner van een node: de kern of Hop zelf.
///
/// Leeg is "niet gemeten": een host-node, een oude agent, of een kern die
/// slot 0 nog niet meldt.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct SysUsage {
    /// CPU als procent van zijn eigen cores; `None` tot er twee standen zijn.
    pub cpu_percent: Option<f64>,
    /// Het geheugen in gebruik in bytes; 0 is onbekend.
    pub mem_bytes: u64,
    /// Zijn RAM in bytes, de noemer van het geheugenprocent; 0 is onbekend.
    pub ram_bytes: u64,
    /// De logische core van zijn slot (de kern: de OS-core, 0); `None` is onbekend.
    pub core: Option<u64>,
}

impl SysUsage {
    /// Of er iets gemeten is.
    pub fn is_known(&self) -> bool {
        self.cpu_percent.is_some() || self.mem_bytes != 0
    }

    /// Het geheugen als procent van zijn RAM, met één decimaal; 0 zonder RAM.
    pub fn mem_percent(&self) -> f64 {
        if self.ram_bytes == 0 {
            return 0.0;
        }
        let tenths =
            u128::from(self.mem_bytes.min(self.ram_bytes)) * 1000 / u128::from(self.ram_bytes);
        // Hoogstens 1000: exact als f64.
        tenths as f64 / 10.0
    }
}

/// Wat een heartbeat naast de identiteit meldt: telemetrie, geen
/// scheduling-input. Op de draad en in [`Agent`] plat, met het voorvoegsel
/// `kern_` of `hop_`; een veld zonder meting ontbreekt.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct Telemetry {
    /// De CPU-temperatuur in milligraden Celsius; 0 is onbekend.
    ///
    /// Eén getal per node, en dat is bewust: wie meer sensoren heeft meldt
    /// de heetste, want dát is het getal waarop je ingrijpt.
    pub temp_milli_c: i64,
    /// De kern (slot 0).
    pub kern: SysUsage,
    /// Hop zelf (zijn eigen slot).
    pub hop: SysUsage,
}

/// De sleutels van [`Telemetry::kern`] en [`Telemetry::hop`]: cpu, geheugen, RAM, core.
const USAGE_KEYS: [[&str; 4]; 2] = [
    [
        "kern_cpu_percent",
        "kern_mem_bytes",
        "kern_ram_bytes",
        "kern_core",
    ],
    [
        "hop_cpu_percent",
        "hop_mem_bytes",
        "hop_ram_bytes",
        "hop_core",
    ],
];

impl Telemetry {
    /// Leest één sleutel; `false` als hij hier niet bij hoort.
    fn read(&mut self, k: &str, v: &Value) -> Result<bool> {
        if k == "temp_milli_c" {
            self.temp_milli_c = de::int(v, k)?;
            return Ok(true);
        }
        for (u, [cpu, mem, ram, core]) in
            [&mut self.kern, &mut self.hop].into_iter().zip(USAGE_KEYS)
        {
            if k == cpu {
                u.cpu_percent = Some(de::float(v, k)?);
            } else if k == mem {
                u.mem_bytes = de::uint(v, k)?;
            } else if k == ram {
                u.ram_bytes = de::uint(v, k)?;
            } else if k == core {
                u.core = Some(de::uint(v, k)?);
            } else {
                continue;
            }
            return Ok(true);
        }
        Ok(false)
    }

    /// Leest de telemetrie uit een object (een heartbeat); de rest telt niet.
    pub fn from_value(v: &Value) -> Result<Self> {
        let mut t = Self::default();
        for (k, v) in de::object(v, "heartbeat")?.iter() {
            if !v.is_null() {
                t.read(k, v)?;
            }
        }
        Ok(t)
    }

    /// Schrijft de gemeten velden in `o`.
    pub fn write(&self, o: &mut ObjectBuilder) -> Result {
        o.int_opt("temp_milli_c", self.temp_milli_c)?;
        for (u, [cpu, mem, ram, core]) in [&self.kern, &self.hop].into_iter().zip(USAGE_KEYS) {
            if let Some(c) = u.cpu_percent {
                o.field(cpu, Value::Number(Number::Float(c)))?;
            }
            o.uint_opt(mem, u.mem_bytes)?;
            o.uint_opt(ram, u.ram_bytes)?;
            if let Some(c) = u.core {
                o.field(core, Value::uint(c))?;
            }
        }
        Ok(())
    }
}

/// Een bij de leader geregistreerde agent: de identiteit van een node.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Agent {
    /// Uniek, en blijvend over herstarts (`data/node-id`).
    pub id: String,
    /// Het HTTP-adres (`http://ip:port`).
    pub endpoint: String,
    /// De versie van de agent.
    pub version: String,
    /// De laatste heartbeat.
    pub last_seen: Time,
    /// Wat de laatste heartbeat meldde.
    pub telemetry: Telemetry,
}

impl Agent {
    /// Leest een agent uit een JSON-waarde.
    pub fn from_value(v: &Value) -> Result<Self> {
        let obj = de::object(v, "agent")?;
        let mut a = Self::default();
        for (k, v) in obj.iter() {
            if v.is_null() {
                continue;
            }
            match k {
                "id" => a.id = de::string(v, k)?,
                "endpoint" => a.endpoint = de::string(v, k)?,
                "version" => a.version = de::string(v, k)?,
                "last_seen" => a.last_seen = time(v, k)?,
                _ => {
                    a.telemetry.read(k, v)?;
                }
            }
        }
        Ok(a)
    }

    /// De agent als JSON-waarde.
    pub fn to_value(&self) -> Result<Value> {
        let mut o = ObjectBuilder::new();
        o.str("id", &self.id)?;
        o.str("endpoint", &self.endpoint)?;
        o.str("version", &self.version)?;
        o.field("last_seen", time_value(self.last_seen)?)?;
        self.telemetry.write(&mut o)?;
        Ok(o.build())
    }

    /// De systeemtaken van deze node, `kern` en `hop`, uit de laatste
    /// heartbeat; alleen wat gemeten is.
    ///
    /// Ze staan in geen enkel boek (zie [`TaskState::System`]): de leader
    /// voegt ze toe aan het antwoord van `/v1/tasks`, verder niets.
    pub fn system_tasks(&self) -> Result<Vec<Task>> {
        let mut out = Vec::new();
        for (name, pid, u) in [
            ("kern", KERN_SLOT, &self.telemetry.kern),
            ("hop", HOP_SLOT, &self.telemetry.hop),
        ] {
            if !u.is_known() {
                continue;
            }
            let t = Task {
                id: crate::try_string(name)?,
                job_name: crate::try_string(name)?,
                driver: crate::try_string("hop")?,
                pid,
                state: TaskState::System,
                cpu_percent: u.cpu_percent.unwrap_or(0.0),
                cores: 1,
                core: u.core,
                mem_percent: u.mem_percent(),
                ..Task::default()
            };
            crate::try_push(&mut out, t)?;
        }
        Ok(out)
    }

    /// De agent als compacte JSON-tekst.
    pub fn to_json(&self) -> Result<String> {
        json::to_string(&self.to_value()?)
    }
}

impl TryClone for Agent {
    fn try_clone(&self) -> Result<Self> {
        Ok(Self {
            id: self.id.try_clone()?,
            endpoint: self.endpoint.try_clone()?,
            version: self.version.try_clone()?,
            last_seen: self.last_seen,
            telemetry: self.telemetry,
        })
    }
}

/// Een tijdstip uit een RFC 3339-string.
pub(crate) fn time(v: &Value, field: &str) -> Result<Time> {
    let s = v.as_str().ok_or(Error::WrongType {
        field: Name::new(field),
        want: "an RFC 3339 string",
    })?;
    Time::parse_rfc3339(s).map_err(|_| Error::Invalid {
        field: Name::new(field),
        why: "not an RFC 3339 timestamp",
    })
}

/// Een tijdstip als RFC 3339-string.
pub(crate) fn time_value(t: Time) -> Result<Value> {
    let mut s = String::new();
    t.write_rfc3339(&mut s)?;
    Ok(Value::String(s))
}
