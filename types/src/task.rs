//! De taak (een draaiende instantie van een job) en de agent (een node die taken draait).

use alloc::string::String;

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
        o.field(
            "mem_percent",
            Value::Number(Number::Float(self.mem_percent)),
        )?;
        o.uint_opt("downloaded_bytes", self.downloaded)?;
        o.uint_opt("image_size_bytes", self.image_size)?;
        Ok(o.build())
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
    /// De CPU-temperatuur in milligraden Celsius; 0 is onbekend.
    ///
    /// Eén getal per node, en dat is bewust: wie meer sensoren heeft meldt
    /// de heetste, want dát is het getal waarop je ingrijpt.
    pub temp_milli_c: i64,
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
                "temp_milli_c" => a.temp_milli_c = de::int(v, k)?,
                _ => {}
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
        o.int_opt("temp_milli_c", self.temp_milli_c)?;
        Ok(o.build())
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
            temp_milli_c: self.temp_milli_c,
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
