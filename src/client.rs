//! De verzoeken aan een agent: het adres, de sleutel, de ondertekening, en per aanroep een [`Call`].
//!
//! Bezit het adres en de sleutel van één agent. Bezit geen verbinding: een
//! aanroep levert een [`Request`] (methode, URL, koppen, body) en een
//! parser voor het [`Response`]; wie de bytes verstuurt, is het transport
//! (`host`, `hopos`, of een test).

use alloc::string::String;
use alloc::vec::Vec;

use types::{Job, try_push, try_push_str, try_string};

use crate::model::{self, AgentTasks, Applied, Capacity, JobStatus, Leader, Status};
use crate::{AgentInfo, Error, Result};

/// Eén verzoek, klaar voor de draad.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Request {
    /// `GET`, `POST` of `DELETE`.
    pub method: &'static str,
    /// De absolute URL (`http://host:poort/pad`).
    pub url: String,
    /// De koppen: `X-Hop-Auth` als er een sleutel is, `Content-Type` bij een body.
    pub headers: Vec<(String, String)>,
    /// De body; leeg voor GET en DELETE.
    pub body: Vec<u8>,
}

impl Request {
    /// Het pad van de URL (zonder schema, host en query): wat de handtekening dekt.
    pub fn path(&self) -> &str {
        auth::path_of(&self.url)
    }

    /// Een kop op naam (hoofdletterongevoelig).
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

/// Een antwoord zoals het transport het las: de status en de body.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Response {
    /// De HTTP-status.
    pub status: u16,
    /// De body, hoogstens [`crate::MAX_BODY`] bytes.
    pub body: Vec<u8>,
}

/// Eén getypeerde aanroep: het verzoek, en hoe zijn antwoord een `T` wordt.
///
/// Sans-I/O: de aanroeper verstuurt [`Call::request`] zoals hij wil en
/// geeft het antwoord aan [`Call::parse`].
#[derive(Clone, Debug)]
pub struct Call<T> {
    request: Request,
    /// Wat de parser naast het antwoord nodig heeft (de jobnaam van [`Client::job`]).
    arg: String,
    parse: fn(&Response, &str) -> Result<T>,
}

impl<T> Call<T> {
    /// Het verzoek om te versturen.
    pub fn request(&self) -> &Request {
        &self.request
    }

    /// Leest het antwoord op [`Call::request`].
    pub fn parse(&self, resp: &Response) -> Result<T> {
        (self.parse)(resp, &self.arg)
    }

    fn new(request: Request, parse: fn(&Response, &str) -> Result<T>) -> Self {
        Self {
            request,
            arg: String::new(),
            parse,
        }
    }
}

/// Het adres en de sleutel van één agent.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Client {
    /// `http://host:poort` (of `https://`), zonder slash erachter.
    pub base: String,
    /// De clustersleutel; `None` of leeg is de ongeauthenticeerde modus.
    pub api_key: Option<String>,
}

impl Client {
    /// Een client voor `base` (`host:poort` of een `http(s)://`-URL) met `api_key`.
    pub fn new(base: &str, api_key: Option<&str>) -> Result<Self> {
        Ok(Self {
            base: normalize(base)?,
            api_key: match api_key {
                Some(k) => Some(try_string(k)?),
                None => None,
            },
        })
    }

    /// De kop `X-Hop-Auth` voor een verzoek, of `None` zonder sleutel.
    ///
    /// `path` mag een pad of een volledige URL zijn: alleen het pad telt
    /// (de query zit bewust niet in de handtekening). De waarde is
    /// `hex(HMAC-SHA256(key, METHOD\nPATH\nhex(sha256(body))))`.
    pub fn sign(&self, method: &str, path: &str, body: &[u8]) -> Option<(&'static str, String)> {
        let key = self.api_key.as_deref().unwrap_or_default();
        let sig = auth::sign_call(key.as_bytes(), method, path, body)?;
        let hex = core::str::from_utf8(&sig).ok()?;
        Some((auth::AUTH_HEADER, try_string(hex).ok()?))
    }

    /// Een ondertekend verzoek aan `path` op deze agent.
    pub fn request(&self, method: &'static str, path: &str, body: Vec<u8>) -> Result<Request> {
        self.request_at(&self.base, method, path, body)
    }

    /// Een ondertekend verzoek aan `path` op `base` (een ander adres, met dezelfde sleutel).
    fn request_at(
        &self,
        base: &str,
        method: &'static str,
        path: &str,
        body: Vec<u8>,
    ) -> Result<Request> {
        let mut url = String::new();
        url.try_reserve_exact(base.len() + path.len())
            .map_err(|_| Error::OutOfMemory)?;
        url.push_str(base);
        url.push_str(path);
        let mut headers = Vec::new();
        headers
            .try_reserve_exact(2)
            .map_err(|_| Error::OutOfMemory)?;
        if let Some((k, v)) = self.sign(method, path, &body) {
            headers.push((try_string(k)?, v));
        } else if self.api_key.as_deref().is_some_and(|k| !k.is_empty()) {
            // Een sleutel die niet tekende, is een heap die op was.
            return Err(Error::OutOfMemory);
        }
        if !body.is_empty() {
            headers.push((try_string("Content-Type")?, try_string("application/json")?));
        }
        Ok(Request {
            method,
            url,
            headers,
            body,
        })
    }

    fn get<T>(&self, path: &str, parse: fn(&Response, &str) -> Result<T>) -> Result<Call<T>> {
        Ok(Call::new(self.request("GET", path, Vec::new())?, parse))
    }

    /// `GET /v1/status`: het clusteroverzicht.
    pub fn status(&self) -> Result<Call<Status>> {
        self.get("/v1/status", |r, _| model::parse_status(r))
    }

    /// `GET /v1/agents`: de geregistreerde agents.
    pub fn agents(&self) -> Result<Call<Vec<AgentInfo>>> {
        self.get("/v1/agents", |r, _| model::parse_agents(r))
    }

    /// `GET /v1/jobs`: alle jobs.
    pub fn jobs(&self) -> Result<Call<Vec<Job>>> {
        self.get("/v1/jobs", |r, _| model::parse_jobs(r))
    }

    /// Eén job op naam, of `None` als het cluster hem niet kent.
    ///
    /// Leest `GET /v1/jobs` en kiest: v3 heeft geen `GET /v1/jobs/{naam}`.
    pub fn job(&self, name: &str) -> Result<Call<Option<Job>>> {
        let mut call = self.get("/v1/jobs", model::parse_job)?;
        call.arg = try_string(name)?;
        Ok(call)
    }

    /// `GET /v1/jobs/{naam}/status`: de agents en taken van één job.
    ///
    /// `None` is een leader zonder die route (404; hop tot en met
    /// v3.0.0-alpha.10): lees dan [`Client::tasks`] en filter op de job.
    pub fn job_status(&self, name: &str) -> Result<Call<Option<JobStatus>>> {
        let mut path = try_string("/v1/jobs/")?;
        try_push_str(&mut path, name)?;
        try_push_str(&mut path, "/status")?;
        self.get(&path, |r, _| model::parse_job_status(r))
    }

    /// `GET /v1/tasks`: de taken per agent in één aanroep bij de leader.
    ///
    /// `None` is een leader zonder die route (404): vraag dan elke agent
    /// zelf met [`Client::agent_tasks`], zoals de CLI.
    pub fn tasks(&self) -> Result<Call<Option<Vec<AgentTasks>>>> {
        self.get("/v1/tasks", |r, _| model::parse_tasks_by_agent(r))
    }

    /// `GET {endpoint}/tasks`: de taken van één agent, rechtstreeks.
    pub fn agent_tasks(&self, agent: &AgentInfo) -> Result<Call<Vec<types::Task>>> {
        let base = normalize(&agent.endpoint)?;
        let req = self.request_at(&base, "GET", "/tasks", Vec::new())?;
        Ok(Call::new(req, |r, _| model::parse_task_list(r)))
    }

    /// `GET /v1/agents/{id}/capacity`: de capaciteit van een agent via de leader.
    ///
    /// `None` is een leader zonder die doorgifte (404 zonder "agent not
    /// found"): vraag de agent dan zelf met [`Client::agent_capacity`].
    pub fn capacity(&self, agent: &AgentInfo) -> Result<Call<Option<Capacity>>> {
        let mut path = try_string("/v1/agents/")?;
        try_push_str(&mut path, &agent.id)?;
        try_push_str(&mut path, "/capacity")?;
        self.get(&path, |r, _| model::parse_capacity_via_leader(r))
    }

    /// `GET {endpoint}/capacity`: de capaciteit van één agent, rechtstreeks.
    pub fn agent_capacity(&self, agent: &AgentInfo) -> Result<Call<Capacity>> {
        let base = normalize(&agent.endpoint)?;
        let req = self.request_at(&base, "GET", "/capacity", Vec::new())?;
        Ok(Call::new(req, |r, _| model::parse_capacity(r)))
    }

    /// `GET /leader`: wie er leidt, volgens de lokale agent.
    pub fn leader(&self) -> Result<Call<Leader>> {
        self.get("/leader", |r, _| model::parse_leader(r))
    }

    /// `POST /v1/jobs`: maakt de job of werkt hem bij (upsert op naam).
    pub fn apply(&self, job: &Job) -> Result<Call<Applied>> {
        let body = job.to_json()?.into_bytes();
        let req = self.request("POST", "/v1/jobs", body)?;
        Ok(Call::new(req, |r, _| model::parse_applied(r)))
    }

    /// `DELETE /v1/jobs/{naam}`: verwijdert de job en zijn taken.
    pub fn delete(&self, name: &str) -> Result<Call<()>> {
        let mut path = try_string("/v1/jobs/")?;
        try_push_str(&mut path, name)?;
        let req = self.request("DELETE", &path, Vec::new())?;
        Ok(Call::new(req, |r, _| model::check(r).map(|_| ())))
    }

    /// `GET /v1/events`: het verzoek dat de SSE-stroom opent.
    ///
    /// Een stroom heeft geen [`Call`]: zijn body lees je hap voor hap met
    /// [`crate::Events`]. Elke stroom begint met `ping`.
    pub fn events(&self) -> Result<Request> {
        let mut req = self.request("GET", "/v1/events", Vec::new())?;
        try_push(
            &mut req.headers,
            (try_string("Accept")?, try_string("text/event-stream")?),
        )?;
        Ok(req)
    }
}

/// `host:poort` wordt `http://host:poort`; een `http(s)`-URL blijft, zonder slash erachter.
pub(crate) fn normalize(addr: &str) -> Result<String> {
    let a = addr.trim();
    let (scheme, rest) = if let Some(r) = a.strip_prefix("http://") {
        ("http://", r)
    } else if let Some(r) = a.strip_prefix("https://") {
        ("https://", r)
    } else if a.contains("://") {
        return Err(Error::BadUrl);
    } else {
        ("http://", a)
    };
    let host = rest.trim_end_matches('/');
    // Een pad achter de host zou elk verzoekpad veranderen, en dus de
    // handtekening: dat is geen adres van een agent.
    if host.is_empty() || host.starts_with(':') || host.contains(['/', '?', '#']) {
        return Err(Error::BadUrl);
    }
    let mut out = try_string(scheme)?;
    try_push_str(&mut out, host)?;
    Ok(out)
}
