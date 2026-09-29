//! De Docker-API over de unix-socket: pull, create, start, stop, rm, inspect, logs en stats.
//!
//! Bezit alleen het pad van de socket; elk verzoek dialt een verse
//! verbinding en laat hem na het antwoord vallen (geen pool: een daemon op
//! dezelfde machine kost een `connect` bijna niets, en zo is er per verzoek
//! één eigenaar van de verbinding). Zoals Go: leanhttp over de socket, geen
//! `docker`-CLI op de host nodig.

use std::io;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::mpsc::SyncSender;
use std::thread;
use std::time::Duration;

use hostnet::{StdConn, block_on};
use types::json::{self, Value};

use super::process::{Lines, send_line};
use super::runner::Event;
use super::{HostError, Result, TaskSpec};
use crate::Stream;

/// Het voorvoegsel van elke container van Hop; [`super::HostRunner::init`] ruimt ze op.
pub(crate) const PREFIX: &str = "hop-";

/// Hoe lang `docker stop` de container geeft voor SIGKILL, in seconden (Go: `dockerStopTimeout`).
pub(crate) const STOP_SECS: u64 = 10;

/// De termijn van een gewoon API-verzoek (Go: `dockerCommandTimeout`).
const CMD_TIMEOUT: Duration = Duration::from_secs(30);

/// De termijn van een pull; lagen uitpakken kan minuten stil zijn (Go: `dockerPullTimeout`).
const PULL_TIMEOUT: Duration = Duration::from_secs(30 * 60);

/// De termijn van `_ping` (Go: `dockerPingTimeout`).
const PING_TIMEOUT: Duration = Duration::from_secs(2);

/// Het grootste logframe dat we aannemen: 16 MiB (Go: `maxDockerLogFrame`).
pub(crate) const MAX_LOG_FRAME: u32 = 16 << 20;

/// Hoeveel van een foutbody in de melding komt: 64 KiB, zoals Go.
const ERROR_BODY: usize = 64 << 10;

/// De grootste JSON die we van de daemon lezen (de grens van `types::json`).
const JSON_LIMIT: usize = json::MAX_INPUT;

/// De leesbuffer van gestroomde antwoorden.
const CHUNK: usize = 16 << 10;

/// Een client voor de daemon op één socket.
#[derive(Clone, Debug)]
pub(crate) struct Docker {
    socket: PathBuf,
}

/// De naam van de container van een taak.
pub(crate) fn container_name(task_id: &str) -> String {
    format!("{PREFIX}{task_id}")
}

/// Een leanhttp-fout als tekst.
fn why(e: &leanhttp::Error) -> String {
    format!("{e}")
}

impl Docker {
    /// Een client voor `socket`.
    pub(crate) fn new(socket: &Path) -> Self {
        Self {
            socket: socket.to_path_buf(),
        }
    }

    /// Een verse verbinding met termijn `timeout` per lees of schrijf.
    fn connect(&self, op: &'static str, timeout: Option<Duration>) -> Result<StdConn<UnixStream>> {
        let sock = UnixStream::connect(&self.socket).map_err(|e| HostError::DockerIo {
            op,
            why: format!("connect {}: {e}", self.socket.display()),
        })?;
        Ok(StdConn::new(sock, timeout))
    }

    /// Eén verzoek; geeft de status en de body tot `limit` bytes.
    fn call(
        &self,
        op: &'static str,
        method: &str,
        path: &str,
        body: Option<&[u8]>,
        timeout: Duration,
    ) -> Result<(u16, Vec<u8>)> {
        let conn = self.connect(op, Some(timeout))?;
        let url = format!("http://docker{path}");
        let mut header = leanhttp::Header::new();
        if body.is_some() {
            header
                .set("Content-Type", "application/json")
                .map_err(|e| HostError::DockerIo { op, why: why(&e) })?;
        }
        let call = leanhttp::Call {
            method,
            url: &url,
            header,
            body,
            header_timeout: Some(timeout),
            no_follow: true,
            ..leanhttp::Call::default()
        };
        block_on(async {
            let mut resp = leanhttp::send(conn, call).await?;
            let limit = if resp.status < 300 {
                JSON_LIMIT
            } else {
                ERROR_BODY
            };
            let body = resp.read_to_end(limit).await?;
            Ok((resp.status, body))
        })
        .map_err(|e: leanhttp::Error| HostError::DockerIo { op, why: why(&e) })
    }

    /// Een verzoek dat alleen met een van `allowed` slaagt (Go: `consumeDockerResponse`).
    fn expect(
        &self,
        op: &'static str,
        method: &str,
        path: &str,
        body: Option<&[u8]>,
        allowed: &[u16],
    ) -> Result<(u16, Vec<u8>)> {
        let (status, body) = self.call(op, method, path, body, CMD_TIMEOUT)?;
        if allowed.contains(&status) {
            return Ok((status, body));
        }
        Err(HostError::Docker {
            op,
            status,
            body: String::from_utf8_lossy(&body).trim().to_string(),
        })
    }

    /// Of de daemon antwoordt (Go: `DockerPresent`): dat is wat `node.docker` betekent.
    pub(crate) fn ping(&self) -> bool {
        let Ok(conn) = self.connect("docker ping", Some(PING_TIMEOUT)) else {
            return false;
        };
        let call = leanhttp::Call {
            method: "GET",
            url: "http://docker/_ping",
            header_timeout: Some(PING_TIMEOUT),
            ..leanhttp::Call::default()
        };
        block_on(async { leanhttp::send(conn, call).await.map(|r| r.status) }) == Ok(200)
    }

    /// Haalt het image op en leest de hele voortgangsstroom (Go: `consumeDockerPullResponse`).
    ///
    /// Docker antwoordt 200 en meldt een mislukte pull pas ín de stroom
    /// (`{"errorDetail":{"message":...}}`); die fout komt hier boven. Pas
    /// als de stroom dicht is, is de pull klaar: vroeg afkappen annuleert hem.
    pub(crate) fn pull(&self, image: &str) -> Result {
        let op = "docker pull";
        let conn = self.connect(op, Some(PULL_TIMEOUT))?;
        let url = format!(
            "http://docker/images/create?fromImage={}",
            escape(image, b":/@")
        );
        let call = leanhttp::Call {
            method: "POST",
            url: &url,
            header_timeout: Some(PULL_TIMEOUT),
            no_follow: true,
            ..leanhttp::Call::default()
        };
        block_on(async {
            let io = |e: leanhttp::Error| HostError::DockerIo {
                op: "docker pull",
                why: why(&e),
            };
            let mut resp = leanhttp::send(conn, call).await.map_err(io)?;
            if resp.status != 200 {
                let body = resp.read_to_end(ERROR_BODY).await.unwrap_or_default();
                return Err(HostError::Docker {
                    op,
                    status: resp.status,
                    body: String::from_utf8_lossy(&body).trim().to_string(),
                });
            }
            let mut objects = JsonStream::default();
            let mut buf = vec![0u8; CHUNK];
            loop {
                let n = resp.read(&mut buf).await.map_err(io)?;
                if n == 0 {
                    break;
                }
                objects.feed(buf.get(..n).unwrap_or(&[]), &mut pull_error)?;
            }
            objects.finish()
        })
    }

    /// Maakt de container van de taak aan (Go: `Run`, het create-deel).
    pub(crate) fn create(&self, spec: &TaskSpec) -> Result {
        let body = create_body(spec).map_err(|e| HostError::DockerIo {
            op: "docker create",
            why: format!("{e}"),
        })?;
        let path = format!(
            "/containers/create?name={}",
            escape(&container_name(&spec.task_id), b"")
        );
        self.expect(
            "docker create",
            "POST",
            &path,
            Some(body.as_bytes()),
            &[201],
        )?;
        Ok(())
    }

    /// Start de container; 304 is "draaide al".
    pub(crate) fn start(&self, name: &str) -> Result {
        let path = format!("/containers/{}/start", escape(name, b""));
        self.expect("docker start", "POST", &path, None, &[204, 200, 304])?;
        Ok(())
    }

    /// Stopt (met [`STOP_SECS`] genade) en verwijdert de container (Go: `Stop`).
    ///
    /// Een mislukte stop is alleen erg als de geforceerde rm ook faalt; de rm
    /// krijgt een eigen, verse termijn.
    pub(crate) fn stop_and_remove(&self, name: &str) -> Result {
        let path = format!("/containers/{}/stop?t={STOP_SECS}", escape(name, b""));
        let stop = self
            .call(
                "docker stop",
                "POST",
                &path,
                None,
                CMD_TIMEOUT + Duration::from_secs(STOP_SECS),
            )
            .and_then(|(status, body)| match status {
                204 | 304 | 404 => Ok(()),
                _ => Err(HostError::Docker {
                    op: "docker stop",
                    status,
                    body: String::from_utf8_lossy(&body).trim().to_string(),
                }),
            });
        self.remove(name)?;
        if let Err(e) = stop {
            eprintln!("runner: docker-stop: {e} (forced remove succeeded)");
        }
        Ok(())
    }

    /// Verwijdert een container geforceerd; een onbekende is geen fout.
    pub(crate) fn remove(&self, name_or_id: &str) -> Result {
        let path = format!("/containers/{}?force=true", escape(name_or_id, b""));
        self.expect("docker rm", "DELETE", &path, None, &[204, 404])?;
        Ok(())
    }

    /// Of de container draait en zijn exitcode; `None` als hij niet bestaat.
    pub(crate) fn inspect(&self, name: &str) -> Result<Option<(bool, i32)>> {
        let path = format!("/containers/{}/json", escape(name, b""));
        let (status, body) = self.expect("docker inspect", "GET", &path, None, &[200, 404])?;
        if status == 404 {
            return Ok(None);
        }
        let v = parse(&body, "docker inspect")?;
        let state = v.as_object().and_then(|o| o.get("State"));
        let field = |k: &str| state.and_then(Value::as_object).and_then(|s| s.get(k));
        let running = field("Running").and_then(Value::as_bool).unwrap_or(false);
        let code = field("ExitCode")
            .and_then(Value::as_i64)
            .and_then(|c| i32::try_from(c).ok())
            .unwrap_or(0);
        Ok(Some((running, code)))
    }

    /// De ids van alle containers van Hop, ook gestopte (Go: `Cleanup`).
    pub(crate) fn list_hop(&self) -> Result<Vec<String>> {
        let filter = escape(r#"{"name":["hop-"]}"#, b"");
        let path = format!("/containers/json?all=true&filters={filter}");
        let (_, body) = self.expect("docker cleanup list", "GET", &path, None, &[200])?;
        let v = parse(&body, "docker cleanup list")?;
        let mut ids = Vec::new();
        for c in v.as_array().unwrap_or(&[]) {
            let Some(o) = c.as_object() else { continue };
            // Het filter is een substring; alleen een naam die met hop- begint is van ons.
            let ours = o
                .get("Names")
                .and_then(Value::as_array)
                .unwrap_or(&[])
                .iter()
                .filter_map(Value::as_str)
                .any(|n| n.trim_start_matches('/').starts_with(PREFIX));
            if let (true, Some(id)) = (ours, o.get("Id").and_then(Value::as_str)) {
                ids.push(id.to_string());
            }
        }
        Ok(ids)
    }

    /// Eén stats-meting: cumulatieve CPU in ns en geheugen zonder page cache (Go: `Usage`).
    pub(crate) fn stats(&self, name: &str) -> Result<(u64, u64)> {
        let path = format!(
            "/containers/{}/stats?stream=false&one-shot=true",
            escape(name, b"")
        );
        let (_, body) = self.expect("docker stats", "GET", &path, None, &[200])?;
        let v = parse(&body, "docker stats")?;
        let get = |path: &[&str]| -> Option<&Value> {
            let mut cur = &v;
            for k in path {
                cur = cur.as_object()?.get(k)?;
            }
            Some(cur)
        };
        let cpu = get(&["cpu_stats", "cpu_usage", "total_usage"])
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let mut mem = get(&["memory_stats", "usage"])
            .and_then(Value::as_u64)
            .unwrap_or(0);
        // Zoals `docker stats`: page cache telt niet. cgroup v2 zegt
        // inactive_file, v1 total_inactive_file.
        let inactive = get(&["memory_stats", "stats", "inactive_file"])
            .or_else(|| get(&["memory_stats", "stats", "total_inactive_file"]))
            .and_then(Value::as_u64);
        if let Some(i) = inactive.filter(|&i| i < mem) {
            mem -= i;
        }
        Ok((cpu, mem))
    }

    /// Start een thread die de logs van `name` volgt en als regels stuurt.
    ///
    /// De thread bezit zijn verbinding en eindigt als de daemon de stroom
    /// sluit (de container is weg); een fout komt als regel op stderr.
    pub(crate) fn follow_logs(
        &self,
        name: String,
        task: String,
        generation: u64,
        tx: SyncSender<Event>,
    ) -> io::Result<()> {
        let docker = self.clone();
        thread::Builder::new()
            .name(format!("hop-dlog-{task}"))
            .spawn(move || {
                let mut sink = |stream: Stream, line: String| {
                    send_line(&tx, &task, generation, stream, line);
                };
                if let Err(msg) = docker.stream_logs(&name, &mut sink) {
                    sink(Stream::Stderr, msg);
                }
                let _ = tx.send(Event::Eof { task, generation });
            })?;
        Ok(())
    }

    /// Volgt `GET /containers/<naam>/logs` en demultiplext de frames naar `sink`.
    pub(crate) fn stream_logs(
        &self,
        name: &str,
        sink: &mut dyn FnMut(Stream, String),
    ) -> core::result::Result<(), String> {
        // Geen leestermijn: een stille container is geen fout.
        let conn = self
            .connect("docker logs", None)
            .map_err(|e| e.to_string())?;
        let url = format!(
            "http://docker/containers/{}/logs?follow=true&stdout=true&stderr=true",
            escape(name, b"")
        );
        let call = leanhttp::Call {
            method: "GET",
            url: &url,
            header_timeout: Some(CMD_TIMEOUT),
            no_follow: true,
            ..leanhttp::Call::default()
        };
        block_on(async {
            let mut resp = leanhttp::send(conn, call).await.map_err(|e| why(&e))?;
            if resp.status != 200 {
                let body = resp.read_to_end(ERROR_BODY).await.unwrap_or_default();
                return Err(format!(
                    "docker logs failed ({}): {}",
                    resp.status,
                    String::from_utf8_lossy(&body).trim()
                ));
            }
            let mut demux = Demux::default();
            let mut buf = vec![0u8; CHUNK];
            loop {
                let n = match resp.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => n,
                };
                demux.feed(buf.get(..n).unwrap_or(&[]), sink)?;
            }
            demux.finish(sink);
            Ok(())
        })
    }
}

/// Leest JSON van de daemon.
fn parse(body: &[u8], op: &'static str) -> Result<Value> {
    json::parse(body).map_err(|e| HostError::DockerIo {
        op,
        why: format!("invalid JSON: {e}"),
    })
}

/// De fout in één object van de pull-stroom, als die er is.
fn pull_error(obj: &[u8]) -> Result {
    let v = parse(obj, "docker pull response")?;
    let Some(o) = v.as_object() else {
        return Ok(());
    };
    let detail = o
        .get("errorDetail")
        .and_then(Value::as_object)
        .and_then(|d| d.get("message"))
        .and_then(Value::as_str)
        .unwrap_or("");
    let msg = if detail.is_empty() {
        o.get("error").and_then(Value::as_str).unwrap_or("")
    } else {
        detail
    };
    if msg.is_empty() {
        Ok(())
    } else {
        Err(HostError::DockerPull(msg.to_string()))
    }
}

/// Snijdt een stroom van JSON-objecten achter elkaar in losse objecten (Go: `json.Decoder`).
#[derive(Debug, Default)]
pub(crate) struct JsonStream {
    buf: Vec<u8>,
    depth: u32,
    in_str: bool,
    esc: bool,
}

impl JsonStream {
    /// Voert bytes; elk compleet object gaat naar `on_obj`.
    pub(crate) fn feed(&mut self, data: &[u8], on_obj: &mut dyn FnMut(&[u8]) -> Result) -> Result {
        let bad = |why: &str| HostError::DockerIo {
            op: "docker pull response",
            why: why.to_string(),
        };
        for &b in data {
            if self.depth == 0 && self.buf.is_empty() {
                if b.is_ascii_whitespace() {
                    continue;
                }
                if b != b'{' {
                    return Err(bad("expected a JSON object"));
                }
            }
            if self.buf.len() >= JSON_LIMIT {
                return Err(bad("progress object too large"));
            }
            self.buf.push(b);
            if self.in_str {
                if self.esc {
                    self.esc = false;
                } else if b == b'\\' {
                    self.esc = true;
                } else if b == b'"' {
                    self.in_str = false;
                }
                continue;
            }
            match b {
                b'"' => self.in_str = true,
                b'{' | b'[' => self.depth += 1,
                b'}' | b']' => {
                    self.depth = self.depth.saturating_sub(1);
                    if self.depth == 0 {
                        on_obj(&self.buf)?;
                        self.buf.clear();
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// Een half object aan het einde is een afgekapte stroom.
    pub(crate) fn finish(&self) -> Result {
        if self.buf.is_empty() {
            Ok(())
        } else {
            Err(HostError::DockerIo {
                op: "docker pull response",
                why: "unexpected EOF".to_string(),
            })
        }
    }
}

/// Demultiplext Docker's logframes: 8 bytes kop (`[stroom, 0, 0, 0, lengte-BE]`), dan de lading.
#[derive(Debug, Default)]
pub(crate) struct Demux {
    head: [u8; 8],
    filled: usize,
    left: u32,
    stderr: bool,
    out: Lines,
    err: Lines,
}

impl Demux {
    /// Voert bytes; regels gaan per stroom naar `sink`. Een kapotte kop of
    /// een frame boven [`MAX_LOG_FRAME`] stopt de stroom, zonder de lading te alloceren.
    pub(crate) fn feed(
        &mut self,
        mut data: &[u8],
        sink: &mut dyn FnMut(Stream, String),
    ) -> core::result::Result<(), String> {
        while !data.is_empty() {
            if self.left == 0 {
                let take = (8 - self.filled).min(data.len());
                if let (Some(dst), Some(src)) = (
                    self.head.get_mut(self.filled..self.filled + take),
                    data.get(..take),
                ) {
                    dst.copy_from_slice(src);
                }
                self.filled += take;
                data = data.get(take..).unwrap_or(&[]);
                if self.filled < 8 {
                    return Ok(());
                }
                self.filled = 0;
                let [kind, a, b, c, s0, s1, s2, s3] = self.head;
                if !(kind == 1 || kind == 2) || a != 0 || b != 0 || c != 0 {
                    return Err("docker logs: invalid multiplex header".to_string());
                }
                let size = u32::from_be_bytes([s0, s1, s2, s3]);
                if size > MAX_LOG_FRAME {
                    return Err(format!("docker logs: frame too large: {size} bytes"));
                }
                self.left = size;
                self.stderr = kind == 2;
                continue;
            }
            let take = data
                .len()
                .min(usize::try_from(self.left).unwrap_or(usize::MAX));
            let (chunk, rest) = data.split_at(take);
            let (lines, stream) = if self.stderr {
                (&mut self.err, Stream::Stderr)
            } else {
                (&mut self.out, Stream::Stdout)
            };
            lines.feed(chunk, &mut |l| sink(stream, l));
            self.left -= u32::try_from(take).unwrap_or(self.left);
            data = rest;
        }
        Ok(())
    }

    /// Geeft de onafgemaakte laatste regels.
    pub(crate) fn finish(&mut self, sink: &mut dyn FnMut(Stream, String)) {
        self.out.finish(&mut |l| sink(Stream::Stdout, l));
        self.err.finish(&mut |l| sink(Stream::Stderr, l));
    }
}

/// Percent-codeert `s` voor een URL; `keep` blijft staan naast de ongereserveerde tekens.
pub(crate) fn escape(s: &str, keep: &[u8]) -> String {
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        if b.is_ascii_alphanumeric() || b"-._~".contains(&b) || keep.contains(&b) {
            out.push(char::from(b));
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// Voegt `s` als JSON-string toe.
fn push_str(out: &mut String, s: &str) -> types::Result {
    json::write_string(s, out)
}

/// De body van `POST /containers/create` (Go: `createRequest`), met Go's `omitempty`.
pub(crate) fn create_body(spec: &TaskSpec) -> types::Result<String> {
    // De env zoals exec: de job, dan ER_PORT_*, dan ER_ATTR_*; de laatste wint.
    let mut env = spec.env.clone();
    crate::port_env_vars(&spec.ports, &mut env);
    crate::attr_env_vars(&spec.node_attrs, &mut env);

    let mut b = String::from("{\"Image\":");
    push_str(&mut b, &spec.image)?;
    if !env.is_empty() {
        b.push_str(",\"Env\":[");
        for (i, (k, v)) in env.iter().enumerate() {
            if i > 0 {
                b.push(',');
            }
            push_str(&mut b, &format!("{k}={v}"))?;
        }
        b.push(']');
    }
    if !spec.command.is_empty() {
        b.push_str(",\"Cmd\":[\"/bin/sh\",\"-c\",");
        push_str(&mut b, &spec.command)?;
        b.push(']');
    }
    // Containerpoort = hostpoort, zoals Go.
    let ports: Vec<u16> = spec.ports.values().copied().collect();
    if !ports.is_empty() {
        b.push_str(",\"ExposedPorts\":{");
        for (i, p) in ports.iter().enumerate() {
            let sep = if i > 0 { "," } else { "" };
            b.push_str(&format!("{sep}\"{p}/tcp\":{{}}"));
        }
        b.push('}');
    }
    b.push_str(",\"HostConfig\":{");
    let mut fields = Vec::new();
    if !ports.is_empty() {
        let binds: Vec<String> = ports
            .iter()
            .map(|p| format!("\"{p}/tcp\":[{{\"HostPort\":\"{p}\"}}]"))
            .collect();
        fields.push(format!("\"PortBindings\":{{{}}}", binds.join(",")));
    }
    if !spec.volumes.is_empty() {
        let mut s = String::from("\"Binds\":[");
        for (i, (host, inner)) in spec.volumes.iter().enumerate() {
            if i > 0 {
                s.push(',');
            }
            push_str(&mut s, &format!("{host}:{inner}"))?;
        }
        s.push(']');
        fields.push(s);
    }
    if spec.memory_limit > 0 {
        fields.push(format!("\"Memory\":{}", spec.memory_limit));
    }
    if spec.cpu_shares > 0 {
        fields.push(format!("\"CpuShares\":{}", spec.cpu_shares));
    }
    b.push_str(&fields.join(","));
    b.push_str("}}");
    Ok(b)
}
