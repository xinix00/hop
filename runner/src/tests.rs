//! De tests uit `hopos_test.go`, `hopos_stream_test.go`, `hopos_adopt_logs_test.go`
//! en `logs_test.go`, zelfde namen en bedoeling.

use super::*;
use alloc::collections::BTreeMap;
use alloc::string::ToString;
use alloc::vec::Vec;
use std::collections::VecDeque;

/// Drijft een future van de runner tot hij klaar is.
///
/// Geen geneste executor-ronde: in een host-test is er geen executor, en
/// de nep-kern hieronder antwoordt altijd bij de eerste poll.
fn bl<F: core::future::Future>(f: F) -> F::Output {
    let mut f = core::pin::pin!(f);
    let mut cx = core::task::Context::from_waker(core::task::Waker::noop());
    match f.as_mut().poll(&mut cx) {
        core::task::Poll::Ready(v) => v,
        core::task::Poll::Pending => panic!("de nep-kern wacht nooit"),
    }
}

/// Eén slot van de nep-kern.
#[derive(Default, Clone)]
struct FakeSlot {
    image: Vec<u8>,
    size: u64,
    spec: Option<StartSpec>,
    core_on: bool,
    exited: bool,
    streaming: bool,
    quarantined: bool,
    exit_code: u64,
    logs: VecDeque<String>,
}

/// Een kern in het geheugen: het spiegelbeeld van `kern/slots`.
#[derive(Default)]
struct FakeSys {
    slots: BTreeMap<u32, FakeSlot>,
    num: u32,
    classes: BTreeMap<u32, String>,
    cage: String,
    stop_err: bool,
    /// De plaatsing na de laatste byte faalt (een kapot image).
    place_err: bool,
    stops: Vec<u32>,
    clock: u64,
}

impl FakeSys {
    fn new(num: u32) -> Self {
        let mut classes = BTreeMap::new();
        for i in 1..=num {
            let c = match i {
                1..=3 => "small",
                4..=7 => "mid",
                _ => "big",
            };
            classes.insert(i, c.to_string());
        }
        Self {
            num,
            classes,
            ..Default::default()
        }
    }
    fn slot(&self, i: u32) -> &FakeSlot {
        self.slots.get(&i).unwrap()
    }
    fn live(&mut self, i: u32) {
        self.slots.entry(i).or_default().core_on = true;
    }
}

impl SystemApi for FakeSys {
    fn num_cores(&self) -> u32 {
        self.num
    }
    async fn start_slot(&mut self, spec: &StartSpec) -> Result<Slot, SysError> {
        if spec.cores.max(spec.pool_cores) > self.num {
            return Err(SysError::NoCapacity("insufficient physical cores".into()));
        }
        if !spec.core_class.is_empty() && !self.classes.values().any(|c| *c == spec.core_class) {
            return Err(SysError::NoCapacity(alloc::format!(
                "no physical cores of class {:?}",
                spec.core_class
            )));
        }
        // De kern kiest: het laagste slot zonder eigenaar. Een levende,
        // stromende of in quarantaine gehouden kooi is bezet.
        let free = |i: &u32| {
            self.slots
                .get(i)
                .is_none_or(|s| !s.core_on && !s.streaming && !s.quarantined)
        };
        let slot = (1..=64)
            .find(free)
            .ok_or(SysError::NoCapacity("no free slot".into()))?;
        self.slots.insert(
            slot,
            FakeSlot {
                size: spec.image_size,
                spec: Some(spec.clone()),
                streaming: true,
                ..Default::default()
            },
        );
        Ok(Slot(slot))
    }
    async fn stream_image(&mut self, slot: Slot, chunk: &[u8]) -> Result<Streamed, SysError> {
        let place_err = self.place_err;
        let s = self
            .slots
            .get_mut(&slot.0)
            .filter(|s| s.streaming)
            .ok_or(SysError::Refused("no stream".into()))?;
        s.image.extend_from_slice(chunk);
        if (s.image.len() as u64) < s.size {
            return Ok(Streamed::More);
        }
        s.streaming = false;
        if place_err {
            return Ok(Streamed::Failed(SysError::Refused(
                "placement: no PT_LOAD segments".into(),
            )));
        }
        s.core_on = true;
        s.logs.push_back("app leeft".into());
        Ok(Streamed::Placed)
    }
    async fn stop_slot(&mut self, slot: Slot, _timeout_ms: u64) -> Result<(), SysError> {
        self.stops.push(slot.0);
        if self.stop_err {
            if let Some(s) = self.slots.get_mut(&slot.0) {
                s.quarantined = true;
            }
            return Err(SysError::NotConfirmed);
        }
        if let Some(s) = self.slots.get_mut(&slot.0) {
            s.core_on = false;
            s.streaming = false;
            s.exited = true;
        }
        Ok(())
    }
    async fn slot_status(&mut self, slot: Slot) -> SlotStatus {
        let mut st = SlotStatus::empty();
        if let Some(s) = self.slots.get(&slot.0) {
            st.core_on = s.core_on;
            st.state = if s.quarantined {
                SlotState::Quarantined
            } else if s.streaming {
                SlotState::Streaming
            } else if s.core_on {
                SlotState::Running
            } else {
                SlotState::Empty
            };
            st.app = if s.core_on {
                SlotApp::Ready
            } else if s.exited {
                SlotApp::Exited
            } else {
                SlotApp::Empty
            };
            st.exit_code = s.exit_code;
            st.cage = self.cage.clone();
        }
        st
    }
    async fn next_log_line(&mut self, slot: Slot, buf: &mut [u8]) -> Option<usize> {
        let line = self.slots.get_mut(&slot.0)?.logs.pop_front()?;
        let n = line.len().min(buf.len());
        buf[..n].copy_from_slice(&line.as_bytes()[..n]);
        Some(n)
    }
    async fn set_clock(&mut self, unix_ns: u64) -> Result<(), SysError> {
        self.clock = unix_ns;
        Ok(())
    }
}

/// Een hop-job zoals de agent hem na de artifact-resolutie doorgeeft.
struct Job {
    image: String,
    artifacts: usize,
    extract: String,
    cpu_shares: u32,
    memory_limit: u64,
    env: BTreeMap<String, String>,
    tags: BTreeMap<String, String>,
    volumes: BTreeMap<String, String>,
    ports: BTreeMap<String, u16>,
}

fn hop_job() -> Job {
    Job {
        image: String::new(),
        artifacts: 1,
        extract: String::new(),
        cpu_shares: 0,
        memory_limit: 64 << 20,
        env: [("BUCKET".to_string(), "hop-apps".to_string())].into(),
        tags: BTreeMap::new(),
        volumes: BTreeMap::new(),
        ports: BTreeMap::new(),
    }
}

fn req<'a>(id: &'a str, j: &'a Job) -> StartRequest<'a> {
    StartRequest {
        task_id: id,
        job_name: "demo",
        image: &j.image,
        artifacts: j.artifacts,
        extract: &j.extract,
        cpu_shares: j.cpu_shares,
        memory_limit: j.memory_limit,
        env: &j.env,
        tags: &j.tags,
        volumes: &j.volumes,
        ports: &j.ports,
    }
}

fn runner(num: u32, attrs: &[(&str, &str)]) -> HopRunner<FakeSys> {
    let attrs = attrs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    HopRunner::new(FakeSys::new(num), attrs, LogPolicy::DEFAULT)
}

const IMG: &[u8] = b"ELF-achtige bytes";

/// De hele start: kooi, image-lengte, bytes. Geeft de kooi.
fn run(r: &mut HopRunner<FakeSys>, id: &str, j: &Job) -> Result<u32> {
    bl(r.start(0, &req(id, j)))?;
    bl(r.image_begin(0, id, IMG.len() as u64))?;
    match bl(r.image_chunk(0, id, IMG))? {
        Started::Running { pid } => Ok(pid),
        other => panic!("unexpected {other:?}"),
    }
}

fn tail(r: &HopRunner<FakeSys>, now: u64, id: &str) -> Vec<String> {
    r.logs(now, id, Stream::Stdout)
        .map(|l| l.tail().map(String::from).collect())
        .unwrap_or_default()
}

#[test]
fn hop_runner_lifecycle() {
    let mut r = runner(11, &[("node.os", "hopos")]);
    let mut j = hop_job();
    j.volumes.insert("/data".into(), "/data".into());
    let pid = run(&mut r, "t1", &j).unwrap();
    assert_ne!(pid, 0);
    let s = r.system().slot(pid);
    assert_eq!(s.image, IMG);
    let spec = s.spec.as_ref().unwrap();
    assert_eq!(spec.mem_limit, 64 << 20);
    assert_eq!(spec.env["BUCKET"], "hop-apps");
    assert_eq!(spec.env["ER_ATTR_NODE_OS"], "hopos");
    assert_eq!(spec.mounts["/data"], "/data");
    let t = TaskRef { id: "t1", pid };
    assert_eq!(bl(r.status(0, &t)).unwrap(), RunState::Running);
    bl(r.pump_logs(0));
    assert_eq!(tail(&r, 0, "t1"), ["app leeft"]);
    bl(r.stop(0, &t)).unwrap();
    assert_eq!(bl(r.status(0, &t)).unwrap(), RunState::Failed);
}

#[test]
fn hop_runner_cage_line_reaches_task_log() {
    let mut r = runner(11, &[("node.os", "hopos")]);
    r.system_mut().cage =
        "hart of slot 1: misa 0x800000000094112d (rv64 acdfimsux, S-mode present)".into();
    run(&mut r, "t-cage", &hop_job()).unwrap();
    bl(r.pump_logs(0));
    let t = tail(&r, 0, "t-cage");
    assert_eq!(t[0], r.system().cage);
}

#[test]
fn hop_runner_no_cage_line_when_empty() {
    let mut r = runner(11, &[("node.os", "hopos")]);
    run(&mut r, "t-nocage", &hop_job()).unwrap();
    bl(r.pump_logs(0));
    assert_eq!(tail(&r, 0, "t-nocage")[0], "app leeft");
}

#[test]
fn hop_runner_ports() {
    let mut r = runner(11, &[]);
    let mut j = hop_job();
    j.ports.insert("http".into(), 18080);
    let pid = run(&mut r, "t-ports", &j).unwrap();
    let spec = r.system().slot(pid).spec.clone().unwrap();
    assert_eq!(spec.ports["http"], 18080);
    assert_eq!(spec.env["ER_PORT_HTTP"], "18080");
}

#[test]
fn hop_runner_deleted_during_staging() {
    let mut r = runner(11, &[]);
    let j = hop_job();
    assert_eq!(
        bl(r.start(0, &req("t-del", &j))).unwrap(),
        Started::AwaitImage
    );
    bl(r.stop(
        0,
        &TaskRef {
            id: "t-del",
            pid: 0,
        },
    ))
    .unwrap();
    // Het image komt nog binnen: niets publiceren, niets aan de kooi koppelen.
    assert!(bl(r.image_begin(0, "t-del", 3)).is_err());
    assert_eq!(
        bl(r.image_chunk(0, "t-del", b"img")).unwrap(),
        Started::Aborted
    );
    assert_eq!(r.cages_in_use(), 0);
    assert!(r.slot_of("t-del").is_none());
    assert!(tail(&r, 0, "t-del").is_empty());
}

#[test]
fn hop_runner_core_class_allocation() {
    let mut r = runner(11, &[]);
    let mut j = hop_job();
    j.tags.insert("core-class".into(), "big".into());
    let pid = run(&mut r, "t-big", &j).unwrap();
    assert_eq!(pid, 1);
    assert_eq!(r.system().slot(1).spec.as_ref().unwrap().core_class, "big");
}

#[test]
fn hop_runner_smp_cores() {
    let mut r = runner(11, &[]);
    let mut j = hop_job();
    j.cpu_shares = 3072;
    let pid = run(&mut r, "t-smp", &j).unwrap();
    assert_eq!(r.system().slot(pid).spec.as_ref().unwrap().cores, 3);
    assert_eq!(r.cages_in_use(), 1);
    let n = run(&mut r, "neighbor", &hop_job()).unwrap();
    assert_eq!(n, 2);
    bl(r.stop(0, &TaskRef { id: "t-smp", pid })).unwrap();
    assert_eq!(r.cages_in_use(), 1);
    assert_eq!(r.slot_of("neighbor"), Some(Slot(2)));
    assert!(bl(r.system_mut().slot_status(Slot(2))).core_on);
}

#[test]
fn hop_runner_sharegroup() {
    let mut r = runner(11, &[]);
    let mut j = hop_job();
    j.cpu_shares = 2048;
    j.tags.insert("sharegroup".into(), "web".into());
    let pid = run(&mut r, "t-sg", &j).unwrap();
    let spec = r.system().slot(pid).spec.clone().unwrap();
    assert_eq!((spec.sharegroup.as_str(), spec.pool_cores), ("web", 2));
    assert_eq!(spec.cores, 1);
    assert_eq!(r.cages_in_use(), 1);
}

#[test]
fn hop_runner_rejections() {
    let mut r = runner(2, &[]);
    let mut container = hop_job();
    container.image = "nginx".into();
    let mut none = hop_job();
    none.artifacts = 0;
    let mut extract = hop_job();
    extract.extract = "tar.gz".into();
    for (name, j) in [
        ("container", &container),
        ("no artifact", &none),
        ("extract", &extract),
    ] {
        assert!(bl(r.start(0, &req(name, j))).is_err(), "{name}");
    }
    // Kooien zijn niet aan het aantal cores gebonden: de node weigert, niet de runner.
    for id in ["a", "b", "c"] {
        run(&mut r, id, &hop_job()).unwrap();
    }
    let mut big = hop_job();
    big.tags.insert("core-class".into(), "big".into());
    assert!(run(&mut r, "big", &big).is_err());
}

#[test]
fn hop_runner_logs_blijven_na_het_einde() {
    let mut r = runner(11, &[("node.os", "hopos")]);
    let pid = run(&mut r, "t-retire", &hop_job()).unwrap();
    bl(r.pump_logs(0));
    let before = tail(&r, 0, "t-retire");
    assert!(!before.is_empty());
    bl(r.stop(
        10,
        &TaskRef {
            id: "t-retire",
            pid,
        },
    ))
    .unwrap();
    let after = r.logs(20, "t-retire", Stream::Stdout).unwrap();
    assert_eq!(after.len(), before.len());
    // De taak is voorbij, dus zijn logstroom ook: een lezer weet dat hij klaar is.
    assert!(after.is_closed());
}

#[test]
fn hop_runner_logs_verlopen_na_de_termijn() {
    let mut r = runner(11, &[("node.os", "hopos")]);
    let pid = run(&mut r, "t-expire", &hop_job()).unwrap();
    bl(r.stop(
        0,
        &TaskRef {
            id: "t-expire",
            pid,
        },
    ))
    .unwrap();
    assert!(
        r.logs(LogPolicy::DEFAULT.keep_ms + 1, "t-expire", Stream::Stdout)
            .is_none()
    );
}

#[test]
fn hop_runner_failure_lands_in_task_log() {
    let mut r = runner(3, &[]);
    let mut j = hop_job();
    j.tags.insert("core-class".into(), "big".into());
    let err = run(&mut r, "kanniet", &j).unwrap_err();
    let got: String = tail(&r, 0, "kanniet").concat();
    assert!(got.contains(&err.to_string()), "{got:?} / {err}");
}

#[test]
fn allocate_skips_cages_the_node_reports_live() {
    let mut r = runner(9, &[("node.os", "hopos")]);
    r.system_mut().live(1);
    r.system_mut().live(3);
    // De kern kiest, en slaat de levende kooien over.
    assert_eq!(run(&mut r, "t", &hop_job()).unwrap(), 2);
}

#[test]
fn hop_runner_shared_and_smp_use_consecutive_cages() {
    let mut r = runner(9, &[]);
    for i in 0..2u32 {
        let mut j = hop_job();
        j.tags.insert("sharegroup".into(), "trusted".into());
        j.tags.insert("core-class".into(), "big".into());
        assert_eq!(
            run(&mut r, &alloc::format!("shared-{i}"), &j).unwrap(),
            1 + i
        );
    }
    let mut j = hop_job();
    j.cpu_shares = 2048;
    j.tags.insert("core-class".into(), "big".into());
    assert_eq!(run(&mut r, "smp", &j).unwrap(), 3);
    assert_eq!(r.cages_in_use(), 3);
}

#[test]
fn hop_runner_one_core_class_sharing() {
    let mut r = runner(1, &[]);
    r.system_mut().classes.insert(1, "big".into());
    for i in 0..6u32 {
        let mut j = hop_job();
        j.tags.insert("sharegroup".into(), "trusted".into());
        j.tags.insert("core-class".into(), "big".into());
        let pid = run(&mut r, &alloc::format!("shared-{i}"), &j).unwrap();
        assert_eq!(pid, 1 + i);
        assert_eq!(
            r.system().slot(pid).spec.as_ref().unwrap().core_class,
            "big"
        );
    }
}

#[test]
fn hop_runner_adoption_and_reuse_keep_neighbor() {
    let mut r = runner(3, &[]);
    r.system_mut().live(1);
    r.system_mut().live(2);
    r.adopt_running(&[("smp".into(), Slot(1)), ("shared".into(), Slot(2))]);
    assert_eq!(run(&mut r, "new", &hop_job()).unwrap(), 3);
    bl(r.stop(0, &TaskRef { id: "smp", pid: 1 })).unwrap();
    assert_eq!(run(&mut r, "again", &hop_job()).unwrap(), 1);
    assert_eq!(r.slot_of("shared"), Some(Slot(2)));
    assert!(bl(r.system_mut().slot_status(Slot(2))).core_on);
}

#[test]
fn hop_runner_capacity_failure_releases_cage() {
    let mut r = runner(1, &[]);
    let mut j = hop_job();
    j.cpu_shares = 2048;
    assert!(matches!(
        run(&mut r, "too-wide", &j),
        Err(Error::NoCapacity(_))
    ));
    assert_eq!(r.cages_in_use(), 0);
    assert_eq!(run(&mut r, "fits", &hop_job()).unwrap(), 1);
}

#[test]
fn stream_path_downloads_into_the_slot() {
    let mut r = runner(11, &[]);
    let j = hop_job();
    bl(r.start(0, &req("s1", &j))).unwrap();
    bl(r.image_begin(0, "s1", IMG.len() as u64)).unwrap();
    assert_eq!(
        bl(r.image_chunk(0, "s1", &IMG[..4])).unwrap(),
        Started::AwaitImage
    );
    assert_eq!(
        bl(r.image_chunk(0, "s1", &IMG[4..])).unwrap(),
        Started::Running { pid: 1 }
    );
    assert_eq!(r.system().slot(1).image, IMG);
}

#[test]
fn stream_path_rejects_missing_content_length() {
    let mut r = runner(11, &[]);
    let j = hop_job();
    bl(r.start(0, &req("s1", &j))).unwrap();
    assert!(matches!(
        bl(r.image_begin(0, "s1", 0)),
        Err(Error::Stream(_))
    ));
    assert_eq!(r.cages_in_use(), 0);
}

#[test]
fn stop_aborts_a_queued_download() {
    let mut r = runner(11, &[]);
    let j = hop_job();
    for i in 0..MAX_CONCURRENT_DOWNLOADS {
        let id = alloc::format!("d{i}");
        bl(r.start(0, &req(&id, &j))).unwrap();
        bl(r.image_begin(0, &id, 100)).unwrap();
    }
    bl(r.start(0, &req("queued", &j))).unwrap();
    // De vijfde wacht op zijn beurt en blijft "queued".
    assert_eq!(bl(r.image_begin(0, "queued", 100)), Err(Error::Busy));
    bl(r.stop(
        0,
        &TaskRef {
            id: "queued",
            pid: 0,
        },
    ))
    .unwrap();
    assert!(r.slot_of("queued").is_none());
    // Een afgebroken stroom geeft zijn beurt vrij.
    bl(r.stop(0, &TaskRef { id: "d0", pid: 0 })).unwrap();
    bl(r.start(0, &req("next", &j))).unwrap();
    bl(r.image_begin(0, "next", 100)).unwrap();
}

#[test]
fn stop_during_successful_start_does_not_publish_ghost() {
    let mut r = runner(11, &[]);
    let j = hop_job();
    bl(r.start(0, &req("g", &j))).unwrap();
    bl(r.image_begin(0, "g", IMG.len() as u64)).unwrap();
    bl(r.image_chunk(0, "g", &IMG[..3])).unwrap();
    bl(r.stop(0, &TaskRef { id: "g", pid: 0 })).unwrap();
    assert_eq!(
        bl(r.image_chunk(0, "g", &IMG[3..])).unwrap(),
        Started::Aborted
    );
    assert_eq!(r.cages_in_use(), 0);
    assert_eq!(r.system().stops, [1]);
}

#[test]
fn hop_stop_failure_keeps_slot_quarantined() {
    let mut r = runner(11, &[]);
    let pid = run(&mut r, "q", &hop_job()).unwrap();
    r.system_mut().stop_err = true;
    assert_eq!(
        bl(r.stop(0, &TaskRef { id: "q", pid })),
        Err(Error::Quarantined(Slot(pid)))
    );
    assert_eq!(r.slot_of("q"), Some(Slot(pid)));
    // De kooi is niet opnieuw uit te delen: de kern geeft een andere.
    r.system_mut().stop_err = false;
    assert_ne!(run(&mut r, "next", &hop_job()).unwrap(), pid);
}

#[test]
fn repeated_hop_stop_cannot_kill_reused_slot() {
    let mut r = runner(11, &[]);
    let old = run(&mut r, "old", &hop_job()).unwrap();
    bl(r.stop(
        0,
        &TaskRef {
            id: "old",
            pid: old,
        },
    ))
    .unwrap();
    let new = run(&mut r, "new", &hop_job()).unwrap();
    assert_eq!(old, new);
    // Een tweede stop op het verouderde record raakt de nieuwe bewoner niet.
    bl(r.stop(
        0,
        &TaskRef {
            id: "old",
            pid: old,
        },
    ))
    .unwrap();
    assert!(bl(r.system_mut().slot_status(Slot(new))).core_on);
    assert_eq!(r.slot_of("new"), Some(Slot(new)));
}

#[test]
fn stream_placement_failure_releases_the_slot() {
    let mut r = runner(1, &[]);
    let mut j = hop_job();
    j.cpu_shares = 4096;
    bl(r.start(0, &req("wide", &j))).unwrap();
    assert!(matches!(
        bl(r.image_begin(0, "wide", 3)),
        Err(Error::NoCapacity(_))
    ));
    assert_eq!(r.cages_in_use(), 0);
}

#[test]
fn stream_failure_at_the_last_byte_releases_and_logs() {
    let mut r = runner(11, &[]);
    r.system_mut().place_err = true;
    let err = run(&mut r, "kapot", &hop_job()).unwrap_err();
    assert!(
        matches!(err, Error::System(SysError::Refused(_))),
        "{err:?}"
    );
    assert_eq!(r.cages_in_use(), 0);
    assert!(tail(&r, 0, "kapot").concat().contains("PT_LOAD"));
    // De kern ruimde op; het volgende image krijgt hetzelfde slot.
    r.system_mut().place_err = false;
    assert_eq!(run(&mut r, "heel", &hop_job()).unwrap(), 1);
}

#[test]
fn kern_picks_the_slot_at_image_begin() {
    let mut r = runner(11, &[]);
    let j = hop_job();
    bl(r.start(0, &req("k", &j))).unwrap();
    assert_eq!(r.slot_of("k"), None, "no slot before the kern gave one");
    bl(r.image_begin(0, "k", IMG.len() as u64)).unwrap();
    assert_eq!(r.slot_of("k"), Some(Slot(1)));
    assert_eq!(
        bl(r.system_mut().slot_status(Slot(1))).state,
        SlotState::Streaming
    );
    bl(r.system_mut().set_clock(1_759_000_000)).unwrap();
    assert_eq!(r.system().clock, 1_759_000_000);
}

#[test]
fn adopt_running_restores_logs() {
    let mut r = runner(3, &[]);
    r.system_mut().live(2);
    r.system_mut()
        .slots
        .get_mut(&2)
        .unwrap()
        .logs
        .push_back("na de flip".into());
    r.adopt_running(&[("kept".into(), Slot(2))]);
    bl(r.pump_logs(0));
    assert_eq!(tail(&r, 0, "kept"), ["na de flip"]);
}

#[test]
fn log_broadcaster_write() {
    let mut ring = LogRing::new(3);
    ring.write("a");
    assert_eq!(ring.tail().collect::<Vec<_>>(), ["a"]);
}

#[test]
fn log_broadcaster_write_after_close() {
    let mut ring = LogRing::new(3);
    ring.close();
    ring.write("late");
    assert!(ring.is_empty());
}

#[test]
fn log_broadcaster_large_message() {
    let mut ring = LogRing::new(3);
    let big = "x".repeat(1 << 20);
    ring.write(&big);
    assert_eq!(ring.tail().next().unwrap().len(), 1 << 20);
}

#[test]
fn log_broadcaster_slow_subscriber() {
    // Een lezer die achterloopt verliest de oudste regels, nooit de schrijver.
    let mut ring = LogRing::new(2);
    let seen = ring.seq();
    for l in ["1", "2", "3"] {
        ring.write(l);
    }
    assert_eq!(ring.since(seen).collect::<Vec<_>>(), ["2", "3"]);
    assert_eq!(ring.since(ring.seq()).count(), 0);
}

#[test]
fn log_store_bewaart_logs_na_het_aflopen() {
    let mut s = LogStore::new(LogPolicy::DEFAULT);
    s.open("t");
    s.live_mut("t", Stream::Stdout).unwrap().write("regel");
    s.retire(0, "t");
    assert_eq!(s.get(1, "t", Stream::Stdout).unwrap().len(), 1);
}

#[test]
fn log_store_verloopt_na_de_termijn() {
    let mut s = LogStore::new(LogPolicy::DEFAULT);
    s.open("t");
    s.retire(0, "t");
    assert!(
        s.get(LogPolicy::DEFAULT.keep_ms, "t", Stream::Stdout)
            .is_none()
    );
    s.sweep(LogPolicy::DEFAULT.keep_ms);
    assert_eq!(s.counts(), (0, 0));
}

#[test]
fn log_store_retire_onbekend_en_dubbel() {
    let mut s = LogStore::new(LogPolicy::DEFAULT);
    s.retire(0, "onbekend");
    s.open("t");
    s.retire(0, "t");
    s.retire(0, "t");
    assert_eq!(s.counts(), (0, 1));
}

#[test]
fn log_store_hergebruikte_task_id_krijgt_verse_logs() {
    let mut s = LogStore::new(LogPolicy::DEFAULT);
    s.open("t");
    s.live_mut("t", Stream::Stdout).unwrap().write("oud");
    s.retire(0, "t");
    s.open("t");
    assert!(s.get(1, "t", Stream::Stdout).unwrap().is_empty());
}

#[test]
fn log_policy_tail_and_keep() {
    let p = LogPolicy {
        tail_lines: 0,
        keep_ms: 0,
    }
    .or_default();
    assert_eq!(p, LogPolicy::DEFAULT);
    let mut s = LogStore::new(LogPolicy {
        tail_lines: 2,
        keep_ms: 10,
    });
    s.open("t");
    for l in ["1", "2", "3"] {
        s.live_mut("t", Stream::Stdout).unwrap().write(l);
    }
    assert_eq!(s.get(0, "t", Stream::Stdout).unwrap().len(), 2);
    s.retire(0, "t");
    assert!(s.get(10, "t", Stream::Stdout).is_none());
}

#[test]
fn env_key_and_vars() {
    assert_eq!(env_key("node.os"), "NODE_OS");
    assert_eq!(env_key("http-port"), "HTTP_PORT");
}
