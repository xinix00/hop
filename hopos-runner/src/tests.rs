//! De frames van elke op, door de echte `sys::Client` heen, tegen de nep-kern.

use alloc::collections::BTreeMap;
use alloc::string::String;

use abi::hopabi;
use abi::systemapi::{self, PrivOp, StartReq};
use runner::{
    HopRunner, LogPolicy, Runner, SlotApp, SlotState, StartRequest, StartSpec, Started, Streamed,
    SysError, SystemApi, TaskRef,
};

use crate::fake::{FakeDial, FakeKern, NeverTimer};
use crate::{KernSys, STATE_PATH};

type Sys = KernSys<applib::sys::Client<FakeDial, NeverTimer>>;

/// Drijft een future tot hij klaar is. Geen geneste ronde: een host-test
/// heeft geen executor, en de nep-kern antwoordt bij de eerste poll.
fn bl<F: core::future::Future>(f: F) -> F::Output {
    let mut f = core::pin::pin!(f);
    let mut cx = core::task::Context::from_waker(core::task::Waker::noop());
    match f.as_mut().poll(&mut cx) {
        core::task::Poll::Ready(v) => v,
        core::task::Poll::Pending => panic!("de nep-kern wacht nooit"),
    }
}

fn sys(max_slots: usize) -> (Sys, FakeKern) {
    let k = FakeKern::new(max_slots);
    (KernSys::new(k.client(), 4), k)
}

pub(crate) fn spec(size: u64) -> StartSpec {
    let mut env = BTreeMap::new();
    env.insert(String::from("ER_PORT_HTTP"), String::from("8000"));
    StartSpec {
        image_size: size,
        mem_limit: 32 << 20,
        core_class: String::from("big"),
        cores: 1,
        sharegroup: String::new(),
        pool_cores: 1,
        env,
        mounts: BTreeMap::new(),
        ports: BTreeMap::new(),
        job: String::from("web"),
    }
}

const ELF: &[u8] = b"\x7fELF-an-app-image";

#[test]
fn volumes_travel_in_the_start_and_the_kern_decodes_them() {
    let (mut s, k) = sys(4);
    let mut sp = spec(ELF.len() as u64);
    sp.mounts
        .insert(String::from("/volumes/media"), String::from("/media"));
    sp.mounts
        .insert(String::from("/volumes/demo"), String::from("data"));
    let slot = bl(s.start_slot(&sp)).unwrap();
    let got = k.0.borrow().slots[&u64::from(slot.0)].mounts.clone();
    assert_eq!(
        got,
        [
            (String::from("data"), String::from("/volumes/demo")),
            (String::from("/media"), String::from("/volumes/media")),
        ]
    );
    // Te veel volumes gaan niet de draad op, en er wordt niets geclaimd.
    for i in 0..systemapi::MAX_START_MOUNTS {
        sp.mounts
            .insert(alloc::format!("/volumes/{i}"), alloc::format!("/v/{i}"));
    }
    let before = k.0.borrow().ops.len();
    assert!(bl(s.start_slot(&sp)).is_err());
    assert_eq!(k.0.borrow().ops.len(), before);
}

#[test]
fn start_slot_is_op_0x40_with_the_start_head_of_abi() {
    // Toets de payload zoals de kern hem leest: StartReq::decode van abi.
    let (mut s, k) = sys(4);
    let slot = bl(s.start_slot(&spec(ELF.len() as u64))).unwrap();
    assert_eq!(slot.0, 1, "de kern kiest het slot");
    let st = k.0.borrow();
    assert_eq!(st.ops, [PrivOp::StartSlot.op()]);
    let fs = &st.slots[&1];
    assert_eq!(fs.job, "web");
    assert_eq!(fs.env, b"ER_PORT_HTTP=8000\n");
    assert_eq!(fs.memory_limit, 32 << 20);
    assert_eq!(fs.size, ELF.len() as u64);
}

#[test]
fn start_slot_carries_the_ports_of_the_jobspec() {
    let (mut s, k) = sys(4);
    let mut sp = spec(ELF.len() as u64);
    sp.ports.insert(String::from("http"), 80);
    sp.ports.insert(String::from("web"), 80);
    sp.ports.insert(String::from("admin"), 8443);
    bl(s.start_slot(&sp)).unwrap();
    // Eén keer per nummer, in de volgorde van de namen (admin, http).
    assert_eq!(k.0.borrow().slots[&1].ports, [8443, 80]);
    // Te veel poorten gaan niet de draad op.
    for p in 1..=17u16 {
        sp.ports.insert(alloc::format!("p{p}"), 9000 + p);
    }
    assert!(bl(s.start_slot(&sp)).is_err());
}

#[test]
fn start_payload_bytes_roundtrip_through_abi() {
    // Dezelfde bytes als de adapter verstuurt, los van de nep: de kop van
    // abi eromheen en weer terug.
    let sp = spec(99);
    let env = crate::env_blob(&sp.env);
    let req = StartReq {
        memory_limit: sp.mem_limit,
        image_size: 99,
        cores: 1,
        pool_cores: 1,
        core_class: systemapi::CoreClass::Big,
        group: b"",
        env: &env,
        ports: b"",
        job: b"web",
        ..Default::default()
    };
    let mut buf = [0u8; 256];
    let n = req.encode(&mut buf, 3).unwrap();
    let back = hopabi::decode_req(&buf[..n]).unwrap();
    assert_eq!(StartReq::decode(&back).unwrap(), req);
}

#[test]
fn start_ports_use_the_wire_form_of_abi() {
    // De poorten gaan in de vorm van abi::systemapi::port_blob de draad op:
    // het aantal op offset 22 van de StartHead, en per poort een u16
    // little-endian achter de env. De kern leest ze met StartReq::ports.
    let sp = spec(99);
    let env = crate::env_blob(&sp.env);
    let mut blob = [0u8; systemapi::MAX_START_PORTS * systemapi::PORT_LEN];
    let n = systemapi::port_blob(&[80, 0x1f90], &mut blob).unwrap();
    assert_eq!(&blob[..n], &[80, 0, 0x90, 0x1f]);
    let req = StartReq {
        memory_limit: sp.mem_limit,
        image_size: 99,
        cores: 1,
        pool_cores: 1,
        core_class: systemapi::CoreClass::Big,
        group: b"",
        env: &env,
        ports: &blob[..n],
        job: b"web",
        ..Default::default()
    };
    let mut buf = [0u8; 256];
    let len = req.encode(&mut buf, 3).unwrap();
    let back = hopabi::decode_req(&buf[..len]).unwrap();
    let head = hopabi::HDR_LEN + 3;
    assert_eq!(&buf[head + 22..head + 24], &[2, 0]);
    let got = StartReq::decode(&back).unwrap();
    assert_eq!(got, req);
    assert_eq!(got.ports().collect::<alloc::vec::Vec<u16>>(), [80, 0x1f90]);
    // Zeventien poorten of een poort 0 komen niet door port_blob.
    let many: alloc::vec::Vec<u16> = (1..=17).collect();
    assert!(systemapi::port_blob(&many, &mut blob).is_err());
    assert!(systemapi::port_blob(&[80, 0], &mut blob).is_err());
}

#[test]
fn stream_more_then_placed_with_offsets() {
    let (mut s, k) = sys(4);
    let slot = bl(s.start_slot(&spec(ELF.len() as u64))).unwrap();
    assert_eq!(bl(s.stream_image(slot, &ELF[..5])).unwrap(), Streamed::More);
    assert_eq!(
        bl(s.stream_image(slot, &ELF[5..])).unwrap(),
        Streamed::Placed
    );
    let st = k.0.borrow();
    assert_eq!(st.slots[&1].image, ELF);
    assert!(st.slots[&1].placed);
    assert_eq!(
        st.ops,
        [
            PrivOp::StartSlot.op(),
            PrivOp::StreamImage.op(),
            PrivOp::StreamImage.op()
        ]
    );
}

#[test]
fn a_failed_placement_carries_the_kernels_reason() {
    let (mut s, _k) = sys(4);
    let slot = bl(s.start_slot(&spec(4))).unwrap();
    assert_eq!(
        bl(s.stream_image(slot, b"junk")).unwrap(),
        Streamed::Failed(SysError::Refused(String::from("not an ELF image")))
    );
    // De stroom is dicht: een volgende brok is een weigering, geen call.
    assert!(bl(s.stream_image(slot, b"x")).is_err());
}

#[test]
fn a_full_kernel_is_no_capacity_not_a_crash() {
    let (mut s, _k) = sys(1);
    bl(s.start_slot(&spec(4))).unwrap();
    match bl(s.start_slot(&spec(4))) {
        Err(SysError::NoCapacity(why)) => assert!(why.contains("no free run")),
        other => panic!("verwacht NoCapacity, kreeg {other:?}"),
    }
}

#[test]
fn a_refused_chunk_is_an_error_and_closes_the_stream() {
    let (mut s, _k) = sys(4);
    let slot = bl(s.start_slot(&spec(3))).unwrap();
    match bl(s.stream_image(slot, b"toolong")) {
        Err(SysError::Refused(why)) => assert!(why.contains("more bytes"), "{why}"),
        other => panic!("{other:?}"),
    }
}

#[test]
fn status_decodes_slot_info() {
    let (mut s, k) = sys(4);
    let slot = bl(s.start_slot(&spec(ELF.len() as u64))).unwrap();
    let st = bl(s.slot_status(slot));
    assert_eq!((st.state, st.core_on), (SlotState::Streaming, false));
    bl(s.stream_image(slot, ELF)).unwrap();
    let st = bl(s.slot_status(slot));
    assert_eq!(
        (st.state, st.core_on, st.app),
        (SlotState::Running, true, SlotApp::Ready)
    );
    assert_eq!(st.heartbeat, 7);
    k.0.borrow_mut().slots.get_mut(&1).unwrap().crashed = true;
    let st = bl(s.slot_status(slot));
    assert_eq!(
        (st.core_on, st.app, st.exit_code),
        (false, SlotApp::Exited, 1)
    );
}

#[test]
fn stop_frees_and_an_unknown_stop_is_not_confirmed() {
    let (mut s, k) = sys(4);
    let slot = bl(s.start_slot(&spec(4))).unwrap();
    assert_eq!(bl(s.stop_slot(slot, 3000)), Ok(()));
    assert!(k.0.borrow().slots.is_empty());
    assert_eq!(bl(s.stop_slot(slot, 3000)), Err(SysError::NotConfirmed));
}

#[test]
fn next_log_and_set_clock() {
    let (mut s, k) = sys(4);
    let slot = bl(s.start_slot(&spec(ELF.len() as u64))).unwrap();
    k.0.borrow_mut()
        .slots
        .get_mut(&1)
        .unwrap()
        .logs
        .push_back(b"hello from web".to_vec());
    let mut buf = [0u8; 64];
    assert_eq!(bl(s.next_log_line(slot, &mut buf)), Some(14));
    assert_eq!(&buf[..14], b"hello from web");
    assert_eq!(bl(s.next_log_line(slot, &mut buf)), None);
    bl(s.set_clock(1_759_000_000_000_000_000)).unwrap();
    assert_eq!(k.0.borrow().clock, 1_759_000_000_000_000_000);
}

#[test]
fn state_store_roundtrips_over_hopfs() {
    use agent::Store;
    let (mut s, k) = sys(4);
    assert_eq!(bl(s.load()), Ok(None));
    bl(s.save(b"{\"version\":1}")).unwrap();
    bl(s.save(b"{}")).unwrap();
    assert_eq!(k.0.borrow().files[STATE_PATH], b"{}");
    assert_eq!(bl(s.load()), Ok(Some(b"{}".to_vec())));
}

#[test]
fn a_chunk_bigger_than_one_io_bite_goes_in_bites() {
    let big = applib::sys::MAX_CHUNK + 10;
    let mut image = alloc::vec![0u8; big];
    image[..4].copy_from_slice(b"\x7fELF");
    let (mut s, k) = sys(4);
    let slot = bl(s.start_slot(&spec(big as u64))).unwrap();
    assert_eq!(bl(s.stream_image(slot, &image)).unwrap(), Streamed::Placed);
    let streams =
        k.0.borrow()
            .ops
            .iter()
            .filter(|&&o| o == PrivOp::StreamImage.op())
            .count();
    assert_eq!(streams, 2);
}

#[test]
fn hop_runner_places_a_task_through_the_frames() {
    // De hele runner-kant: start, lengte, brokken, en de kooi als pid.
    let (s, k) = sys(4);
    let mut r = HopRunner::new(s, BTreeMap::new(), LogPolicy::default());
    let empty = BTreeMap::new();
    let ports = BTreeMap::new();
    let req = StartRequest {
        task_id: "t1",
        job_name: "web",
        image: "",
        artifacts: 1,
        extract: "",
        cpu_shares: 1024,
        memory_limit: 32 << 20,
        env: &empty,
        tags: &empty,
        volumes: &empty,
        ports: &ports,
    };
    assert_eq!(bl(r.start(0, &req)).unwrap(), Started::AwaitImage);
    bl(r.image_begin(0, "t1", ELF.len() as u64)).unwrap();
    assert_eq!(
        bl(r.image_chunk(0, "t1", &ELF[..3])).unwrap(),
        Started::AwaitImage
    );
    assert_eq!(
        bl(r.image_chunk(0, "t1", &ELF[3..])).unwrap(),
        Started::Running { pid: 1 }
    );
    let task = TaskRef { id: "t1", pid: 1 };
    assert_eq!(bl(r.status(0, &task)).unwrap(), runner::RunState::Running);
    bl(r.stop(0, &task)).unwrap();
    assert!(k.0.borrow().slots.is_empty());
}

#[test]
fn stop_frame_bytes_on_the_wire() {
    // De rauwe bytes: framekop "HOPS" v1 Call, dan de requestkop van 24
    // bytes met op 0x42, off = slot en n = de termijn in ms.
    let (mut s, k) = sys(4);
    let slot = bl(s.start_slot(&spec(4))).unwrap();
    bl(s.stop_slot(slot, 3000)).unwrap();
    let st = k.0.borrow();
    let f = &st.frames[1];
    assert_eq!(&f[0..4], b"HOPS");
    assert_eq!((f[4], f[5]), (1, 1));
    assert_eq!(u32::from_le_bytes(f[8..12].try_into().unwrap()), 24);
    assert_eq!((f[12], f[13]), (hopabi::VERSION, 0x42));
    assert_eq!(u64::from_le_bytes(f[20..28].try_into().unwrap()), 1);
    assert_eq!(u64::from_le_bytes(f[28..36].try_into().unwrap()), 3000);
    assert_eq!(st.dials, 1, "een blijvende verbinding");
}
