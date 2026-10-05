//! Fresh-process probes: genuine Host wait status before/after native setup.
use std::{process::Command, thread, time::Duration};

const PREFIX: &str = "tests::entity_properties::combined::source_handoff::child_ownership::";

fn host_statuses() {
    for code in [0, 7, 23] {
        let mut child = Command::new("/bin/sh")
            .args(["-c", &format!("exit {code}")])
            .spawn()
            .expect("spawn Host-owned child");
        // Let SIGCHLD run before wait. Waiting immediately can win the reaper race.
        thread::sleep(Duration::from_millis(100));
        let status = child
            .wait()
            .expect("Host must retain its child's wait status");
        assert_eq!(status.code(), Some(code));
        eprintln!("host-child-status pid={} code={code}", child.id());
    }
}

#[test]
#[ignore = "fresh-process ownership control; invoke exactly under process supervision"]
fn host_wait_baseline() {
    host_statuses();
}

#[test]
#[ignore = "native worker for isolated ownership qualification"]
fn native_compile_worker() {
    super::compile();
    eprintln!("native-child-owner original-source-compiled");
}

#[test]
#[ignore = "fresh-process ownership control; parent must never initialize Scheme"]
fn isolated_native_preserves_host_wait() {
    let status = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            &format!("{PREFIX}native_compile_worker"),
            "--ignored",
            "--nocapture",
            "--test-threads=1",
        ])
        .status()
        .expect("wait for actual native worker exit");
    assert!(status.success());
    eprintln!("native-child-owner isolated-worker-exit={status}");
    host_statuses();
}

#[test]
#[ignore = "unqualified shared native/Host process ownership; diagnostic must preserve failures"]
fn initialized_native_preserves_host_wait() {
    host_statuses();
    super::compile();
    eprintln!("native-child-owner original-source-compiled");
    host_statuses();
}
