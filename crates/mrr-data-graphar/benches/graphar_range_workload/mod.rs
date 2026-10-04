//! Process-separated physical range-read measurement. Opt in with `--measure`.
#[path = "../../tests/support/binary_entity.rs"]
mod binary_entity;
#[path = "model.rs"]
mod model;
#[path = "worker.rs"]
mod worker;
use std::{
    io::{BufRead, BufReader},
    path::Path,
    process::{Command, Stdio},
    sync::mpsc,
    time::{Duration, Instant},
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args()
        .skip(1)
        .filter(|arg| arg != "--bench")
        .collect::<Vec<_>>();
    match args.first().map(String::as_str) {
        Some("--worker") if args.len() == 4 => {
            worker::run(Path::new(&args[1]), &args[2], args[3].parse()?)
        }
        Some("--measure") if args.len() == 1 => measure(),
        _ => {
            println!("graphar_range_workload: opt in with --measure");
            Ok(())
        }
    }
}
fn measure() -> Result<(), Box<dyn std::error::Error>> {
    let head = Command::new("git").args(["rev-parse", "HEAD"]).output()?;
    assert!(head.status.success());
    let state = Command::new("git")
        .args(["status", "--porcelain=v1"])
        .output()?;
    assert!(state.status.success());
    let dirty = !state.stdout.is_empty();
    println!(
        "GRAPHAR_PROCESS_BASELINE head={} dirty={dirty} os={} edges={} nodes={} vertex_chunk={} edge_chunk={}",
        String::from_utf8(head.stdout)?.trim(),
        std::env::consts::OS,
        model::EDGES,
        model::NODES,
        model::NODES,
        model::CHUNK
    );
    for skewed in [false, true] {
        let root = tempfile::tempdir()?;
        println!("GRAPHAR_PROCESS_FIXTURE skewed={skewed} phase=prepare");
        let input = model::prepare(root.path(), skewed);
        std::fs::write(root.path().join("input.json"), serde_json::to_vec(&input)?)?;
        for source in [0, 1] {
            let full = run_child(root.path(), "full", source)?;
            let selected = run_child(root.path(), "selected", source)?;
            assert_eq!(full.result_digest, selected.result_digest);
            assert_eq!(full.selected_edges, selected.selected_edges);
            assert!(selected.read_bytes < full.read_bytes);
            assert!(selected.materialized_rows < full.materialized_rows);
            println!(
                "GRAPHAR_PROCESS_COMPARISON {}",
                serde_json::to_string(&serde_json::json!({
                    "skewed": skewed, "source": source, "full": full, "selected": selected,
                    "os_cache": "uncontrolled", "state": "new-process then retained-source reuse"
                }))?
            );
        }
    }
    Ok(())
}
struct ChildGuard(std::process::Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn run_child(
    root: &Path,
    mode: &str,
    source: usize,
) -> Result<worker::Report, Box<dyn std::error::Error>> {
    let child = Command::new(std::env::current_exe()?)
        .arg("--worker")
        .arg(root)
        .arg(mode)
        .arg(source.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .stdin(Stdio::null())
        .spawn()?;
    let mut child = ChildGuard(child);
    let stdout = child.0.stdout.take().ok_or("missing child stdout")?;
    let (send, receive) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            if send.send(line).is_err() {
                break;
            }
        }
    });
    let started = Instant::now();
    let mut report = None;
    loop {
        if started.elapsed() > Duration::from_secs(90) {
            let _ = child.0.kill();
            let _ = child.0.wait();
            let _ = reader.join();
            return Err("GraphAr worker exceeded 90s watchdog".into());
        }
        match receive.recv_timeout(Duration::from_secs(5)) {
            Ok(Ok(line)) => {
                println!("{line}");
                if let Some(payload) = line.strip_prefix("GRAPHAR_PROCESS_REPORT ") {
                    report = Some(serde_json::from_str(payload)?);
                }
            }
            Ok(Err(error)) => {
                let _ = child.0.kill();
                let _ = child.0.wait();
                let _ = reader.join();
                return Err(error.into());
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                let _ = child.0.kill();
                let _ = child.0.wait();
                let _ = reader.join();
                return Err("GraphAr worker produced no progress for 5s".into());
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
    let status = child.0.wait()?;
    reader
        .join()
        .map_err(|_| "GraphAr progress reader panicked")?;
    if !status.success() {
        return Err(format!("GraphAr worker failed: {status}").into());
    }
    report.ok_or_else(|| "GraphAr worker omitted its result receipt".into())
}
