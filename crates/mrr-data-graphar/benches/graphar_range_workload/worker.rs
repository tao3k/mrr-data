//! Capture, execution and OS process high-water measurements in one fresh child.
use super::{binary_entity, model};
use meta_relational_reasoning::{Fact, Value};
use mrr_data_core::{GraphDatasetBinding, GraphInventoryLimits};
use mrr_data_graphar::{
    GraphArChunkLayout, GraphArSelectiveCaptureOptions, GraphArSelectiveCaptureRequest,
    GraphArSelectiveSnapshot, capture_graphar_selective_snapshot,
};
use nix::{
    sys::resource::{UsageWho, getrusage},
    sys::time::TimeValLike,
};
use serde::{Deserialize, Serialize};
use std::{io::Write, path::Path, time::Instant};

#[derive(Serialize, Deserialize)]
pub(super) struct Usage {
    max_rss_bytes: u64,
    user_cpu_us: i64,
    system_cpu_us: i64,
    major_faults: i64,
    minor_faults: i64,
    block_reads: i64,
    block_writes: i64,
    involuntary_switches: i64,
}
impl Usage {
    fn read() -> Result<Self, Box<dyn std::error::Error>> {
        let usage = getrusage(UsageWho::RUSAGE_SELF)?;
        let rss = u64::try_from(usage.max_rss())?;
        // Darwin reports bytes; Linux reports KiB. Do not apply the legacy
        // archived BSD manpage's kilobyte description to current Darwin.
        #[cfg(target_os = "macos")]
        let max_rss_bytes = rss;
        #[cfg(target_os = "linux")]
        let max_rss_bytes = rss.checked_mul(1024).ok_or("RSS conversion overflow")?;
        Ok(Self {
            max_rss_bytes,
            user_cpu_us: usage.user_time().num_microseconds(),
            system_cpu_us: usage.system_time().num_microseconds(),
            major_faults: usage.major_page_faults(),
            minor_faults: usage.minor_page_faults(),
            block_reads: usage.block_reads(),
            block_writes: usage.block_writes(),
            involuntary_switches: usage.involuntary_context_switches(),
        })
    }
}
#[derive(Serialize, Deserialize)]
pub(super) struct Report {
    pub mode: String,
    pub source: usize,
    pub result_digest: String,
    dataset_digest: String,
    dataset_edges: usize,
    pub selected_edges: usize,
    pub read_bytes: usize,
    pub materialized_rows: usize,
    pub files_read: usize,
    verified_bytes: u64,
    preparation_ns: u128,
    first_query_ns: u128,
    retained_source_p50_ns: u128,
    retained_source_p95_ns: u128,
    baseline: Usage,
    after_preparation: Usage,
    after_first_query: Usage,
    after_reuse: Usage,
}
struct QueryWork {
    facts: Vec<Fact>,
    read_bytes: usize,
    materialized_rows: usize,
    files_read: usize,
    elapsed_ns: u128,
}
fn execute(
    snapshot: &GraphArSelectiveSnapshot,
    query: &mrr_data_core::BoundDataQuery,
    projection: &mrr_data_graphar::BinaryEntityProjection,
    mode: &str,
    source: usize,
) -> QueryWork {
    let started = Instant::now();
    let source = model::source(source);
    let selection = match mode {
        "full" => snapshot.scan_all(query, projection, model::EDGES),
        "selected" => snapshot.outgoing(query, projection, source, model::EDGES),
        _ => unreachable!("mode checked before capture"),
    }
    .expect("execute authenticated physical source");
    let metrics = selection.metrics();
    let facts = selection
        .into_facts()
        .into_iter()
        .filter(|f| f.values()[0] == Value::Entity(source))
        .collect();
    QueryWork {
        facts,
        read_bytes: metrics.read_bytes,
        materialized_rows: metrics.materialized_rows,
        files_read: metrics.files_read,
        elapsed_ns: started.elapsed().as_nanos(),
    }
}
fn verify(work: &QueryWork, oracle: &model::Oracle) {
    assert_eq!(work.facts.len(), oracle.count);
    assert_eq!(model::digest(&work.facts), oracle.digest);
}
pub(super) fn run(
    root: &Path,
    mode: &str,
    source: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    if !matches!(mode, "full" | "selected") || source > 1 {
        return Err("invalid bounded worker case".into());
    }
    println!("GRAPHAR_PROCESS_PROGRESS mode={mode} source={source} phase=start");
    std::io::stdout().flush()?;
    let input: model::Input = serde_json::from_slice(&std::fs::read(root.join("input.json"))?)?;
    let baseline = Usage::read()?;
    let query = binary_entity::query_for_count(
        &input.inventory,
        "generation",
        u64::try_from(model::EDGES)?,
    );
    let projection = model::projection();
    let limits = GraphInventoryLimits::default();
    let binding =
        GraphDatasetBinding::admit(&query, projection.relation_id(), &input.inventory, limits)?;
    let snapshot = capture_graphar_selective_snapshot(GraphArSelectiveCaptureRequest {
        source: &root.join("source"),
        query: &query,
        binding,
        inventory: &input.inventory,
        projection: &projection,
        options: GraphArSelectiveCaptureOptions {
            inventory_limits: limits,
            max_vertices: model::NODES,
            layout: GraphArChunkLayout::new(model::NODES, model::CHUNK)?,
        },
    })?;
    let after_preparation = Usage::read()?;
    println!("GRAPHAR_PROCESS_PROGRESS mode={mode} source={source} phase=prepared");
    std::io::stdout().flush()?;
    let first = execute(&snapshot, &query, &projection, mode, source);
    let after_first_query = Usage::read()?;
    verify(&first, &input.selected[source]);
    let mut report = Report {
        mode: mode.into(),
        source,
        result_digest: model::digest(&first.facts),
        dataset_digest: input.full.digest.clone(),
        dataset_edges: input.full.count,
        selected_edges: first.facts.len(),
        read_bytes: first.read_bytes,
        materialized_rows: first.materialized_rows,
        files_read: first.files_read,
        verified_bytes: snapshot.preparation_metrics().verified_bytes,
        preparation_ns: snapshot.preparation_metrics().elapsed.as_nanos(),
        first_query_ns: first.elapsed_ns,
        retained_source_p50_ns: 0,
        retained_source_p95_ns: 0,
        baseline,
        after_preparation,
        after_first_query,
        after_reuse: Usage::read()?,
    };
    drop(first);
    println!("GRAPHAR_PROCESS_PROGRESS mode={mode} source={source} phase=first-verified");
    std::io::stdout().flush()?;
    let mut times = Vec::new();
    for iteration in 0..23 {
        let work = execute(&snapshot, &query, &projection, mode, source);
        verify(&work, &input.selected[source]);
        assert_eq!(work.read_bytes, report.read_bytes);
        assert_eq!(work.materialized_rows, report.materialized_rows);
        if iteration >= 2 {
            times.push(work.elapsed_ns);
        }
        println!(
            "GRAPHAR_PROCESS_PROGRESS mode={mode} source={source} phase=reuse iteration={iteration}"
        );
    }
    times.sort_unstable();
    report.retained_source_p50_ns = times[(times.len() - 1) / 2];
    report.retained_source_p95_ns = times[(times.len() - 1) * 95 / 100];
    report.after_reuse = Usage::read()?;
    assert!(report.after_reuse.max_rss_bytes > 0);
    println!("GRAPHAR_PROCESS_REPORT {}", serde_json::to_string(&report)?);
    Ok(())
}
