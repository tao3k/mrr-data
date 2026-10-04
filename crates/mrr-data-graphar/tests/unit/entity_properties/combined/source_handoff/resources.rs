//! Matched original-source measurements; cold means an empty verified cache.
use super::{authority, executor, metadata, restore};
use crate::tests::entity_properties::combined::source_handoff::{
    backend::{RESERVED, execution_transport},
    compile,
};
use crate::tests::entity_properties::{
    combined::{
        fixture::{Fixture, capture_limits},
        remote::Remote,
    },
    fixture as properties,
};
use crate::{CapturedCombinedGraphArSelective, GraphArChunkLayout};
use meta_relational_reasoning as mrr;
use mrr::PropertyQueryBackend;
use mrr_data_backend::{Backend, BackendConfig, ResourceControl, ResourceHandle, ResourceStop};
use mrr_data_content::MemoryContentStore;
use nix::sys::{
    resource::{UsageWho, getrusage},
    time::TimeValLike,
};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};
#[path = "resources/fixture.rs"]
mod fixture;
#[path = "resources/matrix.rs"]
mod matrix;
#[path = "resources/reads.rs"]
mod reads;

fn schema() -> Value {
    serde_json::from_str(include_str!("../../../../../source-resources-schema.json")).unwrap()
}
fn nanoseconds(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).unwrap()
}
fn cpu_ns() -> u64 {
    let usage = getrusage(UsageWho::RUSAGE_SELF).unwrap();
    u64::try_from(usage.user_time().num_microseconds() + usage.system_time().num_microseconds())
        .unwrap()
        * 1000
}
fn rss_bytes() -> u64 {
    let rss = u64::try_from(getrusage(UsageWho::RUSAGE_SELF).unwrap().max_rss()).unwrap();
    #[cfg(target_os = "macos")]
    {
        rss
    }
    #[cfg(target_os = "linux")]
    {
        rss * 1024
    }
}
struct MeasuredPhysical {
    inner: executor::CapturedBackend,
    elapsed: Arc<AtomicU64>,
}
impl PropertyQueryBackend for MeasuredPhysical {
    type PhysicalEvidence = mrr_data_core::BoundDataQuery;
    type Error = mrr_data_datafusion::DataFusionQueryError;
    async fn execute<'a>(
        &'a self,
        query: &'a mrr::CatalogBoundQuery,
    ) -> Result<mrr::PropertyExecutionCandidate<Self::PhysicalEvidence>, Self::Error> {
        let started = Instant::now();
        let result = self.inner.execute(query).await;
        self.elapsed.store(nanoseconds(started), Ordering::Relaxed);
        result
    }
}
struct Case {
    f: Fixture,
    backend: Backend,
    remote: Arc<Remote>,
    cache: Arc<MemoryContentStore>,
    reuse: Option<ResourceHandle<CapturedCombinedGraphArSelective>>,
}
impl Case {
    async fn capture(
        &self,
        cache: Arc<MemoryContentStore>,
    ) -> (ResourceHandle<CapturedCombinedGraphArSelective>, u64, u64) {
        let started = Instant::now();
        let restored = restore(&self.f, &self.backend, self.remote.clone(), cache).await;
        let restore_ns = nanoseconds(started);
        let query = self.f.query.clone();
        let relations = self.f.relations.clone();
        let projection = self.f.projection.clone();
        let started = Instant::now();
        let source = self
            .backend
            .prepare_resource_controlled(RESERVED, ResourceControl::default(), move |_| {
                crate::capture_combined_graphar_selective(
                    restored.get(),
                    &query,
                    &relations,
                    &projection,
                    capture_limits(),
                    GraphArChunkLayout::new(2, 2).unwrap(),
                )
            })
            .await
            .unwrap();
        (source, restore_ns, nanoseconds(started))
    }
    async fn sample(&self, mode: &str) -> Value {
        println!("original-source resource sample started mode={mode}");
        let cpu = cpu_ns();
        let started = Instant::now();
        let reads = self.remote.read_bytes.load(Ordering::Relaxed);
        let blocks = self.remote.read_blocks.load(Ordering::Relaxed);
        let (source, restore_ns, capture_ns) = if let Some(source) = &self.reuse {
            (source.clone(), 0, 0)
        } else {
            let cache = if mode == "cold-full" {
                Arc::new(MemoryContentStore::default())
            } else {
                self.cache.clone()
            };
            self.capture(cache).await
        };
        let preparation = source.get().preparation_metrics();
        let read = Instant::now();
        let (facts, materialized_rows, selected_edges, read_bytes) =
            reads::read(&self.f, source.get(), mode.ends_with("selective"));
        let read_ns = nanoseconds(read);
        let registration = Instant::now();
        let tables = source
            .get()
            .tables(&self.f.query)
            .unwrap()
            .iter()
            .map(|t| mrr_data_datafusion::EntityPropertyTable {
                schema: t.schema.clone(),
                batch: t.batch.clone(),
            })
            .collect();
        let physical_ns = Arc::new(AtomicU64::new(0));
        let physical = MeasuredPhysical {
            inner: executor::CapturedBackend {
                binding: self.f.query.clone(),
                tables,
                relations: crate::tests::entity_properties::combined::acceptance::fact_tables(
                    &self.f, &facts,
                ),
                limits: properties::limits(),
            },
            elapsed: physical_ns.clone(),
        };
        let conversion_ns = nanoseconds(registration);
        let binding = Instant::now();
        let bound = compile()
            .bind(
                &self.f.relations,
                &self.f.entities,
                &self.f.original.semantic,
            )
            .unwrap();
        let compile_bind_ns = nanoseconds(binding);
        let execute = Instant::now();
        let output = self
            .backend
            .prepare_resource_async_controlled(
                RESERVED,
                ResourceControl::default(),
                move |control| async move {
                    control.check()?;
                    let output = bound
                        .execute_with(&physical, super::super::result_limits())
                        .await
                        .unwrap();
                    control.check()?;
                    drop(source);
                    Ok::<_, ResourceStop>(output)
                },
            )
            .await
            .unwrap();
        let execute_admit_ns = nanoseconds(execute);
        let transport = Instant::now();
        let result = execution_transport(&self.f, output);
        let transport_ns = nanoseconds(transport);
        let result_ready_ns = nanoseconds(started);
        let exported_result_bytes = result.get().result_bytes().len();
        drop(result);
        let remote_read_bytes = self.remote.read_bytes.load(Ordering::Relaxed) - reads;
        let remote_read_blocks = self.remote.read_blocks.load(Ordering::Relaxed) - blocks;
        if mode == "cold-full" {
            assert!(remote_read_bytes > 0);
        } else {
            assert_eq!(remote_read_bytes, 0);
        }
        assert_eq!(
            self.backend.status().resource_bytes,
            if self.reuse.is_some() { RESERVED } else { 0 }
        );
        let total_ns = nanoseconds(started);
        json!({"result_ready_ns":result_ready_ns,"exported_result_bytes":exported_result_bytes,"total_ns":total_ns,"cpu_ns":cpu_ns()-cpu,"restore_ns":restore_ns,"capture_ns":capture_ns,"read_ns":read_ns,"conversion_ns":conversion_ns,"compile_bind_ns":compile_bind_ns,"execute_admit_ns":execute_admit_ns,"physical_backend_ns":physical_ns.load(Ordering::Relaxed),"transport_ns":transport_ns,"remote_read_bytes":remote_read_bytes,"remote_read_blocks":remote_read_blocks,"relation_read_bytes":read_bytes,"relation_materialized_rows":materialized_rows,"relation_selected_edges":selected_edges,"capture_verified_bytes":if self.reuse.is_some() {0} else {preparation.iter().map(|m| m.preparation.verified_bytes).sum::<u64>()},"capture_validation_rows":if self.reuse.is_some() {0} else {preparation.iter().map(|m| m.validation.materialized_rows).sum::<usize>()},"process_peak_rss_bytes":rss_bytes()})
    }
}

#[tokio::test]
#[ignore = "isolated original-source resource case; use the Rust matrix"]
async fn original_source_resource_case() {
    let shape = std::env::var("MRR_DATA_SOURCE_SHAPE").unwrap();
    let mode = std::env::var("MRR_DATA_SOURCE_MODE").unwrap();
    let contract = schema();
    assert!(
        contract["properties"]["shapes"]["const"]
            .as_array()
            .unwrap()
            .contains(&json!(shape))
    );
    assert!(
        contract["properties"]["modes"]["const"]
            .as_array()
            .unwrap()
            .contains(&json!(mode))
    );
    println!("original-source resource fixture preparation started shape={shape} mode={mode}");
    let startup = Instant::now();
    let f = fixture::load(&shape);
    let backend = Backend::open(
        BackendConfig {
            max_resource_bytes: 3 * RESERVED,
            ..BackendConfig::default()
        },
        metadata::SimulatedMetadata::default(),
        tokio::runtime::Handle::current(),
    )
    .await
    .unwrap();
    let remote = Arc::new(Remote::default());
    let (home, policy, _) = authority::publish(&f, &backend, remote.as_ref()).await;
    let mut case = Case {
        f,
        backend,
        remote,
        cache: Arc::new(MemoryContentStore::default()),
        reuse: None,
    };
    let mut setup = json!({"restore_ns":0,"capture_ns":0});
    if mode != "cold-full" {
        let (source, restore_ns, capture_ns) = case.capture(case.cache.clone()).await;
        setup = json!({"restore_ns":restore_ns,"capture_ns":capture_ns});
        if mode == "reused-selective" {
            case.reuse = Some(source);
        }
    }
    let fixture_setup_ns = nanoseconds(startup);
    let baseline_peak_rss_bytes = rss_bytes();
    let mut warmups = Vec::new();
    for _ in 0..contract["properties"]["warmups"]["const"].as_u64().unwrap() {
        warmups.push(case.sample(&mode).await);
    }
    let mut samples = Vec::new();
    for _ in 0..contract["properties"]["samples"]["const"].as_u64().unwrap() {
        samples.push(case.sample(&mode).await);
        assert!(authority::disclose(&home, policy).await);
    }
    let snapshot = case.f.query.snapshot_root().to_string();
    drop(case.reuse.take());
    case.backend.shutdown().await.unwrap();
    assert_eq!(case.backend.status().resource_bytes, 0);
    println!(
        "SOURCE-RESOURCE {}",
        json!({"shape":shape,"mode":mode,"source_digest":super::super::super::SOURCE_DIGEST,"snapshot_root":snapshot,"fixture_setup_ns":fixture_setup_ns,"warmups":warmups,"setup":setup,"baseline_peak_rss_bytes":baseline_peak_rss_bytes,"samples":samples,"all_results_admitted":true,"expected_rows":4,"cleanup_bytes":0,"spill_bytes":null,"copied_bytes":null,"streaming_first_result_ns":null})
    );
}
