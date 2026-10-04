//! Independent native full-source and selective offset-range parity.
use super::snapshot::{fact, query, query_for_count, schema};
use crate::{
    BinaryEntityProjection, GraphArAdjacency, GraphArReadLimits, GraphArSelectiveError,
    capture_graphar_selective_snapshot, prepare_graphar_source_with_adjacency,
    write_graphar_dataset_with_options,
};
use meta_relational_reasoning::{EntityId, Fact, FactId, RelationCatalog, Value};
use mrr_data_core::{GraphDatasetBinding, GraphInventoryLimits};

fn facts() -> Vec<Fact> {
    let entity = |name: &str| EntityId::from_canonical_bytes(name).unwrap();
    [
        ("alice", "bob"),
        ("alice", "bob"),
        ("alice", "alice"),
        ("bob", "alice"),
        ("carol", "dave"),
    ]
    .into_iter()
    .enumerate()
    .map(|(index, (source, target))| {
        Fact::new(
            FactId::from_canonical_bytes(format!("edge-{index}")).unwrap(),
            schema().id(),
            vec![Value::Entity(entity(source)), Value::Entity(entity(target))],
            *fact().context(),
        )
    })
    .collect()
}
struct OrderedFixture {
    _directory: tempfile::TempDir,
    source: std::path::PathBuf,
    projection: BinaryEntityProjection,
    expected: Vec<Fact>,
    receipt: crate::GraphArDatasetReceipt,
    bound: mrr_data_core::BoundDataQuery,
    binding: GraphDatasetBinding,
    layout: crate::GraphArChunkLayout,
}
impl OrderedFixture {
    fn new(expected: Vec<Fact>) -> Self {
        Self::with_layout(expected, crate::GraphArChunkLayout::new(2, 2).unwrap())
    }
    fn with_layout(expected: Vec<Fact>, layout: crate::GraphArChunkLayout) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("ordered");
        let projection = BinaryEntityProjection::admit_catalog(
            &RelationCatalog::admit(vec![schema()]).unwrap(),
            schema().id(),
        )
        .unwrap();
        let edges = expected
            .iter()
            .map(|f| projection.project(f).unwrap())
            .collect::<Vec<_>>();
        let receipt = write_graphar_dataset_with_options(
            &source,
            &projection,
            &edges,
            crate::GraphArWriteOptions {
                inventory_limits: GraphInventoryLimits::default(),
                adjacency: GraphArAdjacency::OrderedBySource,
                layout,
            },
        )
        .unwrap();
        assert!(
            receipt
                .inventory()
                .files()
                .iter()
                .any(|f| f.path().contains("ordered_by_source/offset/chunk"))
        );
        let bound = query_for_count(
            receipt.inventory(),
            "generation",
            u64::try_from(expected.len()).unwrap(),
        );
        let binding = GraphDatasetBinding::admit(
            &bound,
            schema().id(),
            receipt.inventory(),
            GraphInventoryLimits::default(),
        )
        .unwrap();
        Self {
            _directory: directory,
            source,
            projection,
            expected,
            receipt,
            bound,
            binding,
            layout,
        }
    }
    fn capture(&self) -> Result<crate::GraphArSelectiveSnapshot, GraphArSelectiveError> {
        capture_graphar_selective_snapshot(crate::GraphArSelectiveCaptureRequest {
            source: &self.source,
            query: &self.bound,
            binding: self.binding.clone(),
            inventory: self.receipt.inventory(),
            projection: &self.projection,
            options: crate::GraphArSelectiveCaptureOptions {
                inventory_limits: GraphInventoryLimits::default(),
                max_vertices: self.expected.len() * 2,
                layout: self.layout,
            },
        })
    }
    fn rebound(&self) -> crate::GraphArSelectiveSnapshot {
        let inventory =
            crate::inventory_graphar_directory(&self.source, GraphInventoryLimits::default())
                .unwrap();
        let bound = query_for_count(
            &inventory,
            "generation",
            u64::try_from(self.expected.len()).unwrap(),
        );
        let binding = GraphDatasetBinding::admit(
            &bound,
            schema().id(),
            &inventory,
            GraphInventoryLimits::default(),
        )
        .unwrap();
        capture_graphar_selective_snapshot(crate::GraphArSelectiveCaptureRequest {
            source: &self.source,
            query: &bound,
            binding,
            inventory: &inventory,
            projection: &self.projection,
            options: crate::GraphArSelectiveCaptureOptions {
                inventory_limits: GraphInventoryLimits::default(),
                max_vertices: self.expected.len() * 2,
                layout: self.layout,
            },
        })
        .unwrap()
    }
}
#[test]
fn selective_ordered_ranges_match_full_native_facts_after_source_deletion() {
    let fixture = OrderedFixture::new(facts());
    let full = prepare_graphar_source_with_adjacency(
        &fixture.source,
        GraphArReadLimits::new(10, 10),
        GraphArAdjacency::OrderedBySource,
    )
    .unwrap()
    .admit(&fixture.projection)
    .unwrap();
    let selected = fixture.capture().unwrap();
    let scan = selected
        .scan_all(&fixture.bound, &fixture.projection, 10)
        .unwrap();
    assert_eq!(scan.facts(), full.facts());
    assert!(selected.preparation_metrics().verified_bytes > 0);
    std::fs::remove_dir_all(&fixture.source).unwrap();
    for name in ["alice", "bob", "carol", "dave", "absent"] {
        let entity = EntityId::from_canonical_bytes(name).unwrap();
        let mut expected = fixture
            .expected
            .iter()
            .filter(|f| f.values()[0] == Value::Entity(entity))
            .cloned()
            .collect::<Vec<_>>();
        expected.sort_by_key(Fact::id);
        let actual = selected
            .outgoing(&fixture.bound, &fixture.projection, entity, 10)
            .unwrap();
        assert_eq!(actual.facts(), expected);
        let mut reference = full
            .facts()
            .iter()
            .filter(|f| f.values()[0] == Value::Entity(entity))
            .cloned()
            .collect::<Vec<_>>();
        reference.sort_by_key(Fact::id);
        assert_eq!(actual.facts(), reference);
        assert_eq!(actual.metrics().selected_edges, expected.len());
        if name == "absent" {
            assert_eq!(actual.metrics().files_read, 0);
        } else {
            assert!(actual.metrics().read_bytes > 0);
        }
    }
    let alice = EntityId::from_canonical_bytes("alice").unwrap();
    assert!(matches!(
        selected.outgoing(&fixture.bound, &fixture.projection, alice, 2),
        Err(GraphArSelectiveError::Limit)
    ));
    let stale = query(fixture.receipt.inventory(), "other-generation");
    assert!(matches!(
        selected.outgoing(&stale, &fixture.projection, alice, 10),
        Err(GraphArSelectiveError::Scope)
    ));
}

#[test]
fn selective_checkpoints_stop_between_chunks_and_allow_fresh_retry() {
    let fixture = OrderedFixture::new(facts());
    let snapshot = fixture.capture().unwrap();
    let alice = EntityId::from_canonical_bytes("alice").unwrap();
    let mut checkpoints = 0;
    let result = snapshot.outgoing_checked(&fixture.bound, &fixture.projection, alice, 10, || {
        checkpoints += 1;
        if checkpoints == 3 {
            Err(GraphArSelectiveError::Cancelled)
        } else {
            Ok(())
        }
    });
    assert!(matches!(result, Err(GraphArSelectiveError::Cancelled)));
    assert_eq!(checkpoints, 3);
    assert!(matches!(
        snapshot.outgoing_checked(&fixture.bound, &fixture.projection, alice, 10, || Err(
            GraphArSelectiveError::Deadline
        ),),
        Err(GraphArSelectiveError::Deadline)
    ));
    let result = snapshot
        .outgoing(&fixture.bound, &fixture.projection, alice, 10)
        .unwrap();
    assert_eq!(
        result.facts(),
        expected_neighborhood(&fixture.expected, alice)
    );
}

#[test]
fn selective_shared_descriptors_keep_parallel_neighborhoods_independent() {
    let fixture = OrderedFixture::new(facts());
    let snapshot = fixture.capture().unwrap();
    std::fs::remove_dir_all(&fixture.source).unwrap();
    std::thread::scope(|threads| {
        let workers = (0..8)
            .map(|worker| {
                let snapshot = &snapshot;
                let fixture = &fixture;
                threads.spawn(move || {
                    for round in 0..8 {
                        let name = ["alice", "bob", "carol", "dave"][(worker + round) % 4];
                        let source = EntityId::from_canonical_bytes(name).unwrap();
                        let selected = snapshot
                            .outgoing(&fixture.bound, &fixture.projection, source, 10)
                            .unwrap();
                        assert_eq!(
                            selected.facts(),
                            expected_neighborhood(&fixture.expected, source)
                        );
                    }
                })
            })
            .collect::<Vec<_>>();
        for worker in workers {
            worker.join().unwrap();
        }
    });
}

fn rewrite_offsets(fixture: &OrderedFixture, alter: impl FnOnce(&mut Vec<i64>)) {
    use arrow_array::{Int64Array, RecordBatch};
    use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};
    let path = fixture
        .source
        .join("edge/entity_mrr_relation_entity/ordered_by_source/offset/chunk0");
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&path).unwrap())
        .unwrap()
        .with_batch_size(1 << 19)
        .build()
        .unwrap();
    let batch = reader.next().unwrap().unwrap();
    let mut values = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec();
    alter(&mut values);
    let batch = RecordBatch::try_new(
        batch.schema(),
        vec![std::sync::Arc::new(Int64Array::from(values))],
    )
    .unwrap();
    let mut writer =
        ArrowWriter::try_new(std::fs::File::create(path).unwrap(), batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

#[test]
fn selective_capture_and_ranges_refuse_corrupt_or_truncated_offsets() {
    let negative = OrderedFixture::new(facts());
    rewrite_offsets(&negative, |values| values[0] = -1);
    assert!(matches!(
        negative.capture(),
        Err(GraphArSelectiveError::Capture(_))
    ));
    let inventory =
        crate::inventory_graphar_directory(&negative.source, GraphInventoryLimits::default())
            .unwrap();
    let bound = query_for_count(
        &inventory,
        "generation",
        u64::try_from(negative.expected.len()).unwrap(),
    );
    let binding = GraphDatasetBinding::admit(
        &bound,
        schema().id(),
        &inventory,
        GraphInventoryLimits::default(),
    )
    .unwrap();
    assert!(matches!(
        capture_graphar_selective_snapshot(crate::GraphArSelectiveCaptureRequest {
            source: &negative.source,
            query: &bound,
            binding,
            inventory: &inventory,
            projection: &negative.projection,
            options: crate::GraphArSelectiveCaptureOptions {
                inventory_limits: GraphInventoryLimits::default(),
                max_vertices: 10,
                layout: negative.layout,
            },
        }),
        Err(GraphArSelectiveError::Layout)
    ));

    // An authenticated but truncated offset interval can still be monotone.
    // Neighbor probes must catch an omitted edge instead of admitting a subset.
    let truncated =
        OrderedFixture::with_layout(facts(), crate::GraphArChunkLayout::new(16, 2).unwrap());
    let edges = truncated
        .expected
        .iter()
        .map(|f| truncated.projection.project(f).unwrap())
        .collect::<Vec<_>>();
    let index = crate::PhysicalVertexIndex::from_edges(&edges);
    let alice = EntityId::from_canonical_bytes("alice").unwrap();
    let physical = index.entities().position(|e| e == alice).unwrap();
    rewrite_offsets(&truncated, |values| {
        if physical == 0 {
            values[1] -= 1;
        } else {
            values[physical] += 1;
        }
    });
    let snapshot = truncated.rebound();
    assert!(matches!(
        snapshot.outgoing(&truncated.bound, &truncated.projection, alice, 10),
        Err(GraphArSelectiveError::Layout)
    ));
}

#[cfg(feature = "backend")]
fn outgoing_request(fixture: &OrderedFixture) -> crate::GraphArOutgoingRequest {
    crate::GraphArOutgoingRequest {
        query: fixture.bound.clone(),
        projection: fixture.projection.clone(),
        source: EntityId::from_canonical_bytes("alice").unwrap(),
        max_edges: 10,
    }
}

#[cfg(feature = "backend")]
#[tokio::test]
async fn selective_backend_keeps_source_output_budgets_and_final_clone_drain() {
    use mrr_data_backend::{
        Backend, BackendConfig, BackendError, Lifecycle, ResourceControl,
        ResourcePreparationError as Error,
    };
    const SOURCE: usize = 8 * 1024 * 1024;
    const OUTPUT: usize = 1024 * 1024;
    let fixture = OrderedFixture::new(facts());
    let backend = Backend::open(
        BackendConfig {
            max_resource_bytes: SOURCE + OUTPUT,
            ..BackendConfig::default()
        },
        super::snapshot::backend_qualification::MetadataStub,
        tokio::runtime::Handle::current(),
    )
    .await
    .unwrap();
    let source = crate::prepare_graphar_selective_snapshot(
        &backend,
        crate::GraphArSelectiveSnapshotRequest {
            source: fixture.source.clone(),
            query: fixture.bound.clone(),
            binding: fixture.binding.clone(),
            inventory: fixture.receipt.inventory().clone(),
            projection: fixture.projection.clone(),
            inventory_limits: GraphInventoryLimits::default(),
            max_vertices: 10,
            layout: fixture.layout,
        },
        SOURCE,
        ResourceControl::new(None),
    )
    .await
    .unwrap();
    std::fs::remove_dir_all(&fixture.source).unwrap();
    let canceled = ResourceControl::new(None);
    canceled.cancel();
    assert!(matches!(
        crate::prepare_graphar_outgoing(
            &backend,
            source.clone(),
            outgoing_request(&fixture),
            OUTPUT,
            canceled
        )
        .await,
        Err(Error::Preparation(GraphArSelectiveError::Cancelled))
    ));
    assert_eq!(backend.status().resource_bytes, SOURCE);
    let output = crate::prepare_graphar_outgoing(
        &backend,
        source.clone(),
        outgoing_request(&fixture),
        OUTPUT,
        ResourceControl::new(None),
    )
    .await
    .unwrap();
    assert_eq!(output.get().facts().len(), 3);
    assert_eq!(backend.status().resource_bytes, SOURCE + OUTPUT);
    assert!(matches!(
        crate::prepare_graphar_outgoing(
            &backend,
            source.clone(),
            outgoing_request(&fixture),
            OUTPUT,
            ResourceControl::new(None)
        )
        .await,
        Err(Error::Backend(BackendError::Saturated))
    ));
    let source_clone = source.clone();
    let output_clone = output.clone();
    let mut shutdown = tokio::spawn({
        let backend = backend.clone();
        async move { backend.shutdown().await }
    });
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while backend.status().lifecycle != Lifecycle::Draining {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    drop(source);
    drop(source_clone);
    assert_eq!(backend.status().resource_bytes, OUTPUT);
    drop(output);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), &mut shutdown)
            .await
            .is_err()
    );
    drop(output_clone);
    tokio::time::timeout(std::time::Duration::from_secs(3), shutdown)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(backend.status().resource_bytes, 0);
}

fn workload(skewed: bool) -> Vec<Fact> {
    let nodes = (0..64)
        .map(|index| EntityId::from_canonical_bytes(format!("node-{index}")).unwrap())
        .collect::<Vec<_>>();
    let context = *fact().context();
    (0..4096)
        .map(|index| {
            let source = if skewed {
                if index < 2048 { 0 } else { 1 + index % 63 }
            } else {
                index % 64
            };
            Fact::new(
                FactId::from_canonical_bytes(format!("experiment-{index}")).unwrap(),
                schema().id(),
                vec![
                    Value::Entity(nodes[source]),
                    Value::Entity(nodes[(index * 7 + 1) % 64]),
                ],
                context,
            )
        })
        .collect()
}
fn expected_neighborhood(facts: &[Fact], source: EntityId) -> Vec<Fact> {
    let mut values = facts
        .iter()
        .filter(|f| f.values()[0] == Value::Entity(source))
        .cloned()
        .collect::<Vec<_>>();
    values.sort_by_key(Fact::id);
    values
}
fn percentiles(values: &mut [std::time::Duration]) -> (u128, u128) {
    values.sort_unstable();
    (
        values[(values.len() - 1) / 2].as_nanos(),
        values[(values.len() - 1) * 95 / 100].as_nanos(),
    )
}

#[test]
#[ignore = "matched physical read experiment; run explicitly on each qualified OS"]
fn selective_matched_workload_experiment() {
    for skewed in [false, true] {
        let fixture = OrderedFixture::with_layout(
            workload(skewed),
            crate::GraphArChunkLayout::new(128, 128).unwrap(),
        );
        let native = prepare_graphar_source_with_adjacency(
            &fixture.source,
            GraphArReadLimits::new(128, 4096),
            GraphArAdjacency::OrderedBySource,
        )
        .unwrap()
        .admit(&fixture.projection)
        .unwrap();
        let snapshot = fixture.capture().unwrap();
        let full = snapshot
            .scan_all(&fixture.bound, &fixture.projection, 4096)
            .unwrap();
        assert_eq!(full.facts(), native.facts());
        for name in ["node-0", "node-1"] {
            compare_workload(&fixture, &snapshot, &full, skewed, name);
        }
    }
}
fn compare_workload(
    fixture: &OrderedFixture,
    snapshot: &crate::GraphArSelectiveSnapshot,
    reference: &crate::GraphArSelection,
    skewed: bool,
    name: &str,
) {
    let source = EntityId::from_canonical_bytes(name).unwrap();
    let expected = expected_neighborhood(&fixture.expected, source);
    let first = snapshot
        .outgoing(&fixture.bound, &fixture.projection, source, 4096)
        .unwrap();
    assert_eq!(first.facts(), expected);
    assert!(first.metrics().read_bytes < reference.metrics().read_bytes);
    assert!(first.metrics().materialized_rows < reference.metrics().materialized_rows);
    let mut full_times = Vec::new();
    let mut selected_times = Vec::new();
    for iteration in 0..23 {
        let full = snapshot
            .scan_all(&fixture.bound, &fixture.projection, 4096)
            .unwrap();
        assert_eq!(expected_neighborhood(full.facts(), source), expected);
        let selected = snapshot
            .outgoing(&fixture.bound, &fixture.projection, source, 4096)
            .unwrap();
        assert_eq!(selected.facts(), expected);
        if iteration >= 2 {
            full_times.push(full.metrics().elapsed);
            selected_times.push(selected.metrics().elapsed);
        }
    }
    let (full_p50, full_p95) = percentiles(&mut full_times);
    let (selected_p50, selected_p95) = percentiles(&mut selected_times);
    println!(
        "GRAPHAR_SELECTION skewed={skewed} source={name} edges={} verified_bytes={} preparation_ns={} offset_read_bytes={} full_read_bytes={} selected_read_bytes={} full_materialized_rows={} selected_materialized_rows={} full_files={} selected_files={} first_selected_ns={} full_p50_ns={full_p50} full_p95_ns={full_p95} selected_p50_ns={selected_p50} selected_p95_ns={selected_p95}",
        expected.len(),
        snapshot.preparation_metrics().verified_bytes,
        snapshot.preparation_metrics().elapsed.as_nanos(),
        snapshot.preparation_metrics().offset_read_bytes,
        reference.metrics().read_bytes,
        first.metrics().read_bytes,
        reference.metrics().materialized_rows,
        first.metrics().materialized_rows,
        reference.metrics().files_read,
        first.metrics().files_read,
        first.metrics().elapsed.as_nanos()
    );
}
