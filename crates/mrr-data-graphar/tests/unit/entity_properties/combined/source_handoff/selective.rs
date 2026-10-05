//! One original MRR query admits full and selected reads from the same Dataset.
use super::{
    RESERVED, authority, dispatch, execution_transport, executor, metadata, restore, source_fixture,
};
use crate::tests::entity_properties::{
    combined::{
        acceptance::alternate_root,
        fixture::{Fixture, capture_limits},
        remote::Remote,
    },
    fixture as properties,
};
use crate::{CapturedGraphArRelation, GraphArAdjacency, GraphArChunkLayout, GraphArWriteOptions};
use arrow_array::{ArrayRef, RecordBatch, StringArray};
use meta_relational_reasoning as mrr;
use mrr_data_backend::{Backend, BackendConfig, ResourceControl};
use mrr_data_content::MemoryContentStore;
use std::sync::Arc;

#[cfg(any(target_os = "macos", target_os = "linux"))]
#[path = "resources.rs"]
mod resources;

#[tokio::test]
async fn original_source_handoff_selective_dataset_matches_full_reference() {
    verify_dataset(source_fixture(), "reference").await;
}

#[tokio::test]
async fn original_source_handoff_uniform_and_skewed_selective_dataset_match_full_reference() {
    for skewed in [false, true] {
        let shape = if skewed { "skewed" } else { "uniform" };
        verify_dataset(shaped_fixture(skewed), shape).await;
    }
}

fn shaped_fixture(skewed: bool) -> properties::Fixture {
    let mut original = source_fixture();
    let extra = (0..4 * properties::workload_scale())
        .map(|index| properties::entity(&format!("other-scenario-{index}")))
        .collect::<Vec<_>>();
    let ids = extra.iter().map(ToString::to_string).collect::<Vec<_>>();
    let other = vec!["other".to_owned(); extra.len()];
    append(&mut original.entities[0].batch, &[ids.clone(), other]);
    let sources = if skewed {
        vec![properties::entity("s2").to_string(); extra.len()]
    } else {
        ids
    };
    let cases = vec![properties::entity("c3").to_string(); extra.len()];
    append(&mut original.relations[0].batch, &[sources, cases.clone()]);
    append(
        &mut original.relations[1].batch,
        &[
            cases,
            vec![properties::entity("p1").to_string(); extra.len()],
        ],
    );
    let join_bound = original
        .relations
        .iter()
        .map(|table| table.batch.num_rows())
        .product::<usize>();
    assert!(join_bound <= properties::limits().max_join_rows);
    original
}

fn append(batch: &mut RecordBatch, added: &[Vec<String>]) {
    assert_eq!(batch.num_columns(), added.len());
    let columns = batch
        .columns()
        .iter()
        .zip(added)
        .map(|(column, added)| {
            let strings = column.as_any().downcast_ref::<StringArray>().unwrap();
            let values = strings
                .iter()
                .map(|value| value.map(str::to_owned))
                .chain(added.iter().cloned().map(Some))
                .collect::<Vec<_>>();
            Arc::new(StringArray::from(values)) as ArrayRef
        })
        .collect();
    *batch = RecordBatch::try_new(batch.schema(), columns).unwrap();
}

async fn verify_dataset(original: properties::Fixture, shape: &str) {
    println!("original-source selective Dataset shape={shape} preparation started");
    let layout = GraphArChunkLayout::new(2, 2).unwrap();
    let f = Fixture::with_original_options(
        original,
        GraphArWriteOptions {
            adjacency: GraphArAdjacency::OrderedBySource,
            layout,
            ..GraphArWriteOptions::default()
        },
    );
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
    let (home, policy, _) = authority::publish(&f, &backend, &remote).await;
    let restored = restore(
        &f,
        &backend,
        remote,
        Arc::new(MemoryContentStore::default()),
    )
    .await;
    let query = f.query.clone();
    let catalog = f.relations.clone();
    let properties = f.projection.clone();
    let source = backend
        .prepare_resource_controlled(RESERVED, ResourceControl::default(), move |_| {
            crate::capture_combined_graphar_selective(
                restored.get(),
                &query,
                &catalog,
                &properties,
                capture_limits(),
                layout,
            )
        })
        .await
        .unwrap();
    assert_eq!(backend.status().resource_bytes, RESERVED);
    let other = alternate_root(&f);
    let first = f.original.relations[0].schema.id();
    let second = f.original.relations[1].schema.id();
    let start = properties::entity("s1");
    assert!(source.get().tables(&other).is_err());
    assert!(source.get().outgoing(&other, first, start, 100).is_err());
    assert!(source.get().scan_all(&other, first, 100).is_err());
    assert!(
        source
            .get()
            .outgoing(
                &f.query,
                mrr::RelationId::from_canonical_bytes("undeclared").unwrap(),
                start,
                100
            )
            .is_err()
    );
    assert!(source.get().outgoing(&f.query, first, start, 1).is_err());
    let (full, selected) = read_relations(&f, source.get(), first, second, start);
    // The fixture selects a declared caller's outgoing two-hop source. MRR's
    // unmodified original query still performs filtering, projection and admission.
    for facts in [full, selected] {
        let tables = source
            .get()
            .tables(&f.query)
            .unwrap()
            .iter()
            .map(|t| mrr_data_datafusion::EntityPropertyTable {
                schema: t.schema.clone(),
                batch: t.batch.clone(),
            })
            .collect();
        let physical = executor::CapturedBackend {
            binding: f.query.clone(),
            tables,
            relations: crate::tests::entity_properties::combined::acceptance::fact_tables(
                &f, &facts,
            ),
            limits: properties::limits(),
        };
        let result = dispatch(&f, &backend, source.clone(), physical).await;
        let transport = execution_transport(&f, result);
        assert!(authority::disclose(&home, policy).await);
        drop(transport);
        assert_eq!(backend.status().resource_bytes, RESERVED);
    }
    drop(source);
    backend.shutdown().await.unwrap();
    assert_eq!(backend.status().resource_bytes, 0);
}
fn read_relations(
    f: &Fixture,
    source: &crate::CapturedCombinedGraphArSelective,
    first: mrr::RelationId,
    second: mrr::RelationId,
    start: mrr::EntityId,
) -> (Vec<CapturedGraphArRelation>, Vec<CapturedGraphArRelation>) {
    for metrics in source.preparation_metrics() {
        assert!(metrics.preparation.verified_bytes > 0);
        assert!(metrics.validation.selected_edges > 0);
        println!(
            "combined preparation relation={} verified_bytes={} validation_rows={} validation_bytes={}",
            metrics.relation,
            metrics.preparation.verified_bytes,
            metrics.validation.materialized_rows,
            metrics.validation.read_bytes
        );
    }
    let mut full = Vec::new();
    for id in [first, second] {
        let scan = source.scan_all(&f.query, id, 100).unwrap();
        println!(
            "combined full relation={id} rows={} bytes={}",
            scan.metrics().materialized_rows,
            scan.metrics().read_bytes
        );
        full.push(CapturedGraphArRelation {
            relation: id,
            facts: scan.into_facts().into(),
        });
    }
    let first_slice = source.outgoing(&f.query, first, start, 100).unwrap();
    assert_slice(&full[0], start, first_slice.facts());
    let targets = first_slice
        .facts()
        .iter()
        .map(|fact| {
            let mrr::Value::Entity(target) = fact.values()[1] else {
                panic!("binary Entity")
            };
            target
        })
        .collect::<std::collections::BTreeSet<_>>();
    let mut downstream = Vec::new();
    for &target in &targets {
        let slice = source.outgoing(&f.query, second, target, 100).unwrap();
        assert_slice(&full[1], target, slice.facts());
        println!(
            "combined selective relation={second} rows={} bytes={}",
            slice.metrics().materialized_rows,
            slice.metrics().read_bytes
        );
        downstream.extend(slice.into_facts());
    }
    let sources = targets.into_iter().collect::<Vec<_>>();
    let batch = source
        .outgoing_many(&f.query, second, &sources, 100)
        .unwrap();
    let mut expected = downstream;
    expected.sort_by_key(mrr::Fact::id);
    let mut downstream = batch.into_facts();
    downstream.sort_by_key(mrr::Fact::id);
    assert_eq!(downstream, expected);
    if !downstream.is_empty() {
        assert!(
            source
                .outgoing_many(&f.query, second, &sources, downstream.len() - 1)
                .is_err()
        );
    }
    let selected = vec![
        CapturedGraphArRelation {
            relation: first,
            facts: first_slice.into_facts().into(),
        },
        CapturedGraphArRelation {
            relation: second,
            facts: downstream.into(),
        },
    ];
    assert!(
        selected.iter().map(|r| r.facts.len()).sum::<usize>()
            < full.iter().map(|r| r.facts.len()).sum::<usize>()
    );
    (full, selected)
}

fn assert_slice(full: &CapturedGraphArRelation, source: mrr::EntityId, selected: &[mrr::Fact]) {
    let expected = full
        .facts
        .iter()
        .filter(|fact| fact.values()[0] == mrr::Value::Entity(source))
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(selected, expected);
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
pub(super) fn process_fixture(shape: &str) -> Fixture {
    resources::load_fixture(shape)
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
pub(super) async fn process_query_transport(
    f: &Fixture,
    backend: &Backend,
    remote: Arc<Remote>,
    cache: Arc<MemoryContentStore>,
    home: &mrr_data_backend::ProfilePort,
    policy: mrr_data_backend::AuthorityState,
) -> mrr_data_backend::ResourceHandle<mrr_data_core::DataQueryResultHandoff> {
    let restored = restore(f, backend, remote, cache).await;
    let query = f.query.clone();
    let catalog = f.relations.clone();
    let projection = f.projection.clone();
    // The immutable producer declares ordered, chunked topology. The legacy
    // capture uses the default unordered profile; this owner uses the declared
    // layout and a full scan, preserving all rows before original MRR admission.
    let source = backend
        .prepare_resource_controlled(RESERVED, ResourceControl::default(), move |_| {
            crate::capture_combined_graphar_selective(
                restored.get(),
                &query,
                &catalog,
                &projection,
                capture_limits(),
                properties::workload_layout(),
            )
        })
        .await
        .unwrap();
    let relations = f
        .original
        .relations
        .iter()
        .map(|relation| {
            let scan = source
                .get()
                .scan_all(&f.query, relation.schema.id(), properties::workload_rows())
                .unwrap();
            CapturedGraphArRelation {
                relation: relation.schema.id(),
                facts: scan.into_facts().into(),
            }
        })
        .collect::<Vec<_>>();
    let tables = source
        .get()
        .tables(&f.query)
        .unwrap()
        .iter()
        .map(|table| mrr_data_datafusion::EntityPropertyTable {
            schema: table.schema.clone(),
            batch: table.batch.clone(),
        })
        .collect();
    let physical = executor::CapturedBackend {
        binding: f.query.clone(),
        tables,
        relations: crate::tests::entity_properties::combined::acceptance::fact_tables(
            f, &relations,
        ),
        limits: properties::limits(),
    };
    let execution = dispatch(f, backend, source, physical).await;
    let transport = execution_transport(f, execution);
    assert!(authority::disclose(home, policy).await);
    assert_eq!(backend.status().resource_bytes, RESERVED);
    transport
}
