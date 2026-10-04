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
use meta_relational_reasoning as mrr;
use mrr_data_backend::{Backend, BackendConfig, ResourceControl};
use mrr_data_content::MemoryContentStore;
use std::sync::Arc;

#[tokio::test]
async fn original_source_handoff_selective_dataset_matches_full_reference() {
    let layout = GraphArChunkLayout::new(2, 2).unwrap();
    let f = Fixture::with_original_options(
        source_fixture(),
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
    for target in targets {
        let slice = source.outgoing(&f.query, second, target, 100).unwrap();
        assert_slice(&full[1], target, slice.facts());
        println!(
            "combined selective relation={second} rows={} bytes={}",
            slice.metrics().materialized_rows,
            slice.metrics().read_bytes
        );
        downstream.extend(slice.into_facts());
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
