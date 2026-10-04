use super::{
    fixture::{Fixture, capture_limits, transfer_limits},
    remote::Remote,
};
use crate::capture_combined_graphar;
use meta_relational_reasoning as mrr;
use mrr_data_content::{
    GraphTransferError, MemoryContentStore, prepare_combined_graph, publish_combined_graph,
    restore_combined_graph,
};
use mrr_data_core::{GraphDatasetDescriptor, GraphInventoryError, dag_cbor_cid};
use std::{num::NonZeroUsize, sync::Arc};

#[tokio::test]
async fn native_combined_closure_cold_restores_before_two_hop_mrr_admission() {
    let f = Fixture::new();
    let prepared = f.prepare();
    let remote = Remote::default();
    let receipt =
        publish_combined_graph(&prepared, &f.local, &remote, &remote, || async { Ok(()) })
            .await
            .unwrap();
    assert_eq!(receipt.cid, *prepared.root());
    let writes = remote.writes.lock().unwrap().clone();
    assert_eq!(writes.last(), Some(prepared.root()));
    assert_eq!(
        writes[writes.len() - 2],
        *f.query.graph_projection_manifest().unwrap()
    );
    assert_eq!(writes.len(), prepared.block_count());
    let restored = restore_combined_graph(
        &MemoryContentStore::default(),
        &remote,
        &f.query,
        (&f.relations, &f.entities),
        (transfer_limits(), capture_limits().dataset),
    )
    .await
    .unwrap();
    assert_eq!(restored.total_bytes(), prepared.total_bytes());
    let captured = capture_combined_graphar(
        &restored,
        &f.query,
        &f.relations,
        &f.projection,
        capture_limits(),
    )
    .unwrap();
    let entities = captured
        .tables(&f.query)
        .unwrap()
        .iter()
        .map(|t| mrr_data_datafusion::EntityPropertyTable {
            schema: t.schema.clone(),
            batch: t.batch.clone(),
        })
        .collect::<Vec<_>>();
    assert_eq!(
        entities.iter().map(|t| t.batch.num_rows()).sum::<usize>(),
        8
    );
    let relations = relation_tables(&f, &captured);
    let candidate = mrr_data_datafusion::execute_property_path_query(
        f.query.query(),
        &entities,
        &relations,
        crate::tests::entity_properties::fixture::limits(),
    )
    .await
    .unwrap();
    let mut actual = candidate.rows().to_vec();
    let mut expected = crate::tests::entity_properties::acceptance::expected();
    actual.sort_by_key(|r| format!("{r:?}"));
    expected.sort_by_key(|r| format!("{r:?}"));
    assert_eq!(actual, expected);
    let result_limits = mrr::QueryResultLimits::new(
        NonZeroUsize::new(100).unwrap(),
        NonZeroUsize::new(300).unwrap(),
    );
    let cap = NonZeroUsize::new(1 << 20).unwrap();
    let handoff = mrr_data_core::DataQueryResultHandoff::export(
        &f.query,
        &mrr_data_datafusion::datafusion_engine_profile().unwrap(),
        candidate,
        result_limits,
        cap,
    )
    .unwrap();
    handoff.verify(&f.query, result_limits, cap).unwrap();
    let other = alternate_root(&f);
    assert_eq!(
        other.graph_projection_manifest(),
        f.query.graph_projection_manifest()
    );
    assert_ne!(other.snapshot_root(), f.query.snapshot_root());
    assert!(captured.tables(&other).is_err());
    assert!(captured.relations(&other).is_err());
    assert!(handoff.verify(&other, result_limits, cap).is_err());
    let mut bounds = capture_limits();
    bounds.topology = crate::GraphArReadLimits::new(1, 1);
    assert!(
        capture_combined_graphar(&restored, &f.query, &f.relations, &f.projection, bounds).is_err()
    );
}
#[tokio::test]
async fn combined_failed_ack_gate_lost_ack_and_corrupt_cold_child_refuse() {
    let f = Fixture::new();
    let prepared = f.prepare();
    let complete = Remote::default();
    publish_combined_graph(&prepared, &f.local, &complete, &complete, || async {
        Ok(())
    })
    .await
    .unwrap();
    let writes = complete.writes.lock().unwrap().clone();
    for cid in writes {
        let remote = Remote::default();
        *remote.fail.lock().unwrap() = Some(cid);
        assert!(
            publish_combined_graph(&prepared, &f.local, &remote, &remote, || async { Ok(()) })
                .await
                .is_err()
        );
        if cid != *prepared.root() {
            assert!(!remote.writes.lock().unwrap().contains(prepared.root()));
        }
    }
    let remote = Remote::default();
    assert!(
        publish_combined_graph(&prepared, &f.local, &remote, &remote, || async {
            Err(GraphTransferError::RootDenied)
        })
        .await
        .is_err()
    );
    assert!(!remote.writes.lock().unwrap().contains(prepared.root()));
    *remote.lost_ack.lock().unwrap() = Some(*prepared.root());
    assert!(
        publish_combined_graph(&prepared, &f.local, &remote, &remote, || async { Ok(()) })
            .await
            .is_err()
    );
    *remote.lost_ack.lock().unwrap() = None;
    publish_combined_graph(&prepared, &f.local, &remote, &remote, || async { Ok(()) })
        .await
        .unwrap();
    let child = *f.dataset.relations()[0].inventory.files()[0].cid();
    remote
        .blocks
        .lock()
        .unwrap()
        .insert(child, b"corrupt native child".to_vec());
    assert!(
        restore_combined_graph(
            &MemoryContentStore::default(),
            &remote,
            &f.query,
            (&f.relations, &f.entities),
            (transfer_limits(), capture_limits().dataset)
        )
        .await
        .is_err()
    );
    remote.blocks.lock().unwrap().remove(&child);
    assert!(
        restore_combined_graph(
            &MemoryContentStore::default(),
            &remote,
            &f.query,
            (&f.relations, &f.entities),
            (transfer_limits(), capture_limits().dataset)
        )
        .await
        .is_err()
    );
}
#[test]
fn combined_catalog_canonical_bytes_and_global_budgets_refuse() {
    let f = Fixture::new();
    let bounds = capture_limits().dataset;
    let bytes = f.dataset.canonical_bytes(bounds).unwrap();
    let decoded =
        GraphDatasetDescriptor::decode_checked(&bytes, &dag_cbor_cid(&bytes), bounds).unwrap();
    assert_eq!(decoded, f.dataset);
    let mut members = f.dataset.relations().to_vec();
    members.pop();
    assert!(
        GraphDatasetDescriptor::admit(
            &f.original.query,
            &f.relations,
            f.dataset.properties().clone(),
            members,
            bounds
        )
        .is_err()
    );
    let mut members = f.dataset.relations().to_vec();
    members.push(members[0].clone());
    assert!(
        GraphDatasetDescriptor::admit(
            &f.original.query,
            &f.relations,
            f.dataset.properties().clone(),
            members,
            bounds
        )
        .is_err()
    );
    let mut reduced = bounds;
    reduced.inventory.max_total_bytes = 1;
    assert!(matches!(
        f.dataset.canonical_bytes(reduced),
        Err(GraphInventoryError::Limit)
    ));
    let mut inputs = f.inputs();
    inputs.limits.max_blocks = 2;
    assert!(prepare_combined_graph(&f.local, inputs).is_err());
    let mut inputs = f.inputs();
    inputs.limits.max_total_bytes = 1;
    assert!(prepare_combined_graph(&f.local, inputs).is_err());
    assert!(prepare_combined_graph(&MemoryContentStore::default(), f.inputs()).is_err());
    let mut wire: ipld_core::ipld::Ipld = serde_ipld_dagcbor::from_slice(&bytes).unwrap();
    let ipld_core::ipld::Ipld::Map(ref mut map) = wire else {
        panic!("descriptor map")
    };
    map.insert("version".into(), ipld_core::ipld::Ipld::Integer(2));
    let invalid = serde_ipld_dagcbor::to_vec(&wire).unwrap();
    assert!(
        GraphDatasetDescriptor::decode_checked(&invalid, &dag_cbor_cid(&invalid), bounds).is_err()
    );
}

pub(super) fn relation_tables(
    f: &Fixture,
    captured: &crate::CapturedCombinedGraphAr,
) -> Vec<mrr_data_datafusion::BinaryRelationTable> {
    captured
        .relations(&f.query)
        .unwrap()
        .iter()
        .map(|relation| {
            let id = &relation.relation;
            let facts = &relation.facts;
            let schema = f
                .original
                .relations
                .iter()
                .find(|t| t.schema.id() == *id)
                .unwrap();
            let mut columns = [Vec::new(), Vec::new()];
            for fact in facts.iter() {
                assert_eq!(
                    fact.context().generation(),
                    f.original.semantic.generation()
                );
                for (index, value) in fact.values().iter().enumerate() {
                    let mrr::Value::Entity(id) = value else {
                        panic!("binary Entity")
                    };
                    columns[index].push(id.to_string());
                }
            }
            let arrays = columns
                .into_iter()
                .map(|values| {
                    Arc::new(arrow_array::StringArray::from(values)) as arrow_array::ArrayRef
                })
                .collect();
            mrr_data_datafusion::BinaryRelationTable {
                schema: schema.schema.clone(),
                batch: arrow_array::RecordBatch::try_new(schema.batch.schema(), arrays).unwrap(),
            }
        })
        .collect::<Vec<_>>()
}

pub(super) fn alternate_root(f: &Fixture) -> mrr_data_core::BoundDataQuery {
    use mrr_data_core::{
        CoverageDescriptor, CoverageKind, SnapshotBlock, SnapshotManifest, SnapshotManifestRequest,
        bind_data_query, raw_cid,
    };
    let manifest = f.snapshot.manifest();
    let snapshot = SnapshotBlock::encode(
        SnapshotManifest::admit(
            SnapshotManifestRequest::new(
                f.original.semantic.clone(),
                &f.relations,
                &f.entities,
                manifest.relations().to_vec(),
                CoverageDescriptor::new(CoverageKind::Complete, raw_cid(b"alternate coverage"))
                    .unwrap(),
            )
            .with_entities(manifest.entities().to_vec())
            .with_graph_projection(manifest.graph_projection().unwrap().clone()),
        )
        .unwrap(),
    )
    .unwrap();
    bind_data_query(
        &f.original.query,
        &snapshot,
        &mrr_data_datafusion::datafusion_engine_profile().unwrap(),
    )
    .unwrap()
}
