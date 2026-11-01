//! Root registration, exact child identity and same-generation physical drift.
use super::{
    acceptance::{inputs, limits},
    fixture,
};
use crate::{
    GraphArChunkLayout, GraphArEntityPropertyBlock, GraphArEntityPropertyError as Error,
    capture_registered_graphar_entity_properties, write_graphar_entity_properties,
};
use meta_relational_reasoning as mrr;
use mrr_data_core::{
    BatchDescriptor, BoundDataQuery, CoverageDescriptor, CoverageKind, DataEngineProfile,
    RelationDescriptor, SnapshotBlock, SnapshotManifest, SnapshotManifestRequest, bind_data_query,
    raw_cid,
};

pub(super) fn snapshot(f: &fixture::Fixture, block: &GraphArEntityPropertyBlock) -> SnapshotBlock {
    snapshot_with_coverage(f, block, b"simulated complete fixture")
}
fn snapshot_with_coverage(
    f: &fixture::Fixture,
    block: &GraphArEntityPropertyBlock,
    coverage: &[u8],
) -> SnapshotBlock {
    let relations =
        mrr::RelationCatalog::admit(f.relations.iter().map(|t| t.schema.clone()).collect())
            .unwrap();
    let entities =
        mrr::EntityCatalog::admit(f.entities.iter().map(|t| t.schema.clone()).collect()).unwrap();
    let descriptors = f
        .relations
        .iter()
        .map(|t| {
            let batch = ipc(&t.batch);
            RelationDescriptor::new(t.schema.id(), t.batch.num_rows() as u64, vec![batch]).unwrap()
        })
        .collect();
    let properties = f
        .entities
        .iter()
        .map(|t| {
            mrr_data_core::EntityDescriptor::new(
                t.schema.clone(),
                t.batch.num_rows() as u64,
                vec![ipc(&t.batch)],
            )
            .unwrap()
        })
        .collect();
    SnapshotBlock::encode(
        SnapshotManifest::admit(
            SnapshotManifestRequest::new(
                f.semantic.clone(),
                &relations,
                &entities,
                descriptors,
                CoverageDescriptor::new(CoverageKind::Complete, raw_cid(coverage)).unwrap(),
            )
            .with_entities(properties)
            .with_graph_projection(block.snapshot_projection("1").unwrap()),
        )
        .unwrap(),
    )
    .unwrap()
}
fn ipc(batch: &arrow_array::RecordBatch) -> BatchDescriptor {
    let mut bytes = Vec::new();
    {
        let mut writer =
            arrow_ipc::writer::StreamWriter::try_new(&mut bytes, &batch.schema()).unwrap();
        writer.write(batch).unwrap();
        writer.finish().unwrap();
    }
    BatchDescriptor::new(raw_cid(&bytes), batch.num_rows() as u64, bytes.len() as u64).unwrap()
}
pub(super) fn bound(f: &fixture::Fixture, snapshot: &SnapshotBlock) -> BoundDataQuery {
    let profile = DataEngineProfile::new("graphar-property-tables-v1", true, []).unwrap();
    bind_data_query(&f.query, snapshot, &profile).unwrap()
}

#[test]
fn registered_properties_round_trip_root_and_refuse_physical_root_drift() {
    let f = fixture::fixture();
    let (projection, tables) = inputs(&f);
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    let receipt = write_graphar_entity_properties(
        &source,
        &projection,
        &f.semantic,
        &tables,
        GraphArChunkLayout::new(2, 4).unwrap(),
        limits(),
    )
    .unwrap();
    let block = receipt.descriptor(limits()).unwrap();
    let snapshot = snapshot(&f, &block);
    let decoded = SnapshotManifest::decode_checked(snapshot.bytes(), snapshot.cid()).unwrap();
    assert_eq!(decoded, *snapshot.manifest());
    assert!(decoded.referenced_cids().contains(block.cid()));
    assert_eq!(
        decoded.graph_projection().unwrap().kind(),
        mrr_data_core::GraphProjectionKind::EntityProperties
    );
    let query = bound(&f, &snapshot);
    let captured = capture_registered_graphar_entity_properties(
        &source,
        &query,
        &projection,
        block.bytes(),
        limits(),
    )
    .unwrap();
    assert_eq!(
        captured
            .tables(&query)
            .unwrap()
            .iter()
            .map(|t| t.batch.num_rows())
            .sum::<usize>(),
        8
    );
    let alternate = write_graphar_entity_properties(
        &dir.path().join("alternate"),
        &projection,
        &f.semantic,
        &tables,
        GraphArChunkLayout::new(4, 4).unwrap(),
        limits(),
    )
    .unwrap()
    .descriptor(limits())
    .unwrap();
    let different = bound(&f, &self::snapshot(&f, &alternate));
    assert_ne!(query.snapshot_root(), different.snapshot_root());
    assert_eq!(
        query.query().snapshot_digest(),
        different.query().snapshot_digest()
    );
    assert!(matches!(captured.tables(&different), Err(Error::Scope)));
    assert!(matches!(
        capture_registered_graphar_entity_properties(
            &dir.path().join("absent"),
            &different,
            &projection,
            block.bytes(),
            limits()
        ),
        Err(Error::Integrity)
    ));
    assert!(matches!(
        captured.into_tables(&different),
        Err(Error::Scope)
    ));
}

#[cfg(feature = "backend")]
#[tokio::test]
async fn registered_root_capture_keeps_shared_backend_lease_and_cancel_scope() {
    use mrr_data_backend::{
        Backend, BackendConfig, ResourceControl, ResourcePreparationError, ResourceStop,
    };
    const RESERVED: usize = 1 << 20;
    let f = fixture::fixture();
    let (projection, tables) = inputs(&f);
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    let receipt = write_graphar_entity_properties(
        &source,
        &projection,
        &f.semantic,
        &tables,
        GraphArChunkLayout::default(),
        limits(),
    )
    .unwrap();
    let block = receipt.descriptor(limits()).unwrap();
    let query = bound(&f, &snapshot(&f, &block));
    let request = || crate::RegisteredGraphArEntityPropertiesRequest {
        source: source.clone(),
        query: query.clone(),
        projection: projection.clone(),
        descriptor_bytes: block.bytes().to_vec(),
        limits: limits(),
    };
    let backend = Backend::open(
        BackendConfig {
            max_resource_bytes: RESERVED,
            ..BackendConfig::default()
        },
        crate::tests::snapshot::backend_qualification::MetadataStub,
        tokio::runtime::Handle::current(),
    )
    .await
    .unwrap();
    assert!(matches!(
        crate::prepare_registered_graphar_entity_properties(
            &backend,
            request(),
            1,
            ResourceControl::new(None)
        )
        .await,
        Err(ResourcePreparationError::Backend(
            mrr_data_backend::BackendError::Limit
        ))
    ));
    let control = ResourceControl::new(None);
    control.cancel();
    assert!(matches!(
        crate::prepare_registered_graphar_entity_properties(&backend, request(), RESERVED, control)
            .await,
        Err(ResourcePreparationError::Preparation(Error::Stop(
            ResourceStop::Cancelled
        )))
    ));
    assert_eq!(backend.status().resource_bytes, 0);
    let captured = crate::prepare_registered_graphar_entity_properties(
        &backend,
        request(),
        RESERVED,
        ResourceControl::new(None),
    )
    .await
    .unwrap();
    let pointer = captured.get().tables(&query).unwrap().as_ptr();
    let tables = captured
        .try_transform(|s| s.into_tables(&query))
        .unwrap_or_else(|_| panic!("unique registered source transfers"));
    assert_eq!(tables.get().as_ptr(), pointer);
    let clone = tables.clone();
    drop(tables);
    assert_eq!(backend.status().resource_bytes, RESERVED);
    drop(clone);
    assert_eq!(backend.status().resource_bytes, 0);
    backend.shutdown().await.unwrap();
}

#[test]
fn identical_descriptor_under_changed_root_never_reuses_previous_capture() {
    let f = fixture::fixture();
    let (projection, tables) = inputs(&f);
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    let block = write_graphar_entity_properties(
        &source,
        &projection,
        &f.semantic,
        &tables,
        GraphArChunkLayout::default(),
        limits(),
    )
    .unwrap()
    .descriptor(limits())
    .unwrap();
    let first = bound(&f, &snapshot(&f, &block));
    let other = bound(
        &f,
        &snapshot_with_coverage(&f, &block, b"another coverage declaration"),
    );
    assert_eq!(
        first.graph_projection_manifest(),
        other.graph_projection_manifest()
    );
    assert_eq!(
        first.query().snapshot_digest(),
        other.query().snapshot_digest()
    );
    assert_ne!(first.snapshot_root(), other.snapshot_root());
    let captured = capture_registered_graphar_entity_properties(
        &source,
        &first,
        &projection,
        block.bytes(),
        limits(),
    )
    .unwrap();
    assert!(matches!(captured.tables(&other), Err(Error::Scope)));
    assert!(matches!(captured.into_tables(&other), Err(Error::Scope)));
}
