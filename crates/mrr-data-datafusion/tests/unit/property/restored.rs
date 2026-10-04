use super::{DataFusionQueryError, Fixture, execute_property_path_query, fixture, limits, mrr};
use crate::{RestoredPropertyQuery, execute_restored_property_path_query};
use arrow_array::RecordBatch;
use arrow_ipc::{
    CompressionType,
    writer::{IpcWriteOptions, StreamWriter},
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use mrr_data_content::{
    ContentBlock, ContentCodec, ContentSource, ContentStore, MemoryContentStore,
    RemoteContentStore, RemoteError, RemoteFuture, RestoredSnapshot, SnapshotTransferLimits,
    publish_snapshot, restore_snapshot,
};
use mrr_data_core::{
    BatchDescriptor, CoverageDescriptor, CoverageKind, EntityDescriptor, RelationDescriptor,
    SnapshotBlock, SnapshotManifest, SnapshotManifestRequest, raw_cid,
};
use std::{
    collections::BTreeMap,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};

#[tokio::test]
async fn mrr_dispatches_to_data_backend_and_admits_original_physical_candidate() {
    let f = fixture();
    let (cold, _, relations, entities) = restored(&f, EntityChildMode::Valid).await;
    println!("Dispatch fixture immutable physical root restored");
    let query = f.query.clone();
    println!("Original MRR query supplied to backend");
    let backend = crate::RestoredPropertyBackend {
        restored: &cold,
        relation_catalog: &relations,
        entity_catalog: &entities,
        limits: limits(),
    };
    let result_limits = mrr::QueryResultLimits::new(
        NonZeroUsize::new(100).unwrap(),
        NonZeroUsize::new(300).unwrap(),
    );
    let execution = query
        .execute_with(&backend, result_limits)
        .await
        .expect("MRR dispatch and result admission");
    println!("Data backend execution returned to MRR admission");
    assert_eq!(execution.receipt().row_count(), 3);
    assert_eq!(execution.physical_evidence().query(), &query);
    assert_eq!(
        execution.physical_evidence().snapshot_root(),
        cold.snapshot().cid()
    );
    assert_eq!(execution.candidate().rows().len(), 3);
    let cap = NonZeroUsize::new(1_048_576).unwrap();
    let handoff =
        mrr_data_core::DataQueryResultHandoff::export_execution(&execution, result_limits, cap)
            .unwrap();
    let received = handoff
        .verify(execution.physical_evidence(), result_limits, cap)
        .unwrap();
    assert_eq!(received.candidate(), execution.candidate());
    assert_eq!(received.receipt(), execution.receipt());
    println!("Original dispatch candidate preserved through Scheme v2 handoff");
}

#[tokio::test]
async fn mrr_dispatch_preserves_corrupt_backend_failure_instead_of_empty_success() {
    let f = fixture();
    let (cold, _, relations, entities) = restored(&f, EntityChildMode::InvalidIpc).await;
    println!("Corrupt dispatch fixture immutable root restored");
    let query = f.query.clone();
    println!("Original MRR query admitted before physical decode");
    let backend = crate::RestoredPropertyBackend {
        restored: &cold,
        relation_catalog: &relations,
        entity_catalog: &entities,
        limits: limits(),
    };
    let result_limits = mrr::QueryResultLimits::new(
        NonZeroUsize::new(100).unwrap(),
        NonZeroUsize::new(300).unwrap(),
    );
    assert!(matches!(
        query.execute_with(&backend, result_limits).await,
        Err(mrr::PropertyExecutionError::Backend(
            DataFusionQueryError::RestoredSnapshot("truncated IPC metadata")
        ))
    ));
}

#[tokio::test]
async fn mrr_dispatch_rejects_a_new_generation_bound_to_an_old_physical_root() {
    let f = fixture();
    let (cold, _, relations, entities) = restored(&f, EntityChildMode::Valid).await;
    println!("Old-generation physical root restored");
    let generation = mrr::GenerationId::from_canonical_bytes("dispatch-new-generation").unwrap();
    let semantic = mrr::SemanticSnapshot::admit(
        generation,
        vec![
            mrr::RevisionBinding::admit(
                mrr::ExternalRevisionIdentity::new("test", "source", "new-revision").unwrap(),
                generation,
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let bundle = mrr::ReasoningBundle::admit(mrr::ReasoningBundleDeclaration {
        entities: f
            .entities
            .iter()
            .map(|table| table.schema.clone())
            .collect(),
        relations: f
            .relations
            .iter()
            .map(|table| table.schema.clone())
            .collect(),
        query_templates: vec![mrr::QueryTemplate::new(f.query.query().clone(), vec![])],
        ..Default::default()
    })
    .unwrap();
    let query = mrr::bind_query_to_catalog(&bundle, f.query.query().id(), &semantic).unwrap();
    println!("New-generation query admitted by MRR");
    let backend = crate::RestoredPropertyBackend {
        restored: &cold,
        relation_catalog: &relations,
        entity_catalog: &entities,
        limits: limits(),
    };
    let result_limits = mrr::QueryResultLimits::new(
        NonZeroUsize::new(100).unwrap(),
        NonZeroUsize::new(300).unwrap(),
    );
    assert!(matches!(
        query.execute_with(&backend, result_limits).await,
        Err(mrr::PropertyExecutionError::Backend(
            DataFusionQueryError::PhysicalBinding(
                mrr_data_core::DataQueryBindingError::GenerationMismatch { .. }
            )
        ))
    ));
}

#[tokio::test]
async fn backend_success_still_requires_mrr_result_admission() {
    let f = fixture();
    let (cold, _, relations, entities) = restored(&f, EntityChildMode::Valid).await;
    println!("Physical root restored before MRR admission limit gate");
    let backend = crate::RestoredPropertyBackend {
        restored: &cold,
        relation_catalog: &relations,
        entity_catalog: &entities,
        limits: limits(),
    };
    let result_limits =
        mrr::QueryResultLimits::new(NonZeroUsize::new(1).unwrap(), NonZeroUsize::new(3).unwrap());
    assert!(matches!(
        f.query.execute_with(&backend, result_limits).await,
        Err(mrr::PropertyExecutionError::Admission(_))
    ));
}
#[cfg(feature = "source-handoff")]
mod source_handoff;
