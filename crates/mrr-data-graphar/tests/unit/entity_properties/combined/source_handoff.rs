//! Original MRR source admission over the existing native combined closure.
use super::{
    acceptance::relation_tables,
    fixture::{Fixture, capture_limits, transfer_limits},
    remote::Remote,
};
use crate::{capture_combined_graphar, tests::entity_properties::fixture as properties};
use meta_relational_reasoning as mrr;
use mrr_data_content::{MemoryContentStore, publish_combined_graph, restore_combined_graph};
use mrr_property_source::{CompiledPropertySourceQuery, compile_property_source_query};
use std::num::NonZeroUsize;
const SOURCE: &str = include_str!(
    "../../../../../mrr-data-datafusion/tests/fixtures/healthcare-case-profile-relations.gql"
);
const SOURCE_DIGEST: &str =
    "sha256:7a3a88a9ebd24cd738d426c0def633247d1a0fc13e9e37cca13bb23e90ba0c63";
fn compile() -> CompiledPropertySourceQuery {
    compile_property_source_query("case-profile-relations.gql", SOURCE, SOURCE_DIGEST).unwrap()
}
struct Executor {
    entities: Vec<mrr_data_datafusion::EntityPropertyTable>,
    relations: Vec<mrr_data_datafusion::BinaryRelationTable>,
}
impl Executor {
    async fn execute<'a>(
        &'a self,
        query: &'a mrr::CatalogBoundQuery,
    ) -> Result<mrr::CandidateQueryResult, mrr_data_datafusion::DataFusionQueryError> {
        let result = mrr_data_datafusion::execute_property_path_query(
            query,
            &self.entities,
            &self.relations,
            properties::limits(),
        )
        .await?;
        Ok(mrr::CandidateQueryResult::new(
            mrr::QueryResultBinding::for_query(query),
            result.columns().to_vec(),
            result.rows().to_vec(),
        ))
    }
}
#[tokio::test]
async fn original_source_handoff_native_combined_cold_and_warm_reaches_mrr_admission() {
    let f = Fixture::with_original(source_fixture());
    let prepared = f.prepare();
    let remote = Remote::default();
    publish_combined_graph(&prepared, &f.local, &remote, &remote, || async { Ok(()) })
        .await
        .unwrap();
    let cache = MemoryContentStore::default();
    let mut previous = None;
    for _ in 0..2 {
        let restored = restore_combined_graph(
            &cache,
            &remote,
            &f.query,
            (&f.relations, &f.entities),
            (transfer_limits(), capture_limits().dataset),
        )
        .await
        .unwrap();
        let captured = capture_combined_graphar(
            &restored,
            &f.query,
            &f.relations,
            &f.projection,
            capture_limits(),
        )
        .unwrap();
        let executor = Executor {
            entities: captured
                .tables(&f.query)
                .unwrap()
                .iter()
                .map(|t| mrr_data_datafusion::EntityPropertyTable {
                    schema: t.schema.clone(),
                    batch: t.batch.clone(),
                })
                .collect(),
            relations: relation_tables(&f, &captured),
        };
        let bound = compile()
            .bind(&f.relations, &f.entities, &f.original.semantic)
            .unwrap();
        let candidate = executor.execute(bound.query()).await.unwrap();
        let _admitted = bound
            .admit(
                &candidate,
                mrr::QueryResultLimits::new(
                    NonZeroUsize::new(100).unwrap(),
                    NonZeroUsize::new(300).unwrap(),
                ),
            )
            .unwrap();
        assert_eq!(bound.compilation().source_digest, SOURCE_DIGEST);
        let mut rows = candidate.rows().to_vec();
        rows.sort_by_key(|r| format!("{r:?}"));
        let mut expected = crate::tests::entity_properties::acceptance::expected();
        expected.push(expected[0].clone());
        expected.sort_by_key(|r| format!("{r:?}"));
        assert_eq!(rows, expected);
        if let Some(prior) = previous {
            assert_eq!(rows, prior);
        }
        previous = Some(rows);
        // The second restore must use the complete immutable local cache.
        remote.blocks.lock().unwrap().clear();
    }
}
fn source_fixture() -> properties::Fixture {
    let compiled = compile();
    let [path] = compiled.query().graph().paths() else {
        panic!("one original path")
    };
    let mut f = properties::fixture();
    // The caller's catalogs use the type identities emitted by MRR's frontend.
    // MRR Data does not infer labels or implement another identity convention.
    for (table, node) in f.entities.iter_mut().zip(
        std::iter::once(path.start()).chain(path.segments().iter().map(|segment| segment.node())),
    ) {
        let [id] = node.types() else {
            panic!("one declared node type")
        };
        table.schema =
            mrr::EntitySchema::new(*id, table.schema.name(), table.schema.properties().to_vec())
                .unwrap();
    }
    for (table, segment) in f.relations.iter_mut().zip(path.segments()) {
        let [id] = segment.relation().types() else {
            panic!("one declared relation type")
        };
        table.schema = mrr::RelationSchema::new(
            *id,
            table.schema.predicate(),
            table.schema.fields().to_vec(),
            table.schema.constraints().to_vec(),
        )
        .unwrap();
    }
    // Two distinct physical edges project to the same row under RETURN ALL.
    let table = &mut f.relations[1];
    let arrays = table
        .batch
        .columns()
        .iter()
        .map(|column| {
            let strings = column
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap();
            let values = strings
                .iter()
                .chain(strings.iter().take(1))
                .collect::<Vec<_>>();
            std::sync::Arc::new(arrow_array::StringArray::from(values)) as arrow_array::ArrayRef
        })
        .collect();
    table.batch = arrow_array::RecordBatch::try_new(table.batch.schema(), arrays).unwrap();
    let relations = mrr::RelationCatalog::admit(
        f.relations
            .iter()
            .map(|table| table.schema.clone())
            .collect(),
    )
    .unwrap();
    let entities = mrr::EntityCatalog::admit(
        f.entities
            .iter()
            .map(|table| table.schema.clone())
            .collect(),
    )
    .unwrap();
    f.query = compiled
        .bind(&relations, &entities, &f.semantic)
        .unwrap()
        .query()
        .clone();
    f
}

/// Canonical simulated caller: original source, cold transfer, native capture,
/// physical query and typed consumer transport retain one shared Backend.
#[cfg(feature = "backend")]
#[tokio::test]
async fn original_source_handoff_shared_backend_reaches_retained_consumer_transport() {
    use crate::{CombinedGraphArRestoreRequest, restore_combined_graph_content};
    use mrr_data_backend::{Backend, BackendConfig, ResourceControl, ResourceStop};
    use std::sync::Arc;
    const RESERVED: usize = 8 << 20;
    let f = Fixture::with_original(source_fixture());
    let backend = Backend::open(
        BackendConfig {
            max_resource_bytes: 3 * RESERVED,
            ..BackendConfig::default()
        },
        crate::tests::snapshot::backend_qualification::MetadataStub,
        tokio::runtime::Handle::current(),
    )
    .await
    .unwrap();
    let remote = Arc::new(Remote::default());
    publish_combined_graph(
        &f.prepare(),
        &f.local,
        remote.as_ref(),
        remote.as_ref(),
        || async { Ok(()) },
    )
    .await
    .unwrap();
    let restored = restore_combined_graph_content(
        &backend,
        CombinedGraphArRestoreRequest {
            local: Arc::new(MemoryContentStore::default()),
            remote,
            query: f.query.clone(),
            relations: f.relations.clone(),
            entities: f.entities.clone(),
            dataset: capture_limits().dataset,
            transfer: transfer_limits(),
        },
        RESERVED,
        ResourceControl::default(),
    )
    .await
    .unwrap();
    assert_eq!(backend.status().resource_bytes, RESERVED);
    let input = restored.clone();
    let query = f.query.clone();
    let relations = f.relations.clone();
    let projection = f.projection.clone();
    let captured = backend
        .prepare_resource_controlled(RESERVED, ResourceControl::default(), move |_| {
            capture_combined_graphar(
                input.get(),
                &query,
                &relations,
                &projection,
                capture_limits(),
            )
        })
        .await
        .unwrap();
    drop(restored);
    assert_eq!(backend.status().resource_bytes, RESERVED);
    let tables = captured
        .get()
        .tables(&f.query)
        .unwrap()
        .iter()
        .map(|t| mrr_data_datafusion::EntityPropertyTable {
            schema: t.schema.clone(),
            batch: t.batch.clone(),
        })
        .collect::<Vec<_>>();
    let relations = relation_tables(&f, captured.get());
    let bound = compile()
        .bind(&f.relations, &f.entities, &f.original.semantic)
        .unwrap();
    assert_eq!(bound.compilation().source_digest, SOURCE_DIGEST);
    let query = bound.query().clone();
    let retained_input = captured.clone();
    let output = backend
        .prepare_resource_async_controlled(
            RESERVED,
            ResourceControl::default(),
            move |control| async move {
                control.check()?;
                let output = mrr_data_datafusion::execute_property_path_query(
                    &query,
                    &tables,
                    &relations,
                    properties::limits(),
                )
                .await
                .unwrap();
                control.check()?;
                drop(retained_input);
                Ok::<_, ResourceStop>(output)
            },
        )
        .await
        .unwrap();
    let mut rows = output.get().rows().to_vec();
    rows.sort_by_key(|row| format!("{row:?}"));
    let mut expected = crate::tests::entity_properties::acceptance::expected();
    expected.push(expected[0].clone());
    expected.sort_by_key(|row| format!("{row:?}"));
    assert_eq!(rows, expected);
    drop(captured);
    let limits = mrr::QueryResultLimits::new(
        NonZeroUsize::new(100).unwrap(),
        NonZeroUsize::new(300).unwrap(),
    );
    let cap = NonZeroUsize::new(1 << 20).unwrap();
    let transport = output
        .try_transform(|output| {
            mrr_data_core::DataQueryResultHandoff::export(
                &f.query,
                &mrr_data_datafusion::datafusion_engine_profile().unwrap(),
                output,
                limits,
                cap,
            )
        })
        .unwrap_or_else(|_| panic!("unique candidate transport conversion"));
    transport.get().verify(&f.query, limits, cap).unwrap();
    assert_eq!(backend.status().resource_bytes, RESERVED);
    let retained = transport.clone();
    drop(transport);
    let closing = backend.clone();
    let shutdown = tokio::spawn(async move { closing.shutdown().await });
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while backend.status().lifecycle != mrr_data_backend::Lifecycle::Draining {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!shutdown.is_finished());
    retained.get().verify(&f.query, limits, cap).unwrap();
    drop(retained);
    tokio::time::timeout(std::time::Duration::from_secs(3), shutdown)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(backend.status().resource_bytes, 0);
}
