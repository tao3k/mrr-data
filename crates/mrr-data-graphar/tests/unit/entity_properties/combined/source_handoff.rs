//! Original MRR source admission over the existing native combined closure.
#[cfg(unix)]
mod child_ownership;
use super::{
    acceptance::relation_tables,
    fixture::{Fixture, capture_limits, transfer_limits},
    remote::Remote,
};
use crate::{capture_combined_graphar, tests::entity_properties::fixture as properties};
use meta_relational_reasoning as mrr;
use meta_relational_reasoning::PathSegment;
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
    binding: mrr_data_core::BoundDataQuery,
}
impl mrr::PropertyQueryBackend for Executor {
    type PhysicalEvidence = mrr_data_core::BoundDataQuery;
    type Error = mrr_data_datafusion::DataFusionQueryError;
    async fn execute<'a>(
        &'a self,
        query: &'a mrr::CatalogBoundQuery,
    ) -> Result<mrr::PropertyExecutionCandidate<Self::PhysicalEvidence>, Self::Error> {
        if self.binding.query() != query {
            return Err(mrr_data_datafusion::DataFusionQueryError::CatalogMismatch);
        }
        let result = mrr_data_datafusion::execute_property_path_query(
            query,
            &self.entities,
            &self.relations,
            properties::limits(),
        )
        .await?;
        let profile = mrr_data_datafusion::datafusion_engine_profile()?;
        let candidate = mrr_data_core::project_data_query_output(&self.binding, &profile, result)
            .map_err(mrr_data_datafusion::DataFusionQueryError::PhysicalOutput)?;
        Ok(mrr::PropertyExecutionCandidate {
            candidate,
            physical_evidence: self.binding.clone(),
        })
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
            binding: f.query.clone(),
        };
        let bound = compile()
            .bind(&f.relations, &f.entities, &f.original.semantic)
            .unwrap();
        let admitted = bound
            .execute_with(
                &executor,
                mrr::QueryResultLimits::new(
                    NonZeroUsize::new(100).unwrap(),
                    NonZeroUsize::new(300).unwrap(),
                ),
            )
            .await
            .unwrap();
        let candidate = admitted.candidate();
        assert_eq!(admitted.receipt().row_count(), 4);
        assert_eq!(admitted.physical_evidence().query(), bound.query());
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
    source_fixture_with_semantic(None)
}
fn source_fixture_with_semantic(semantic: Option<mrr::SemanticSnapshot>) -> properties::Fixture {
    let compiled = compile();
    let [path] = compiled.query().graph().paths() else {
        panic!("one original path")
    };
    let mut f = properties::fixture();
    if let Some(semantic) = semantic {
        f.semantic = semantic;
    }
    // The caller's catalogs use the type identities emitted by MRR's frontend.
    // MRR Data does not infer labels or implement another identity convention.
    for (table, node) in f
        .entities
        .iter_mut()
        .zip(std::iter::once(path.start()).chain(path.segments().iter().map(PathSegment::node)))
    {
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

#[cfg(feature = "backend")]
mod backend;
