//! MRR dispatch port implemented by the restored `DataFusion` backend.

use meta_relational_reasoning::{CatalogBoundQuery, EntityCatalog, RelationCatalog};
use meta_relational_reasoning::{PropertyExecutionCandidate, PropertyQueryBackend};
use mrr_data_content::RestoredSnapshot;
use mrr_data_core::{BoundDataQuery, bind_data_query, project_data_query_output};

use super::{PropertyQueryLimits, RestoredPropertyQuery, execute_restored_property_path_query};
use crate::DataFusionQueryError;

/// Backend configuration retains verified physical inputs and execution limits.
/// MRR submits its original bound query through `PropertyQueryBackend`.
pub struct RestoredPropertyBackend<'a> {
    pub restored: &'a RestoredSnapshot,
    pub relation_catalog: &'a RelationCatalog,
    pub entity_catalog: &'a EntityCatalog,
    pub limits: PropertyQueryLimits,
}

impl PropertyQueryBackend for RestoredPropertyBackend<'_> {
    type PhysicalEvidence = BoundDataQuery;
    type Error = DataFusionQueryError;

    async fn execute<'a>(
        &'a self,
        query: &'a CatalogBoundQuery,
    ) -> Result<PropertyExecutionCandidate<Self::PhysicalEvidence>, Self::Error> {
        let profile = crate::datafusion_engine_profile()?;
        let binding = bind_data_query(query, self.restored.snapshot(), &profile)
            .map_err(DataFusionQueryError::PhysicalBinding)?;
        let output = execute_restored_property_path_query(RestoredPropertyQuery {
            query,
            restored: self.restored,
            relation_catalog: self.relation_catalog,
            entity_catalog: self.entity_catalog,
            limits: self.limits,
        })
        .await?;
        let candidate = project_data_query_output(&binding, &profile, output)
            .map_err(DataFusionQueryError::PhysicalOutput)?;
        Ok(PropertyExecutionCandidate {
            candidate,
            physical_evidence: binding,
        })
    }
}
