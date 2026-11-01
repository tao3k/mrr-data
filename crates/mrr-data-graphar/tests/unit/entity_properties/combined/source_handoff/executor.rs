//! Test caller supplies captured native tables to MRR's physical dispatch port.
use meta_relational_reasoning::{
    CatalogBoundQuery, PropertyExecutionCandidate, PropertyQueryBackend,
};
use mrr_data_core::{BoundDataQuery, project_data_query_output};
use mrr_data_datafusion::{
    BinaryRelationTable, DataFusionQueryError, EntityPropertyTable, PropertyQueryLimits,
    datafusion_engine_profile, execute_property_path_query,
};

pub(super) struct CapturedBackend {
    pub binding: BoundDataQuery,
    pub tables: Vec<EntityPropertyTable>,
    pub relations: Vec<BinaryRelationTable>,
    pub limits: PropertyQueryLimits,
}
impl PropertyQueryBackend for CapturedBackend {
    type PhysicalEvidence = BoundDataQuery;
    type Error = DataFusionQueryError;

    async fn execute<'a>(
        &'a self,
        query: &'a CatalogBoundQuery,
    ) -> Result<PropertyExecutionCandidate<Self::PhysicalEvidence>, Self::Error> {
        assert_eq!(query, self.binding.query(), "original MRR dispatch query");
        let output =
            execute_property_path_query(query, &self.tables, &self.relations, self.limits).await?;
        let candidate =
            project_data_query_output(&self.binding, &datafusion_engine_profile()?, output)
                .map_err(DataFusionQueryError::PhysicalOutput)?;
        Ok(PropertyExecutionCandidate {
            candidate,
            physical_evidence: self.binding.clone(),
        })
    }
}
