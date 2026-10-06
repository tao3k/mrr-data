//! `DataFusion` execution for bounded, catalog-admitted Entity and property queries.
#![forbid(unsafe_code)]

mod adapter;
mod property;
pub use property::{
    BinaryRelationTable, EntityPropertyTable, PropertyExecutionMetrics, PropertyQueryLimits,
    PropertyTransformationRuntime, RestoredPropertyBackend, RestoredPropertyQuery,
    execute_property_path_query, execute_property_path_query_observed,
    execute_restored_property_path_query, execute_restored_property_query_handoff,
    property_transformation_artifact, property_transformation_endpoint,
    property_transformation_grant_catalog, property_transformation_root,
    reference_property_path_query, verify_property_path_output,
    verify_restored_property_path_output,
};

pub use adapter::{
    DataFusionExecutionTimings, DataFusionQueryError, datafusion_engine_profile,
    execute_binary_entity_query, execute_binary_entity_query_observed,
};

#[cfg(test)]
#[path = "../tests/unit/mod.rs"]
mod tests;
