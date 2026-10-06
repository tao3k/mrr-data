//! Catalog-backed bounded property path queries.
mod backend;
mod execution;
mod reference;
mod transformation;
pub use transformation::{
    PropertyTransformationRuntime, property_transformation_artifact,
    property_transformation_endpoint, property_transformation_root,
};
mod restored;
mod validation;
pub use backend::RestoredPropertyBackend;
pub use execution::{
    BinaryRelationTable, EntityPropertyTable, PropertyExecutionMetrics, PropertyQueryLimits,
    execute_property_path_query, execute_property_path_query_observed,
};
pub use reference::{reference_property_path_query, verify_property_path_output};
pub use restored::{
    RestoredPropertyQuery, execute_restored_property_path_query,
    execute_restored_property_query_handoff, verify_restored_property_path_output,
};
