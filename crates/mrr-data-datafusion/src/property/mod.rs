//! Catalog-backed bounded property path queries.
mod backend;
mod execution;
mod restored;
mod validation;
pub use backend::RestoredPropertyBackend;
pub use execution::{
    BinaryRelationTable, EntityPropertyTable, PropertyExecutionMetrics, PropertyQueryLimits,
    execute_property_path_query, execute_property_path_query_observed,
};
pub use restored::{
    RestoredPropertyQuery, execute_restored_property_path_query,
    execute_restored_property_query_handoff,
};
