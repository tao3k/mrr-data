//! Catalog-backed bounded property path queries.
mod execution;
mod restored;
mod validation;
pub use execution::{
    BinaryRelationTable, EntityPropertyTable, PropertyQueryLimits, execute_property_path_query,
};
pub use restored::{RestoredPropertyQuery, execute_restored_property_path_query};
