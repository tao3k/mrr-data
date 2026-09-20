//! Catalog-backed bounded property path queries.
mod execution;
mod validation;
pub use execution::{
    BinaryRelationTable, EntityPropertyTable, PropertyQueryLimits, execute_property_path_query,
};
