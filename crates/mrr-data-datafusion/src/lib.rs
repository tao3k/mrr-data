//! `DataFusion` execution for bounded, catalog-admitted Entity and property queries.
#![forbid(unsafe_code)]

mod adapter;
mod property;
pub use property::{
    BinaryRelationTable, EntityPropertyTable, PropertyQueryLimits, execute_property_path_query,
};

pub use adapter::{DataFusionQueryError, datafusion_engine_profile, execute_binary_entity_query};

#[cfg(test)]
#[path = "../tests/unit/mod.rs"]
mod tests;
