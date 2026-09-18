//! `DataFusion` execution for the admitted single-hop binary Entity query slice.
#![forbid(unsafe_code)]

mod adapter;

pub use adapter::{DataFusionQueryError, datafusion_engine_profile, execute_binary_entity_query};

#[cfg(test)]
#[path = "../tests/unit/mod.rs"]
mod tests;
