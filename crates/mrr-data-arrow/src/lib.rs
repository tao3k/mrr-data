//! Lossless, relation-specific Arrow interchange for complete MRR V1 facts.
#![forbid(unsafe_code)]

mod codec;
mod error;
mod ipc;
mod schema;

pub use codec::{
    facts_to_ipc, facts_to_record_batch, ipc_to_facts, project_fact_schema, record_batch_to_facts,
};
pub use error::ArrowRelationError;
pub use ipc::IpcImportLimits;
pub use mrr_data_profile::{ARROW_FACT_SCHEMA_NAMESPACE, ARROW_FACT_SCHEMA_VERSION};

#[cfg(test)]
#[path = "../tests/unit/mod.rs"]
mod tests;
