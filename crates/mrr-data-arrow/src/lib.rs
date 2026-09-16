//! Lossless, relation-specific Arrow interchange for complete MRR V1 facts.
#![forbid(unsafe_code)]

mod codec;

pub use codec::{
    ARROW_FACT_PROFILE_V1, ArrowRelationError, IpcImportLimits, facts_to_ipc,
    facts_to_record_batch, ipc_to_facts, project_fact_schema, record_batch_to_facts,
};

#[cfg(test)]
#[path = "../tests/unit/mod.rs"]
mod tests;
