//! Fail-closed admission for the MRR V1 binary-Entity `GraphAr` projection.
//!
//! This crate owns no `GraphAr` wire format. It produces semantic edge records
//! that an adapter must hand to the upstream Apache `GraphAr` implementation.
#![forbid(unsafe_code)]

mod projection;
#[cfg(feature = "upstream-graphar")]
mod writer;

pub use mrr_data_core::{GRAPHAR_BINARY_ENTITY_NAMESPACE, GRAPHAR_BINARY_ENTITY_VERSION};
pub use projection::{
    BinaryEntityProjection, GraphEdgeRecord, GraphProjectionError, IndexedGraphEdge,
    PhysicalVertexIndex,
};
#[cfg(feature = "upstream-graphar")]
pub use writer::{GraphArDatasetReceipt, GraphArWriteError, write_graphar_dataset};

#[cfg(test)]
#[path = "../tests/unit/mod.rs"]
mod tests;
