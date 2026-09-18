//! Fail-closed admission for the MRR V1 binary-Entity `GraphAr` projection.
//!
//! This crate owns no `GraphAr` wire format. It produces semantic edge records
//! that the adapter hands to the project-maintained Apache `GraphAr` fork.
#![forbid(unsafe_code)]

mod projection;
mod query_source;
#[cfg(feature = "native-graphar")]
mod reader;
#[cfg(feature = "native-graphar")]
mod writer;

pub use mrr_data_profile::{GRAPHAR_BINARY_ENTITY_NAMESPACE, GRAPHAR_BINARY_ENTITY_VERSION};
pub use projection::{
    BinaryEntityProjection, GraphEdgeRecord, GraphProjectionError, IndexedGraphEdge,
    PhysicalVertexIndex,
};
pub use query_source::GraphArQuerySource;
#[cfg(feature = "native-graphar")]
pub use reader::{
    GraphArDataset, GraphArNativeEdgeTimings, GraphArPrepareTimings, GraphArReadError,
    GraphArReadLimits, GraphArReadTimings, PreparedGraphArSource, prepare_graphar_source,
    read_graphar_dataset, read_graphar_dataset_observed,
};
#[cfg(feature = "native-graphar")]
pub use writer::{GraphArDatasetReceipt, GraphArWriteError, write_graphar_dataset};

#[cfg(test)]
#[path = "../tests/unit/mod.rs"]
mod tests;
