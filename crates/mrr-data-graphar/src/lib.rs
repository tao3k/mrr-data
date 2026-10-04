//! Fail-closed admission for the MRR V1 binary-Entity `GraphAr` projection.
//!
//! This crate owns no `GraphAr` wire format. It produces semantic edge records
//! that the adapter hands to the project-maintained Apache `GraphAr` fork.
#![forbid(unsafe_code)]

#[cfg(feature = "backend")]
mod backend;
#[cfg(feature = "file-inventory")]
mod inventory;
#[cfg(all(feature = "backend", feature = "selective-graphar"))]
pub use backend::{
    GraphArOutgoingRequest, GraphArSelectiveSnapshotRequest, prepare_graphar_outgoing,
    prepare_graphar_selective_snapshot,
};
#[cfg(feature = "backend")]
pub use backend::{GraphArSnapshotRequest, prepare_graphar_snapshot};
mod projection;
mod query_source;
#[cfg(feature = "native-graphar")]
mod reader;
#[cfg(feature = "selective-graphar")]
mod selective;
#[cfg(feature = "native-graphar")]
mod snapshot;
#[cfg(feature = "selective-graphar")]
pub use selective::{
    GraphArSelection, GraphArSelectionMetrics, GraphArSelectiveCaptureOptions,
    GraphArSelectiveCaptureRequest, GraphArSelectiveError, GraphArSelectivePreparationMetrics,
    GraphArSelectiveSnapshot, capture_graphar_selective_snapshot,
    capture_graphar_selective_snapshot_checked,
};
#[cfg(feature = "native-graphar")]
mod writer;
#[cfg(feature = "native-graphar")]
pub use snapshot::{CapturedGraphArSnapshot, GraphArCaptureError, capture_graphar_snapshot};

#[cfg(feature = "file-inventory")]
pub use inventory::{GraphArInventoryError, inventory_graphar_directory, verify_graphar_directory};
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
    prepare_graphar_source_with_adjacency, read_graphar_dataset, read_graphar_dataset_observed,
};
#[cfg(feature = "native-graphar")]
pub use writer::{
    GraphArAdjacency, GraphArChunkLayout, GraphArDatasetReceipt, GraphArWriteError,
    GraphArWriteOptions, write_graphar_dataset, write_graphar_dataset_with_limits,
    write_graphar_dataset_with_options,
};

#[cfg(test)]
#[path = "../tests/unit/mod.rs"]
mod tests;
