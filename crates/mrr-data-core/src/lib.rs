//! Physical engine contracts and optional CID/DAG-CBOR snapshot identity.
#![forbid(unsafe_code)]

#[cfg(feature = "content-identity")]
mod error;
#[cfg(feature = "content-identity")]
mod graph_binding;
#[cfg(feature = "content-identity")]
mod graph_dataset;
#[cfg(feature = "content-identity")]
mod graph_inventory;
#[cfg(feature = "content-identity")]
mod graph_properties;
#[cfg(feature = "content-identity")]
mod manifest;
#[cfg(feature = "content-identity")]
mod operation_binding;
#[cfg(feature = "content-identity")]
mod profile;
mod query_binding;
#[cfg(feature = "content-identity")]
mod snapshot_descriptors;

#[cfg(feature = "content-identity")]
pub use error::DataError;
#[cfg(feature = "content-identity")]
pub use graph_binding::GraphDatasetBinding;
#[cfg(feature = "content-identity")]
pub use graph_dataset::{GraphDatasetDescriptor, GraphDatasetLimits, GraphRelationMember};
#[cfg(feature = "content-identity")]
pub use graph_inventory::{
    GraphDatasetInventory, GraphFile, GraphFileKind, GraphInventoryError, GraphInventoryLimits,
};
#[cfg(feature = "content-identity")]
pub use graph_properties::{GraphEntityPropertyDescriptor, GraphEntityPropertyScope};
#[cfg(feature = "content-identity")]
pub use manifest::{
    BatchDescriptor, RelationDescriptor, SnapshotBlock, SnapshotManifest, SnapshotManifestRequest,
    raw_cid,
};
pub use mrr_data_profile::{
    ARROW_FACT_SCHEMA_NAMESPACE, ARROW_FACT_SCHEMA_VERSION, ARROW_IPC_FILE_FORMAT, CID_VERSION,
    DAG_CBOR_CODEC, DAG_CBOR_CODEC_NAME, GRAPHAR_BINARY_ENTITY_NAMESPACE,
    GRAPHAR_BINARY_ENTITY_VERSION, GRAPHAR_DATASET_NAMESPACE, GRAPHAR_ENTITY_PROPERTIES_NAMESPACE,
    GRAPHAR_ENTITY_PROPERTIES_VERSION, PROPERTY_SNAPSHOT_SCHEMA_VERSION, RAW_CODEC, RAW_CODEC_NAME,
    SHA2_256_CODE, SHA2_256_NAME, SNAPSHOT_SCHEMA_NAMESPACE, SNAPSHOT_SCHEMA_VERSION,
};
#[cfg(feature = "content-identity")]
pub use operation_binding::{
    DataOperationBinding, QueryOperationBinding, ReleaseBindingError, ReleaseOperationBinding,
    SnapshotOperationBinding, SnapshotRowBinding, SnapshotRowBindingError,
};
#[cfg(feature = "content-identity")]
pub use profile::dag_cbor_cid;
#[cfg(feature = "content-identity")]
pub use query_binding::{
    BoundDataQuery, DataGraphSourceBindingError, DataQueryHandoffError, DataQueryOutputError,
    DataQueryResultHandoff, admit_graph_projection_source, bind_data_query,
    project_data_query_output,
};
pub use query_binding::{
    DataEngineProfile, DataQueryBindingError, DataQueryFeature, PhysicalQueryOutput,
};
#[cfg(feature = "content-identity")]
pub use snapshot_descriptors::{
    CoverageDescriptor, CoverageKind, EntityDescriptor, GraphProjectionDescriptor,
    GraphProjectionKind,
};

#[cfg(test)]
#[path = "../tests/unit/mod.rs"]
mod tests;
