//! Physical engine contracts and optional CID/DAG-CBOR snapshot identity.
#![forbid(unsafe_code)]

#[cfg(feature = "ipfs")]
mod error;
#[cfg(feature = "ipfs")]
mod manifest;
#[cfg(feature = "ipfs")]
mod profile;
mod query_binding;
#[cfg(feature = "ipfs")]
mod snapshot_descriptors;

#[cfg(feature = "ipfs")]
pub use error::DataError;
#[cfg(feature = "ipfs")]
pub use manifest::{
    BatchDescriptor, RelationDescriptor, SnapshotBlock, SnapshotManifest, SnapshotManifestRequest,
    raw_cid,
};
pub use mrr_data_profile::{
    ARROW_FACT_SCHEMA_NAMESPACE, ARROW_FACT_SCHEMA_VERSION, ARROW_IPC_FILE_FORMAT, CID_VERSION_V1,
    DAG_CBOR_CODEC, DAG_CBOR_CODEC_NAME, GRAPHAR_BINARY_ENTITY_NAMESPACE,
    GRAPHAR_BINARY_ENTITY_VERSION, PROPERTY_SNAPSHOT_SCHEMA_VERSION, RAW_CODEC, RAW_CODEC_NAME,
    SHA2_256_CODE, SHA2_256_NAME, SNAPSHOT_SCHEMA_NAMESPACE, SNAPSHOT_SCHEMA_VERSION,
};
#[cfg(feature = "ipfs")]
pub use profile::dag_cbor_cid;
#[cfg(feature = "ipfs")]
pub use query_binding::{
    BoundDataQuery, DataGraphSourceBindingError, DataQueryOutputError,
    admit_graph_projection_source, bind_data_query, project_data_query_output,
};
pub use query_binding::{
    DataEngineProfile, DataQueryBindingError, DataQueryFeature, PhysicalQueryOutput,
};
#[cfg(feature = "ipfs")]
pub use snapshot_descriptors::{
    CoverageDescriptor, CoverageKind, EntityDescriptor, GraphProjectionDescriptor,
};

#[cfg(test)]
#[path = "../tests/unit/mod.rs"]
mod tests;
