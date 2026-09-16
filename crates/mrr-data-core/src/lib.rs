//! Canonical physical content identity for admitted MRR semantic snapshots.
#![forbid(unsafe_code)]

mod error;
mod manifest;
mod profile;

pub use error::DataError;
pub use manifest::{
    BatchDescriptor, CoverageDescriptor, CoverageKind, GraphProjectionDescriptor,
    RelationDescriptor, SnapshotBlock, SnapshotManifest, SnapshotManifestRequest, raw_cid,
};
pub use profile::{
    ARROW_FACT_SCHEMA_NAMESPACE, ARROW_FACT_SCHEMA_VERSION, ARROW_IPC_FILE_FORMAT, CID_VERSION_V1,
    DAG_CBOR_CODEC, DAG_CBOR_CODEC_NAME, GRAPHAR_BINARY_ENTITY_NAMESPACE,
    GRAPHAR_BINARY_ENTITY_VERSION, RAW_CODEC, RAW_CODEC_NAME, SHA2_256_CODE, SHA2_256_NAME,
    SNAPSHOT_SCHEMA_NAMESPACE, SNAPSHOT_SCHEMA_VERSION,
};

#[cfg(test)]
#[path = "../tests/unit/mod.rs"]
mod tests;
