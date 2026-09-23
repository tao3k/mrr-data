//! Frozen V1 schema and physical profile identifiers.

/// Stable namespace of the content snapshot schema; version is a separate field.
pub const SNAPSHOT_SCHEMA_NAMESPACE: &str = "mrr.data.snapshot";
/// First admitted version of the content snapshot schema.
pub const SNAPSHOT_SCHEMA_VERSION: u64 = 1;
/// Property-bearing snapshot version; V1 relation-only roots remain readable.
pub const PROPERTY_SNAPSHOT_SCHEMA_VERSION: u64 = 2;
/// Stable namespace for Arrow fact-batch schemas.
pub const ARROW_FACT_SCHEMA_NAMESPACE: &str = "mrr.data.arrow.fact-batch";
/// First admitted Arrow fact-batch schema version.
pub const ARROW_FACT_SCHEMA_VERSION: u64 = 1;
/// Stable namespace for the V1 binary-Entity `GraphAr` projection.
pub const GRAPHAR_BINARY_ENTITY_NAMESPACE: &str = "mrr.graphar.binary-entity";
/// First admitted binary-Entity `GraphAr` projection version.
pub const GRAPHAR_BINARY_ENTITY_VERSION: u64 = 1;
/// Payload-format label for one complete Arrow IPC file.
pub const ARROW_IPC_FILE_FORMAT: &str = "arrow-ipc-file";
/// Multicodec name used for opaque child payloads.
pub const RAW_CODEC_NAME: &str = "raw";
/// Multicodec name used for the canonical root manifest.
pub const DAG_CBOR_CODEC_NAME: &str = "dag-cbor";
/// Multihash name pinned by the V1 content identity profile.
pub const SHA2_256_NAME: &str = "sha2-256";
/// Numeric CID version pinned by the V1 content identity profile.
pub const CID_VERSION_V1: u64 = 1;
/// Registered multicodec number for opaque raw bytes.
pub const RAW_CODEC: u64 = 0x55;
/// Registered multicodec number for DAG-CBOR.
pub const DAG_CBOR_CODEC: u64 = 0x71;
/// Registered multihash number for SHA-256.
pub const SHA2_256_CODE: u64 = 0x12;
