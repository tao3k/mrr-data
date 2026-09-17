//! Frozen `MRR Data` V1 schema and multiformats profile constants.

use cid::{Cid, Version};
use multihash_codetable::{Code, MultihashDigest};

use crate::DataError;

pub use mrr_data_profile::{
    ARROW_FACT_SCHEMA_NAMESPACE, ARROW_FACT_SCHEMA_VERSION, ARROW_IPC_FILE_FORMAT, CID_VERSION_V1,
    DAG_CBOR_CODEC, DAG_CBOR_CODEC_NAME, GRAPHAR_BINARY_ENTITY_NAMESPACE,
    GRAPHAR_BINARY_ENTITY_VERSION, RAW_CODEC, RAW_CODEC_NAME, SHA2_256_CODE, SHA2_256_NAME,
    SNAPSHOT_SCHEMA_NAMESPACE, SNAPSHOT_SCHEMA_VERSION,
};

pub(crate) fn cid_for(codec: u64, bytes: &[u8]) -> Cid {
    Cid::new_v1(codec, Code::Sha2_256.digest(bytes))
}

/// Computes a `CIDv1` DAG-CBOR/SHA-256 address for canonical manifest bytes.
#[must_use]
pub fn dag_cbor_cid(bytes: &[u8]) -> Cid {
    cid_for(DAG_CBOR_CODEC, bytes)
}

pub(crate) fn validate_cid(cid: &Cid, expected_codec: u64) -> Result<(), DataError> {
    if cid.version() != Version::V1
        || cid.codec() != expected_codec
        || cid.hash().code() != SHA2_256_CODE
        || cid.hash().digest().len() != 32
    {
        return Err(DataError::InvalidCidProfile {
            cid: Box::new(*cid),
            expected_codec,
        });
    }
    Ok(())
}
