//! Frozen `MRR Data` V1 schema and multiformats profile constants.

use cid::{Cid, Version};
use multihash_codetable::{Code, MultihashDigest};

use crate::DataError;

use mrr_data_profile::{DAG_CBOR_CODEC, SHA2_256_CODE};

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
