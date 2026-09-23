//! Shared physical child-closure validation for CAR and snapshot transfers.
use crate::{
    ContentError,
    store::{cid_for, codec_for},
};
use cid::Cid;
use mrr_data_core::SnapshotManifest;
use std::collections::BTreeMap;

pub(crate) fn verify_closure(
    manifest: &SnapshotManifest,
    blocks: &BTreeMap<Cid, &[u8]>,
) -> Result<(), ContentError> {
    for cid in manifest.referenced_cids() {
        if !blocks.contains_key(&cid) {
            return Err(ContentError::MissingReferencedBlock(Box::new(cid)));
        }
    }
    for batch in manifest
        .relations()
        .iter()
        .flat_map(mrr_data_core::RelationDescriptor::batches)
        .chain(
            manifest
                .entities()
                .iter()
                .flat_map(mrr_data_core::EntityDescriptor::batches),
        )
    {
        let bytes = blocks
            .get(batch.cid())
            .ok_or_else(|| ContentError::MissingReferencedBlock(Box::new(*batch.cid())))?;
        let actual = bytes.len() as u64;
        if actual != batch.byte_length() {
            return Err(ContentError::ChildLengthMismatch {
                cid: Box::new(*batch.cid()),
                declared: batch.byte_length(),
                actual,
            });
        }
    }
    for (cid, bytes) in blocks {
        let actual = cid_for(codec_for(cid)?, bytes);
        if actual != *cid {
            return Err(ContentError::CidMismatch {
                expected: Box::new(*cid),
                actual: Box::new(actual),
            });
        }
    }
    Ok(())
}
