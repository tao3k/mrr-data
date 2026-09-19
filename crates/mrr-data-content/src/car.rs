//! Bounded, verified `CARv1` packaging for content snapshot roots.

use std::{collections::BTreeMap, io::Cursor};

use cid::Cid;
use fvm_ipld_car::{Block, CarHeader, CarReader, CarWriter};
use meta_relational_reasoning::{EntityCatalog, RelationCatalog};
use mrr_data_core::{SnapshotBlock, SnapshotManifest};
use unsigned_varint::decode;

use crate::closure::verify_closure;
use crate::{
    ContentBlock, ContentCodec, ContentError, ContentStore, ImportResource, store::codec_for,
};

/// Explicit limits applied before an imported CAR is committed to a store.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CarImportLimits {
    archive_bytes: u64,
    blocks: u64,
    block_bytes: u64,
    total_block_bytes: u64,
}

impl CarImportLimits {
    #[must_use]
    pub const fn new(
        max_archive_bytes: u64,
        max_blocks: u64,
        max_block_bytes: u64,
        max_total_block_bytes: u64,
    ) -> Self {
        Self {
            archive_bytes: max_archive_bytes,
            blocks: max_blocks,
            block_bytes: max_block_bytes,
            total_block_bytes: max_total_block_bytes,
        }
    }
}

/// Result of a fully validated and committed snapshot CAR import.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ImportedSnapshot {
    root: Cid,
    manifest: SnapshotManifest,
    block_count: usize,
}

impl ImportedSnapshot {
    #[must_use]
    pub const fn root(&self) -> &Cid {
        &self.root
    }

    #[must_use]
    pub const fn manifest(&self) -> &SnapshotManifest {
        &self.manifest
    }

    #[must_use]
    pub const fn block_count(&self) -> usize {
        self.block_count
    }
}

/// Encodes one root and a complete set of raw children with the upstream `CARv1` writer.
///
/// Child order is preserved. Extra raw blocks are allowed because CAR composition
/// is transport state, while every child referenced by the manifest is required.
///
/// # Errors
///
/// Returns [`ContentError`] for duplicate blocks, a missing referenced child,
/// a descriptor length mismatch, or CAR writer failure.
pub fn encode_snapshot_car(
    snapshot: &SnapshotBlock,
    children: &[ContentBlock<'_>],
) -> Result<Vec<u8>, ContentError> {
    let mut blocks = BTreeMap::new();
    for child in children {
        if child.codec() != ContentCodec::Raw {
            return Err(ContentError::InvalidCidProfile(Box::new(child.cid())));
        }
        let cid = child.cid();
        if blocks.insert(cid, child.bytes()).is_some() {
            return Err(ContentError::DuplicateBlock(Box::new(cid)));
        }
    }
    verify_closure(snapshot.manifest(), &blocks)?;

    let mut bytes = Vec::new();
    let mut writer = CarWriter::new(CarHeader::from(vec![*snapshot.cid()]), &mut bytes)
        .map_err(|error| ContentError::Car(error.to_string()))?;
    writer
        .write(Block {
            cid: *snapshot.cid(),
            data: snapshot.bytes().to_vec(),
        })
        .map_err(|error| ContentError::Car(error.to_string()))?;
    for child in children {
        writer
            .write(Block {
                cid: child.cid(),
                data: child.bytes().to_vec(),
            })
            .map_err(|error| ContentError::Car(error.to_string()))?;
    }
    writer
        .flush()
        .map_err(|error| ContentError::Car(error.to_string()))?;
    drop(writer);
    Ok(bytes)
}

/// Imports one `CARv1` snapshot after validating all budgets, blocks, closure, and catalogs.
///
/// Validation completes before the first store mutation. Children are committed
/// before the root so an interrupted import cannot publish an incomplete root.
///
/// # Errors
///
/// Returns [`ContentError`] on any budget, framing, CID, closure, manifest,
/// catalog, or store failure.
pub fn import_snapshot_car<S: ContentStore>(
    archive: &[u8],
    limits: CarImportLimits,
    relation_catalog: &RelationCatalog,
    entity_catalog: &EntityCatalog,
    store: &S,
) -> Result<ImportedSnapshot, ContentError> {
    check_limit(
        ImportResource::ArchiveBytes,
        limits.archive_bytes,
        archive.len() as u64,
    )?;
    preflight_frames(archive, limits)?;
    let mut reader = CarReader::new(Cursor::new(archive))
        .map_err(|error| ContentError::Car(error.to_string()))?;
    if reader.header.roots.len() != 1 {
        return Err(ContentError::RootCount {
            actual: reader.header.roots.len(),
        });
    }
    let root = reader.header.roots[0];
    if codec_for(&root)? != ContentCodec::DagCbor {
        return Err(ContentError::InvalidCidProfile(Box::new(root)));
    }

    let mut blocks = BTreeMap::new();
    let mut total = 0_u64;
    for item in &mut reader {
        let block = item.map_err(|error| ContentError::Car(error.to_string()))?;
        let count = (blocks.len() as u64).saturating_add(1);
        check_limit(ImportResource::Blocks, limits.blocks, count)?;
        check_limit(
            ImportResource::BlockBytes,
            limits.block_bytes,
            block.data.len() as u64,
        )?;
        total = total
            .checked_add(block.data.len() as u64)
            .ok_or(ContentError::LimitExceeded {
                resource: ImportResource::TotalBlockBytes,
                limit: limits.total_block_bytes,
                actual: u64::MAX,
            })?;
        check_limit(
            ImportResource::TotalBlockBytes,
            limits.total_block_bytes,
            total,
        )?;
        codec_for(&block.cid)?;
        if blocks.insert(block.cid, block.data).is_some() {
            return Err(ContentError::DuplicateBlock(Box::new(block.cid)));
        }
    }

    let root_bytes = blocks
        .get(&root)
        .ok_or_else(|| ContentError::MissingRoot(Box::new(root)))?;
    let manifest = SnapshotManifest::decode_checked(root_bytes, &root)?;
    manifest.verify_catalogs(relation_catalog, entity_catalog)?;
    let borrowed = blocks
        .iter()
        .map(|(cid, bytes)| (*cid, bytes.as_slice()))
        .collect();
    verify_closure(&manifest, &borrowed)?;

    for (cid, bytes) in blocks.iter().filter(|(cid, _)| **cid != root) {
        let stored = store.put(ContentBlock::new(codec_for(cid)?, bytes))?;
        if stored != *cid {
            return Err(ContentError::CidMismatch {
                expected: Box::new(*cid),
                actual: Box::new(stored),
            });
        }
    }
    let stored_root = store.put(ContentBlock::new(ContentCodec::DagCbor, root_bytes))?;
    if stored_root != root {
        return Err(ContentError::CidMismatch {
            expected: Box::new(root),
            actual: Box::new(stored_root),
        });
    }

    Ok(ImportedSnapshot {
        root,
        manifest,
        block_count: blocks.len(),
    })
}

fn preflight_frames(mut archive: &[u8], limits: CarImportLimits) -> Result<(), ContentError> {
    let (header_bytes, remaining) = decode::usize(archive)
        .map_err(|error| ContentError::Car(format!("invalid CAR header length: {error}")))?;
    archive = remaining
        .get(header_bytes..)
        .ok_or_else(|| ContentError::Car("truncated CAR header".to_owned()))?;

    let cid_bytes = mrr_data_core::raw_cid(&[]).encoded_len() as u64;
    let max_frame_bytes = limits.block_bytes.saturating_add(cid_bytes);
    let mut block_count = 0_u64;
    let mut total_block_bytes = 0_u64;
    while !archive.is_empty() {
        let (frame_bytes, remaining) = decode::usize(archive)
            .map_err(|error| ContentError::Car(format!("invalid CAR block length: {error}")))?;
        block_count = block_count.saturating_add(1);
        check_limit(ImportResource::Blocks, limits.blocks, block_count)?;
        if frame_bytes as u64 > max_frame_bytes {
            return Err(ContentError::LimitExceeded {
                resource: ImportResource::BlockBytes,
                limit: limits.block_bytes,
                actual: (frame_bytes as u64).saturating_sub(cid_bytes),
            });
        }
        let frame = remaining
            .get(..frame_bytes)
            .ok_or_else(|| ContentError::Car("truncated CAR block".to_owned()))?;
        let mut cursor = Cursor::new(frame);
        let cid = Cid::read_bytes(&mut cursor)
            .map_err(|error| ContentError::Car(format!("invalid CAR block CID: {error}")))?;
        codec_for(&cid)?;
        let payload_bytes = (frame_bytes as u64)
            .checked_sub(cursor.position())
            .ok_or_else(|| ContentError::Car("CAR block CID exceeds frame".to_owned()))?;
        check_limit(
            ImportResource::BlockBytes,
            limits.block_bytes,
            payload_bytes,
        )?;
        total_block_bytes =
            total_block_bytes
                .checked_add(payload_bytes)
                .ok_or(ContentError::LimitExceeded {
                    resource: ImportResource::TotalBlockBytes,
                    limit: limits.total_block_bytes,
                    actual: u64::MAX,
                })?;
        check_limit(
            ImportResource::TotalBlockBytes,
            limits.total_block_bytes,
            total_block_bytes,
        )?;
        archive = &remaining[frame_bytes..];
    }
    Ok(())
}

fn check_limit(resource: ImportResource, limit: u64, actual: u64) -> Result<(), ContentError> {
    if actual > limit {
        Err(ContentError::LimitExceeded {
            resource,
            limit,
            actual,
        })
    } else {
        Ok(())
    }
}
