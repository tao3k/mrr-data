//! Bounded publication and restoration of explicitly addressed snapshot closures.
use crate::{
    AsyncContentStore, CacheAdmission, ContentBlock, ContentCodec, ContentError,
    ContentProtocolError, ContentSource, RemoteContentStore, closure::verify_closure,
    publish_content, read_through,
};
use cid::Cid;
use meta_relational_reasoning::{EntityCatalog, RelationCatalog};
use mrr_data_core::{SnapshotBlock, SnapshotManifest};
use std::collections::BTreeMap;

/// Logical resident payload limits. Count and total include the root, while
/// `block_bytes` applies to children. Execution is serial; there are no retries.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SnapshotTransferLimits {
    root_bytes: usize,
    blocks: usize,
    block_bytes: usize,
    total_bytes: usize,
}
impl SnapshotTransferLimits {
    #[must_use]
    pub const fn new(
        root_bytes: usize,
        blocks: usize,
        block_bytes: usize,
        total_bytes: usize,
    ) -> Self {
        Self {
            root_bytes,
            blocks,
            block_bytes,
            total_bytes,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SnapshotResource {
    RootBytes,
    Blocks,
    BlockBytes,
    TotalBytes,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SnapshotTransferError {
    Content(ContentError),
    Transfer {
        cid: Box<Cid>,
        error: ContentProtocolError,
    },
    MissingBlock(Box<Cid>),
    UnsupportedGraphProjection,
    Cancelled,
    DeadlineExceeded,
    LimitExceeded {
        resource: SnapshotResource,
        limit: usize,
        actual: usize,
    },
}
impl std::fmt::Display for SnapshotTransferError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "snapshot transfer: {self:?}")
    }
}
impl std::error::Error for SnapshotTransferError {}
impl From<ContentError> for SnapshotTransferError {
    fn from(error: ContentError) -> Self {
        Self::Content(error)
    }
}

/// Produced only after all declared children and the root are acknowledged.
/// The receipt is physical and does not publish a mutable discovery pointer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotPublication {
    root: Cid,
    total_bytes: usize,
    cache: BTreeMap<Cid, CacheAdmission>,
}
impl SnapshotPublication {
    #[must_use]
    pub const fn root(&self) -> &Cid {
        &self.root
    }
    #[must_use]
    pub const fn total_bytes(&self) -> usize {
        self.total_bytes
    }
    #[must_use]
    pub const fn cache_admissions(&self) -> &BTreeMap<Cid, CacheAdmission> {
        &self.cache
    }
}

/// Fully verified physical closure. Owned children remain available even if a
/// disposable cache could not admit them. No semantic fact admission is implied.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RestoredSnapshot {
    snapshot: SnapshotBlock,
    children: BTreeMap<Cid, Vec<u8>>,
    sources: BTreeMap<Cid, ContentSource>,
}
impl RestoredSnapshot {
    #[must_use]
    pub const fn snapshot(&self) -> &SnapshotBlock {
        &self.snapshot
    }
    #[must_use]
    pub const fn children(&self) -> &BTreeMap<Cid, Vec<u8>> {
        &self.children
    }
    #[must_use]
    pub const fn sources(&self) -> &BTreeMap<Cid, ContentSource> {
        &self.sources
    }
}

fn check(
    resource: SnapshotResource,
    limit: usize,
    actual: usize,
) -> Result<(), SnapshotTransferError> {
    if actual > limit {
        Err(SnapshotTransferError::LimitExceeded {
            resource,
            limit,
            actual,
        })
    } else {
        Ok(())
    }
}
fn add_total(total: usize, bytes: usize, limit: usize) -> Result<usize, SnapshotTransferError> {
    let next = total
        .checked_add(bytes)
        .ok_or(SnapshotTransferError::LimitExceeded {
            resource: SnapshotResource::TotalBytes,
            limit,
            actual: usize::MAX,
        })?;
    check(SnapshotResource::TotalBytes, limit, next)?;
    Ok(next)
}
fn inventory(
    manifest: &SnapshotManifest,
    relations: &RelationCatalog,
    entities: &EntityCatalog,
    limits: SnapshotTransferLimits,
) -> Result<Vec<Cid>, SnapshotTransferError> {
    manifest
        .verify_catalogs(relations, entities)
        .map_err(ContentError::from)?;
    if manifest.graph_projection().is_some() {
        return Err(SnapshotTransferError::UnsupportedGraphProjection);
    }
    let children = manifest.referenced_cids();
    check(
        SnapshotResource::Blocks,
        limits.blocks,
        children.len().saturating_add(1),
    )?;
    // Reject declared oversize batches before reading their payloads.
    for relation in manifest.relations() {
        for batch in relation.batches() {
            if batch.byte_length() > limits.block_bytes as u64 {
                return Err(SnapshotTransferError::LimitExceeded {
                    resource: SnapshotResource::BlockBytes,
                    limit: limits.block_bytes,
                    actual: usize::try_from(batch.byte_length()).unwrap_or(usize::MAX),
                });
            }
        }
    }
    Ok(children)
}
fn closure(
    manifest: &SnapshotManifest,
    children: &BTreeMap<Cid, Vec<u8>>,
) -> Result<(), SnapshotTransferError> {
    let borrowed = children
        .iter()
        .map(|(cid, bytes)| (*cid, bytes.as_slice()))
        .collect();
    verify_closure(manifest, &borrowed)?;
    Ok(())
}
fn transfer(cid: Cid, error: ContentProtocolError) -> SnapshotTransferError {
    SnapshotTransferError::Transfer {
        cid: Box::new(cid),
        error,
    }
}

/// Validates and captures the entire supported closure before any remote write,
/// then acknowledges children before publishing the root. Graph snapshots are
/// rejected until their nested file inventory has an owning implementation.
/// Verified captured bytes isolate publication from subsequent local eviction.
/// # Errors
/// Returns catalog, integrity, budget, local read or remote publication failures.
/// Failed attempts may leave immutable children or an already published root;
/// no success receipt is returned and shared remote objects are never removed.
pub async fn publish_snapshot(
    local: &(impl AsyncContentStore + ?Sized),
    remote: &(impl RemoteContentStore + ?Sized),
    snapshot: &SnapshotBlock,
    relations: &RelationCatalog,
    entities: &EntityCatalog,
    limits: SnapshotTransferLimits,
) -> Result<SnapshotPublication, SnapshotTransferError> {
    check(
        SnapshotResource::RootBytes,
        limits.root_bytes,
        snapshot.bytes().len(),
    )?;
    let mut total = add_total(0, snapshot.bytes().len(), limits.total_bytes)?;
    let cids = inventory(snapshot.manifest(), relations, entities, limits)?;
    let mut children = BTreeMap::new();
    for cid in cids {
        let remaining = limits.total_bytes - total;
        let bytes = local.load(&cid, limits.block_bytes.min(remaining)).await?;
        check(
            SnapshotResource::BlockBytes,
            limits.block_bytes,
            bytes.len(),
        )?;
        total = add_total(total, bytes.len(), limits.total_bytes)?;
        children.insert(cid, bytes);
    }
    closure(snapshot.manifest(), &children)?;
    let mut cache = BTreeMap::new();
    for (cid, bytes) in &children {
        let block = ContentBlock::new(ContentCodec::from_cid(cid)?, bytes);
        let receipt = publish_content(local, remote, block)
            .await
            .map_err(|e| transfer(*cid, e))?;
        cache.insert(*cid, receipt.cache);
    }
    let receipt = publish_content(
        local,
        remote,
        ContentBlock::new(ContentCodec::DagCbor, snapshot.bytes()),
    )
    .await
    .map_err(|e| transfer(*snapshot.cid(), e))?;
    cache.insert(*snapshot.cid(), receipt.cache);
    Ok(SnapshotPublication {
        root: *snapshot.cid(),
        total_bytes: total,
        cache,
    })
}

/// Restores a root's declared closure within a cumulative logical byte budget.
/// Cache fills are not snapshot admission: a failed restore can leave verified
/// blocks, but never yields a restored-snapshot result. CAR is not required.
/// # Errors
/// Returns missing-block, catalog, integrity, unsupported-graph or budget failures.
pub async fn restore_snapshot(
    local: &(impl AsyncContentStore + ?Sized),
    remote: &(impl RemoteContentStore + ?Sized),
    root: &Cid,
    relations: &RelationCatalog,
    entities: &EntityCatalog,
    limits: SnapshotTransferLimits,
) -> Result<RestoredSnapshot, SnapshotTransferError> {
    check(SnapshotResource::Blocks, limits.blocks, 1)?;
    if ContentCodec::from_cid(root)? != ContentCodec::DagCbor {
        return Err(ContentError::InvalidCidProfile(Box::new(*root)).into());
    }
    let read = read_through(
        local,
        remote,
        root,
        limits.root_bytes.min(limits.total_bytes),
    )
    .await
    .map_err(|e| transfer(*root, e))?
    .ok_or(SnapshotTransferError::MissingBlock(Box::new(*root)))?;
    check(
        SnapshotResource::RootBytes,
        limits.root_bytes,
        read.bytes.len(),
    )?;
    let mut total = add_total(0, read.bytes.len(), limits.total_bytes)?;
    let manifest =
        SnapshotManifest::decode_checked(&read.bytes, root).map_err(ContentError::from)?;
    let cids = inventory(&manifest, relations, entities, limits)?;
    let snapshot = SnapshotBlock::encode(manifest).map_err(ContentError::from)?;
    // Release the downloaded buffer after reconstructing the canonical root.
    drop(read.bytes);
    let mut sources = BTreeMap::from([(*root, read.source)]);
    let mut children = BTreeMap::new();
    for cid in cids {
        let remaining = limits.total_bytes - total;
        let read = read_through(local, remote, &cid, limits.block_bytes.min(remaining))
            .await
            .map_err(|e| transfer(cid, e))?
            .ok_or(SnapshotTransferError::MissingBlock(Box::new(cid)))?;
        total = add_total(total, read.bytes.len(), limits.total_bytes)?;
        children.insert(cid, read.bytes);
        sources.insert(cid, read.source);
    }
    closure(snapshot.manifest(), &children)?;
    Ok(RestoredSnapshot {
        snapshot,
        children,
        sources,
    })
}

/// Restores a complete snapshot exclusively from durable local content.
/// No remote fallback is attempted, so success is evidence that every block
/// needed for an offline query is present and verified locally.
/// # Errors
/// Returns missing-block, catalog, integrity, unsupported-graph or budget failures.
pub async fn restore_snapshot_local(
    local: &(impl AsyncContentStore + ?Sized),
    root: &Cid,
    relations: &RelationCatalog,
    entities: &EntityCatalog,
    limits: SnapshotTransferLimits,
) -> Result<RestoredSnapshot, SnapshotTransferError> {
    check(SnapshotResource::Blocks, limits.blocks, 1)?;
    if ContentCodec::from_cid(root)? != ContentCodec::DagCbor {
        return Err(ContentError::InvalidCidProfile(Box::new(*root)).into());
    }
    let bytes = local
        .load(root, limits.root_bytes.min(limits.total_bytes))
        .await?;
    check(SnapshotResource::RootBytes, limits.root_bytes, bytes.len())?;
    let mut total = add_total(0, bytes.len(), limits.total_bytes)?;
    let manifest = SnapshotManifest::decode_checked(&bytes, root).map_err(ContentError::from)?;
    let cids = inventory(&manifest, relations, entities, limits)?;
    let snapshot = SnapshotBlock::encode(manifest).map_err(ContentError::from)?;
    let mut sources = BTreeMap::from([(*root, ContentSource::Local)]);
    let mut children = BTreeMap::new();
    for cid in cids {
        let remaining = limits.total_bytes - total;
        let bytes = local.load(&cid, limits.block_bytes.min(remaining)).await?;
        total = add_total(total, bytes.len(), limits.total_bytes)?;
        children.insert(cid, bytes);
        sources.insert(cid, ContentSource::Local);
    }
    closure(snapshot.manifest(), &children)?;
    Ok(RestoredSnapshot {
        snapshot,
        children,
        sources,
    })
}
