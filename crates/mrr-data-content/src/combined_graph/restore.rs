//! Cold restore authenticates the selected snapshot and its entire native closure.
use super::{
    CombinedGraphInputs, PreparedCombinedGraph,
    prepare::{begin, children, finish, insert},
};
use crate::{
    AsyncContentStore, GraphTransferError as Error, GraphTransferLimits, RemoteContentStore,
    read_through,
};
use meta_relational_reasoning::{EntityCatalog, RelationCatalog};
use mrr_data_core::{
    BoundDataQuery, GraphDatasetLimits, GraphProjectionKind, SnapshotBlock, SnapshotManifest,
};

/// The caller authenticates the snapshot root retained by the bound query.
/// Local cache admission is optional and cannot replace remote integrity checks.
/// # Errors
/// Refuses corrupt/missing metadata/files, semantic/profile drift or resource limits.
pub async fn restore_combined_graph(
    local: &(dyn AsyncContentStore + Sync),
    remote: &dyn RemoteContentStore,
    query: &BoundDataQuery,
    catalogs: (&RelationCatalog, &EntityCatalog),
    bounds: (GraphTransferLimits, GraphDatasetLimits),
) -> Result<PreparedCombinedGraph, Error> {
    restore_combined_graph_checked(local, remote, query, catalogs, bounds, || {
        Ok::<(), Error>(())
    })
    .await
}

/// Cold restore with caller-owned lifecycle checkpoints around each read-through.
/// The callback may enforce cancellation or deadlines without a runtime dependency.
/// A read-through includes optional cache admission; in-flight provider operations
/// are not interrupted, and acknowledged cache blocks may remain after refusal.
/// The caller must retain resource admission throughout this future and its output.
/// # Errors
/// Preserves typed caller stops and all integrity, scope and transfer refusals.
pub async fn restore_combined_graph_checked<E>(
    local: &(dyn AsyncContentStore + Sync),
    remote: &dyn RemoteContentStore,
    query: &BoundDataQuery,
    catalogs: (&RelationCatalog, &EntityCatalog),
    bounds: (GraphTransferLimits, GraphDatasetLimits),
    mut checkpoint: impl FnMut() -> Result<(), E>,
) -> Result<PreparedCombinedGraph, E>
where
    E: From<Error>,
{
    checkpoint()?;
    let (limits, dataset_limits) = bounds;
    if limits.max_blocks < 2 || limits.max_block_bytes == 0 || limits.max_total_bytes == 0 {
        return Err(E::from(Error::Limit));
    }
    if query.graph_projection_kind() != Some(GraphProjectionKind::Dataset) {
        return Err(E::from(Error::Integrity));
    }
    let read = read_through(
        local,
        remote,
        query.snapshot_root(),
        limits.max_block_bytes.min(limits.max_total_bytes),
    )
    .await;
    checkpoint()?;
    let bytes = read
        .map_err(Error::from)
        .map_err(E::from)?
        .ok_or(Error::Missing)
        .map_err(E::from)?
        .bytes;
    let manifest = SnapshotManifest::decode_checked(&bytes, query.snapshot_root())
        .map_err(|_| E::from(Error::Integrity))?;
    let snapshot = SnapshotBlock::encode(manifest).map_err(|_| E::from(Error::Integrity))?;
    drop(bytes);
    let dataset = query
        .graph_projection_manifest()
        .ok_or(Error::Integrity)
        .map_err(E::from)?;
    let cap = limits
        .max_total_bytes
        .checked_sub(snapshot.bytes().len())
        .ok_or(Error::Limit)
        .map_err(E::from)?;
    checkpoint()?;
    let read = read_through(
        local,
        remote,
        dataset,
        cap.min(limits.max_block_bytes)
            .min(dataset_limits.inventory.max_manifest_bytes),
    )
    .await;
    checkpoint()?;
    let bytes = read
        .map_err(Error::from)
        .map_err(E::from)?
        .ok_or(Error::Missing)
        .map_err(E::from)?
        .bytes;
    let input = CombinedGraphInputs {
        query,
        snapshot: &snapshot,
        relations: catalogs.0,
        entities: catalogs.1,
        dataset_limits,
        limits,
    };
    let mut prepared = begin(&input, bytes).map_err(E::from)?;
    for cid in children(&prepared, limits).map_err(E::from)? {
        checkpoint()?;
        let read = read_through(
            local,
            remote,
            &cid,
            limits
                .max_block_bytes
                .min(limits.max_total_bytes - prepared.total_bytes),
        )
        .await;
        checkpoint()?;
        let bytes = read
            .map_err(Error::from)
            .map_err(E::from)?
            .ok_or(Error::Missing)
            .map_err(E::from)?
            .bytes;
        insert(
            &mut prepared.blocks,
            cid,
            bytes,
            &mut prepared.total_bytes,
            limits,
        )
        .map_err(E::from)?;
    }
    checkpoint()?;
    finish(&prepared).map_err(E::from)?;
    checkpoint()?;
    Ok(prepared)
}
