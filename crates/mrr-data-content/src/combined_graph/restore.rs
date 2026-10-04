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
    let (limits, dataset_limits) = bounds;
    if limits.max_blocks < 2 || limits.max_block_bytes == 0 || limits.max_total_bytes == 0 {
        return Err(Error::Limit);
    }
    if query.graph_projection_kind() != Some(GraphProjectionKind::Dataset) {
        return Err(Error::Integrity);
    }
    let bytes = read_through(
        local,
        remote,
        query.snapshot_root(),
        limits.max_block_bytes.min(limits.max_total_bytes),
    )
    .await?
    .ok_or(Error::Missing)?
    .bytes;
    let manifest = SnapshotManifest::decode_checked(&bytes, query.snapshot_root())
        .map_err(|_| Error::Integrity)?;
    let snapshot = SnapshotBlock::encode(manifest).map_err(|_| Error::Integrity)?;
    drop(bytes);
    let dataset = query.graph_projection_manifest().ok_or(Error::Integrity)?;
    let cap = limits
        .max_total_bytes
        .checked_sub(snapshot.bytes().len())
        .ok_or(Error::Limit)?;
    let bytes = read_through(
        local,
        remote,
        dataset,
        cap.min(limits.max_block_bytes)
            .min(dataset_limits.inventory.max_manifest_bytes),
    )
    .await?
    .ok_or(Error::Missing)?
    .bytes;
    let input = CombinedGraphInputs {
        query,
        snapshot: &snapshot,
        relations: catalogs.0,
        entities: catalogs.1,
        dataset_limits,
        limits,
    };
    let mut prepared = begin(&input, bytes)?;
    for cid in children(&prepared, limits)? {
        let bytes = read_through(
            local,
            remote,
            &cid,
            limits
                .max_block_bytes
                .min(limits.max_total_bytes - prepared.total_bytes),
        )
        .await?
        .ok_or(Error::Missing)?
        .bytes;
        insert(
            &mut prepared.blocks,
            cid,
            bytes,
            &mut prepared.total_bytes,
            limits,
        )?;
    }
    finish(&prepared)?;
    Ok(prepared)
}
