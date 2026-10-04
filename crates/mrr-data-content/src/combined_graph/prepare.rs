//! Scope and aggregate validation precede all publication side effects.
use super::{CombinedGraphInputs, PreparedCombinedGraph};
use crate::{
    ContentBlock, ContentCodec, ContentStore, GraphTransferError as Error, GraphTransferLimits,
    closure::verify_closure,
};
use cid::Cid;
use mrr_data_core::{GraphDatasetDescriptor, GraphProjectionKind};
use std::collections::{BTreeMap, BTreeSet};

pub(super) fn check(input: &CombinedGraphInputs<'_>) -> Result<Cid, Error> {
    let limits = input.limits;
    if limits.max_blocks < 2 || limits.max_block_bytes == 0 || limits.max_total_bytes == 0 {
        return Err(Error::Limit);
    }
    if input.query.snapshot_root() != input.snapshot.cid()
        || input.query.graph_projection_kind() != Some(GraphProjectionKind::Dataset)
    {
        return Err(Error::Integrity);
    }
    input
        .snapshot
        .manifest()
        .verify_catalogs(input.relations, input.entities)
        .map_err(|_| Error::Integrity)?;
    input
        .query
        .graph_projection_manifest()
        .copied()
        .ok_or(Error::Integrity)
}
pub(super) fn insert(
    blocks: &mut BTreeMap<Cid, Vec<u8>>,
    cid: Cid,
    bytes: Vec<u8>,
    total: &mut usize,
    limits: GraphTransferLimits,
) -> Result<(), Error> {
    if bytes.len() > limits.max_block_bytes {
        return Err(Error::Limit);
    }
    if ContentBlock::new(ContentCodec::from_cid(&cid)?, &bytes).cid() != cid {
        return Err(Error::Integrity);
    }
    if blocks.contains_key(&cid) {
        return Ok(());
    }
    let next = total.checked_add(bytes.len()).ok_or(Error::Limit)?;
    if blocks.len() >= limits.max_blocks || next > limits.max_total_bytes {
        return Err(Error::Limit);
    }
    *total = next;
    blocks.insert(cid, bytes);
    Ok(())
}
pub(super) fn begin(
    input: &CombinedGraphInputs<'_>,
    dataset_bytes: Vec<u8>,
) -> Result<PreparedCombinedGraph, Error> {
    let dataset_root = check(input)?;
    let dataset =
        GraphDatasetDescriptor::decode_checked(&dataset_bytes, &dataset_root, input.dataset_limits)
            .map_err(|_| Error::Integrity)?;
    dataset
        .admit_query(input.query, input.dataset_limits)
        .map_err(|_| Error::Integrity)?;
    dataset
        .verify_catalogs(input.relations, input.entities)
        .map_err(|_| Error::Integrity)?;
    let mut prepared = PreparedCombinedGraph {
        snapshot: input.snapshot.clone(),
        dataset,
        dataset_root,
        blocks: BTreeMap::new(),
        total_bytes: 0,
    };
    for (cid, bytes) in [
        (*input.snapshot.cid(), input.snapshot.bytes().to_vec()),
        (dataset_root, dataset_bytes),
    ] {
        insert(
            &mut prepared.blocks,
            cid,
            bytes,
            &mut prepared.total_bytes,
            input.limits,
        )?;
    }
    Ok(prepared)
}
pub(super) fn children(
    prepared: &PreparedCombinedGraph,
    limits: GraphTransferLimits,
) -> Result<BTreeSet<Cid>, Error> {
    let mut children = BTreeSet::new();
    let files = std::iter::once(prepared.dataset.properties().inventory())
        .chain(prepared.dataset.relations().iter().map(|r| &r.inventory))
        .flat_map(mrr_data_core::GraphDatasetInventory::files)
        .map(|f| *f.cid());
    for cid in prepared
        .snapshot
        .manifest()
        .referenced_cids()
        .into_iter()
        .chain(files)
    {
        if prepared.blocks.contains_key(&cid) {
            continue;
        }
        children.insert(cid);
        if children
            .len()
            .checked_add(prepared.blocks.len())
            .ok_or(Error::Limit)?
            > limits.max_blocks
        {
            return Err(Error::Limit);
        }
    }
    Ok(children)
}
pub(super) fn finish(prepared: &PreparedCombinedGraph) -> Result<(), Error> {
    let borrowed = prepared
        .blocks
        .iter()
        .map(|(cid, bytes)| (*cid, bytes.as_slice()))
        .collect();
    verify_closure(prepared.snapshot.manifest(), &borrowed)?;
    for inventory in std::iter::once(prepared.dataset.properties().inventory())
        .chain(prepared.dataset.relations().iter().map(|r| &r.inventory))
    {
        for file in inventory.files() {
            let bytes = prepared.block(file.cid()).ok_or(Error::Missing)?;
            if bytes.len() as u64 != file.byte_length() {
                return Err(Error::Integrity);
            }
        }
    }
    Ok(())
}
/// Prepare all declared Arrow, coverage and native children before remote writes.
/// Disk providers belong on a Host blocking worker; retain the result's Backend lease.
/// # Errors
/// Refuses missing/corrupt children, scope/profile drift and aggregate budgets.
pub fn prepare_combined_graph(
    local: &dyn ContentStore,
    input: CombinedGraphInputs<'_>,
) -> Result<PreparedCombinedGraph, Error> {
    let dataset = check(&input)?;
    let cap = input
        .limits
        .max_total_bytes
        .checked_sub(input.snapshot.bytes().len())
        .ok_or(Error::Limit)?;
    let bytes = local.get_bounded(
        &dataset,
        cap.min(input.limits.max_block_bytes)
            .min(input.dataset_limits.inventory.max_manifest_bytes),
    )?;
    let mut prepared = begin(&input, bytes)?;
    for cid in children(&prepared, input.limits)? {
        let bytes = local.get_bounded(
            &cid,
            input
                .limits
                .max_block_bytes
                .min(input.limits.max_total_bytes - prepared.total_bytes),
        )?;
        insert(
            &mut prepared.blocks,
            cid,
            bytes,
            &mut prepared.total_bytes,
            input.limits,
        )?;
    }
    finish(&prepared)?;
    Ok(prepared)
}
