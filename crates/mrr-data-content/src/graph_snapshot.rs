//! Complete graph closure ACKs precede protected metadata head selection.
use crate::{
    AsyncContentStore, ContentBlock, ContentCodec, ContentError, ContentProtocolError,
    ContentStore, PublishReceipt, RemoteContentStore, closure::verify_closure, publish_content,
    read_through,
};
use cid::Cid;
use meta_relational_reasoning::{EntityCatalog, RelationCatalog};
use mrr_data_core::{
    BoundDataQuery, GraphDatasetBinding, GraphDatasetInventory, GraphInventoryLimits,
    SnapshotBlock, SnapshotManifest, dag_cbor_cid,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
};

/// Bounds retained closure payloads, including all three metadata roots.
/// Encoding scratch, decoded metadata and cache copies need separate Host budgets.
/// This is a materialized bounded closure, not a streaming or native RSS bound.
#[derive(Clone, Copy)]
pub struct GraphTransferLimits {
    pub max_blocks: usize,
    pub max_block_bytes: usize,
    pub max_total_bytes: usize,
}
/// Failures preserve the distinction between limits, integrity and transport.
#[derive(Debug)]
pub enum GraphTransferError {
    Limit,
    Integrity,
    Content(ContentError),
    Transfer(ContentProtocolError),
    RootDenied,
    Missing,
}
impl std::fmt::Display for GraphTransferError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "graph transfer: {self:?}")
    }
}
impl std::error::Error for GraphTransferError {}
impl From<ContentError> for GraphTransferError {
    fn from(value: ContentError) -> Self {
        Self::Content(value)
    }
}
impl From<ContentProtocolError> for GraphTransferError {
    fn from(value: ContentProtocolError) -> Self {
        Self::Transfer(value)
    }
}
/// Borrowed semantic and physical inputs; preparation performs no remote writes.
#[derive(Clone, Copy)]
pub struct GraphPublishInputs<'a> {
    pub query: &'a BoundDataQuery,
    pub binding: &'a GraphDatasetBinding,
    pub inventory: &'a GraphDatasetInventory,
    pub snapshot: &'a SnapshotBlock,
    pub relations: &'a RelationCatalog,
    pub entities: &'a EntityCatalog,
    pub inventory_limits: GraphInventoryLimits,
    pub limits: GraphTransferLimits,
}
/// Owned verified closure. Keep behind a Backend `ResourceHandle` while publishing.
/// No mutable buffers or independent buffer ownership are exported.
pub struct PreparedGraphPublication {
    root: Cid,
    snapshot: Cid,
    inventory: Cid,
    blocks: BTreeMap<Cid, Vec<u8>>,
    total_bytes: usize,
}
impl PreparedGraphPublication {
    #[must_use]
    pub const fn root(&self) -> &Cid {
        &self.root
    }
    #[must_use]
    pub const fn total_bytes(&self) -> usize {
        self.total_bytes
    }
    #[must_use]
    pub fn block_count(&self) -> usize {
        self.blocks.len()
    }
}
/// Constructed only after every child and metadata root is acknowledged.
/// It is a physical receipt, not authorization or provider durability attestation.
#[derive(Debug)]
pub struct GraphPublication {
    receipt: PublishReceipt,
}
impl GraphPublication {
    #[must_use]
    pub const fn receipt(&self) -> &PublishReceipt {
        &self.receipt
    }
}
fn check_inputs(inputs: &GraphPublishInputs<'_>) -> Result<(), GraphTransferError> {
    if inputs.limits.max_blocks < 3
        || inputs.limits.max_block_bytes == 0
        || inputs.limits.max_total_bytes == 0
    {
        return Err(GraphTransferError::Limit);
    }
    inputs
        .binding
        .admit_query(inputs.query, inputs.inventory, inputs.inventory_limits)
        .map_err(|_| GraphTransferError::Integrity)?;
    if inputs.snapshot.cid() != inputs.binding.snapshot_root() {
        return Err(GraphTransferError::Integrity);
    }
    inputs
        .snapshot
        .manifest()
        .verify_catalogs(inputs.relations, inputs.entities)
        .map_err(|_| GraphTransferError::Integrity)
}
fn insert(
    blocks: &mut BTreeMap<Cid, Vec<u8>>,
    cid: Cid,
    bytes: Vec<u8>,
    total: &mut usize,
    limits: GraphTransferLimits,
) -> Result<(), GraphTransferError> {
    if bytes.len() > limits.max_block_bytes {
        return Err(GraphTransferError::Limit);
    }
    if ContentBlock::new(ContentCodec::from_cid(&cid)?, &bytes).cid() != cid {
        return Err(GraphTransferError::Integrity);
    }
    if blocks.contains_key(&cid) {
        return Ok(());
    }
    let next = total
        .checked_add(bytes.len())
        .ok_or(GraphTransferError::Limit)?;
    if blocks.len() >= limits.max_blocks || next > limits.max_total_bytes {
        return Err(GraphTransferError::Limit);
    }
    *total = next;
    blocks.insert(cid, bytes);
    Ok(())
}
fn begin(inputs: &GraphPublishInputs<'_>) -> Result<PreparedGraphPublication, GraphTransferError> {
    check_inputs(inputs)?;
    let binding = inputs
        .binding
        .canonical_bytes()
        .map_err(|_| GraphTransferError::Integrity)?;
    let inventory = inputs
        .inventory
        .canonical_bytes(inputs.inventory_limits)
        .map_err(|_| GraphTransferError::Integrity)?;
    let snapshot_bytes = inputs.snapshot.bytes();
    let roots_total = binding
        .len()
        .checked_add(inventory.len())
        .and_then(|n| n.checked_add(snapshot_bytes.len()))
        .ok_or(GraphTransferError::Limit)?;
    if [binding.len(), inventory.len(), snapshot_bytes.len()]
        .into_iter()
        .any(|n| n > inputs.limits.max_block_bytes)
        || roots_total > inputs.limits.max_total_bytes
    {
        return Err(GraphTransferError::Limit);
    }
    let mut value = PreparedGraphPublication {
        root: dag_cbor_cid(&binding),
        snapshot: *inputs.snapshot.cid(),
        inventory: *inputs.binding.inventory_root(),
        blocks: BTreeMap::new(),
        total_bytes: 0,
    };
    for (cid, bytes) in [
        (value.root, binding),
        (value.inventory, inventory),
        (value.snapshot, snapshot_bytes.to_vec()),
    ] {
        insert(
            &mut value.blocks,
            cid,
            bytes,
            &mut value.total_bytes,
            inputs.limits,
        )?;
    }
    Ok(value)
}
fn children(
    inputs: &GraphPublishInputs<'_>,
    prepared: &PreparedGraphPublication,
) -> Result<BTreeSet<Cid>, GraphTransferError> {
    let mut children = BTreeSet::new();
    for cid in inputs
        .snapshot
        .manifest()
        .referenced_cids()
        .into_iter()
        .chain(inputs.inventory.files().iter().map(|f| *f.cid()))
    {
        if prepared.blocks.contains_key(&cid) {
            continue;
        }
        children.insert(cid);
        if children
            .len()
            .checked_add(prepared.blocks.len())
            .ok_or(GraphTransferError::Limit)?
            > inputs.limits.max_blocks
        {
            return Err(GraphTransferError::Limit);
        }
    }
    Ok(children)
}
fn finish(
    inputs: &GraphPublishInputs<'_>,
    prepared: &PreparedGraphPublication,
) -> Result<(), GraphTransferError> {
    let borrowed = prepared
        .blocks
        .iter()
        .map(|(cid, bytes)| (*cid, bytes.as_slice()))
        .collect();
    verify_closure(inputs.snapshot.manifest(), &borrowed)?;
    for file in inputs.inventory.files() {
        file.verify(
            prepared
                .blocks
                .get(file.cid())
                .ok_or(GraphTransferError::Missing)?,
        )
        .map_err(|_| GraphTransferError::Integrity)?;
    }
    Ok(())
}
/// Verify the complete closure before any remote side effect. Disk stores belong
/// on the Host blocking executor; use `Backend::prepare_resource` to retain it.
/// # Errors
/// Refuses scope/catalog drift, absent/changed children and aggregate budgets.
pub fn prepare_graph_publication(
    local: &dyn ContentStore,
    inputs: GraphPublishInputs<'_>,
) -> Result<PreparedGraphPublication, GraphTransferError> {
    let mut prepared = begin(&inputs)?;
    for cid in children(&inputs, &prepared)? {
        let limit = inputs
            .limits
            .max_block_bytes
            .min(inputs.limits.max_total_bytes - prepared.total_bytes);
        let bytes = local.get_bounded(&cid, limit)?;
        insert(
            &mut prepared.blocks,
            cid,
            bytes,
            &mut prepared.total_bytes,
            inputs.limits,
        )?;
    }
    finish(&inputs, &prepared)?;
    Ok(prepared)
}
/// ACK children, inventory and legacy snapshot before the binding root. A Host
/// supplies transport policy and a fresh before-root gate. Only a later protected
/// metadata CAS selects a discovery head. The gate alone does not synchronize
/// revocation: `root_remote` must retain the Host authority guard during root PUT.
/// Failure/cancellation may leave immutable
/// orphans; they are never deleted and grant no permission for a new operation.
/// # Errors
/// Refuses any failed ACK or root gate; returns no partial success receipt.
pub async fn publish_graph_dataset<F, Fut>(
    prepared: &PreparedGraphPublication,
    local: &(dyn AsyncContentStore + Sync),
    remote: &dyn RemoteContentStore,
    root_remote: &dyn RemoteContentStore,
    before_root: F,
) -> Result<GraphPublication, GraphTransferError>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<(), GraphTransferError>>,
{
    for (cid, bytes) in &prepared.blocks {
        if [prepared.root, prepared.snapshot, prepared.inventory].contains(cid) {
            continue;
        }
        publish_content(
            local,
            remote,
            ContentBlock::new(ContentCodec::from_cid(cid)?, bytes),
        )
        .await?;
    }
    for cid in [prepared.inventory, prepared.snapshot] {
        publish_content(
            local,
            remote,
            ContentBlock::new(ContentCodec::DagCbor, &prepared.blocks[&cid]),
        )
        .await?;
    }
    before_root().await?;
    let receipt = publish_content(
        local,
        root_remote,
        ContentBlock::new(ContentCodec::DagCbor, &prepared.blocks[&prepared.root]),
    )
    .await?;
    Ok(GraphPublication { receipt })
}
/// Restore from a Host-authenticated graph binding head, checking every referenced
/// byte before exposing owned buffers. Local cache success is optional.
/// # Errors
/// Refuses missing/corrupt roots/children, scope drift or count/byte budgets.
pub async fn restore_graph_dataset(
    local: &(dyn AsyncContentStore + Sync),
    remote: &dyn RemoteContentStore,
    trusted_root: &Cid,
    query: &BoundDataQuery,
    relations: &RelationCatalog,
    entities: &EntityCatalog,
    bounds: (GraphTransferLimits, GraphInventoryLimits),
) -> Result<PreparedGraphPublication, GraphTransferError> {
    let (limits, inventory_limits) = bounds;
    if limits.max_blocks < 3 || limits.max_block_bytes == 0 || limits.max_total_bytes == 0 {
        return Err(GraphTransferError::Limit);
    }
    let bytes = read_through(
        local,
        remote,
        trusted_root,
        4096.min(limits.max_block_bytes).min(limits.max_total_bytes),
    )
    .await?
    .ok_or(GraphTransferError::Missing)?
    .bytes;
    let binding = GraphDatasetBinding::decode_checked(&bytes, trusted_root)
        .map_err(|_| GraphTransferError::Integrity)?;
    binding
        .admit_query_scope(query)
        .map_err(|_| GraphTransferError::Integrity)?;
    let mut consumed = bytes.len();
    drop(bytes);
    let inventory_bytes = read_through(
        local,
        remote,
        binding.inventory_root(),
        inventory_limits
            .max_manifest_bytes
            .min(limits.max_block_bytes)
            .min(limits.max_total_bytes - consumed),
    )
    .await?
    .ok_or(GraphTransferError::Missing)?
    .bytes;
    consumed = consumed
        .checked_add(inventory_bytes.len())
        .ok_or(GraphTransferError::Limit)?;
    let inventory = GraphDatasetInventory::decode_checked(
        &inventory_bytes,
        binding.inventory_root(),
        inventory_limits,
    )
    .map_err(|_| GraphTransferError::Integrity)?;
    drop(inventory_bytes);
    let snapshot_bytes = read_through(
        local,
        remote,
        binding.snapshot_root(),
        limits
            .max_block_bytes
            .min(limits.max_total_bytes - consumed),
    )
    .await?
    .ok_or(GraphTransferError::Missing)?
    .bytes;
    let snapshot = SnapshotBlock::encode(
        SnapshotManifest::decode_checked(&snapshot_bytes, binding.snapshot_root())
            .map_err(|_| GraphTransferError::Integrity)?,
    )
    .map_err(|_| GraphTransferError::Integrity)?;
    drop(snapshot_bytes);
    let inputs = GraphPublishInputs {
        query,
        binding: &binding,
        inventory: &inventory,
        snapshot: &snapshot,
        relations,
        entities,
        inventory_limits,
        limits,
    };
    let mut prepared = begin(&inputs)?;
    for cid in children(&inputs, &prepared)? {
        let cap = limits
            .max_block_bytes
            .min(limits.max_total_bytes - prepared.total_bytes);
        let bytes = read_through(local, remote, &cid, cap)
            .await?
            .ok_or(GraphTransferError::Missing)?
            .bytes;
        insert(
            &mut prepared.blocks,
            cid,
            bytes,
            &mut prepared.total_bytes,
            limits,
        )?;
    }
    finish(&inputs, &prepared)?;
    Ok(prepared)
}
