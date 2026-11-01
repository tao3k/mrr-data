//! Shared owned closure and borrowed authority inputs.
use crate::GraphTransferLimits;
use cid::Cid;
use meta_relational_reasoning::{EntityCatalog, RelationCatalog};
use mrr_data_core::{BoundDataQuery, GraphDatasetDescriptor, GraphDatasetLimits, SnapshotBlock};
use std::collections::BTreeMap;

/// Preparation performs no remote writes; semantic authority belongs to MRR.
#[derive(Clone, Copy)]
pub struct CombinedGraphInputs<'a> {
    pub query: &'a BoundDataQuery,
    pub snapshot: &'a SnapshotBlock,
    pub relations: &'a RelationCatalog,
    pub entities: &'a EntityCatalog,
    pub dataset_limits: GraphDatasetLimits,
    pub limits: GraphTransferLimits,
}
/// Verified immutable payload closure. Retain behind the shared Backend lease.
/// Payload budgets exclude decoded metadata, encoding scratch and provider copies.
pub struct PreparedCombinedGraph {
    pub(super) snapshot: SnapshotBlock,
    pub(super) dataset: GraphDatasetDescriptor,
    pub(super) dataset_root: Cid,
    pub(super) blocks: BTreeMap<Cid, Vec<u8>>,
    pub(super) total_bytes: usize,
}
impl PreparedCombinedGraph {
    #[must_use]
    pub fn root(&self) -> &Cid {
        self.snapshot.cid()
    }
    #[must_use]
    pub const fn dataset(&self) -> &GraphDatasetDescriptor {
        &self.dataset
    }
    #[must_use]
    pub const fn snapshot(&self) -> &SnapshotBlock {
        &self.snapshot
    }
    #[must_use]
    pub const fn total_bytes(&self) -> usize {
        self.total_bytes
    }
    #[must_use]
    pub fn block_count(&self) -> usize {
        self.blocks.len()
    }
    #[must_use]
    pub fn block(&self, cid: &Cid) -> Option<&[u8]> {
        self.blocks.get(cid).map(Vec::as_slice)
    }
}
