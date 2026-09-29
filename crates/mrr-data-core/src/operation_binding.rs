//! Borrowed physical identities for downstream data operations.
//!
//! These views do not authorize an operation. A deployment authenticates
//! governance facts, selects a policy, and controls the actual effect.

use cid::Cid;
use meta_relational_reasoning::{
    GenerationId, QueryResultAdmissionReceipt, QueryResultBinding, RelationId,
};

use crate::{BoundDataQuery, SnapshotBlock};

/// Physical and semantic identity of one already constructed snapshot block.
///
/// Construction borrows existing values. It does not encode the manifest,
/// hash payloads, allocate, or read storage.
#[derive(Clone, Copy, Debug)]
pub struct SnapshotOperationBinding<'a> {
    snapshot: &'a SnapshotBlock,
}

impl<'a> SnapshotOperationBinding<'a> {
    #[must_use]
    pub const fn new(snapshot: &'a SnapshotBlock) -> Self {
        Self { snapshot }
    }

    /// The immutable manifest and canonical root used by this binding.
    #[must_use]
    pub const fn snapshot(&self) -> &'a SnapshotBlock {
        self.snapshot
    }

    #[must_use]
    pub const fn root(&self) -> &Cid {
        self.snapshot.cid()
    }

    #[must_use]
    pub const fn generation(&self) -> GenerationId {
        self.snapshot.manifest().semantic_snapshot().generation()
    }

    #[must_use]
    pub const fn semantic_digest(&self) -> &[u8; 32] {
        self.snapshot.manifest().semantic_snapshot().digest()
    }

    #[must_use]
    pub const fn relation_catalog_digest(&self) -> &[u8; 32] {
        self.snapshot.manifest().relation_catalog_digest()
    }

    #[must_use]
    pub const fn entity_catalog_digest(&self) -> &[u8; 32] {
        self.snapshot.manifest().entity_catalog_digest()
    }
}

/// A selected row does not exist in the stated relation and child block.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SnapshotRowBindingError {
    RelationUnavailable,
    ChildUnavailable,
    RowOutOfBounds,
}

/// Location of one row in an immutable snapshot's ordered Arrow child blocks.
///
/// Membership and the child-local row bound are checked against the manifest.
/// The Host still authenticates the child bytes, field schema, and selected
/// value before using this location for an effect.
#[derive(Clone, Copy, Debug)]
pub struct SnapshotRowBinding<'a> {
    source: SnapshotOperationBinding<'a>,
    relation_id: RelationId,
    child_cid: &'a Cid,
    row_index: u64,
}

impl<'a> SnapshotRowBinding<'a> {
    /// Bind a child-local row ordinal to a relation in the selected snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotRowBindingError`] for a missing relation, missing
    /// child, or an ordinal outside the child row count.
    pub fn new(
        snapshot: &'a SnapshotBlock,
        relation_id: RelationId,
        child_cid: &Cid,
        row_index: u64,
    ) -> Result<Self, SnapshotRowBindingError> {
        let relation = snapshot
            .manifest()
            .relations()
            .iter()
            .find(|relation| relation.relation_id() == relation_id)
            .ok_or(SnapshotRowBindingError::RelationUnavailable)?;
        let child = relation
            .batches()
            .iter()
            .find(|child| child.cid() == child_cid)
            .ok_or(SnapshotRowBindingError::ChildUnavailable)?;
        if row_index >= child.row_count() {
            return Err(SnapshotRowBindingError::RowOutOfBounds);
        }
        Ok(Self {
            source: SnapshotOperationBinding::new(snapshot),
            relation_id,
            child_cid: child.cid(),
            row_index,
        })
    }

    #[must_use]
    pub const fn source(&self) -> SnapshotOperationBinding<'a> {
        self.source
    }

    #[must_use]
    pub const fn relation_id(&self) -> RelationId {
        self.relation_id
    }

    #[must_use]
    pub const fn child_cid(&self) -> &'a Cid {
        self.child_cid
    }

    #[must_use]
    pub const fn row_index(&self) -> u64 {
        self.row_index
    }
}

/// Identity of an MRR-admitted query bound to an mrr-data physical snapshot.
#[derive(Clone, Copy, Debug)]
pub struct QueryOperationBinding<'a> {
    query: &'a BoundDataQuery,
}

impl<'a> QueryOperationBinding<'a> {
    #[must_use]
    pub const fn new(query: &'a BoundDataQuery) -> Self {
        Self { query }
    }

    #[must_use]
    pub const fn root(&self) -> &Cid {
        self.query.snapshot_root()
    }

    #[must_use]
    pub const fn generation(&self) -> GenerationId {
        self.query.query().generation()
    }

    #[must_use]
    pub const fn query_binding_digest(&self) -> &[u8; 32] {
        self.query.query().digest()
    }

    #[must_use]
    pub const fn query_digest(&self) -> &[u8; 32] {
        self.query.query().query_digest()
    }

    #[must_use]
    pub fn engine_name(&self) -> &str {
        self.query.engine().name()
    }

    #[must_use]
    pub const fn graph_projection_manifest(&self) -> Option<&Cid> {
        self.query.graph_projection_manifest()
    }
}

/// An MRR result receipt does not belong to the selected physical query.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReleaseBindingError {
    QueryBindingMismatch,
}

/// One MRR-admitted result paired with the exact selected physical query.
///
/// The receipt admits candidate semantics. The Host must separately bind the
/// actual released bytes to its output digest, authenticate destination and
/// approval facts, and commit any budget or audit effect.
#[derive(Clone, Copy, Debug)]
pub struct ReleaseOperationBinding<'a> {
    query: QueryOperationBinding<'a>,
    result: &'a QueryResultAdmissionReceipt,
    output_digest: &'a [u8; 32],
}

impl<'a> ReleaseOperationBinding<'a> {
    /// Rejects a result admitted for a different semantic query binding.
    ///
    /// # Errors
    ///
    /// Returns [`ReleaseBindingError::QueryBindingMismatch`] on mismatch.
    pub fn new(
        query: &'a BoundDataQuery,
        result: &'a QueryResultAdmissionReceipt,
        output_digest: &'a [u8; 32],
    ) -> Result<Self, ReleaseBindingError> {
        if result.binding() != QueryResultBinding::for_query(query.query()) {
            return Err(ReleaseBindingError::QueryBindingMismatch);
        }
        Ok(Self {
            query: QueryOperationBinding::new(query),
            result,
            output_digest,
        })
    }

    #[must_use]
    pub const fn query(&self) -> QueryOperationBinding<'a> {
        self.query
    }

    #[must_use]
    pub const fn admitted_result_digest(&self) -> &[u8; 32] {
        self.result.digest()
    }

    #[must_use]
    pub const fn output_digest(&self) -> &[u8; 32] {
        self.output_digest
    }

    #[must_use]
    pub const fn row_count(&self) -> usize {
        self.result.row_count()
    }
}

/// Physical evidence supplied to an external security policy or Host.
///
/// The variants distinguish operation phases. No variant carries a decision
/// or grants permission to execute the effect.
#[derive(Clone, Copy, Debug)]
pub enum DataOperationBinding<'a> {
    /// Authorize access to a root before loading its manifest or children.
    ReadRoot(&'a Cid),
    /// Authorize publication of a constructed snapshot.
    Publish(SnapshotOperationBinding<'a>),
    /// Authorize execution of a bound physical query.
    Query(QueryOperationBinding<'a>),
    /// Authorize release of bytes after MRR result admission.
    Release(ReleaseOperationBinding<'a>),
}
