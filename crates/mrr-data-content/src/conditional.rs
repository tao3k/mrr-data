//! Provider-neutral protocol for conditional publication of a content head.
//!
//! These checks perform no I/O. A Host authenticates the current head, immutable
//! publication acknowledgement and operation ledger, then applies the decision
//! with the head and exact receipt in one atomic backend transaction. An Apply
//! decision is not evidence of commit or durable storage.

use cid::Cid;

use crate::{ContentCodec, ContentError, PublishReceipt};

/// A monotonic head revision and the exact immutable state it names.
/// Revision zero is reserved for absence; it cannot name an existing head.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ContentRevision {
    pub revision: u64,
    pub root: Cid,
}

/// One exact head replacement. IDs are opaque Host-authenticated namespaces,
/// not filesystem paths or database keys supplied directly to an adapter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConditionalContentWrite<'a> {
    pub scope: &'a str,
    pub operation_id: &'a str,
    pub expected: Option<ContentRevision>,
    pub replacement: Cid,
}

/// An authenticated ledger row persisted with the head in the same transaction.
/// A caller-created projection alone carries no provider or transaction evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConditionalContentReceipt<'a> {
    pub write: ConditionalContentWrite<'a>,
    pub committed: ContentRevision,
}

/// Apply authorizes a proposed state transition; Replay reports a prior exact
/// operation and grants no new write. Neither result executes a transaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConditionalCommitDisposition {
    Apply(ContentRevision),
    Replay,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConditionalCommitError {
    EmptyScope,
    EmptyOperation,
    InvalidRevision,
    RevisionExhausted,
    RevisionConflict,
    MissingPublication,
    DifferentPublication,
    OperationConflict,
    InvalidReceipt,
    Content(ContentError),
}

impl std::fmt::Display for ConditionalCommitError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "conditional content commit: {self:?}")
    }
}

impl std::error::Error for ConditionalCommitError {}

fn check_revision(state: ContentRevision) -> Result<(), ConditionalCommitError> {
    if state.revision == 0 {
        return Err(ConditionalCommitError::InvalidRevision);
    }
    ContentCodec::from_cid(&state.root).map_err(ConditionalCommitError::Content)?;
    Ok(())
}

impl ConditionalContentWrite<'_> {
    fn next(&self) -> Result<ContentRevision, ConditionalCommitError> {
        if self.scope.is_empty() {
            return Err(ConditionalCommitError::EmptyScope);
        }
        if self.operation_id.is_empty() {
            return Err(ConditionalCommitError::EmptyOperation);
        }
        ContentCodec::from_cid(&self.replacement).map_err(ConditionalCommitError::Content)?;
        if let Some(expected) = self.expected {
            check_revision(expected)?;
        }
        let revision = self
            .expected
            .map_or(0, |state| state.revision)
            .checked_add(1)
            .ok_or(ConditionalCommitError::RevisionExhausted)?;
        Ok(ContentRevision {
            revision,
            root: self.replacement,
        })
    }

    /// Validate a proposed conditional Host transaction. Compare both revision
    /// and CID, even when bytes return to an earlier value (the ABA case).
    ///
    /// `current = None` must mean authenticated absence, never a transport error.
    /// `existing` is the authenticated row for this scope and operation ID. An
    /// exact replay remains a status query after the head advances and needs no
    /// new publication ACK. A reused ID cannot name different write contents.
    ///
    /// The backend must atomically compare the head, enforce operation uniqueness,
    /// persist the returned next state and receipt, and satisfy its durability
    /// policy. An uncertain backend response requires querying that exact operation
    /// receipt; neither local publication nor this decision implies commit.
    /// # Errors
    /// Returns invalid identity, conflicting state, missing ACK or receipt mismatch.
    pub fn decide_commit(
        &self,
        current: Option<ContentRevision>,
        physical: Option<&PublishReceipt>,
        existing: Option<&ConditionalContentReceipt<'_>>,
    ) -> Result<ConditionalCommitDisposition, ConditionalCommitError> {
        let next = self.next()?;
        if let Some(receipt) = existing {
            if receipt.write != *self {
                return Err(ConditionalCommitError::OperationConflict);
            }
            if receipt.committed != next {
                return Err(ConditionalCommitError::InvalidReceipt);
            }
            return Ok(ConditionalCommitDisposition::Replay);
        }
        if let Some(state) = current {
            check_revision(state)?;
        }
        if current != self.expected {
            return Err(ConditionalCommitError::RevisionConflict);
        }
        let physical = physical.ok_or(ConditionalCommitError::MissingPublication)?;
        if physical.cid != self.replacement {
            return Err(ConditionalCommitError::DifferentPublication);
        }
        Ok(ConditionalCommitDisposition::Apply(next))
    }
}
