//! Projected authorization for one read of an exact committed protected root.
//! The Host authenticates the ledger row, current lineage, reader and claim.

use super::protected::{ProtectedCommitReceipt, ProtectedStorageMismatch};
use super::storage::{CurrentStorageState, EntityRef};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProtectedReadDestination<'a> {
    pub resource: EntityRef<'a>,
    pub tenant: &'a str,
    pub accepted_owners: &'a [EntityRef<'a>],
    pub accepts_restricted: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProtectedReadIntent<'a> {
    pub operation_id: &'a str,
    pub subject: EntityRef<'a>,
    pub purpose: &'a str,
    pub receipt: ProtectedCommitReceipt<'a>,
    pub reader: ProtectedReadDestination<'a>,
    pub policy_root: &'a str,
    pub lineage_revision: &'a str,
}

#[derive(Clone, Copy, Debug)]
pub struct ProtectedReadClaim<'a> {
    pub intent: ProtectedReadIntent<'a>,
    pub epoch: u64,
    pub expires_at: u64,
    pub allowed: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProtectedReadMismatch {
    EmptyScope,
    TooManyOwners,
    InvalidPublication(ProtectedStorageMismatch),
    MissingCommit,
    DifferentCommit,
    InvalidCommit,
    DifferentRead,
    Denied,
    Stale,
    Tenant,
    Owner,
    RestrictedReader,
}

impl std::fmt::Display for ProtectedReadMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "protected read admission: {self:?}")
    }
}

impl std::error::Error for ProtectedReadMismatch {}

impl ProtectedReadIntent<'_> {
    /// Check a new action-specific read before any cache or provider GET.
    /// A prior write approval or commit replay does not authorize the read.
    /// # Errors
    /// Returns a missing, stale, conflicting or custody mismatch.
    pub fn check_read(
        &self,
        claim: &ProtectedReadClaim<'_>,
        current: CurrentStorageState<'_>,
        committed: Option<&ProtectedCommitReceipt<'_>>,
    ) -> Result<(), ProtectedReadMismatch> {
        if self.operation_id.is_empty()
            || self.subject.is_empty()
            || self.purpose.is_empty()
            || self.policy_root.is_empty()
            || self.lineage_revision.is_empty()
            || self.reader.resource.is_empty()
            || self.reader.tenant.is_empty()
            || self.reader.accepted_owners.is_empty()
            || self
                .reader
                .accepted_owners
                .iter()
                .any(|owner| owner.is_empty())
        {
            return Err(ProtectedReadMismatch::EmptyScope);
        }
        if self.reader.accepted_owners.len() > 64 {
            return Err(ProtectedReadMismatch::TooManyOwners);
        }
        self.receipt
            .publication
            .check_static()
            .map_err(ProtectedReadMismatch::InvalidPublication)?;
        if self.receipt.total_outer_bytes == 0 || self.receipt.child_count > 4096 {
            return Err(ProtectedReadMismatch::InvalidCommit);
        }
        let committed = committed.ok_or(ProtectedReadMismatch::MissingCommit)?;
        if *committed != self.receipt {
            return Err(ProtectedReadMismatch::DifferentCommit);
        }
        if *self != claim.intent {
            return Err(ProtectedReadMismatch::DifferentRead);
        }
        if !claim.allowed {
            return Err(ProtectedReadMismatch::Denied);
        }
        if current.policy_root != self.policy_root
            || current.lineage_revision != self.lineage_revision
            || current.epoch != claim.epoch
            || current.now >= claim.expires_at
        {
            return Err(ProtectedReadMismatch::Stale);
        }
        for source in self.receipt.publication.intent.storage.sources {
            if source.tenant != self.reader.tenant {
                return Err(ProtectedReadMismatch::Tenant);
            }
            if !self.reader.accepted_owners.contains(&source.owner) {
                return Err(ProtectedReadMismatch::Owner);
            }
            if source.restricted && !self.reader.accepts_restricted {
                return Err(ProtectedReadMismatch::RestrictedReader);
            }
        }
        Ok(())
    }

    /// Check both the admission observation and a fresh observation before
    /// releasing verified plaintext. The Host authenticates both observations.
    /// # Errors
    /// Returns the first rejected observation.
    pub fn check_release(
        &self,
        claim: &ProtectedReadClaim<'_>,
        before: (CurrentStorageState<'_>, Option<&ProtectedCommitReceipt<'_>>),
        after: (CurrentStorageState<'_>, Option<&ProtectedCommitReceipt<'_>>),
    ) -> Result<(), ProtectedReadMismatch> {
        self.check_read(claim, before.0, before.1)?;
        if after.0.now < before.0.now {
            return Err(ProtectedReadMismatch::Stale);
        }
        self.check_read(claim, after.0, after.1)
    }
}
