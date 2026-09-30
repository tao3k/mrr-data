//! Host projection of SPEC Protected Storage V1.
//!
//! These checks compare an authenticated intent and current governance at
//! admission, before the root write, and for the later commit disposition.
//! The Host authenticates inputs,
//! verifies encrypted bytes and both closures, redeems the operation once,
//! and persists the effect and audit. These checks perform none of that I/O.

use super::storage::{CurrentStorageStateV1, StorageEffectV1};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProtectionIntentV1<'a> {
    pub storage: StorageEffectV1<'a>,
    pub profile: &'a str,
    pub key_ref: &'a str,
    pub key_version: &'a str,
    pub residency: &'a str,
}

#[derive(Clone, Copy, Debug)]
pub struct ProtectionClaimV1<'a> {
    pub intent: ProtectionIntentV1<'a>,
    pub epoch: u64,
    pub expires_at: u64,
    pub allowed: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProtectedPublicationV1<'a> {
    pub intent: ProtectionIntentV1<'a>,
    pub outer_root: &'a cid::Cid,
    pub envelope_version: u32,
    pub key_version: &'a str,
}

/// Projected acknowledgement of a successful protected root write. The
/// publishing library returns the concrete acknowledgement after remote PUT;
/// a bare projection supplied by a caller is not provider evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProtectedPhysicalAckV1<'a> {
    pub inner_root: &'a cid::Cid,
    pub outer_root: &'a cid::Cid,
    pub child_count: usize,
    pub total_outer_bytes: usize,
}

/// An authenticated Host ledger row. The Host persists this together with
/// approval redemption, discoverability and audit in one atomic transaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProtectedCommitReceiptV1<'a> {
    pub publication: ProtectedPublicationV1<'a>,
    pub child_count: usize,
    pub total_outer_bytes: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProtectedCommitDispositionV1 {
    Apply,
    Replay,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProtectedStorageMismatch {
    EmptyScope,
    TooManyLabels,
    Stale,
    DifferentIntent,
    Denied,
    Tenant,
    Owner,
    RestrictedDestination,
    InnerAsOuter,
    EnvelopeVersion,
    KeyVersion,
    MissingPhysicalAck,
    PhysicalMismatch,
    InvalidPhysical,
    CommitConflict,
    InvalidReceipt,
}

impl std::fmt::Display for ProtectedStorageMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "protected storage admission: {self:?}")
    }
}

impl std::error::Error for ProtectedStorageMismatch {}

impl ProtectionIntentV1<'_> {
    fn check_scope(&self) -> Result<(), ProtectedStorageMismatch> {
        let effect = self.storage;
        if effect.operation_id.is_empty()
            || effect.subject.type_name.is_empty()
            || effect.subject.id.is_empty()
            || effect.purpose.is_empty()
            || effect.policy_root.is_empty()
            || effect.lineage_revision.is_empty()
            || effect.destination.resource.type_name.is_empty()
            || effect.destination.resource.id.is_empty()
            || effect.destination.tenant.is_empty()
            || effect.sources.is_empty()
            || effect.destination.accepted_owners.is_empty()
            || effect
                .destination
                .accepted_owners
                .iter()
                .any(|owner| owner.type_name.is_empty() || owner.id.is_empty())
            || effect.sources.iter().any(|source| {
                source.resource.type_name.is_empty()
                    || source.resource.id.is_empty()
                    || source.owner.type_name.is_empty()
                    || source.owner.id.is_empty()
                    || source.tenant.is_empty()
            })
            || self.profile.is_empty()
            || self.key_ref.is_empty()
            || self.key_version.is_empty()
            || self.residency.is_empty()
        {
            return Err(ProtectedStorageMismatch::EmptyScope);
        }
        if effect.sources.len() > 64 || effect.destination.accepted_owners.len() > 64 {
            return Err(ProtectedStorageMismatch::TooManyLabels);
        }
        Ok(())
    }

    /// Verify the exact projected intent before encryption or provider I/O.
    /// The Host must authenticate the projection and actually run Cedar.
    /// # Errors
    /// Returns the first missing, stale or incompatible field.
    pub fn check_intent(
        &self,
        claim: &ProtectionClaimV1<'_>,
        current: CurrentStorageStateV1<'_>,
    ) -> Result<(), ProtectedStorageMismatch> {
        self.check_scope()?;
        let effect = self.storage;
        if current.policy_root != effect.policy_root
            || current.lineage_revision != effect.lineage_revision
            || current.epoch != claim.epoch
            || current.now >= claim.expires_at
        {
            return Err(ProtectedStorageMismatch::Stale);
        }
        if *self != claim.intent {
            return Err(ProtectedStorageMismatch::DifferentIntent);
        }
        if !claim.allowed {
            return Err(ProtectedStorageMismatch::Denied);
        }
        for source in effect.sources {
            if source.tenant != effect.destination.tenant {
                return Err(ProtectedStorageMismatch::Tenant);
            }
            if !effect.destination.accepted_owners.contains(&source.owner) {
                return Err(ProtectedStorageMismatch::Owner);
            }
            if source.restricted && !effect.destination.accepts_restricted {
                return Err(ProtectedStorageMismatch::RestrictedDestination);
            }
        }
        Ok(())
    }
}

impl ProtectedPublicationV1<'_> {
    fn check_static(&self) -> Result<(), ProtectedStorageMismatch> {
        self.intent.check_scope()?;
        if self.outer_root == self.intent.storage.snapshot_root {
            return Err(ProtectedStorageMismatch::InnerAsOuter);
        }
        if self.envelope_version != 1 {
            return Err(ProtectedStorageMismatch::EnvelopeVersion);
        }
        if self.key_version != self.intent.key_version {
            return Err(ProtectedStorageMismatch::KeyVersion);
        }
        Ok(())
    }

    /// Recheck current projected authority immediately before the protected
    /// manifest root PUT. This does not create a Host ledger commit.
    /// # Errors
    /// Returns a mismatch without granting a publication capability.
    pub fn check_pre_root(
        &self,
        claim: &ProtectionClaimV1<'_>,
        current: CurrentStorageStateV1<'_>,
    ) -> Result<(), ProtectedStorageMismatch> {
        self.intent.check_intent(claim, current)?;
        self.check_static()
    }

    /// Validate a proposed atomic Host ledger transition. An exact existing
    /// receipt is a status replay, even after claim expiry, and grants no read
    /// authorization. A new commit requires fresh authority and physical ACK.
    /// The Host must authenticate `existing` and atomically persist an Apply
    /// decision with approval redemption, pointer and audit.
    /// # Errors
    /// Returns a stale, conflicting or missing-physical-evidence error.
    pub fn decide_commit(
        &self,
        claim: &ProtectionClaimV1<'_>,
        current: CurrentStorageStateV1<'_>,
        physical: Option<ProtectedPhysicalAckV1<'_>>,
        existing: Option<&ProtectedCommitReceiptV1<'_>>,
    ) -> Result<ProtectedCommitDispositionV1, ProtectedStorageMismatch> {
        if let Some(receipt) = existing {
            self.check_static()?;
            if receipt.publication != *self {
                return Err(ProtectedStorageMismatch::CommitConflict);
            }
            if receipt.total_outer_bytes == 0 || receipt.child_count > 4096 {
                return Err(ProtectedStorageMismatch::InvalidReceipt);
            }
            return Ok(ProtectedCommitDispositionV1::Replay);
        }
        let physical = physical.ok_or(ProtectedStorageMismatch::MissingPhysicalAck)?;
        self.check_pre_root(claim, current)?;
        if physical.inner_root != self.intent.storage.snapshot_root
            || physical.outer_root != self.outer_root
        {
            return Err(ProtectedStorageMismatch::PhysicalMismatch);
        }
        if physical.total_outer_bytes == 0 || physical.child_count > 4096 {
            return Err(ProtectedStorageMismatch::InvalidPhysical);
        }
        Ok(ProtectedCommitDispositionV1::Apply)
    }
}
