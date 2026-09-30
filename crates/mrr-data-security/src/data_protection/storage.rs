//! Host-projected source labels and a fail-closed raw-storage admission gate.
//!
//! A CID authenticates bytes, not sensitivity or destination authority. The
//! Host authenticates these projections and redeems stateful approval. This
//! gate refuses restricted raw bytes until a protected envelope exists.

use mrr_data_core::SnapshotBlock;

/// Source custody projected from the pinned Cedar POO `DerivedArtifact` rule.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SourceLabel<'a> {
    pub resource: &'a str,
    pub owner: &'a str,
    pub tenant: &'a str,
    pub restricted: bool,
}

/// Physical destination class; the Host authenticates its actual configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RawStorageTier {
    DurableLocal,
    Remote,
}

/// Destination custody and sensitivity acceptance projected by the Host.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RawStorageDestination<'a> {
    pub name: &'a str,
    pub tenant: &'a str,
    pub accepted_owners: &'a [&'a str],
    pub tier: RawStorageTier,
}

/// One requested raw-byte effect on an exact immutable snapshot.
#[derive(Clone, Copy, Debug)]
pub struct RawStorageIntent<'a> {
    pub subject: &'a str,
    pub purpose: &'a str,
    pub snapshot: &'a SnapshotBlock,
    pub sources: &'a [SourceLabel<'a>],
    pub destination: RawStorageDestination<'a>,
}

/// A Host-authenticated, action-specific decision projection.
#[derive(Clone, Copy, Debug)]
pub struct RawStorageClaim<'a> {
    pub subject: &'a str,
    pub purpose: &'a str,
    pub root: &'a cid::Cid,
    pub sources: &'a [SourceLabel<'a>],
    pub destination: RawStorageDestination<'a>,
    pub policy_digest: &'a [u8; 32],
    pub governance_epoch: i64,
    pub expires_at: u64,
    pub allowed: bool,
}

/// Current Host-authenticated state at the effect boundary.
#[derive(Clone, Copy, Debug)]
pub struct CurrentStorageGovernance<'a> {
    pub policy_digest: &'a [u8; 32],
    pub epoch: i64,
    pub now: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RawStorageMismatch {
    EmptyScope,
    TooManyLabels,
    Stale,
    DifferentEffect,
    Denied,
    Tenant,
    Owner,
    RestrictedRequiresProtection,
    DestinationTier,
}

impl std::fmt::Display for RawStorageMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "raw storage admission: {self:?}")
    }
}

impl std::error::Error for RawStorageMismatch {}

impl RawStorageIntent<'_> {
    /// Compare Host-authenticated labels, destination and claim, then reject
    /// restricted bytes before any local cache admission or remote write.
    /// # Errors
    /// Returns the first mismatched or unsafe projected condition.
    pub fn check_claim(
        &self,
        claim: &RawStorageClaim<'_>,
        current: CurrentStorageGovernance<'_>,
    ) -> Result<(), RawStorageMismatch> {
        if self.subject.is_empty()
            || self.purpose.is_empty()
            || self.destination.name.is_empty()
            || self.destination.tenant.is_empty()
            || self.sources.is_empty()
            || self.destination.accepted_owners.is_empty()
            || self.sources.iter().any(|source| {
                source.resource.is_empty() || source.owner.is_empty() || source.tenant.is_empty()
            })
        {
            return Err(RawStorageMismatch::EmptyScope);
        }
        if self.sources.len() > 64 || self.destination.accepted_owners.len() > 64 {
            return Err(RawStorageMismatch::TooManyLabels);
        }
        if claim.policy_digest != current.policy_digest
            || claim.governance_epoch != current.epoch
            || current.now >= claim.expires_at
        {
            return Err(RawStorageMismatch::Stale);
        }
        if claim.subject != self.subject
            || claim.purpose != self.purpose
            || claim.root != self.snapshot.cid()
            || claim.sources != self.sources
            || claim.destination != self.destination
        {
            return Err(RawStorageMismatch::DifferentEffect);
        }
        if !claim.allowed {
            return Err(RawStorageMismatch::Denied);
        }
        for source in self.sources {
            if source.tenant != self.destination.tenant {
                return Err(RawStorageMismatch::Tenant);
            }
            if !self.destination.accepted_owners.contains(&source.owner) {
                return Err(RawStorageMismatch::Owner);
            }
            if source.restricted {
                return Err(RawStorageMismatch::RestrictedRequiresProtection);
            }
        }
        Ok(())
    }
}

/// Transfer inputs kept separate from the governance projection.
#[cfg(feature = "raw-publish")]
pub struct RawSnapshotPublish<'a> {
    pub local: &'a dyn mrr_data_content::AsyncContentStore,
    pub remote: &'a dyn mrr_data_content::RemoteContentStore,
    pub session: &'a mrr_data_content::TransferSession,
    pub relations: &'a meta_relational_reasoning::RelationCatalog,
    pub entities: &'a meta_relational_reasoning::EntityCatalog,
    pub limits: mrr_data_content::SnapshotTransferLimits,
}

#[cfg(feature = "raw-publish")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RawSnapshotPublishError {
    Selection(RawStorageMismatch),
    Transfer(mrr_data_content::SnapshotTransferError),
}

#[cfg(feature = "raw-publish")]
impl std::fmt::Display for RawSnapshotPublishError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "raw snapshot publication: {self:?}")
    }
}

#[cfg(feature = "raw-publish")]
impl std::error::Error for RawSnapshotPublishError {}

/// Publish an unrestricted raw snapshot only after exact current admission.
/// The Host authenticates the inputs and rechecks current state at effect
/// commit; this function does not run Cedar or encrypt bytes. The underlying
/// generic content API remains a transport and must not be used as a sensitive
/// data authorization path.
/// # Errors
/// Returns a selection error before remote I/O, or a transfer error afterward.
#[cfg(feature = "raw-publish")]
pub async fn publish_raw_snapshot(
    intent: RawStorageIntent<'_>,
    claim: &RawStorageClaim<'_>,
    current: CurrentStorageGovernance<'_>,
    transfer: RawSnapshotPublish<'_>,
) -> Result<mrr_data_content::SnapshotPublication, RawSnapshotPublishError> {
    if intent.destination.tier != RawStorageTier::Remote {
        return Err(RawSnapshotPublishError::Selection(
            RawStorageMismatch::DestinationTier,
        ));
    }
    intent
        .check_claim(claim, current)
        .map_err(RawSnapshotPublishError::Selection)?;
    transfer
        .session
        .publish_snapshot(
            transfer.local,
            transfer.remote,
            intent.snapshot,
            transfer.relations,
            transfer.entities,
            transfer.limits,
        )
        .await
        .map_err(RawSnapshotPublishError::Transfer)
}
