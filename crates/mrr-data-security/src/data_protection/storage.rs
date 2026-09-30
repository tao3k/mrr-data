//! Versioned Host projection of one physical storage effect.
//!
//! Cedar POO Spec owns the source and destination rule. The Host authenticates
//! these fields, evaluates policy and redeems approval. A CID authenticates
//! bytes, not sensitivity or destination authority.

/// Cedar `EntityUID` projected as separate type and id fields.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EntityRef<'a> {
    pub type_name: &'a str,
    pub id: &'a str,
}

impl EntityRef<'_> {
    const fn is_empty(self) -> bool {
        self.type_name.is_empty() || self.id.is_empty()
    }
}

/// Source custody projected from `CedarPooSpec.Data.DerivedArtifact`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SourceLabel<'a> {
    pub resource: EntityRef<'a>,
    pub owner: EntityRef<'a>,
    pub tenant: &'a str,
    pub restricted: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RawStorageTier {
    DurableLocal,
    Remote,
}

/// Destination identity and custody; the Host authenticates its configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RawStorageDestination<'a> {
    pub resource: EntityRef<'a>,
    pub tenant: &'a str,
    pub accepted_owners: &'a [EntityRef<'a>],
    pub accepts_restricted: bool,
    pub tier: RawStorageTier,
}

/// V1 of one exact raw-storage effect. `snapshot_root` is a parsed CID, whose
/// wire representation must be canonical lowercase `CIDv1` base32 text.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StorageEffectV1<'a> {
    pub operation_id: &'a str,
    pub subject: EntityRef<'a>,
    pub purpose: &'a str,
    pub snapshot_root: &'a cid::Cid,
    pub sources: &'a [SourceLabel<'a>],
    pub destination: RawStorageDestination<'a>,
    pub policy_root: &'a str,
    pub lineage_revision: &'a str,
}

/// A Host-authenticated, action-specific decision projection.
#[derive(Clone, Copy, Debug)]
pub struct StorageClaimV1<'a> {
    pub effect: StorageEffectV1<'a>,
    pub epoch: u64,
    pub expires_at: u64,
    pub allowed: bool,
}

/// Current Host-authenticated state at the effect boundary.
#[derive(Clone, Copy, Debug)]
pub struct CurrentStorageStateV1<'a> {
    pub policy_root: &'a str,
    pub lineage_revision: &'a str,
    pub epoch: u64,
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

impl StorageEffectV1<'_> {
    /// Compare Host-authenticated scope and current governance, then refuse
    /// every restricted raw source. This is not Cedar evaluation or approval
    /// redemption.
    /// # Errors
    /// Returns the first mismatched or unsafe projected condition.
    pub fn check_raw(
        &self,
        claim: &StorageClaimV1<'_>,
        current: CurrentStorageStateV1<'_>,
    ) -> Result<(), RawStorageMismatch> {
        if self.operation_id.is_empty()
            || self.subject.is_empty()
            || self.purpose.is_empty()
            || self.policy_root.is_empty()
            || self.lineage_revision.is_empty()
            || self.destination.resource.is_empty()
            || self.destination.tenant.is_empty()
            || self.sources.is_empty()
            || self.destination.accepted_owners.is_empty()
            || self
                .destination
                .accepted_owners
                .iter()
                .any(|owner| owner.is_empty())
            || self.sources.iter().any(|source| {
                source.resource.is_empty() || source.owner.is_empty() || source.tenant.is_empty()
            })
        {
            return Err(RawStorageMismatch::EmptyScope);
        }
        if self.sources.len() > 64 || self.destination.accepted_owners.len() > 64 {
            return Err(RawStorageMismatch::TooManyLabels);
        }
        if current.policy_root != self.policy_root
            || current.lineage_revision != self.lineage_revision
            || current.epoch != claim.epoch
            || current.now >= claim.expires_at
        {
            return Err(RawStorageMismatch::Stale);
        }
        if *self != claim.effect {
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
    pub snapshot: &'a mrr_data_core::SnapshotBlock,
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
/// This wrapper checks once before transfer. The Host must keep governance
/// stable across the call or use a future pre-root commit protocol; the generic
/// publisher has no reauthorization hook before root upload. Generic content
/// transports remain label-blind and must not authorize sensitive I/O.
/// # Errors
/// Returns a selection error before remote I/O, or a transfer error afterward.
#[cfg(feature = "raw-publish")]
pub async fn publish_raw_snapshot(
    effect: StorageEffectV1<'_>,
    claim: &StorageClaimV1<'_>,
    current: CurrentStorageStateV1<'_>,
    transfer: RawSnapshotPublish<'_>,
) -> Result<mrr_data_content::SnapshotPublication, RawSnapshotPublishError> {
    if effect.destination.tier != RawStorageTier::Remote {
        return Err(RawSnapshotPublishError::Selection(
            RawStorageMismatch::DestinationTier,
        ));
    }
    if effect.snapshot_root != transfer.snapshot.cid() {
        return Err(RawSnapshotPublishError::Selection(
            RawStorageMismatch::DifferentEffect,
        ));
    }
    effect
        .check_raw(claim, current)
        .map_err(RawSnapshotPublishError::Selection)?;
    transfer
        .session
        .publish_snapshot(
            transfer.local,
            transfer.remote,
            transfer.snapshot,
            transfer.relations,
            transfer.entities,
            transfer.limits,
        )
        .await
        .map_err(RawSnapshotPublishError::Transfer)
}
