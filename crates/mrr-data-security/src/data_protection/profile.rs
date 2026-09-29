//! Provider-neutral cloud data-protection profile and release selection.
//!
//! This mirrors the public Cedar POO Cloud `DataProtection` example's two
//! decisions and the Cloud Pipeline release receipt shape. Values are
//! Host-projected evidence, never policy decisions made by MRR Data.

use mrr_data_core::{SnapshotBlock, SnapshotOperationBinding};

/// The exact release fields in Cedar POO Cloud Pipeline Evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReleaseReceiptClaim<'a> {
    pub artifact_digest: &'a str,
    pub source_commit: &'a str,
    pub policy_root: &'a str,
    /// State epoch, not a wall-clock timestamp.
    pub epoch: i64,
}

/// Two Host-authenticated Cedar decisions bound to one selected release.
///
/// The Host obtains both decisions from the same compiled policy root and
/// authenticates their request identities. These fields prevent accidental
/// reuse of a decision pair for another dataset, artifact, or state epoch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DataProtectionDecisions<'a> {
    pub policy_root: &'a str,
    pub dataset: &'a str,
    pub artifact_digest: &'a str,
    pub epoch: i64,
    pub pipeline_release_allowed: bool,
    pub transformation_allowed: bool,
}

/// Current customer selection tied to an immutable MRR Data snapshot.
///
/// The profile describes which artifact implementation is allowed to perform
/// the data transformation. It does not select a provider or hold key bytes.
#[derive(Clone, Copy, Debug)]
pub struct DataProtectionProfile<'a> {
    source: SnapshotOperationBinding<'a>,
    dataset: &'a str,
    release: ReleaseReceiptClaim<'a>,
}

/// A required Cloud `DataProtection` relation does not hold.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DataProtectionMismatch {
    EmptySelection,
    ReleaseReceipt,
    DecisionScope,
    PipelineDenied,
    TransformationDenied,
}

impl<'a> DataProtectionProfile<'a> {
    #[must_use]
    pub const fn new(
        source: &'a SnapshotBlock,
        dataset: &'a str,
        release: ReleaseReceiptClaim<'a>,
    ) -> Self {
        Self {
            source: SnapshotOperationBinding::new(source),
            dataset,
            release,
        }
    }

    #[must_use]
    pub const fn source(&self) -> SnapshotOperationBinding<'a> {
        self.source
    }

    #[must_use]
    pub const fn dataset(&self) -> &'a str {
        self.dataset
    }

    #[must_use]
    pub const fn release(&self) -> ReleaseReceiptClaim<'a> {
        self.release
    }

    /// Check exact release identity, current state epoch, and both decisions.
    ///
    /// The Host authenticates the receipt and decisions and rechecks current
    /// state when committing the effect. This comparison is not Cedar
    /// evaluation or atomic receipt redemption.
    ///
    /// # Errors
    ///
    /// Returns [`DataProtectionMismatch`] for an incomplete or changed release.
    pub fn check(
        &self,
        authenticated_receipt: ReleaseReceiptClaim<'_>,
        current_epoch: i64,
        decisions: DataProtectionDecisions<'_>,
    ) -> Result<(), DataProtectionMismatch> {
        if self.dataset.is_empty()
            || self.release.artifact_digest.is_empty()
            || self.release.source_commit.is_empty()
            || self.release.policy_root.is_empty()
        {
            return Err(DataProtectionMismatch::EmptySelection);
        }
        if self.release != authenticated_receipt || self.release.epoch != current_epoch {
            return Err(DataProtectionMismatch::ReleaseReceipt);
        }
        if decisions.policy_root != self.release.policy_root
            || decisions.dataset != self.dataset
            || decisions.artifact_digest != self.release.artifact_digest
            || decisions.epoch != current_epoch
        {
            return Err(DataProtectionMismatch::DecisionScope);
        }
        if !decisions.pipeline_release_allowed {
            return Err(DataProtectionMismatch::PipelineDenied);
        }
        if !decisions.transformation_allowed {
            return Err(DataProtectionMismatch::TransformationDenied);
        }
        Ok(())
    }
}
