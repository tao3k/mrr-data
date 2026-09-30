#[cfg(feature = "protected-envelope")]
mod envelope;
mod profile;
mod protected;
#[cfg(feature = "protected-publish")]
mod protected_snapshot;
mod pseudonymization;
mod storage;

#[cfg(feature = "protected-envelope")]
pub use envelope::{
    ProtectedBlockBindingV1, ProtectedBlockRole, ProtectedBlockV1, ProtectedEnvelopeError,
    ProtectedEnvelopeKey, open_block, seal_block,
};
pub use profile::{
    DataProtectionDecisions, DataProtectionMismatch, DataProtectionProfile, ReleaseReceiptClaim,
};
pub use protected::{
    ProtectedCommitDispositionV1, ProtectedCommitReceiptV1, ProtectedPhysicalAckV1,
    ProtectedPublicationV1, ProtectedStorageMismatch, ProtectionClaimV1, ProtectionIntentV1,
};
#[cfg(feature = "protected-publish")]
pub use protected_snapshot::{
    PreparedProtectedSnapshot, ProtectedPhysicalPublication, ProtectedPublish, ProtectedRestore,
    ProtectedSnapshotError, ProtectedSnapshotRecordV1, ProtectedStage, publish_prepared_snapshot,
    restore_protected_snapshot, stage_protected_snapshot,
};
pub use pseudonymization::PseudonymizationInputBinding;
pub use storage::{
    CurrentStorageStateV1, EntityRef, RawStorageDestination, RawStorageMismatch, RawStorageTier,
    SourceLabel, StorageClaimV1, StorageEffectV1,
};
#[cfg(feature = "raw-publish")]
pub use storage::{RawSnapshotPublish, RawSnapshotPublishError, publish_raw_snapshot};
