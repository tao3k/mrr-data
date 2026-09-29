//! Snapshot and selected-value binding for an external pseudonymization
//! profile. This module does not implement encryption or authorize effects.

use mrr_data_core::{SnapshotBlock, SnapshotOperationBinding};

/// A selected input anchored to one immutable mrr-data snapshot.
///
/// `Profile` is owned by the consumer. It can be a Lean/Cedar-derived token
/// profile, a local provider recipe, or another application-defined type.
/// Keeping it generic avoids a policy-language dependency in the data-protection crate.
#[derive(Clone, Copy, Debug)]
pub struct PseudonymizationInputBinding<'a, Profile> {
    source: SnapshotOperationBinding<'a>,
    field: &'a str,
    value_digest: &'a [u8; 32],
    context: &'a str,
    profile: Profile,
}

impl<'a, Profile> PseudonymizationInputBinding<'a, Profile> {
    /// Borrows existing identities without serializing, hashing, or copying
    /// the selected value. The Host must authenticate the actual value,
    /// context, profile, and relationship to the snapshot.
    #[must_use]
    pub const fn new(
        source: &'a SnapshotBlock,
        field: &'a str,
        value_digest: &'a [u8; 32],
        context: &'a str,
        profile: Profile,
    ) -> Self {
        Self {
            source: SnapshotOperationBinding::new(source),
            field,
            value_digest,
            context,
            profile,
        }
    }

    #[must_use]
    pub const fn source(&self) -> SnapshotOperationBinding<'a> {
        self.source
    }

    #[must_use]
    pub const fn field(&self) -> &'a str {
        self.field
    }

    #[must_use]
    pub const fn value_digest(&self) -> &'a [u8; 32] {
        self.value_digest
    }

    #[must_use]
    pub const fn context(&self) -> &'a str {
        self.context
    }

    #[must_use]
    pub const fn profile(&self) -> &Profile {
        &self.profile
    }
}
