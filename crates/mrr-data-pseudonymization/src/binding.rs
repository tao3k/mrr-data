//! Snapshot, selected input, and token-profile composition.

use cedar_poo_bridge::pseudonymization::TokenProfile;
use mrr_data_core::{SnapshotBlock, SnapshotRowBinding};
use mrr_data_security::data_protection::PseudonymizationInputBinding;
use std::collections::HashMap;

/// A selected value anchored to a snapshot and a Cedar POO token profile.
pub type TokenInputBinding<'a> = PseudonymizationInputBinding<'a, TokenProfile<'a>>;

/// One Host-authenticated selection to bind to a snapshot.
/// The fields are declarations; the Host verifies the actual value and key.
#[derive(Clone, Copy, Debug)]
pub struct SelectedTokenInput<'a> {
    pub field: &'a str,
    pub value_digest: &'a [u8; 32],
    pub context: &'a str,
    pub profile: TokenProfile<'a>,
}

impl<'a> SelectedTokenInput<'a> {
    /// Bind the selected value to a checked relation child and row ordinal.
    #[must_use]
    pub const fn bind_to_row(self, row: SnapshotRowBinding<'a>) -> TokenInputBinding<'a> {
        PseudonymizationInputBinding::at_row(
            row,
            self.field,
            self.value_digest,
            self.context,
            self.profile,
        )
    }

    /// Bind the selected value and common token profile to an immutable
    /// snapshot. This does not encrypt or authorize release.
    #[must_use]
    pub const fn bind_to(self, snapshot: &'a SnapshotBlock) -> TokenInputBinding<'a> {
        PseudonymizationInputBinding::new(
            snapshot,
            self.field,
            self.value_digest,
            self.context,
            self.profile,
        )
    }
}

/// Mirror the Lean AES-SIV table compatibility rule: token recipe and actual
/// per-record context must agree. Tenant authorization is a separate control.
#[must_use]
pub fn compatible_inputs(left: &TokenInputBinding<'_>, right: &TokenInputBinding<'_>) -> bool {
    left.profile().same_recipe(right.profile()) && left.context() == right.context()
}

/// Reject an HMAC catalog that declares the same data-key recipe in distinct
/// scopes. This startup check uses a borrowed-key index with expected linear
/// work and remains outside the per-operation projection hot path.
#[must_use]
pub fn hmac_catalog_separated(profiles: &[TokenProfile<'_>]) -> bool {
    let mut scopes = HashMap::with_capacity(profiles.len());
    for profile in profiles {
        if profile.mode != cedar_poo_bridge::pseudonymization::Mode::HmacSha256 {
            continue;
        }
        let key = (
            profile.lineage.key_domain,
            profile.lineage.token_key_version,
            profile.lineage.transform_version,
        );
        if let Some(previous_scope) = scopes.insert(key, profile.scope)
            && previous_scope != profile.scope
        {
            return false;
        }
    }
    true
}
