//! Bind a Google SDP AES-SIV request to an MRR Data selected input.

use cedar_poo_bridge::google_sdp::{SelectedTabularInput, TabularAesSiv, WrappedKeyBinding};
use sha2::{Digest, Sha256};

use crate::{
    ClaimMismatch, CurrentGovernance, Mode, TokenAction, TokenAuthorizationClaim,
    TokenAuthorizationRequest,
};

/// The provider selection differs from the authenticated snapshot selection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GoogleSelectionMismatch {
    Claim(ClaimMismatch),
    WrongAction,
    Dataset,
    Field,
    Context,
    ValueDigest,
    Profile,
    ProviderConfiguration,
}

/// Construct the Google SDP wire plan after an exact claim and selected-input
/// match. The Host still authenticates the claim, selected bytes, and key,
/// runs its policy engine, and controls provider I/O.
///
/// # Errors
///
/// Returns [`GoogleSelectionMismatch`] for a substituted selection or an
/// invalid Google SDP configuration.
pub fn prepare_google_aes_siv_deidentify(
    request: &TokenAuthorizationRequest<'_>,
    claim: &TokenAuthorizationClaim<'_>,
    current: CurrentGovernance<'_>,
    selected: SelectedTabularInput,
    parent: String,
    key: WrappedKeyBinding,
) -> Result<TabularAesSiv, GoogleSelectionMismatch> {
    if request.action != TokenAction::Deidentify {
        return Err(GoogleSelectionMismatch::WrongAction);
    }
    request
        .check_claim(claim, current.policy_digest, current.epoch, current.now)
        .map_err(GoogleSelectionMismatch::Claim)?;
    let bound = request.input;
    if selected.dataset != request.dataset {
        return Err(GoogleSelectionMismatch::Dataset);
    }
    if selected.value_field != bound.field() {
        return Err(GoogleSelectionMismatch::Field);
    }
    if selected.context != bound.context() {
        return Err(GoogleSelectionMismatch::Context);
    }
    if Sha256::digest(selected.value.as_bytes()).as_slice() != bound.value_digest() {
        return Err(GoogleSelectionMismatch::ValueDigest);
    }
    let profile = selected.token_profile(bound.profile().lineage.tenant, bound.profile().scope);
    if bound.profile().mode != Mode::AesSiv || &profile != bound.profile() {
        return Err(GoogleSelectionMismatch::Profile);
    }
    TabularAesSiv::from_selected(parent, selected, key)
        .map_err(|_| GoogleSelectionMismatch::ProviderConfiguration)
}
