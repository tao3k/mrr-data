//! Bind a Google SDP AES-SIV request to an MRR Data selected input.

use cedar_poo_bridge::google_sdp::{
    CheckedTableOutput, GoogleSdpRequest, GoogleSdpResponse, SelectedTabularInput, TabularAesSiv,
    WrappedKeyBinding,
};
use cid::Cid;
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

/// Physical and governance identity retained across provider I/O.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GoogleBoundIdentity {
    root: Cid,
    dataset: String,
    input_digest: [u8; 32],
    policy_digest: [u8; 32],
    governance_epoch: u64,
}

impl GoogleBoundIdentity {
    #[must_use]
    pub const fn root(&self) -> &Cid {
        &self.root
    }

    #[must_use]
    pub fn dataset(&self) -> &str {
        &self.dataset
    }

    #[must_use]
    pub const fn input_digest(&self) -> &[u8; 32] {
        &self.input_digest
    }

    #[must_use]
    pub const fn policy_digest(&self) -> &[u8; 32] {
        &self.policy_digest
    }

    #[must_use]
    pub const fn governance_epoch(&self) -> u64 {
        self.governance_epoch
    }
}

/// A provider plan that retains its exact MRR Data and governance identity.
pub struct BoundGoogleDeidentifyPlan {
    identity: GoogleBoundIdentity,
    plan: TabularAesSiv,
}

impl BoundGoogleDeidentifyPlan {
    #[must_use]
    pub const fn identity(&self) -> &GoogleBoundIdentity {
        &self.identity
    }

    /// The Google SDP de-identification endpoint.
    ///
    /// # Errors
    ///
    /// Returns the bridge validation error for an invalid endpoint.
    pub fn endpoint(&self) -> Result<String, String> {
        self.plan.endpoint(false)
    }

    /// The Google SDP request body; the Host owns authenticated transport.
    ///
    /// # Errors
    ///
    /// Returns the bridge validation error for an invalid request.
    pub fn deidentify_body(&self) -> Result<GoogleSdpRequest, String> {
        self.plan.deidentify_body()
    }

    /// Parse a Host-supplied response under the bridge's table contract and
    /// retain the selected source and governance identity with the result.
    ///
    /// # Errors
    ///
    /// Returns the bridge validation error for a malformed transformation.
    pub fn check_response(
        self,
        response: &GoogleSdpResponse,
    ) -> Result<BoundGoogleDeidentifyOutput, String> {
        let checked = self.plan.check_deidentify_response(response)?;
        Ok(BoundGoogleDeidentifyOutput {
            identity: self.identity,
            checked,
        })
    }
}

/// A structurally checked output with its source and governance identity.
/// The Host still authenticates transport and controls persistence or release.
pub struct BoundGoogleDeidentifyOutput {
    identity: GoogleBoundIdentity,
    checked: CheckedTableOutput,
}

impl BoundGoogleDeidentifyOutput {
    #[must_use]
    pub const fn identity(&self) -> &GoogleBoundIdentity {
        &self.identity
    }

    /// The token is sensitive and must not be logged as a diagnostic.
    #[must_use]
    pub fn token(&self) -> &str {
        &self.checked.value
    }

    #[must_use]
    pub fn token_digest(&self) -> [u8; 32] {
        Sha256::digest(self.checked.value.as_bytes()).into()
    }

    #[must_use]
    pub fn request_sha256(&self) -> &str {
        &self.checked.request_sha256
    }

    #[must_use]
    pub fn response_sha256(&self) -> &str {
        &self.checked.response_sha256
    }
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
) -> Result<BoundGoogleDeidentifyPlan, GoogleSelectionMismatch> {
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
    let plan = TabularAesSiv::from_selected(parent, selected, key)
        .map_err(|_| GoogleSelectionMismatch::ProviderConfiguration)?;
    Ok(BoundGoogleDeidentifyPlan {
        identity: GoogleBoundIdentity {
            root: bound.source().root().to_owned(),
            dataset: request.dataset.to_owned(),
            input_digest: *bound.value_digest(),
            policy_digest: *current.policy_digest,
            governance_epoch: current.epoch,
        },
        plan,
    })
}
