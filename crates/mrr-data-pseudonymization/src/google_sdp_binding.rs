//! Bind a Google SDP AES-SIV request to an MRR Data selected input.

use cedar_poo_bridge::google_sdp::{
    CheckedTableOutput, GoogleSdpRequest, GoogleSdpResponse, SelectedTabularInput, TabularAesSiv,
    WrappedKeyBinding,
};
use cid::Cid;
use meta_relational_reasoning::RelationId;
use mrr_data_security::data_protection::{
    DataProtectionDecisions, DataProtectionMismatch, DataProtectionProfile, ReleaseReceiptClaim,
};
use sha2::{Digest, Sha256};

use crate::{
    ClaimMismatch, CurrentGovernance, Mode, TokenAction, TokenAuthorizationClaim,
    TokenAuthorizationRequest, TokenInputBinding, TokenProfile,
};

/// The provider selection differs from the authenticated snapshot selection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GoogleSelectionMismatch {
    Claim(ClaimMismatch),
    DataProtection(DataProtectionMismatch),
    CloudGate(CloudGateMismatch),
    GovernanceEpoch,
    RowUnbound,
    Source,
    WrongAction,
    Dataset,
    Field,
    Context,
    ValueDigest,
    Profile,
    ProviderConfiguration,
}

/// Cloud release identity from the public Cedar POO Pipeline evidence shape.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CloudReleaseIdentity {
    pub artifact_digest: String,
    pub source_commit: String,
    pub policy_root: String,
    /// State epoch, not a wall-clock timestamp.
    pub epoch: i64,
}

/// The specific Cloud pseudonymization veto relation in Cedar POO.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CloudGateMismatch {
    Mode,
    Recipe,
    Tenant,
    KeyNotAuthorized,
    Context,
    ArtifactDigest,
}

/// Host-authenticated projection of the target dataset and key grant.
///
/// The target profile comes from the selected Cedar dataset entity; the
/// selected input comes from the exact MRR Data snapshot. Key authorization
/// remains a Host fact, not proof of key possession or Cedar evaluation.
#[derive(Clone, Copy, Debug)]
pub struct CloudPseudonymizationGate<'a> {
    pub target_profile: TokenProfile<'a>,
    pub admitted_context: &'a str,
    pub artifact_digest: &'a str,
    pub key_authorized: bool,
}

impl CloudPseudonymizationGate<'_> {
    /// Mirror the public Cedar POO `pseudonymizationReady` relation.
    ///
    /// # Errors
    ///
    /// Returns the first mismatched projected gate fact.
    pub fn check(
        &self,
        selected: &TokenInputBinding<'_>,
        released_artifact_digest: &str,
    ) -> Result<(), CloudGateMismatch> {
        if self.target_profile.mode != Mode::AesSiv {
            return Err(CloudGateMismatch::Mode);
        }
        if !self.target_profile.same_recipe(selected.profile()) {
            return Err(CloudGateMismatch::Recipe);
        }
        if self.target_profile.lineage.tenant != selected.profile().lineage.tenant {
            return Err(CloudGateMismatch::Tenant);
        }
        if !self.key_authorized {
            return Err(CloudGateMismatch::KeyNotAuthorized);
        }
        if selected.context() != self.admitted_context {
            return Err(CloudGateMismatch::Context);
        }
        if released_artifact_digest != self.artifact_digest {
            return Err(CloudGateMismatch::ArtifactDigest);
        }
        Ok(())
    }
}

/// Host-authenticated Cloud `DataProtection` selection for both Cedar decisions.
#[derive(Clone, Copy, Debug)]
pub struct CloudDataProtectionSelection<'a> {
    pub profile: &'a DataProtectionProfile<'a>,
    pub receipt: ReleaseReceiptClaim<'a>,
    pub current_epoch: i64,
    pub decisions: DataProtectionDecisions<'a>,
    pub gate: CloudPseudonymizationGate<'a>,
}

/// Owned location of the selected row within an MRR Data snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CloudRowIdentity {
    pub relation_id: RelationId,
    pub child_cid: Cid,
    /// Ordinal within the selected Arrow child, not the whole relation.
    pub row_index: u64,
}

/// Physical and governance identity retained across provider I/O.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GoogleBoundIdentity {
    root: Cid,
    dataset: String,
    input_digest: [u8; 32],
    policy_digest: [u8; 32],
    governance_epoch: i64,
    row: Option<CloudRowIdentity>,
    cloud_release: Option<CloudReleaseIdentity>,
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
    pub const fn governance_epoch(&self) -> i64 {
        self.governance_epoch
    }

    #[must_use]
    pub const fn cloud_release(&self) -> Option<&CloudReleaseIdentity> {
        self.cloud_release.as_ref()
    }

    #[must_use]
    pub const fn row(&self) -> Option<&CloudRowIdentity> {
        self.row.as_ref()
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
            row: None,
            cloud_release: None,
        },
        plan,
    })
}

/// Require the Cloud `DataProtection` release profile and both Cedar decisions
/// before constructing the selected Google request. All evidence must first
/// be authenticated by the Host; this function only checks exact relations.
///
/// # Errors
///
/// Returns [`GoogleSelectionMismatch`] for release, source, or row drift.
pub fn prepare_cloud_google_aes_siv_deidentify(
    cloud: CloudDataProtectionSelection<'_>,
    request: &TokenAuthorizationRequest<'_>,
    claim: &TokenAuthorizationClaim<'_>,
    current: CurrentGovernance<'_>,
    selected: SelectedTabularInput,
    parent: String,
    key: WrappedKeyBinding,
) -> Result<BoundGoogleDeidentifyPlan, GoogleSelectionMismatch> {
    if cloud.current_epoch != current.epoch {
        return Err(GoogleSelectionMismatch::GovernanceEpoch);
    }
    cloud
        .profile
        .check(cloud.receipt, cloud.current_epoch, cloud.decisions)
        .map_err(GoogleSelectionMismatch::DataProtection)?;
    if cloud.profile.source().root() != request.input.source().root() {
        return Err(GoogleSelectionMismatch::Source);
    }
    if cloud.profile.dataset() != request.dataset {
        return Err(GoogleSelectionMismatch::Dataset);
    }
    let row = request
        .input
        .row()
        .ok_or(GoogleSelectionMismatch::RowUnbound)?;
    cloud
        .gate
        .check(request.input, cloud.profile.release().artifact_digest)
        .map_err(GoogleSelectionMismatch::CloudGate)?;
    let mut plan =
        prepare_google_aes_siv_deidentify(request, claim, current, selected, parent, key)?;
    let release = cloud.profile.release();
    plan.identity.row = Some(CloudRowIdentity {
        relation_id: row.relation_id(),
        child_cid: *row.child_cid(),
        row_index: row.row_index(),
    });
    plan.identity.cloud_release = Some(CloudReleaseIdentity {
        artifact_digest: release.artifact_digest.to_owned(),
        source_commit: release.source_commit.to_owned(),
        policy_root: release.policy_root.to_owned(),
        epoch: release.epoch,
    });
    Ok(plan)
}
