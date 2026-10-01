//! Bind a Google SDP AES-SIV request to an MRR Data selected input.

use cedar_poo_bridge::google_sdp::{
    CheckedTableOutput, GoogleSdpRequest, GoogleSdpResponse, SelectedTabularInput, TabularAesSiv,
    TabularAesSivBatch, WrappedKeyBinding,
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
    PriorOutput,
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

/// Owned authorization scope retained when independently admitted rows are
/// later combined into one provider request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GoogleAuthorizationScope {
    subject: String,
    purpose: String,
    token_scope: String,
    tenant: String,
    key_domain: String,
    token_key_version: String,
    transform_version: String,
    wrapping_version: String,
    observed_at: u64,
}

impl GoogleAuthorizationScope {
    fn from_request(request: &TokenAuthorizationRequest<'_>, observed_at: u64) -> Self {
        let profile = request.input.profile();
        Self {
            subject: request.subject.to_owned(),
            purpose: request.purpose.to_owned(),
            token_scope: profile.scope.to_owned(),
            tenant: profile.lineage.tenant.to_owned(),
            key_domain: profile.lineage.key_domain.to_owned(),
            token_key_version: profile.lineage.token_key_version.to_owned(),
            transform_version: profile.lineage.transform_version.to_owned(),
            wrapping_version: profile.lineage.wrapping_version.to_owned(),
            observed_at,
        }
    }

    #[must_use]
    pub fn subject(&self) -> &str {
        &self.subject
    }

    #[must_use]
    pub fn purpose(&self) -> &str {
        &self.purpose
    }

    #[must_use]
    pub fn tenant(&self) -> &str {
        &self.tenant
    }

    #[must_use]
    pub const fn observed_at(&self) -> u64 {
        self.observed_at
    }
}

/// Physical and governance identity retained across provider I/O.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GoogleBoundIdentity {
    root: Cid,
    dataset: String,
    input_digest: [u8; 32],
    policy_digest: [u8; 32],
    governance_epoch: i64,
    authorization_scope: GoogleAuthorizationScope,
    claim_expires_at: u64,
    row: Option<CloudRowIdentity>,
    cloud_release: Option<CloudReleaseIdentity>,
}

impl GoogleBoundIdentity {
    fn check_current(&self, current: CurrentGovernance<'_>) -> Result<(), String> {
        if *current.policy_digest != self.policy_digest
            || current.epoch != self.governance_epoch
            || current.now < self.authorization_scope.observed_at
            || current.now >= self.claim_expires_at
        {
            return Err("Google plan authorization is stale".to_owned());
        }
        Ok(())
    }

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
    pub const fn authorization_scope(&self) -> &GoogleAuthorizationScope {
        &self.authorization_scope
    }

    #[must_use]
    pub const fn claim_expires_at(&self) -> u64 {
        self.claim_expires_at
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
    pub fn deidentify_body(
        &self,
        current: CurrentGovernance<'_>,
    ) -> Result<GoogleSdpRequest, String> {
        self.identity.check_current(current)?;
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
        current: CurrentGovernance<'_>,
    ) -> Result<BoundGoogleDeidentifyOutput, String> {
        self.identity.check_current(current)?;
        let checked = self.plan.check_deidentify_response(response)?;
        Ok(BoundGoogleDeidentifyOutput {
            identity: self.identity,
            checked,
        })
    }
}

/// A combined Table request over one ordered, independently authorized child.
/// Generated response markers bind provider positions to retained MRR rows.
pub struct BoundGoogleDeidentifyBatchPlan {
    identities: Vec<GoogleBoundIdentity>,
    batch: TabularAesSivBatch,
}

/// A set of row plans cannot be sent as one bounded Table request.
#[derive(Debug)]
pub enum GoogleBatchWireMismatch {
    Empty,
    MixedScope,
    RowUnbound,
    OrdinalOrder,
    Stale,
    Bridge(String),
}

impl BoundGoogleDeidentifyBatchPlan {
    /// Consume ordered plans only when every row retains one authorization
    /// scope, observation time, source and physical Arrow child. This also
    /// checks plans produced separately from the bulk Arrow planner.
    /// # Errors
    /// Rejects a missing row, changed scope, ordering or provider recipe.
    pub fn from_plans(
        plans: Vec<BoundGoogleDeidentifyPlan>,
    ) -> Result<Self, GoogleBatchWireMismatch> {
        let first = plans.first().ok_or(GoogleBatchWireMismatch::Empty)?;
        let first_identity = &first.identity;
        let first_row = first_identity
            .row
            .as_ref()
            .ok_or(GoogleBatchWireMismatch::RowUnbound)?;
        let mut previous = None;
        for plan in &plans {
            let identity = &plan.identity;
            if identity.root != first_identity.root
                || identity.dataset != first_identity.dataset
                || identity.policy_digest != first_identity.policy_digest
                || identity.governance_epoch != first_identity.governance_epoch
                || identity.authorization_scope != first_identity.authorization_scope
                || identity.cloud_release != first_identity.cloud_release
            {
                return Err(GoogleBatchWireMismatch::MixedScope);
            }
            let row = identity
                .row
                .as_ref()
                .ok_or(GoogleBatchWireMismatch::RowUnbound)?;
            if row.relation_id != first_row.relation_id || row.child_cid != first_row.child_cid {
                return Err(GoogleBatchWireMismatch::MixedScope);
            }
            if previous.is_some_and(|ordinal| ordinal >= row.row_index) {
                return Err(GoogleBatchWireMismatch::OrdinalOrder);
            }
            previous = Some(row.row_index);
        }
        let (identities, rows): (Vec<_>, Vec<_>) = plans
            .into_iter()
            .map(|plan| (plan.identity, plan.plan))
            .unzip();
        let batch = TabularAesSivBatch::new(rows).map_err(GoogleBatchWireMismatch::Bridge)?;
        Ok(Self { identities, batch })
    }

    /// The exact de-identification endpoint for the combined request.
    /// # Errors
    /// Returns bridge validation failures.
    pub fn endpoint(&self) -> Result<String, String> {
        self.batch.endpoint()
    }

    /// Recheck all retained claim expiries and the common policy projection
    /// against a newly obtained Host state before dispatch or local output.
    /// # Errors
    /// Rejects policy drift, clock rollback, or any expired row claim.
    pub fn check_current(
        &self,
        current: CurrentGovernance<'_>,
    ) -> Result<(), GoogleBatchWireMismatch> {
        if self
            .identities
            .iter()
            .any(|identity| identity.check_current(current).is_err())
        {
            return Err(GoogleBatchWireMismatch::Stale);
        }
        Ok(())
    }

    /// One Google Table request containing the ordered selected rows. The Host
    /// supplies a freshly authenticated current projection before dispatch.
    /// # Errors
    /// Rejects stale authorization projections or bridge validation failures.
    pub fn deidentify_body(
        &self,
        current: CurrentGovernance<'_>,
    ) -> Result<GoogleSdpRequest, GoogleBatchWireMismatch> {
        self.check_current(current)?;
        self.batch
            .deidentify_body()
            .map_err(GoogleBatchWireMismatch::Bridge)
    }

    /// Retain no checked row output if authority expired, policy drifted, or
    /// any row or aggregate provider summary is malformed. The Host obtains
    /// current state again and authenticates and bounds response bytes.
    /// # Errors
    /// Returns a bridge shape, context, token or summary mismatch.
    pub fn check_response(
        self,
        response: &GoogleSdpResponse,
        current: CurrentGovernance<'_>,
    ) -> Result<Vec<BoundGoogleDeidentifyOutput>, GoogleBatchWireMismatch> {
        self.check_current(current)?;
        let checked = self
            .batch
            .check_deidentify_response(response)
            .map_err(GoogleBatchWireMismatch::Bridge)?;
        Ok(self
            .identities
            .into_iter()
            .zip(checked)
            .map(|(identity, checked)| BoundGoogleDeidentifyOutput { identity, checked })
            .collect())
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

/// A separate re-identification effect bound to a checked token and original row.
pub struct BoundGoogleReidentifyPlan {
    identity: GoogleBoundIdentity,
    plan: TabularAesSiv,
    token: String,
    token_digest: [u8; 32],
    deidentify_response_sha256: String,
}

impl BoundGoogleReidentifyPlan {
    #[must_use]
    pub const fn identity(&self) -> &GoogleBoundIdentity {
        &self.identity
    }

    /// # Errors
    /// Returns a bridge error for an invalid endpoint.
    pub fn endpoint(&self) -> Result<String, String> {
        self.plan.endpoint(true)
    }

    /// The Host sends this body through authenticated transport.
    /// # Errors
    /// Returns a bridge error for an invalid token or request.
    pub fn reidentify_body(
        &self,
        current: CurrentGovernance<'_>,
    ) -> Result<GoogleSdpRequest, String> {
        self.identity.check_current(current)?;
        self.plan.reidentify_body(&self.token)
    }

    /// Check the response against the selected original plaintext.
    /// # Errors
    /// Returns a bridge error for a malformed or mismatched response.
    pub fn check_response(
        self,
        response: &GoogleSdpResponse,
        current: CurrentGovernance<'_>,
    ) -> Result<BoundGoogleReidentifyOutput, String> {
        self.identity.check_current(current)?;
        let checked = self.plan.check_reidentify_response(&self.token, response)?;
        Ok(BoundGoogleReidentifyOutput {
            identity: self.identity,
            checked,
            token_digest: self.token_digest,
            deidentify_response_sha256: self.deidentify_response_sha256,
        })
    }
}

/// Checked plaintext and the source token's provenance digests.
/// The Host controls plaintext release and authenticates both provider responses.
pub struct BoundGoogleReidentifyOutput {
    identity: GoogleBoundIdentity,
    checked: CheckedTableOutput,
    token_digest: [u8; 32],
    deidentify_response_sha256: String,
}

impl BoundGoogleReidentifyOutput {
    #[must_use]
    pub const fn identity(&self) -> &GoogleBoundIdentity {
        &self.identity
    }

    /// Sensitive plaintext; the Host must enforce release and audit.
    #[must_use]
    pub fn value(&self) -> &str {
        &self.checked.value
    }

    #[must_use]
    pub const fn token_digest(&self) -> &[u8; 32] {
        &self.token_digest
    }

    #[must_use]
    pub fn deidentify_response_sha256(&self) -> &str {
        &self.deidentify_response_sha256
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

/// Prepare a separately authorized re-identification of a checked token.
/// The Host authenticates the prior output, original row, current claim, key,
/// provider transport, and the plaintext release decision.
/// # Errors
/// Returns a selection mismatch if the current effect, original input, or
/// reconstructed provider recipe differs from the token's checked origin.
pub fn prepare_google_aes_siv_reidentify(
    request: &TokenAuthorizationRequest<'_>,
    claim: &TokenAuthorizationClaim<'_>,
    current: CurrentGovernance<'_>,
    selected: SelectedTabularInput,
    parent: String,
    key: WrappedKeyBinding,
    prior: &BoundGoogleDeidentifyOutput,
) -> Result<BoundGoogleReidentifyPlan, GoogleSelectionMismatch> {
    let bound = prepare_google_aes_siv(
        request,
        claim,
        current,
        selected,
        parent,
        key,
        TokenAction::Reidentify,
    )?;
    let prior_identity = prior.identity();
    if prior_identity.root != bound.identity.root
        || prior_identity.dataset != bound.identity.dataset
        || prior_identity.input_digest != bound.identity.input_digest
        || prior_identity.row.is_some()
        || prior_identity.cloud_release.is_some()
        || request.input.row().is_some()
    {
        return Err(GoogleSelectionMismatch::PriorOutput);
    }
    let deidentify_bytes = bound
        .plan
        .deidentify_body()
        .and_then(|body| body.to_json_bytes())
        .map_err(|_| GoogleSelectionMismatch::ProviderConfiguration)?;
    let deidentify_digest = Sha256::digest(&deidentify_bytes);
    if format!("{deidentify_digest:x}") != prior.request_sha256() {
        return Err(GoogleSelectionMismatch::PriorOutput);
    }
    bound
        .plan
        .reidentify_body(prior.token())
        .map_err(|_| GoogleSelectionMismatch::PriorOutput)?;
    Ok(BoundGoogleReidentifyPlan {
        identity: bound.identity,
        plan: bound.plan,
        token: prior.token().to_owned(),
        token_digest: prior.token_digest(),
        deidentify_response_sha256: prior.response_sha256().to_owned(),
    })
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
    prepare_google_aes_siv(
        request,
        claim,
        current,
        selected,
        parent,
        key,
        TokenAction::Deidentify,
    )
}

fn prepare_google_aes_siv(
    request: &TokenAuthorizationRequest<'_>,
    claim: &TokenAuthorizationClaim<'_>,
    current: CurrentGovernance<'_>,
    selected: SelectedTabularInput,
    parent: String,
    key: WrappedKeyBinding,
    action: TokenAction,
) -> Result<BoundGoogleDeidentifyPlan, GoogleSelectionMismatch> {
    if request.action != action {
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
            authorization_scope: GoogleAuthorizationScope::from_request(request, current.now),
            claim_expires_at: claim.expires_at,
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
