//! Optional Cedar POO pseudonymization profile for MRR Data snapshots.
#![forbid(unsafe_code)]

pub use cedar_poo_bridge::pseudonymization::{Mode, TokenLineage, TokenProfile};
mod authorization;
mod binding;
pub use authorization::{
    ClaimMismatch, CurrentGovernance, TokenAction, TokenAuthorizationClaim,
    TokenAuthorizationRequest,
};
pub use binding::{
    SelectedTokenInput, TokenInputBinding, compatible_inputs, hmac_catalog_separated,
};

#[cfg(feature = "google-sdp")]
mod google_arrow;
#[cfg(feature = "google-sdp")]
mod google_sdp_binding;
#[cfg(feature = "google-sdp")]
pub use google_arrow::{
    AesSivTableRecipeBinding, ArrowChildInput, CloudGoogleArrowPreparation, GoogleArrowBatchError,
    GoogleArrowSelectionError, GoogleTableBatchInput, GoogleTableBatchMismatch,
    GoogleTableBatchRow, TableRecipeMismatch, VerifiedGoogleArrowChild,
    prepare_cloud_google_arrow_batch, verify_google_arrow_row,
};
#[cfg(feature = "google-sdp")]
pub use google_sdp_binding::{
    BoundGoogleDeidentifyBatchPlan, BoundGoogleDeidentifyOutput, BoundGoogleDeidentifyPlan,
    BoundGoogleReidentifyOutput, BoundGoogleReidentifyPlan, CloudDataProtectionSelection,
    CloudGateMismatch, CloudPseudonymizationGate, CloudReleaseIdentity, CloudRowIdentity,
    GoogleBatchWireMismatch, GoogleBoundIdentity, GoogleSelectionMismatch,
    prepare_cloud_google_aes_siv_deidentify, prepare_google_aes_siv_deidentify,
    prepare_google_aes_siv_reidentify,
};

/// Optional Google Sensitive Data Protection wire contract, without a cloud
/// SDK or Cedar runtime dependency.
#[cfg(feature = "google-sdp")]
pub use cedar_poo_bridge::google_sdp;

#[cfg(test)]
#[path = "../tests/unit/mod.rs"]
mod tests;
