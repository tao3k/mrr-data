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
mod google_sdp_binding;
#[cfg(feature = "google-sdp")]
pub use google_sdp_binding::{
    BoundGoogleDeidentifyOutput, BoundGoogleDeidentifyPlan, CloudDataProtectionSelection,
    CloudReleaseIdentity, GoogleBoundIdentity, GoogleSelectionMismatch,
    prepare_cloud_google_aes_siv_deidentify, prepare_google_aes_siv_deidentify,
};

/// Optional Google Sensitive Data Protection wire contract, without a cloud
/// SDK or Cedar runtime dependency.
#[cfg(feature = "google-sdp")]
pub use cedar_poo_bridge::google_sdp;

#[cfg(test)]
#[path = "../tests/unit/mod.rs"]
mod tests;
