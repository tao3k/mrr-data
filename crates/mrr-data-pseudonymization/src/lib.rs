//! Optional Cedar POO pseudonymization profile for MRR Data snapshots.
#![forbid(unsafe_code)]

pub use cedar_poo_bridge::pseudonymization::{Mode, TokenLineage, TokenProfile};
mod binding;
pub use binding::{
    SelectedTokenInput, TokenInputBinding, compatible_inputs, hmac_catalog_separated,
};

/// Optional Google Sensitive Data Protection wire contract, without a cloud
/// SDK or Cedar runtime dependency.
#[cfg(feature = "google-sdp")]
pub use cedar_poo_bridge::google_sdp;

#[cfg(test)]
#[path = "../tests/unit/mod.rs"]
mod tests;
