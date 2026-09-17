//! Arrow-first physical data plane for Meta-Relational Reasoning.
#![forbid(unsafe_code)]

/// Stable schema and physical profile identifiers.
pub use mrr_data_profile as profile;

/// Lossless relation-specific Arrow interchange, enabled by default.
#[cfg(feature = "arrow")]
pub use mrr_data_arrow as arrow;

/// Snapshot manifests and content identity contracts.
#[cfg(feature = "content")]
pub use mrr_data_core as manifest;

/// Local content stores and CAR packaging.
#[cfg(feature = "content")]
pub use mrr_data_content as content;

/// Property Graph projection contracts.
#[cfg(feature = "graphar")]
pub use mrr_data_graphar as graphar;

#[cfg(test)]
#[path = "../tests/unit/mod.rs"]
mod tests;
