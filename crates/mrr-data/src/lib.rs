//! Arrow-first physical data plane for Meta-Relational Reasoning.
#![forbid(unsafe_code)]

#[cfg(all(feature = "content", feature = "graphar"))]
mod graphar_binding;

#[cfg(all(feature = "content", feature = "graphar"))]
pub use graphar_binding::{GraphArQuerySourceBindingError, admit_graphar_query_source};

/// Stable schema and physical profile identifiers.
pub use mrr_data_profile as profile;

/// Lossless relation-specific Arrow interchange, enabled by default.
#[cfg(feature = "arrow")]
pub use mrr_data_arrow as arrow;

/// `DataFusion` execution for the admitted single-hop binary Entity slice.
#[cfg(feature = "datafusion")]
pub use mrr_data_datafusion as datafusion;

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
