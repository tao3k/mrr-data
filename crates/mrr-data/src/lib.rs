//! Arrow-first physical data plane for Meta-Relational Reasoning.
#![forbid(unsafe_code)]

#[cfg(all(feature = "content-identity", feature = "graphar"))]
mod graphar_binding;

#[cfg(all(feature = "content-identity", feature = "graphar"))]
pub use graphar_binding::{GraphArQuerySourceBindingError, admit_graphar_query_source};

/// Stable schema and physical profile identifiers.
pub use mrr_data_profile as profile;

/// Lossless relation-specific Arrow interchange, enabled by default.
#[cfg(feature = "arrow")]
pub use mrr_data_arrow as arrow;

/// `DataFusion` execution for bounded Entity and string-property path queries.
#[cfg(feature = "datafusion")]
pub use mrr_data_datafusion as datafusion;

/// CID/DAG-CBOR snapshot manifests and content identity contracts.
#[cfg(feature = "content-identity")]
pub use mrr_data_core as manifest;

/// Kache local cache and S3 remote content adapters.
#[cfg(any(feature = "cache", feature = "s3", feature = "transfer"))]
pub use mrr_data_cache as cache;

/// Content protocol; CAR and filesystem support require their own features.
#[cfg(feature = "content")]
pub use mrr_data_content as content;

/// Property Graph projection contracts.
#[cfg(feature = "graphar")]
pub use mrr_data_graphar as graphar;

#[cfg(test)]
#[path = "../tests/unit/mod.rs"]
mod tests;
