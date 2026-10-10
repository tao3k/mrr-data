//! Arrow-first physical data plane for Meta-Relational Reasoning.
#![forbid(unsafe_code)]

#[cfg(all(feature = "content-identity", feature = "graphar"))]
mod graphar_binding;

#[cfg(all(feature = "content-identity", feature = "graphar"))]
pub use graphar_binding::{GraphArQuerySourceBindingError, admit_graphar_query_source};

/// Stable schema and physical profile identifiers.
pub use mrr_data_profile as profile;

/// Provider-neutral Cloud `DataProtection` release profile.
#[cfg(feature = "data-protection")]
pub use mrr_data_security::data_protection;

/// Optional Cedar POO token-profile binding for the physical data plane.
#[cfg(feature = "pseudonymization-cedar")]
pub use mrr_data_pseudonymization as pseudonymization;

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

#[cfg(feature = "backend-duckgql-query")]
pub use mrr_data_duckgql_query as duckgql_query;
/// Property Graph projection contracts.
#[cfg(feature = "graphar")]
pub use mrr_data_graphar as graphar;
#[cfg(feature = "backend-turso-query")]
pub use mrr_data_turso_query as turso_query;

#[cfg(test)]
#[path = "../tests/unit/mod.rs"]
mod tests;

/// Optional Cedar Commerce integration with MRR content commits.
#[cfg(feature = "commerce-cedar")]
pub use mrr_data_commerce as commerce;

/// Shared persistence engine and optional metadata providers for all profiles.
#[cfg(feature = "backend")]
pub use mrr_data_backend as backend;
