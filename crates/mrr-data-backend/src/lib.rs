//! One shared persistence engine; profiles own validation, providers own storage.
//! Host-authenticated namespaces and live authority synchronization remain required.
#![forbid(unsafe_code)]
mod authority;
mod config;
mod dispatch;
mod engine;
pub mod providers;
mod record;
mod resource;
pub use resource::ResourceHandle;
mod scheduler;
mod transaction;
pub use authority::{
    AuthorityChange, AuthorityExpectation, AuthorityKey, AuthorityProposal, AuthorityState,
    AuthorityStatus,
};
pub use config::{
    AuthorityCapability, BackendConfig, BackendError, BackendStatus, Lifecycle,
    ProviderCapabilities,
};
pub use engine::{Backend, ProfilePort};
pub use providers::MetadataProvider;
pub use record::{StoredOutcome, StoredRevision, StoredWrite};
#[cfg(test)]
#[path = "../tests/unit/asp_rust_gate.rs"]
mod asp_rust_gate;

#[cfg(feature = "graph-publish")]
mod graph_publication;
