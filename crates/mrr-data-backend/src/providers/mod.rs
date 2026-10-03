//! Provider modules implement one metadata transaction contract for the engine.
mod contract;
#[cfg(feature = "sqlite")]
mod sqlite;
pub use contract::{MetadataProvider, ProviderResult};
#[cfg(feature = "sqlite")]
pub use sqlite::SqliteProvider;
