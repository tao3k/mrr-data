//! Optional database providers implement physical transactions for one engine.
mod contract;
#[cfg(feature = "duckdb")]
mod duckdb;
#[cfg(feature = "duckdb")]
mod duckdb_database;
mod storage;
#[cfg(feature = "turso")]
mod turso;
pub use contract::{MetadataProvider, ProviderResult};
#[cfg(feature = "duckdb")]
pub use duckdb::DuckDbProvider;
pub use storage::{MetadataTransaction, TransactionProvider};
#[cfg(feature = "turso")]
pub use turso::TursoProvider;
