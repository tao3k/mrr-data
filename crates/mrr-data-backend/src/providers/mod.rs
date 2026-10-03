//! Optional database providers implement physical transactions for one engine.
mod contract;
#[cfg(feature = "duckdb")]
mod duckdb;
#[cfg(feature = "duckdb")]
mod duckdb_arrow;
#[cfg(feature = "duckdb")]
mod duckdb_query;
#[cfg(feature = "duckdb")]
pub use duckdb_query::emit_duckdb_arrow;
#[cfg(feature = "duckdb")]
mod duckdb_database;
mod storage;
#[cfg(feature = "turso")]
mod turso;
pub use contract::{MetadataProvider, ProviderResult};
#[cfg(feature = "duckdb")]
pub use duckdb::DuckDbProvider;
#[cfg(feature = "duckdb")]
pub use duckdb_arrow::DuckDbArrowInput;
pub use storage::{MetadataTransaction, TransactionProvider};
#[cfg(feature = "turso")]
pub use turso::TursoProvider;
