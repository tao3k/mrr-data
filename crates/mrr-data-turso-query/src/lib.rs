//! Bounded Turso execution for already admitted MRR graph queries.
//!
//! MRR's frontends own GQL/openCypher parsing and semantic construction. This
//! crate consumes a `BoundDataQuery`; it has no source-language entrypoint or
//! compatibility fallback. `DuckDB` graph execution belongs to `DuckGQL`.
#![forbid(unsafe_code)]

mod plan;
pub use plan::{SqlQueryError, TursoSingleHopSql, turso_graphar_engine_profile};

#[cfg(feature = "turso-graphar")]
mod turso_graphar;
#[cfg(feature = "turso-graphar")]
pub use turso_graphar::{SqlQueryLimits, execute_turso_graphar_single_hop};
#[cfg(feature = "backend-worker")]
pub use turso_graphar::{TursoBackendQuery, execute_turso_graphar_on_backend};
