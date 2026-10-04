//! Execute an already bound MRR query through the plugin's typed program.
//! MRR owns parsing, semantic binding and final result admission. The adapter
//! registers a private immutable image and never emits a `DuckDB` graph SQL plan.
#![forbid(unsafe_code)]
mod program;
pub use program::{DuckGqlError, DuckGqlSingleHopProgram, duckgql_graphar_engine_profile};
#[cfg(feature = "duckgql-graphar")]
mod native;
#[cfg(feature = "duckgql-graphar")]
pub use native::{DuckGqlArtifact, DuckGqlLimits, execute_duckgql_graphar_single_hop};
#[cfg(feature = "backend-worker")]
mod backend;
#[cfg(feature = "backend-worker")]
pub use backend::{DuckGqlBackendQuery, execute_duckgql_graphar_controlled_on_backend};
