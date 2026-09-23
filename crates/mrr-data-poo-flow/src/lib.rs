//! Opt-in external snapshot resources. POO Flow retains execution policy.
#![forbid(unsafe_code)]
#[cfg(feature = "property-query")]
mod property_source;
#[cfg(feature = "runtime")]
mod protocol;
#[cfg(feature = "runtime")]
mod runtime;
#[cfg(feature = "runtime")]
mod semantic;
#[cfg(feature = "runtime")]
mod worker;
#[cfg(feature = "property-query")]
pub use property_source::{
    AdmittedPropertySourceResult, PropertySourceWorkerQuery, RestoredPropertySourceQuery,
    execute_property_source_worker_query, execute_restored_property_source_query,
};
#[cfg(feature = "runtime")]
pub use runtime::{execute, run_cli};
#[cfg(test)]
#[path = "../tests/unit/mod.rs"]
mod tests;
