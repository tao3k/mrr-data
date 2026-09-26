//! Opt-in external snapshot resources. POO Flow retains execution policy.
#![forbid(unsafe_code)]
#[cfg(feature = "runtime")]
mod outbox;
#[cfg(feature = "property-query")]
mod property_snapshot;
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
pub use property_snapshot::{
    MaterializedPropertySnapshot, PropertyEntityRow, PropertyRelationRow, PropertySnapshotInput,
    PropertySnapshotLimits, PropertySnapshotRows, materialize_property_snapshot,
};
#[cfg(feature = "property-query")]
pub use property_source::{
    AdmittedPropertySourceResult, PropertySourceWorkerQuery, RestoredPropertySourceQuery,
    execute_property_source_worker_query, execute_restored_property_source_query,
};
#[cfg(feature = "runtime")]
pub use runtime::{execute, run_cli, run_sync_pending, run_sync_service};
#[cfg(test)]
#[path = "../tests/unit/mod.rs"]
mod tests;
