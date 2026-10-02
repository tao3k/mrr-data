//! Verified local content stores and bounded `CARv1` packaging.
#![forbid(unsafe_code)]

mod async_store;
#[cfg(feature = "car")]
mod car;
#[cfg(any(feature = "car", feature = "snapshot"))]
mod closure;
mod error;
#[cfg(feature = "property-snapshot")]
mod property_snapshot;
mod protocol;
#[cfg(feature = "snapshot")]
mod snapshot;
mod store;

pub use async_store::{AsyncContentStore, LocalFuture};
#[cfg(feature = "car")]
pub use car::{CarImportLimits, ImportedSnapshot, encode_snapshot_car, import_snapshot_car};
pub use error::ContentError;
#[cfg(feature = "car")]
pub use error::ImportResource;
#[cfg(feature = "property-snapshot")]
pub use property_snapshot::{
    MaterializedPropertySnapshot, PropertyEntityRow, PropertyRelationRow, PropertySnapshotInput,
    PropertySnapshotLimits, PropertySnapshotRows, materialize_property_snapshot,
};
pub use protocol::{
    CacheAdmission, ContentProtocolError, ContentRead, ContentSource, PublishReceipt,
    RemoteContentStore, RemoteError, RemoteFuture, publish_content, read_through,
};
pub use store::{ContentBlock, ContentCodec, ContentStore, MemoryContentStore};

#[cfg(feature = "filesystem")]
pub use store::FilesystemContentStore;

#[cfg(test)]
#[path = "../tests/unit/mod.rs"]
mod tests;

#[cfg(feature = "snapshot")]
pub use snapshot::{
    RestoredSnapshot, SnapshotPublication, SnapshotPublishInputs, SnapshotResource,
    SnapshotTransferError, SnapshotTransferLimits, publish_snapshot, restore_snapshot,
    restore_snapshot_local,
};

#[cfg(feature = "transfer")]
mod transfer;
#[cfg(feature = "transfer")]
pub use transfer::{BudgetedRemote, RemoteTransferLimits, TransferSession, TransferStats};
