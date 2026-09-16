//! Verified local content stores and bounded `CARv1` packaging.
#![forbid(unsafe_code)]

mod car;
mod error;
mod store;

pub use car::{CarImportLimits, ImportedSnapshot, encode_snapshot_car, import_snapshot_car};
pub use error::{ContentError, ImportResource};
pub use store::{
    ContentBlock, ContentCodec, ContentStore, FilesystemContentStore, MemoryContentStore,
};

#[cfg(test)]
#[path = "../tests/unit/mod.rs"]
mod tests;
