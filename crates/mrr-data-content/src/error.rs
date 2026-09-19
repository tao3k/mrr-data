//! Typed failures for local content storage and `CARv1` admission.

use std::{fmt, io};

use cid::Cid;
#[cfg(any(feature = "car", feature = "snapshot"))]
use mrr_data_core::DataError;

/// Resource whose configured CAR import bound was exceeded.
#[cfg(feature = "car")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ImportResource {
    ArchiveBytes,
    Blocks,
    BlockBytes,
    TotalBlockBytes,
}

/// Fail-closed errors from physical content storage and packaging.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ContentError {
    InvalidCidProfile(Box<Cid>),
    CidMismatch {
        expected: Box<Cid>,
        actual: Box<Cid>,
    },
    NotFound(Box<Cid>),
    LockPoisoned,
    BlockTooLarge {
        limit: u64,
        actual: u64,
    },
    Io {
        operation: &'static str,
        kind: io::ErrorKind,
    },
    #[cfg(feature = "car")]
    Car(String),
    #[cfg(feature = "car")]
    RootCount {
        actual: usize,
    },
    #[cfg(feature = "car")]
    DuplicateBlock(Box<Cid>),
    #[cfg(feature = "car")]
    MissingRoot(Box<Cid>),
    #[cfg(any(feature = "car", feature = "snapshot"))]
    MissingReferencedBlock(Box<Cid>),
    #[cfg(any(feature = "car", feature = "snapshot"))]
    ChildLengthMismatch {
        cid: Box<Cid>,
        declared: u64,
        actual: u64,
    },
    #[cfg(feature = "car")]
    LimitExceeded {
        resource: ImportResource,
        limit: u64,
        actual: u64,
    },
    #[cfg(any(feature = "car", feature = "snapshot"))]
    Manifest(DataError),
}

#[cfg(feature = "filesystem")]
impl ContentError {
    pub(crate) fn io(operation: &'static str, error: &io::Error) -> Self {
        Self::Io {
            operation,
            kind: error.kind(),
        }
    }
}

impl fmt::Display for ContentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for ContentError {}

#[cfg(any(feature = "car", feature = "snapshot"))]
impl From<DataError> for ContentError {
    fn from(error: DataError) -> Self {
        Self::Manifest(error)
    }
}
