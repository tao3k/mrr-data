//! Typed failures for local content storage and `CARv1` admission.

use std::{fmt, io};

use cid::Cid;
use mrr_data_core::DataError;

/// Resource whose configured CAR import bound was exceeded.
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
    Io {
        operation: &'static str,
        kind: io::ErrorKind,
    },
    Car(String),
    RootCount {
        actual: usize,
    },
    DuplicateBlock(Box<Cid>),
    MissingRoot(Box<Cid>),
    MissingReferencedBlock(Box<Cid>),
    ChildLengthMismatch {
        cid: Box<Cid>,
        declared: u64,
        actual: u64,
    },
    LimitExceeded {
        resource: ImportResource,
        limit: u64,
        actual: u64,
    },
    Manifest(DataError),
}

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

impl From<DataError> for ContentError {
    fn from(error: DataError) -> Self {
        Self::Manifest(error)
    }
}
