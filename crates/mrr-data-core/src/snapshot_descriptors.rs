//! Entity-property, coverage and `GraphAr` descriptors for content snapshots.

use std::collections::BTreeSet;

use cid::Cid;
use meta_relational_reasoning::EntitySchema;

use crate::manifest::BatchDescriptor;
use crate::profile::validate_cid;
use crate::{DataError, RAW_CODEC};

/// One typed entity-property table and its immutable Arrow IPC batches.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EntityDescriptor {
    pub(crate) schema: EntitySchema,
    pub(crate) row_count: u64,
    pub(crate) batches: Vec<BatchDescriptor>,
}

impl EntityDescriptor {
    /// Admits a complete table bound to an MRR entity schema.
    ///
    /// # Errors
    /// Rejects an invalid schema, duplicate children or inconsistent row counts.
    pub fn new(
        schema: EntitySchema,
        row_count: u64,
        batches: Vec<BatchDescriptor>,
    ) -> Result<Self, DataError> {
        schema
            .validate()
            .map_err(|error| DataError::InvalidEntitySchema(format!("{error:?}")))?;
        let mut child_cids = BTreeSet::new();
        let actual = batches.iter().try_fold(0_u64, |total, batch| {
            if !child_cids.insert(batch.cid()) {
                return Err(DataError::DuplicateChild(Box::new(*batch.cid())));
            }
            total
                .checked_add(batch.row_count())
                .ok_or(DataError::EntityRowCountOverflow(schema.id()))
        })?;
        if actual != row_count {
            return Err(DataError::EntityBatchRowsMismatch {
                entity: schema.id(),
                declared: row_count,
                actual,
            });
        }
        Ok(Self {
            schema,
            row_count,
            batches,
        })
    }

    #[must_use]
    pub const fn schema(&self) -> &EntitySchema {
        &self.schema
    }

    #[must_use]
    pub const fn row_count(&self) -> u64 {
        self.row_count
    }

    #[must_use]
    pub fn batches(&self) -> &[BatchDescriptor] {
        &self.batches
    }
}

/// Declared evidence coverage of one physical snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CoverageKind {
    Complete,
    Partial,
    Unknown,
}

/// Coverage declaration bound to its immutable evidence block.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoverageDescriptor {
    pub(crate) kind: CoverageKind,
    pub(crate) declaration_cid: Cid,
}

impl CoverageDescriptor {
    /// Admits a coverage kind bound to one raw/SHA-256 declaration block.
    ///
    /// # Errors
    /// Returns [`DataError`] when the CID is outside the child profile.
    pub fn new(kind: CoverageKind, declaration_cid: Cid) -> Result<Self, DataError> {
        validate_cid(&declaration_cid, RAW_CODEC)?;
        Ok(Self {
            kind,
            declaration_cid,
        })
    }

    #[must_use]
    pub const fn kind(&self) -> CoverageKind {
        self.kind
    }

    #[must_use]
    pub const fn declaration_cid(&self) -> &Cid {
        &self.declaration_cid
    }
}

/// Optional `GraphAr` projection attached to the same semantic snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphProjectionDescriptor {
    pub(crate) graphar_version: String,
    pub(crate) manifest_cid: Cid,
}

impl GraphProjectionDescriptor {
    /// Admits an explicitly versioned `GraphAr` projection manifest.
    ///
    /// # Errors
    /// Rejects an invalid version or a CID outside the child profile.
    pub fn new(graphar_version: impl Into<String>, manifest_cid: Cid) -> Result<Self, DataError> {
        let graphar_version = graphar_version.into();
        if graphar_version.is_empty() || graphar_version.trim() != graphar_version {
            return Err(DataError::InvalidGraphArVersion);
        }
        validate_cid(&manifest_cid, RAW_CODEC)?;
        Ok(Self {
            graphar_version,
            manifest_cid,
        })
    }

    #[must_use]
    pub fn graphar_version(&self) -> &str {
        &self.graphar_version
    }

    #[must_use]
    pub const fn manifest_cid(&self) -> &Cid {
        &self.manifest_cid
    }
}
