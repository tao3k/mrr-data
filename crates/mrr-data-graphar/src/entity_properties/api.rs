//! Shared physical contracts for the declared entity-property owner.
use crate::{GraphArCaptureError, GraphArChunkLayout, GraphArInventoryError};
use meta_relational_reasoning::{EntityCatalogDigest, GenerationId};
use mrr_data_core::{GraphDatasetInventory, GraphInventoryLimits};
use std::{
    fmt,
    path::{Path, PathBuf},
};

/// Aggregate input/post-decode value limits and pre-native row/inventory limits.
/// Native decompression remains outside a hard process memory ceiling.
#[derive(Clone, Copy, Debug)]
pub struct GraphArEntityPropertyLimits {
    pub max_rows: usize,
    pub max_types: usize,
    pub max_properties: usize,
    pub max_value_bytes: usize,
    pub inventory: GraphInventoryLimits,
}
impl GraphArEntityPropertyLimits {
    pub(super) fn validate(self) -> Result<(), GraphArEntityPropertyError> {
        if self.max_rows == 0
            || self.max_types == 0
            || self.max_properties == 0
            || self.max_value_bytes == 0
        {
            return Err(GraphArEntityPropertyError::Budget(
                "zero entity property limit",
            ));
        }
        crate::inventory::validate_inventory_limits(self.inventory)?;
        Ok(())
    }
}
/// Locally observed physical receipt. Its inventory must be authenticated by
/// the caller when used across an external publication boundary.
#[derive(Clone, Debug)]
pub struct GraphArEntityPropertyReceipt {
    pub(super) root: PathBuf,
    pub(super) inventory: GraphDatasetInventory,
    pub(super) catalog: EntityCatalogDigest,
    pub(super) generation: GenerationId,
    pub(super) snapshot: [u8; 32],
    pub(super) rows: usize,
    pub(super) layout: GraphArChunkLayout,
}
impl GraphArEntityPropertyReceipt {
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }
    #[must_use]
    pub const fn inventory(&self) -> &GraphDatasetInventory {
        &self.inventory
    }
    #[must_use]
    pub const fn row_count(&self) -> usize {
        self.rows
    }
    #[must_use]
    pub const fn catalog_digest(&self) -> EntityCatalogDigest {
        self.catalog
    }
    #[must_use]
    pub const fn generation(&self) -> GenerationId {
        self.generation
    }
    #[must_use]
    pub const fn snapshot_digest(&self) -> &[u8; 32] {
        &self.snapshot
    }
}
/// Typed physical refusal; no result or publication receipt on failure.
#[derive(Debug)]
pub enum GraphArEntityPropertyError {
    UnsupportedSchema,
    Shape(&'static str),
    Budget(&'static str),
    Scope,
    OutputExists,
    Integrity,
    Inventory(GraphArInventoryError),
    Capture(GraphArCaptureError),
    Native(graphar_rs::Error),
    Arrow(arrow_schema::ArrowError),
    Io(std::io::Error),
    #[cfg(feature = "backend")]
    Stop(mrr_data_backend::ResourceStop),
}
impl fmt::Display for GraphArEntityPropertyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GraphAr entity properties: {self:?}")
    }
}
impl std::error::Error for GraphArEntityPropertyError {}
impl From<GraphArInventoryError> for GraphArEntityPropertyError {
    fn from(e: GraphArInventoryError) -> Self {
        Self::Inventory(e)
    }
}
impl From<GraphArCaptureError> for GraphArEntityPropertyError {
    fn from(e: GraphArCaptureError) -> Self {
        Self::Capture(e)
    }
}
impl From<graphar_rs::Error> for GraphArEntityPropertyError {
    fn from(e: graphar_rs::Error) -> Self {
        Self::Native(e)
    }
}
impl From<arrow_schema::ArrowError> for GraphArEntityPropertyError {
    fn from(e: arrow_schema::ArrowError) -> Self {
        Self::Arrow(e)
    }
}
impl From<std::io::Error> for GraphArEntityPropertyError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

#[cfg(feature = "backend")]
impl From<mrr_data_backend::ResourceStop> for GraphArEntityPropertyError {
    fn from(stop: mrr_data_backend::ResourceStop) -> Self {
        Self::Stop(stop)
    }
}
