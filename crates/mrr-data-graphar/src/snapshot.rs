//! Capture authenticated physical files before entering native readers.
use crate::{
    BinaryEntityProjection, GraphArInventoryError, GraphArReadError, GraphArReadLimits,
    GraphArWriteError,
    inventory::{check_framing, file_kind},
    reader::prepare_with_info,
};
use graphar_rs::info::{GraphInfo, InfoVersion};
use meta_relational_reasoning::Fact;
use mrr_data_core::{
    BoundDataQuery, GraphDatasetBinding, GraphDatasetInventory, GraphFile, GraphInventoryError,
    GraphInventoryLimits,
};
use std::{
    fs,
    io::{self, Read, Write},
    path::Path,
    sync::Arc,
    time::Duration,
};

/// Immutable semantic snapshot captured from private verified physical files.
/// No native handles or paths escape; Backend handle clones share the facts.
pub struct CapturedGraphArSnapshot(Captured);
struct Captured {
    binding: GraphDatasetBinding,
    facts: Arc<[Fact]>,
    vertex_count: usize,
}
#[derive(Debug)]
pub enum GraphArCaptureError {
    Inventory(GraphArInventoryError),
    Read(GraphArReadError),
    Schema(GraphArWriteError),
    SemanticScope,
}
impl std::fmt::Display for GraphArCaptureError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "graph capture: {self:?}")
    }
}
impl std::error::Error for GraphArCaptureError {}
impl From<GraphArInventoryError> for GraphArCaptureError {
    fn from(value: GraphArInventoryError) -> Self {
        Self::Inventory(value)
    }
}
impl From<GraphInventoryError> for GraphArCaptureError {
    fn from(value: GraphInventoryError) -> Self {
        Self::Inventory(value.into())
    }
}
impl From<io::Error> for GraphArCaptureError {
    fn from(value: io::Error) -> Self {
        Self::Inventory(value.into())
    }
}
impl CapturedGraphArSnapshot {
    /// Reauthorize reuse against the same snapshot, catalogs and physical inventory.
    /// # Errors
    /// Refuses semantic or physical identity drift; no native storage is reread.
    pub fn facts(&self, query: &BoundDataQuery) -> Result<&[Fact], GraphArCaptureError> {
        self.0.binding.admit_query_scope(query)?;
        Ok(&self.0.facts)
    }
    #[must_use]
    pub fn binding(&self) -> &GraphDatasetBinding {
        &self.0.binding
    }
    #[must_use]
    pub fn vertex_count(&self) -> usize {
        self.0.vertex_count
    }
}
/// Copy only inventory-declared bytes into a private directory, validate every
/// CID, and read using the fixed binary-Entity schema. Metadata YAML is retained
/// as authenticated content but never interpreted as native path instructions.
/// The binding must originate from a Host-authenticated root. Local construction
/// alone is not evidence that the inventory belongs to a published snapshot.
/// # Errors
/// Refuses invalid binding, links, corrupt bytes, limits, schema or fact generation.
/// Partial copies are removed on every failure. Source mutation after return has
/// no effect on the returned facts. This is not an OS sandbox for a hostile process.
pub fn capture_graphar_snapshot(
    source: &Path,
    query: &BoundDataQuery,
    binding: GraphDatasetBinding,
    inventory: &GraphDatasetInventory,
    projection: &BinaryEntityProjection,
    inventory_limits: GraphInventoryLimits,
    read_limits: GraphArReadLimits,
) -> Result<CapturedGraphArSnapshot, GraphArCaptureError> {
    binding.admit_query(query, inventory, inventory_limits)?;
    if projection.relation_id() != binding.relation()
        || projection.catalog_digest() != Some(query.query().catalog_digest())
    {
        return Err(GraphArCaptureError::SemanticScope);
    }
    let dataset = capture_inventory(
        source,
        inventory,
        projection,
        binding.generation(),
        inventory_limits,
        read_limits,
    )?;
    Ok(CapturedGraphArSnapshot(Captured {
        binding,
        vertex_count: dataset.vertex_count(),
        facts: dataset.shared_facts(),
    }))
}
/// Native topology preparation shared by root-bound capture owners.
pub(crate) fn capture_inventory(
    source: &Path,
    inventory: &GraphDatasetInventory,
    projection: &BinaryEntityProjection,
    generation: meta_relational_reasoning::GenerationId,
    inventory_limits: GraphInventoryLimits,
    read_limits: GraphArReadLimits,
) -> Result<crate::GraphArDataset, GraphArCaptureError> {
    inventory.canonical_bytes(inventory_limits)?;
    let directory = tempfile::tempdir()?;
    check_entry(source, true)?;
    for descriptor in inventory.files() {
        copy_verified_file(source, directory.path(), descriptor, inventory_limits)?;
    }
    let version = InfoVersion::new(1).map_err(|_| GraphArCaptureError::SemanticScope)?;
    let prefix = format!(
        "{}/",
        directory
            .path()
            .to_str()
            .ok_or(GraphInventoryError::InvalidPath)?
    );
    let info = GraphInfo::builder("mrr_data")
        .push_vertex_info(
            crate::writer::vertex_info(version.clone()).map_err(GraphArCaptureError::Schema)?,
        )
        .push_edge_info(
            crate::writer::edge_info(version.clone()).map_err(GraphArCaptureError::Schema)?,
        )
        .prefix(prefix)
        .version(version)
        .try_build()
        .map_err(|_| GraphArCaptureError::SemanticScope)?;
    let prepared = prepare_with_info(directory.path(), &info, read_limits, Duration::ZERO)
        .map_err(GraphArCaptureError::Read)?;
    let dataset = prepared
        .admit(projection)
        .map_err(GraphArCaptureError::Read)?;
    if dataset
        .facts()
        .iter()
        .any(|f| f.context().generation() != generation)
    {
        return Err(GraphArCaptureError::SemanticScope);
    }
    // All native storage handles are gone. Clean temporary files on this
    // preparation worker before exposing a resource handle to async callers.
    drop(info);
    directory.close()?;
    Ok(dataset)
}

pub(crate) fn copy_verified_file(
    source: &Path,
    destination: &Path,
    descriptor: &GraphFile,
    limits: GraphInventoryLimits,
) -> Result<(), GraphArCaptureError> {
    if file_kind(descriptor.path())? != descriptor.kind() {
        return Err(GraphArInventoryError::UnsupportedFile.into());
    }
    let mut cursor = source.to_path_buf();
    let components: Vec<_> = Path::new(descriptor.path()).components().collect();
    for (index, component) in components.iter().enumerate() {
        cursor.push(component);
        check_entry(&cursor, index + 1 < components.len())?;
    }
    let input = fs::File::open(source.join(descriptor.path()))?;
    check_file(&input.metadata()?)?;
    let output_path = destination.join(descriptor.path());
    fs::create_dir_all(
        output_path
            .parent()
            .ok_or(GraphInventoryError::InvalidPath)?,
    )?;
    let mut output = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&output_path)?;
    let copied = GraphFile::from_reader(
        descriptor.path().into(),
        CopyReader {
            input,
            output: &mut output,
        },
        descriptor.kind(),
        GraphInventoryLimits {
            max_total_bytes: descriptor.byte_length(),
            ..limits
        },
    )?;
    if copied != *descriptor {
        return Err(GraphInventoryError::Integrity.into());
    }
    drop(output);
    // Verify framing of the authenticated private copy. No second full-file hash
    // or source-directory traversal is needed: only declared paths were created.
    check_framing(
        &mut fs::File::open(output_path)?,
        descriptor.kind(),
        descriptor.byte_length(),
    )?;
    Ok(())
}
pub(crate) fn check_entry(path: &Path, directory: bool) -> Result<(), GraphArCaptureError> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || (directory && !metadata.is_dir()) {
        return Err(GraphArInventoryError::UnsupportedEntry.into());
    }
    if !directory {
        check_file(&metadata)?;
    }
    Ok(())
}
fn check_file(metadata: &fs::Metadata) -> Result<(), GraphArCaptureError> {
    if !metadata.is_file() {
        return Err(GraphArInventoryError::UnsupportedEntry.into());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() != 1 {
            return Err(GraphArInventoryError::UnsupportedEntry.into());
        }
    }
    Ok(())
}
struct CopyReader<'a> {
    input: fs::File,
    output: &'a mut fs::File,
}
impl Read for CopyReader<'_> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        let count = self.input.read(bytes)?;
        self.output.write_all(&bytes[..count])?;
        Ok(count)
    }
}
