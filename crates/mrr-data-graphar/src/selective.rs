//! Snapshot-bound ordered adjacency, offset indexing and selective Arrow reads.
#[path = "selective_io.rs"]
mod io;
#[path = "selective_many.rs"]
mod many;
use crate::{
    BinaryEntityProjection, GraphArAdjacency, GraphArCaptureError, GraphArChunkLayout,
    GraphArReadError,
};
use arrow_array::{Array, Int64Array, RecordBatch};
use graphar_rs::info::{GraphInfo, InfoVersion};
use io::{ReadMeter, read_range};
use meta_relational_reasoning::{EntityId, Fact, GenerationId};
use mrr_data_core::{
    BoundDataQuery, GraphDatasetBinding, GraphDatasetInventory, GraphInventoryLimits,
};
use std::{
    collections::HashMap,
    fs::File,
    ops::Range,
    path::Path,
    sync::Arc,
    sync::atomic::Ordering,
    time::{Duration, Instant},
};
const PREFIX: &str = "edge/entity_mrr_relation_entity/ordered_by_source";
const TOPOLOGY: [&str; 2] = ["_graphArSrcIndex", "_graphArDstIndex"];
const PROPERTIES: [&str; 11] = [
    "fact_id",
    "relation_id",
    "predicate",
    "generation_id",
    "authority_kind",
    "authority_id",
    "provenance_kind",
    "provenance_id",
    "completeness",
    "validity_kind",
    "invalidated_by",
];
/// Host-selected physical layout and bounded source preparation.
#[derive(Clone, Copy, Debug)]
pub struct GraphArSelectiveCaptureOptions {
    pub inventory_limits: GraphInventoryLimits,
    pub max_vertices: usize,
    pub layout: GraphArChunkLayout,
}

/// Borrowed physical inputs plus the owned binding retained by the snapshot.
pub struct GraphArSelectiveCaptureRequest<'a> {
    pub source: &'a Path,
    pub query: &'a BoundDataQuery,
    pub binding: GraphDatasetBinding,
    pub inventory: &'a GraphDatasetInventory,
    pub projection: &'a BinaryEntityProjection,
    pub options: GraphArSelectiveCaptureOptions,
}

/// Typed integrity, physical-layout, scope, budget and cooperative-stop refusals.
#[derive(Debug)]
pub enum GraphArSelectiveError {
    Capture(GraphArCaptureError),
    Read(GraphArReadError),
    Io(std::io::ErrorKind),
    Parquet(String),
    Arrow(String),
    Layout,
    Limit,
    Scope,
    Cancelled,
    Deadline,
}
impl std::fmt::Display for GraphArSelectiveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "selective GraphAr: {self:?}")
    }
}
impl std::error::Error for GraphArSelectiveError {}
impl From<std::io::Error> for GraphArSelectiveError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error.kind())
    }
}
impl From<parquet::errors::ParquetError> for GraphArSelectiveError {
    fn from(error: parquet::errors::ParquetError) -> Self {
        Self::Parquet(error.to_string())
    }
}
impl From<GraphArCaptureError> for GraphArSelectiveError {
    fn from(error: GraphArCaptureError) -> Self {
        Self::Capture(error)
    }
}
impl From<GraphArReadError> for GraphArSelectiveError {
    fn from(error: GraphArReadError) -> Self {
        Self::Read(error)
    }
}

/// Initial complete CID verification and vertex/offset-index preparation.
#[derive(Clone, Copy, Debug)]
pub struct GraphArSelectivePreparationMetrics {
    pub verified_bytes: u64,
    pub offset_read_bytes: usize,
    pub offset_rows: usize,
    pub elapsed: Duration,
}
/// Per-call measured reads. Materialized rows include topology boundary probes;
/// bytes are actual Rust Read returns, not a page-decompression or RSS estimate.
#[derive(Clone, Copy, Debug)]
pub struct GraphArSelectionMetrics {
    pub read_bytes: usize,
    pub files_read: usize,
    pub materialized_rows: usize,
    pub selected_edges: usize,
    pub elapsed: Duration,
}
/// A physical outgoing neighborhood, not an entire relation or admitted result.
pub struct GraphArSelection {
    facts: Vec<Fact>,
    metrics: GraphArSelectionMetrics,
}
impl GraphArSelection {
    /// Transfer physical facts without cloning their values. Backend callers
    /// use `ResourceHandle::try_transform` to preserve the retained lease.
    #[must_use]
    pub fn into_facts(self) -> Vec<Fact> {
        self.facts
    }
    #[must_use]
    pub fn facts(&self) -> &[Fact] {
        &self.facts
    }
    #[must_use]
    pub const fn metrics(&self) -> GraphArSelectionMetrics {
        self.metrics
    }
}
/// Open immutable private file descriptors survive source deletion/mutation.
/// The private directory is removed during preparation, before a Backend handle
/// can escape. Blocking reads belong on Host workers; final drop closes only
/// descriptors, with no recursive directory deletion or native query handles.
pub struct GraphArSelectiveSnapshot {
    binding: GraphDatasetBinding,
    physical: SelectivePhysicalSnapshot,
}

/// Private physical storage shared by authenticated single and combined scopes.
pub(crate) struct SelectivePhysicalSnapshot {
    generation: GenerationId,
    files: HashMap<String, Arc<File>>,
    layout: GraphArChunkLayout,
    entities: Vec<EntityId>,
    physical: HashMap<EntityId, usize>,
    offsets: Vec<Vec<usize>>,
    metrics: GraphArSelectivePreparationMetrics,
}
impl GraphArSelectiveSnapshot {
    #[must_use]
    pub const fn preparation_metrics(&self) -> GraphArSelectivePreparationMetrics {
        self.physical.metrics
    }
    #[must_use]
    pub fn binding(&self) -> &GraphDatasetBinding {
        &self.binding
    }
    /// Read a neighborhood under the authenticated single-relation binding.
    /// # Errors
    /// Refuses scope, physical alignment and budget violations.
    pub fn outgoing(
        &self,
        query: &BoundDataQuery,
        projection: &BinaryEntityProjection,
        source: EntityId,
        max_edges: usize,
    ) -> Result<GraphArSelection, GraphArSelectiveError> {
        self.outgoing_checked(query, projection, source, max_edges, || Ok(()))
    }
    pub(crate) fn outgoing_checked(
        &self,
        query: &BoundDataQuery,
        projection: &BinaryEntityProjection,
        source: EntityId,
        max_edges: usize,
        mut check: impl FnMut() -> Result<(), GraphArSelectiveError>,
    ) -> Result<GraphArSelection, GraphArSelectiveError> {
        check()?;
        validate_scope(query, &self.binding, projection)?;
        self.physical
            .outgoing_checked(projection, source, max_edges, check)
    }
    /// Read a set of source neighborhoods, coalescing ranges within each chunk.
    /// Duplicate source IDs are ignored; duplicate edges remain intact.
    /// # Errors
    /// Refuses foreign scope, malformed boundaries and aggregate edge limits.
    pub fn outgoing_many(
        &self,
        query: &BoundDataQuery,
        projection: &BinaryEntityProjection,
        sources: &[EntityId],
        max_edges: usize,
    ) -> Result<GraphArSelection, GraphArSelectiveError> {
        self.outgoing_many_checked(query, projection, sources, max_edges, || Ok(()))
    }
    pub(crate) fn outgoing_many_checked(
        &self,
        query: &BoundDataQuery,
        projection: &BinaryEntityProjection,
        sources: &[EntityId],
        max_edges: usize,
        mut check: impl FnMut() -> Result<(), GraphArSelectiveError>,
    ) -> Result<GraphArSelection, GraphArSelectiveError> {
        check()?;
        validate_scope(query, &self.binding, projection)?;
        self.physical
            .outgoing_many_checked(projection, sources, max_edges, check)
    }
    /// Matched full scan of the same authenticated physical source.
    /// # Errors
    /// Refuses scope, malformed rows and aggregate edge limits.
    pub fn scan_all(
        &self,
        query: &BoundDataQuery,
        projection: &BinaryEntityProjection,
        max_edges: usize,
    ) -> Result<GraphArSelection, GraphArSelectiveError> {
        validate_scope(query, &self.binding, projection)?;
        self.physical.scan_all(projection, max_edges)
    }
}
impl SelectivePhysicalSnapshot {
    #[cfg(feature = "combined-graph")]
    pub(crate) const fn preparation_metrics(&self) -> GraphArSelectivePreparationMetrics {
        self.metrics
    }
    #[cfg(feature = "combined-graph")]
    pub(crate) fn vertex_count(&self) -> usize {
        self.entities.len()
    }
    /// Read only the offset-selected outgoing topology/property ranges. Current
    /// scope supports the controlled ordered binary-Entity writer, not arbitrary
    /// `GraphAr` layouts. Boundary probes refuse truncated or shifted ranges.
    /// # Errors
    /// Refuses scope/projection drift, row budgets and malformed selected data.
    /// The Host may check cancellation/deadline between chunk reads and before
    /// fact conversion. This does not forcibly interrupt a running Parquet call.
    /// # Errors
    /// Preserves all outgoing refusals and the Host's sticky stop reason.
    pub(crate) fn outgoing_checked(
        &self,
        projection: &BinaryEntityProjection,
        source: EntityId,
        max_edges: usize,
        mut check: impl FnMut() -> Result<(), GraphArSelectiveError>,
    ) -> Result<GraphArSelection, GraphArSelectiveError> {
        let started = Instant::now();
        check()?;
        let mut meter = ReadMeter::default();
        let facts = if let Some(&physical) = self.physical.get(&source) {
            let part = physical / self.layout.vertex_chunk_size();
            let index = physical % self.layout.vertex_chunk_size();
            let range = self.offsets[part][index]..self.offsets[part][index + 1];
            if range.len() > max_edges {
                return Err(GraphArSelectiveError::Limit);
            }
            self.read_neighborhood(part, physical, range, projection, &mut meter, &mut check)?
        } else {
            Vec::new()
        };
        if facts
            .iter()
            .any(|f| f.context().generation() != self.generation)
        {
            return Err(GraphArSelectiveError::Scope);
        }
        check()?;
        let metrics = GraphArSelectionMetrics {
            read_bytes: meter.bytes.load(Ordering::Relaxed),
            files_read: meter.paths.len(),
            materialized_rows: meter.rows,
            selected_edges: facts.len(),
            elapsed: started.elapsed(),
        };
        Ok(GraphArSelection { facts, metrics })
    }

    /// Matched full scan over the same verified descriptors and vertex index.
    /// This is the reference path for measuring range-read work independently
    /// from initial snapshot verification and indexing.
    /// # Errors
    /// Refuses scope, total row limits or malformed physical/semantic data.
    pub fn scan_all(
        &self,
        projection: &BinaryEntityProjection,
        max_edges: usize,
    ) -> Result<GraphArSelection, GraphArSelectiveError> {
        self.scan_all_checked(projection, max_edges, || Ok(()))
    }
    pub(crate) fn scan_all_checked(
        &self,
        projection: &BinaryEntityProjection,
        max_edges: usize,
        mut check: impl FnMut() -> Result<(), GraphArSelectiveError>,
    ) -> Result<GraphArSelection, GraphArSelectiveError> {
        let started = Instant::now();
        check()?;
        let total = self.offsets.iter().try_fold(0usize, |sum, offsets| {
            sum.checked_add(*offsets.last().ok_or(GraphArSelectiveError::Layout)?)
                .ok_or(GraphArSelectiveError::Limit)
        })?;
        if total > max_edges {
            return Err(GraphArSelectiveError::Limit);
        }
        let mut meter = ReadMeter::default();
        let mut batches = Vec::new();
        let chunk_size = self.layout.edge_chunk_size();
        for (part, offsets) in self.offsets.iter().enumerate() {
            let count = *offsets.last().ok_or(GraphArSelectiveError::Layout)?;
            for chunk in 0..count.div_ceil(chunk_size) {
                check()?;
                let rows = (count - chunk * chunk_size).min(chunk_size);
                let topology_name = format!("{PREFIX}/adj_list/part{part}/chunk{chunk}");
                let topology = read_range(
                    self.file(&topology_name)?,
                    &topology_name,
                    &TOPOLOGY,
                    rows,
                    0..rows,
                    &mut meter,
                )?;
                validate_partition(
                    &topology,
                    part * self.layout.vertex_chunk_size(),
                    chunk * chunk_size,
                    offsets,
                )?;
                let properties_name = format!("{PREFIX}/properties/part{part}/chunk{chunk}");
                let properties = read_range(
                    self.file(&properties_name)?,
                    &properties_name,
                    &PROPERTIES,
                    rows,
                    0..rows,
                    &mut meter,
                )?;
                batches.push(join_batches(&topology, &properties)?);
            }
        }
        check()?;
        let facts = crate::reader::admit_selected_batches(&batches, &self.entities, projection)?;
        if facts.len() != total
            || facts
                .iter()
                .any(|f| f.context().generation() != self.generation)
        {
            return Err(GraphArSelectiveError::Scope);
        }
        check()?;
        Ok(GraphArSelection {
            metrics: GraphArSelectionMetrics {
                read_bytes: meter.bytes.load(Ordering::Relaxed),
                files_read: meter.paths.len(),
                materialized_rows: meter.rows,
                selected_edges: facts.len(),
                elapsed: started.elapsed(),
            },
            facts,
        })
    }

    fn file(&self, name: &str) -> Result<Arc<File>, GraphArSelectiveError> {
        self.files
            .get(name)
            .cloned()
            .ok_or(GraphArSelectiveError::Layout)
    }

    fn read_neighborhood(
        &self,
        part: usize,
        source: usize,
        range: Range<usize>,
        projection: &BinaryEntityProjection,
        meter: &mut ReadMeter,
        check: &mut impl FnMut() -> Result<(), GraphArSelectiveError>,
    ) -> Result<Vec<Fact>, GraphArSelectiveError> {
        let chunk_size = self.layout.edge_chunk_size();
        let total = *self.offsets[part]
            .last()
            .ok_or(GraphArSelectiveError::Layout)?;
        let guard = range.start.saturating_sub(1)..range.end.saturating_add(1).min(total);
        if guard.is_empty() {
            return Ok(Vec::new());
        }
        let mut batches = Vec::new();
        for chunk in guard.start / chunk_size..=(guard.end - 1) / chunk_size {
            check()?;
            let base = chunk * chunk_size;
            let rows = (total - base).min(chunk_size);
            let local = guard.start.saturating_sub(base)..(guard.end - base).min(rows);
            let topology_name = format!("{PREFIX}/adj_list/part{part}/chunk{chunk}");
            let topology = read_range(
                self.file(&topology_name)?,
                &topology_name,
                &TOPOLOGY,
                rows,
                local.clone(),
                meter,
            )?;
            validate_topology(
                &topology,
                base + local.start,
                &range,
                source,
                self.entities.len(),
            )?;
            let begin = range.start.saturating_sub(base).max(local.start);
            let end = range.end.saturating_sub(base).min(local.end);
            if begin >= end {
                continue;
            }
            let topology = topology.slice(begin - local.start, end - begin);
            let properties_name = format!("{PREFIX}/properties/part{part}/chunk{chunk}");
            let properties = read_range(
                self.file(&properties_name)?,
                &properties_name,
                &PROPERTIES,
                rows,
                begin..end,
                meter,
            )?;
            batches.push(join_batches(&topology, &properties)?);
        }
        crate::reader::admit_selected_batches(&batches, &self.entities, projection)
            .map_err(Into::into)
    }
}
fn join_batches(
    topology: &RecordBatch,
    properties: &RecordBatch,
) -> Result<RecordBatch, GraphArSelectiveError> {
    let fields = topology
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .chain(
            properties
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone()),
        )
        .collect::<Vec<_>>();
    let arrays = topology
        .columns()
        .iter()
        .cloned()
        .chain(properties.columns().iter().cloned());
    RecordBatch::try_from_iter(fields.into_iter().zip(arrays))
        .map_err(|e| GraphArSelectiveError::Arrow(e.to_string()))
}

fn validate_partition(
    batch: &RecordBatch,
    vertex_base: usize,
    row_base: usize,
    offsets: &[usize],
) -> Result<(), GraphArSelectiveError> {
    let sources = integer_column(batch, 0)?;
    for row in 0..batch.num_rows() {
        let source =
            usize::try_from(sources.value(row)).map_err(|_| GraphArSelectiveError::Layout)?;
        let local = source
            .checked_sub(vertex_base)
            .ok_or(GraphArSelectiveError::Layout)?;
        let begin = *offsets.get(local).ok_or(GraphArSelectiveError::Layout)?;
        let end = *offsets
            .get(local + 1)
            .ok_or(GraphArSelectiveError::Layout)?;
        if !(begin..end).contains(&(row_base + row)) {
            return Err(GraphArSelectiveError::Layout);
        }
    }
    Ok(())
}

fn validate_topology(
    batch: &RecordBatch,
    first_row: usize,
    range: &Range<usize>,
    source: usize,
    vertices: usize,
) -> Result<(), GraphArSelectiveError> {
    let sources = integer_column(batch, 0)?;
    let destinations = integer_column(batch, 1)?;
    for row in 0..batch.num_rows() {
        let id = usize::try_from(sources.value(row)).map_err(|_| GraphArSelectiveError::Layout)?;
        let destination =
            usize::try_from(destinations.value(row)).map_err(|_| GraphArSelectiveError::Layout)?;
        let position = first_row + row;
        if destination >= vertices
            || id >= vertices
            || (position < range.start && id >= source)
            || (position >= range.end && id <= source)
            || (range.contains(&position) && id != source)
        {
            return Err(GraphArSelectiveError::Layout);
        }
    }
    Ok(())
}
fn integer_column(batch: &RecordBatch, index: usize) -> Result<&Int64Array, GraphArSelectiveError> {
    let column = batch
        .column(index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or(GraphArSelectiveError::Layout)?;
    if column.null_count() != 0 {
        return Err(GraphArSelectiveError::Layout);
    }
    Ok(column)
}
fn validate_scope(
    query: &BoundDataQuery,
    binding: &GraphDatasetBinding,
    projection: &BinaryEntityProjection,
) -> Result<(), GraphArSelectiveError> {
    binding
        .admit_query_scope(query)
        .map_err(|_| GraphArSelectiveError::Scope)?;
    if projection.relation_id() != binding.relation()
        || projection.catalog_digest() != Some(query.query().catalog_digest())
    {
        return Err(GraphArSelectiveError::Scope);
    }
    Ok(())
}
/// Authenticate/copy the complete inventory once, then build only the vertex
/// identity and ordered offset indexes. Initial verification is full-source I/O;
/// subsequent outgoing reads do not materialize all edge facts. The binding root
/// must be Host-authenticated. Selected slices never assert whole-source semantic
/// conformance. Native decoding and property profiles remain explicitly bounded.
/// # Errors
/// Refuses CID/layout/semantic drift and preparation budgets; removes partial copies.
pub fn capture_graphar_selective_snapshot(
    request: GraphArSelectiveCaptureRequest<'_>,
) -> Result<GraphArSelectiveSnapshot, GraphArSelectiveError> {
    capture_graphar_selective_snapshot_checked(request, || Ok(()))
}

/// Prepare with Host stop checkpoints between files, index chunks and native
/// calls. A running native call is not forcibly interrupted.
/// # Errors
/// Preserves capture failures and the Host's cooperative stop reason.
pub fn capture_graphar_selective_snapshot_checked(
    request: GraphArSelectiveCaptureRequest<'_>,
    mut check: impl FnMut() -> Result<(), GraphArSelectiveError>,
) -> Result<GraphArSelectiveSnapshot, GraphArSelectiveError> {
    let GraphArSelectiveCaptureRequest {
        source,
        query,
        binding,
        inventory,
        projection,
        options,
    } = request;
    binding
        .admit_query(query, inventory, options.inventory_limits)
        .map_err(|_| GraphArSelectiveError::Scope)?;
    validate_scope(query, &binding, projection)?;
    let physical = capture_verified_physical(
        source,
        inventory,
        options,
        query.query().generation(),
        &mut check,
    )?;
    Ok(GraphArSelectiveSnapshot { binding, physical })
}
pub(crate) fn capture_verified_physical(
    source: &Path,
    inventory: &GraphDatasetInventory,
    options: GraphArSelectiveCaptureOptions,
    generation: GenerationId,
    mut check: impl FnMut() -> Result<(), GraphArSelectiveError>,
) -> Result<SelectivePhysicalSnapshot, GraphArSelectiveError> {
    let started = Instant::now();
    check()?;
    let limits = options.inventory_limits;
    let layout = options.layout;
    inventory
        .canonical_bytes(limits)
        .map_err(GraphArCaptureError::from)?;
    let directory = tempfile::tempdir()?;
    crate::snapshot::check_entry(source, true)?;
    let mut verified_bytes = 0;
    for file in inventory.files() {
        check()?;
        crate::snapshot::copy_verified_file(source, directory.path(), file, limits)?;
        verified_bytes += file.byte_length();
    }
    let info = trusted_graph_info(directory.path(), layout)?;
    let entities =
        crate::reader::read_and_admit_vertices(&info, options.max_vertices)?.physical_entities;
    check()?;
    let physical = entities
        .iter()
        .copied()
        .enumerate()
        .map(|(index, entity)| (entity, index))
        .collect();
    let (offsets, meter) = read_offsets(directory.path(), entities.len(), layout, &mut check)?;
    let files = inventory
        .files()
        .iter()
        .filter(|file| {
            file.path().starts_with(&format!("{PREFIX}/adj_list/"))
                || file.path().starts_with(&format!("{PREFIX}/properties/"))
        })
        .map(|file| {
            check()?;
            Ok((
                file.path().to_owned(),
                Arc::new(File::open(directory.path().join(file.path()))?),
            ))
        })
        .collect::<Result<HashMap<_, _>, GraphArSelectiveError>>()?;
    check()?;
    drop(info);
    directory.close()?;
    let metrics = GraphArSelectivePreparationMetrics {
        verified_bytes,
        offset_rows: meter.rows,
        offset_read_bytes: meter.bytes.load(Ordering::Relaxed),
        elapsed: started.elapsed(),
    };
    Ok(SelectivePhysicalSnapshot {
        generation,
        files,
        layout,
        entities,
        physical,
        offsets,
        metrics,
    })
}
fn trusted_graph_info(
    directory: &Path,
    layout: GraphArChunkLayout,
) -> Result<GraphInfo, GraphArSelectiveError> {
    let version = InfoVersion::new(1).map_err(|_| GraphArSelectiveError::Layout)?;
    let info = GraphInfo::builder("mrr_data")
        .push_vertex_info(
            crate::writer::vertex_info_for(version.clone(), layout)
                .map_err(|_| GraphArSelectiveError::Layout)?,
        )
        .push_edge_info(
            crate::writer::edge_info_with_layout(
                version.clone(),
                GraphArAdjacency::OrderedBySource,
                layout,
            )
            .map_err(|_| GraphArSelectiveError::Layout)?,
        )
        .prefix(format!(
            "{}/",
            directory.to_str().ok_or(GraphArSelectiveError::Layout)?
        ))
        .version(version)
        .try_build()
        .map_err(|_| GraphArSelectiveError::Layout)?;
    Ok(info)
}
fn read_offsets(
    directory: &Path,
    entity_count: usize,
    layout: GraphArChunkLayout,
    mut check: impl FnMut() -> Result<(), GraphArSelectiveError>,
) -> Result<(Vec<Vec<usize>>, ReadMeter), GraphArSelectiveError> {
    let mut offsets = Vec::new();
    let mut meter = ReadMeter::default();
    let vertex_chunk = layout.vertex_chunk_size();
    for part in 0..entity_count.div_ceil(vertex_chunk) {
        check()?;
        let count = read_count(&directory.join(format!("{PREFIX}/edge_count{part}")))?;
        let vertices = (entity_count - part * vertex_chunk).min(vertex_chunk);
        // The maintained native writer pads the last offset chunk to the full
        // vertex chunk size. Validate padding, then retain only real vertices.
        let rows = vertex_chunk + 1;
        let name = format!("{PREFIX}/offset/chunk{part}");
        let file = Arc::new(File::open(directory.join(&name))?);
        let batch = read_range(file, &name, &["_graphArOffset"], rows, 0..rows, &mut meter)?;
        let values = integer_column(&batch, 0)?
            .values()
            .iter()
            .map(|&value| usize::try_from(value).map_err(|_| GraphArSelectiveError::Layout))
            .collect::<Result<Vec<_>, _>>()?;
        if values.first() != Some(&0)
            || values.get(vertices) != Some(&count)
            || values.last() != Some(&count)
            || values.windows(2).any(|pair| pair[0] > pair[1])
        {
            return Err(GraphArSelectiveError::Layout);
        }
        offsets.push(values[..=vertices].to_vec());
    }
    Ok((offsets, meter))
}
fn read_count(path: &Path) -> Result<usize, GraphArSelectiveError> {
    let bytes: [u8; 8] = std::fs::read(path)?
        .try_into()
        .map_err(|_| GraphArSelectiveError::Layout)?;
    usize::try_from(i64::from_le_bytes(bytes)).map_err(|_| GraphArSelectiveError::Layout)
}

#[cfg(feature = "backend")]
impl From<mrr_data_backend::ResourceStop> for GraphArSelectiveError {
    fn from(stop: mrr_data_backend::ResourceStop) -> Self {
        match stop {
            mrr_data_backend::ResourceStop::Cancelled => Self::Cancelled,
            mrr_data_backend::ResourceStop::Deadline => Self::Deadline,
        }
    }
}
