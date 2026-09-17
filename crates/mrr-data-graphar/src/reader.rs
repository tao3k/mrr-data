//! Native `GraphAr` reads and fail-closed MRR semantic reconstruction.

#[path = "reader_prepared.rs"]
mod prepared;

pub use prepared::{GraphArPrepareTimings, PreparedGraphArSource, prepare_graphar_source};

use std::{
    collections::HashSet,
    error::Error,
    fmt,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use arrow_array::{Array, Int64Array, LargeStringArray, RecordBatch, StringArray};
#[cfg(test)]
use graphar_rs::reader::scan_edge_arrow_chunks;
use graphar_rs::{
    info::{AdjListType, GraphInfo},
    reader::{read_edge_arrow_batches, read_vertex_string_batch},
};
use meta_relational_reasoning::{
    DerivationId, EntityId, EvidenceCompleteness, Fact, FactId, FactProvenance, FactValidity,
    GenerationId, RelationAuthority, RelationContext, RelationContextError, RuleId, RulePackId,
};

use crate::writer::{EDGE_TYPE, ENTITY_TYPE, GRAPH_INFO_FILE};
use crate::{BinaryEntityProjection, GraphProjectionError};

const VERTEX_PROPERTIES: [&str; 1] = ["entity_id"];
const EDGE_PROPERTIES: [&str; 11] = [
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

/// Resource limits applied by the native `GraphAr` collections before allocation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GraphArReadLimits {
    max_vertices: usize,
    max_edges: usize,
}

impl GraphArReadLimits {
    #[must_use]
    pub const fn new(max_vertices: usize, max_edges: usize) -> Self {
        Self {
            max_vertices,
            max_edges,
        }
    }
}

/// A `GraphAr` dataset reconstructed into admitted MRR facts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphArDataset {
    root: PathBuf,
    vertex_count: usize,
    facts: Arc<[Fact]>,
}

/// Measured phases of one native `GraphAr` semantic read.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GraphArReadTimings {
    graph_info: Duration,
    native_vertex_read: Duration,
    vertex_admission: Duration,
    edge_storage_read: Duration,
    arrow_c_stream_import: Duration,
    fact_identity_decode: Duration,
    fact_materialization: Duration,
    fact_ordering: Duration,
    fact_preparation: Duration,
    fact_admission: Duration,
}

impl GraphArReadTimings {
    /// Time spent loading the `GraphAr` metadata graph.
    #[must_use]
    pub const fn graph_info(self) -> Duration {
        self.graph_info
    }

    /// Time spent reading vertex Arrow chunks through the native reader.
    #[must_use]
    pub const fn native_vertex_read(self) -> Duration {
        self.native_vertex_read
    }

    /// Time spent parsing and admitting semantic vertex identities.
    #[must_use]
    pub const fn vertex_admission(self) -> Duration {
        self.vertex_admission
    }

    /// Time spent reading `GraphAr` adjacency/property storage and exporting a C stream.
    #[must_use]
    pub const fn edge_storage_read(self) -> Duration {
        self.edge_storage_read
    }

    /// Time spent importing exported C Stream batches into Rust Arrow arrays.
    ///
    /// Arrow buffer ownership crosses the ABI here without copying the buffers.
    #[must_use]
    pub const fn arrow_c_stream_import(self) -> Duration {
        self.arrow_c_stream_import
    }

    /// Time spent decoding persisted edge fields into immutable MRR facts.
    #[must_use]
    pub const fn fact_preparation(self) -> Duration {
        self.fact_preparation
    }

    /// Time spent decoding persisted canonical fact identities.
    #[must_use]
    pub const fn fact_identity_decode(self) -> Duration {
        self.fact_identity_decode
    }

    /// Time spent resolving endpoints and materializing immutable MRR facts.
    #[must_use]
    pub const fn fact_materialization(self) -> Duration {
        self.fact_materialization
    }

    /// Time spent canonically ordering facts and rejecting duplicate identities.
    #[must_use]
    pub const fn fact_ordering(self) -> Duration {
        self.fact_ordering
    }

    /// Time spent validating prepared MRR facts through their relation projection.
    #[must_use]
    pub const fn fact_admission(self) -> Duration {
        self.fact_admission
    }
}

impl GraphArDataset {
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    #[must_use]
    pub const fn vertex_count(&self) -> usize {
        self.vertex_count
    }

    #[must_use]
    pub fn facts(&self) -> &[Fact] {
        &self.facts
    }
}

/// Fail-closed errors from semantic `GraphAr` import.
#[derive(Debug)]
pub enum GraphArReadError {
    MissingProperty(&'static str),
    InvalidIdentity {
        property: &'static str,
        value: String,
        reason: String,
    },
    InvalidDiscriminator {
        property: &'static str,
        value: String,
    },
    DuplicatePhysicalVertex(i64),
    NonDensePhysicalVertex {
        expected: usize,
        actual: i64,
    },
    DuplicateEntity(EntityId),
    UnknownPhysicalEndpoint {
        role: &'static str,
        id: i64,
    },
    DuplicateFact(FactId),
    PredicateMismatch {
        expected: String,
        actual: String,
    },
    UnexpectedInvalidation(FactId),
    Context(RelationContextError),
    Projection(GraphProjectionError),
    InvalidArrowColumn {
        index: usize,
        expected: &'static str,
    },
    ArrowStream(String),
    Native(graphar_rs::Error),
}

impl fmt::Display for GraphArReadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Native(error) => write!(formatter, "native GraphAr: {error}"),
            other => write!(formatter, "{other:?}"),
        }
    }
}

impl Error for GraphArReadError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Projection(error) => Some(error),
            Self::Native(error) => Some(error),
            _ => None,
        }
    }
}

impl From<graphar_rs::Error> for GraphArReadError {
    fn from(error: graphar_rs::Error) -> Self {
        Self::Native(error)
    }
}

impl From<GraphProjectionError> for GraphArReadError {
    fn from(error: GraphProjectionError) -> Self {
        Self::Projection(error)
    }
}

/// Reads native `GraphAr` structures and re-admits every row through its MRR owner.
///
/// Physical endpoint IDs are resolved only through the persisted semantic
/// `EntityId` vertex property. All identity and context fields are parsed into
/// their MRR V1 types before [`BinaryEntityProjection`] validates the rebuilt
/// fact.
///
/// # Errors
///
/// Returns a typed error for resource-budget rejection, malformed or missing
/// properties, physical/semantic identity ambiguity, invalid context, or a fact
/// that is not admitted by `projection`.
pub fn read_graphar_dataset(
    root: impl AsRef<Path>,
    projection: &BinaryEntityProjection,
    limits: GraphArReadLimits,
) -> Result<GraphArDataset, GraphArReadError> {
    read_graphar_dataset_observed(root, projection, limits).map(|(dataset, _timings)| dataset)
}

/// Reads and semantically admits a native `GraphAr` dataset with typed phase timings.
///
/// The timings are intended for ASP Rust Scenario observations. They retain the
/// boundary between upstream Arrow chunk I/O and MRR-owned semantic admission.
///
/// # Errors
///
/// Returns the same fail-closed errors as [`read_graphar_dataset`].
pub fn read_graphar_dataset_observed(
    root: impl AsRef<Path>,
    projection: &BinaryEntityProjection,
    limits: GraphArReadLimits,
) -> Result<(GraphArDataset, GraphArReadTimings), GraphArReadError> {
    let prepared = prepare_graphar_source(root, limits)?;
    let prepare_timings = prepared.timings();
    let (dataset, fact_admission) = prepared.admit_observed(projection)?;

    Ok((
        dataset,
        GraphArReadTimings {
            graph_info: prepare_timings.graph_info(),
            native_vertex_read: prepare_timings.native_vertex_read(),
            vertex_admission: prepare_timings.vertex_admission(),
            edge_storage_read: prepare_timings.edge_storage_read(),
            arrow_c_stream_import: prepare_timings.arrow_c_stream_import(),
            fact_identity_decode: prepare_timings.fact_identity_decode(),
            fact_materialization: prepare_timings.fact_materialization(),
            fact_ordering: prepare_timings.fact_ordering(),
            fact_preparation: prepare_timings.fact_preparation(),
            fact_admission,
        },
    ))
}

struct AdmittedVertices {
    physical_entities: Vec<EntityId>,
    count: usize,
    native_read: Duration,
    semantic_admission: Duration,
}

fn read_and_admit_vertices(
    graph_info: &GraphInfo,
    max_vertices: usize,
) -> Result<AdmittedVertices, GraphArReadError> {
    let vertex_properties = property_names(&VERTEX_PROPERTIES);
    let native_vertex_started = Instant::now();
    let vertices =
        read_vertex_string_batch(graph_info, ENTITY_TYPE, &vertex_properties, max_vertices)?;
    let native_read = native_vertex_started.elapsed();
    let vertex_admission_started = Instant::now();
    let mut physical_entities = Vec::with_capacity(vertices.row_count());
    let mut semantic_entities = HashSet::with_capacity(vertices.row_count());
    for row in 0..vertices.row_count() {
        let physical_id = vertices
            .id(row)
            .ok_or(GraphArReadError::MissingProperty("vertex_id"))?;
        let entity = parse_identity(
            "entity_id",
            required_value(vertices.value(row, 0), "entity_id")?,
        )?;
        if usize::try_from(physical_id).ok() != Some(row) {
            if usize::try_from(physical_id).is_ok_and(|id| id < physical_entities.len()) {
                return Err(GraphArReadError::DuplicatePhysicalVertex(physical_id));
            }
            return Err(GraphArReadError::NonDensePhysicalVertex {
                expected: row,
                actual: physical_id,
            });
        }
        if !semantic_entities.insert(entity) {
            return Err(GraphArReadError::DuplicateEntity(entity));
        }
        physical_entities.push(entity);
    }
    Ok(AdmittedVertices {
        physical_entities,
        count: vertices.row_count(),
        native_read,
        semantic_admission: vertex_admission_started.elapsed(),
    })
}

struct PreparedEdges {
    values: Vec<RecordBatch>,
    count: usize,
    storage_read: Duration,
    c_stream_import: Duration,
}

fn read_edge_batches(
    graph_info: &GraphInfo,
    max_edges: usize,
) -> Result<PreparedEdges, GraphArReadError> {
    let edge_properties = property_names(&EDGE_PROPERTIES);
    let edge_storage_started = Instant::now();
    let edge_stream = read_edge_arrow_batches(
        graph_info,
        ENTITY_TYPE,
        EDGE_TYPE,
        ENTITY_TYPE,
        AdjListType::UnorderedBySource,
        &edge_properties,
        max_edges,
    )?;
    let edge_storage_elapsed = edge_storage_started.elapsed();
    let c_stream_import_started = Instant::now();
    let edge_batches = edge_stream
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| GraphArReadError::ArrowStream(error.to_string()))?;
    let c_stream_import_elapsed = c_stream_import_started.elapsed();
    let edge_count = edge_batches.iter().map(RecordBatch::num_rows).sum();
    Ok(PreparedEdges {
        values: edge_batches,
        count: edge_count,
        storage_read: edge_storage_elapsed,
        c_stream_import: c_stream_import_elapsed,
    })
}

#[cfg(test)]
pub(crate) fn scan_graphar_edge_chunks(
    root: impl AsRef<Path>,
    limits: GraphArReadLimits,
) -> Result<(usize, Duration), GraphArReadError> {
    let graph_info = GraphInfo::load(root.as_ref().join(GRAPH_INFO_FILE))?;
    let edge_properties = property_names(&EDGE_PROPERTIES);
    let started = Instant::now();
    let row_count = scan_edge_arrow_chunks(
        &graph_info,
        ENTITY_TYPE,
        EDGE_TYPE,
        ENTITY_TYPE,
        AdjListType::UnorderedBySource,
        &edge_properties,
        limits.max_edges,
    )?;
    Ok((row_count, started.elapsed()))
}

fn property_names<const N: usize>(properties: &[&str; N]) -> Vec<String> {
    properties
        .iter()
        .map(|property| (*property).to_owned())
        .collect()
}

fn required_value<'a>(
    value: Option<&'a str>,
    property: &'static str,
) -> Result<&'a str, GraphArReadError> {
    value.ok_or(GraphArReadError::MissingProperty(property))
}

fn parse_identity<T>(property: &'static str, value: &str) -> Result<T, GraphArReadError>
where
    T: FromStr,
    T::Err: fmt::Display,
{
    value
        .parse()
        .map_err(|error: T::Err| GraphArReadError::InvalidIdentity {
            property,
            value: value.to_owned(),
            reason: error.to_string(),
        })
}

struct EdgeArrowColumns<'a> {
    source: &'a Int64Array,
    destination: &'a Int64Array,
    properties: Vec<Utf8Column<'a>>,
}

impl<'a> EdgeArrowColumns<'a> {
    fn try_new(batch: &'a RecordBatch) -> Result<Self, GraphArReadError> {
        if batch.num_columns() != EDGE_PROPERTIES.len() + 2 {
            return Err(GraphArReadError::InvalidArrowColumn {
                index: batch.num_columns(),
                expected: "source, destination, and all requested edge properties",
            });
        }
        let source = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or(GraphArReadError::InvalidArrowColumn {
                index: 0,
                expected: "non-null int64 source endpoint",
            })?;
        let destination = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or(GraphArReadError::InvalidArrowColumn {
                index: 1,
                expected: "non-null int64 destination endpoint",
            })?;
        let properties = batch.columns()[2..]
            .iter()
            .enumerate()
            .map(|(offset, column)| Utf8Column::try_new(column.as_ref(), offset + 2))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            source,
            destination,
            properties,
        })
    }

    fn source(&self, row: usize) -> Result<i64, GraphArReadError> {
        required_endpoint(self.source, row, "source")
    }

    fn destination(&self, row: usize) -> Result<i64, GraphArReadError> {
        required_endpoint(self.destination, row, "destination")
    }
}

fn required_endpoint(
    column: &Int64Array,
    row: usize,
    property: &'static str,
) -> Result<i64, GraphArReadError> {
    (!column.is_null(row))
        .then(|| column.value(row))
        .ok_or(GraphArReadError::MissingProperty(property))
}

enum Utf8Column<'a> {
    Utf8(&'a StringArray),
    LargeUtf8(&'a LargeStringArray),
}

impl<'a> Utf8Column<'a> {
    fn try_new(column: &'a dyn Array, index: usize) -> Result<Self, GraphArReadError> {
        if let Some(column) = column.as_any().downcast_ref::<StringArray>() {
            return Ok(Self::Utf8(column));
        }
        if let Some(column) = column.as_any().downcast_ref::<LargeStringArray>() {
            return Ok(Self::LargeUtf8(column));
        }
        Err(GraphArReadError::InvalidArrowColumn {
            index,
            expected: "UTF-8 or large UTF-8 property",
        })
    }

    fn value(&self, row: usize) -> Option<&'a str> {
        match self {
            Self::Utf8(column) => (!column.is_null(row)).then(|| column.value(row)),
            Self::LargeUtf8(column) => (!column.is_null(row)).then(|| column.value(row)),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct ContextKey<'a> {
    generation: &'a str,
    authority_kind: &'a str,
    authority_id: &'a str,
    provenance_kind: &'a str,
    provenance_id: &'a str,
    completeness: &'a str,
    validity_kind: &'a str,
    invalidated_by: Option<&'a str>,
}

fn parse_context(
    key: ContextKey<'_>,
    fact_id: FactId,
) -> Result<RelationContext, GraphArReadError> {
    let generation = parse_identity::<GenerationId>("generation_id", key.generation)?;
    let authority = parse_authority(key.authority_kind, key.authority_id)?;
    let provenance = parse_provenance(key.provenance_kind, key.provenance_id)?;
    let completeness = parse_completeness(key.completeness)?;
    let validity = parse_validity(key.validity_kind, key.invalidated_by, fact_id)?;
    RelationContext::new(generation, authority, provenance, completeness, validity)
        .map_err(GraphArReadError::Context)
}

fn endpoint(
    entities: &[EntityId],
    role: &'static str,
    id: i64,
) -> Result<EntityId, GraphArReadError> {
    usize::try_from(id)
        .ok()
        .and_then(|id| entities.get(id))
        .copied()
        .ok_or(GraphArReadError::UnknownPhysicalEndpoint { role, id })
}

fn parse_authority(kind: &str, id: &str) -> Result<RelationAuthority, GraphArReadError> {
    match kind {
        "entity" => parse_identity("authority_id", id).map(RelationAuthority::Entity),
        "rule" => parse_identity::<RuleId>("authority_id", id).map(RelationAuthority::Rule),
        "rule_pack" => {
            parse_identity::<RulePackId>("authority_id", id).map(RelationAuthority::RulePack)
        }
        _ => Err(GraphArReadError::InvalidDiscriminator {
            property: "authority_kind",
            value: kind.to_owned(),
        }),
    }
}

fn parse_provenance(kind: &str, id: &str) -> Result<FactProvenance, GraphArReadError> {
    match kind {
        "source" => parse_identity("provenance_id", id).map(FactProvenance::Source),
        "derivation" => {
            parse_identity::<DerivationId>("provenance_id", id).map(FactProvenance::Derivation)
        }
        _ => Err(GraphArReadError::InvalidDiscriminator {
            property: "provenance_kind",
            value: kind.to_owned(),
        }),
    }
}

fn parse_completeness(value: &str) -> Result<EvidenceCompleteness, GraphArReadError> {
    match value {
        "complete" => Ok(EvidenceCompleteness::Complete),
        "partial" => Ok(EvidenceCompleteness::Partial),
        "unknown" => Ok(EvidenceCompleteness::Unknown),
        _ => Err(GraphArReadError::InvalidDiscriminator {
            property: "completeness",
            value: value.to_owned(),
        }),
    }
}

fn parse_validity(
    validity_kind: &str,
    invalidated_by: Option<&str>,
    fact_id: FactId,
) -> Result<FactValidity, GraphArReadError> {
    match validity_kind {
        "valid" => {
            if invalidated_by.is_some() {
                return Err(GraphArReadError::UnexpectedInvalidation(fact_id));
            }
            Ok(FactValidity::Valid)
        }
        "invalidated_by" => parse_identity(
            "invalidated_by",
            invalidated_by.ok_or(GraphArReadError::MissingProperty("invalidated_by"))?,
        )
        .map(FactValidity::InvalidatedBy),
        value => Err(GraphArReadError::InvalidDiscriminator {
            property: "validity_kind",
            value: value.to_owned(),
        }),
    }
}
