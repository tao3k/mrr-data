use std::{
    collections::BTreeSet,
    env,
    error::Error,
    fmt, fs, io,
    path::{Path, PathBuf},
};

use graphar_rs::{
    builder::{Edge, EdgesBuilder, Vertex, VerticesBuilder},
    info::{AdjListType, AdjacentList, EdgeInfo, GraphInfo, InfoVersion, VertexInfo},
    property::{Property, PropertyGroup, PropertyVec},
    types::{Cardinality, DataType, FileType},
};
use meta_relational_reasoning::{
    EvidenceCompleteness, FactId, FactProvenance, FactValidity, RelationAuthority, RelationId,
};

use crate::{BinaryEntityProjection, GraphEdgeRecord, GraphProjectionError, PhysicalVertexIndex};

pub(crate) const ENTITY_TYPE: &str = "entity";
pub(crate) const EDGE_TYPE: &str = "mrr_relation";
const VERTEX_INFO_FILE: &str = "entity.vertex.yaml";
const EDGE_INFO_FILE: &str = "entity_mrr_relation_entity.edge.yaml";
pub(crate) const GRAPH_INFO_FILE: &str = "mrr.graph.yaml";

/// Receipt for one dataset written by the project-maintained `GraphAr` runtime.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphArDatasetReceipt {
    root: PathBuf,
    vertex_count: usize,
    edge_count: usize,
}

impl GraphArDatasetReceipt {
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    #[must_use]
    pub const fn vertex_count(&self) -> usize {
        self.vertex_count
    }

    #[must_use]
    pub const fn edge_count(&self) -> usize {
        self.edge_count
    }

    #[must_use]
    pub fn graph_info_path(&self) -> PathBuf {
        self.root.join(GRAPH_INFO_FILE)
    }
}

/// Fail-closed errors from the native `GraphAr` adapter.
#[derive(Debug)]
pub enum GraphArWriteError {
    OutputExists(PathBuf),
    NonUtf8Path(PathBuf),
    VertexCountOverflow,
    RelationMismatch {
        fact: FactId,
        expected: RelationId,
        actual: RelationId,
    },
    DuplicateFact(FactId),
    Io {
        operation: &'static str,
        kind: io::ErrorKind,
    },
    Projection(GraphProjectionError),
    Native(graphar_rs::Error),
}

impl GraphArWriteError {
    fn io(operation: &'static str, error: &io::Error) -> Self {
        Self::Io {
            operation,
            kind: error.kind(),
        }
    }
}

impl fmt::Display for GraphArWriteError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Native(error) => write!(formatter, "native GraphAr: {error}"),
            other => write!(formatter, "{other:?}"),
        }
    }
}

impl Error for GraphArWriteError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Projection(error) => Some(error),
            Self::Native(error) => Some(error),
            _ => None,
        }
    }
}

impl From<GraphProjectionError> for GraphArWriteError {
    fn from(error: GraphProjectionError) -> Self {
        Self::Projection(error)
    }
}

/// Writes one admitted binary-Entity relation through native `graphar-rs`.
///
/// The destination must not exist. Data and metadata are first written to a
/// sibling staging directory, then renamed into place as one filesystem commit.
///
/// # Errors
///
/// Returns a typed error for an existing output, filesystem failure, physical
/// index failure, vertex-count overflow, or upstream `GraphAr` rejection.
pub fn write_graphar_dataset(
    output: impl AsRef<Path>,
    projection: &BinaryEntityProjection,
    edges: &[GraphEdgeRecord],
) -> Result<GraphArDatasetReceipt, GraphArWriteError> {
    let output = output.as_ref();
    validate_batch(projection, edges)?;
    match fs::symlink_metadata(output) {
        Ok(_) => return Err(GraphArWriteError::OutputExists(output.to_path_buf())),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(GraphArWriteError::io("inspect output", &error)),
    }
    let parent = output.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).map_err(|error| GraphArWriteError::io("create parent", &error))?;
    let staging = tempfile::Builder::new()
        .prefix(".mrr-data-graphar-")
        .tempdir_in(parent)
        .map_err(|error| GraphArWriteError::io("create staging directory", &error))?;
    let committed_root = if output.is_absolute() {
        output.to_path_buf()
    } else {
        env::current_dir()
            .map_err(|error| GraphArWriteError::io("resolve current directory", &error))?
            .join(output)
    };
    let committed_root = committed_root
        .to_str()
        .ok_or_else(|| GraphArWriteError::NonUtf8Path(committed_root.clone()))?;
    let committed_prefix = format!("{committed_root}/");

    let (vertex_count, edge_count) =
        write_staged(staging.path(), &committed_prefix, projection, edges)?;
    let staged_path = staging.keep();
    if let Err(error) = fs::rename(&staged_path, output) {
        let _ = fs::remove_dir_all(&staged_path);
        return Err(GraphArWriteError::io("commit dataset", &error));
    }
    Ok(GraphArDatasetReceipt {
        root: output.to_path_buf(),
        vertex_count,
        edge_count,
    })
}

fn validate_batch(
    projection: &BinaryEntityProjection,
    edges: &[GraphEdgeRecord],
) -> Result<(), GraphArWriteError> {
    let mut facts = BTreeSet::new();
    for edge in edges {
        if edge.relation_id() != projection.relation_id() {
            return Err(GraphArWriteError::RelationMismatch {
                fact: edge.fact_id(),
                expected: projection.relation_id(),
                actual: edge.relation_id(),
            });
        }
        if !facts.insert(edge.fact_id()) {
            return Err(GraphArWriteError::DuplicateFact(edge.fact_id()));
        }
    }
    Ok(())
}

fn write_staged(
    root: &Path,
    committed_prefix: &str,
    projection: &BinaryEntityProjection,
    edges: &[GraphEdgeRecord],
) -> Result<(usize, usize), GraphArWriteError> {
    let index = PhysicalVertexIndex::from_edges(edges);
    let vertex_count =
        i64::try_from(index.len()).map_err(|_| GraphArWriteError::VertexCountOverflow)?;
    let root_string = root
        .to_str()
        .ok_or_else(|| GraphArWriteError::NonUtf8Path(root.to_path_buf()))?;
    let prefix = format!("{root_string}/");
    let version = InfoVersion::new(1).map_err(upstream)?;
    let vertex_info = vertex_info(version.clone())?;
    let edge_info = edge_info(version.clone())?;

    let mut vertices = VerticesBuilder::try_new(&vertex_info, &prefix, 0).map_err(upstream)?;
    for entity in index.entities() {
        let mut vertex = Vertex::new();
        vertex.add_property_string("entity_id", entity.to_string());
        vertices.add_vertex(vertex).map_err(upstream)?;
    }
    vertices.dump().map_err(upstream)?;

    let mut edge_builder = EdgesBuilder::try_new(
        &edge_info,
        &prefix,
        AdjListType::UnorderedBySource,
        vertex_count,
    )
    .map_err(upstream)?;
    for edge in edges {
        let indexed = index.index_edge(*edge)?;
        let mut physical =
            Edge::try_new(indexed.source(), indexed.destination()).map_err(upstream)?;
        add_edge_properties(&mut physical, projection, edge);
        edge_builder.add_edge(physical).map_err(upstream)?;
    }
    edge_builder.dump().map_err(upstream)?;

    vertex_info
        .save(root.join(VERTEX_INFO_FILE))
        .map_err(upstream)?;
    edge_info
        .save(root.join(EDGE_INFO_FILE))
        .map_err(upstream)?;
    GraphInfo::builder("mrr_data")
        .push_vertex_info(vertex_info)
        .push_edge_info(edge_info)
        .prefix(committed_prefix)
        .version(version)
        .try_build()
        .map_err(upstream)?
        .save(root.join(GRAPH_INFO_FILE))
        .map_err(upstream)?;

    Ok((index.len(), edges.len()))
}

fn vertex_info(version: InfoVersion) -> Result<VertexInfo, GraphArWriteError> {
    let group = property_group(
        [Property::new(
            "entity_id",
            DataType::string(),
            true,
            false,
            Cardinality::Single,
        )],
        "properties/",
    );
    VertexInfo::builder(ENTITY_TYPE, 1024)
        .push_property_group(group)
        .prefix("vertex/entity/")
        .version(version)
        .try_build()
        .map_err(upstream)
}

fn edge_info(version: InfoVersion) -> Result<EdgeInfo, GraphArWriteError> {
    let names = [
        ("fact_id", true, false),
        ("relation_id", false, false),
        ("predicate", false, false),
        ("generation_id", false, false),
        ("authority_kind", false, false),
        ("authority_id", false, false),
        ("provenance_kind", false, false),
        ("provenance_id", false, false),
        ("completeness", false, false),
        ("validity_kind", false, false),
        ("invalidated_by", false, true),
    ];
    let group = property_group(
        names.map(|(name, primary, nullable)| {
            Property::new(
                name,
                DataType::string(),
                primary,
                nullable,
                Cardinality::Single,
            )
        }),
        "properties/",
    );
    EdgeInfo::builder(ENTITY_TYPE, EDGE_TYPE, ENTITY_TYPE, 1024, 1024, 1024)
        .directed(true)
        .push_adjacent_list(AdjacentList::new(
            AdjListType::UnorderedBySource,
            FileType::Parquet,
            Some("unordered_by_source/"),
        ))
        .push_property_group(group)
        .prefix("edge/entity_mrr_relation_entity/")
        .version(version)
        .try_build()
        .map_err(upstream)
}

fn property_group<const N: usize>(
    properties: [Property; N],
    prefix: &str,
) -> graphar_rs::property::PropertyGroup {
    let mut values = PropertyVec::new();
    for property in properties {
        values.push(property);
    }
    PropertyGroup::new(values, FileType::Parquet, prefix)
}

fn add_edge_properties(
    output: &mut Edge,
    projection: &BinaryEntityProjection,
    edge: &GraphEdgeRecord,
) {
    output.add_property_string("fact_id", edge.fact_id().to_string());
    output.add_property_string("relation_id", edge.relation_id().to_string());
    output.add_property_string("predicate", projection.predicate());
    output.add_property_string("generation_id", edge.generation_id().to_string());

    let (authority_kind, authority_id) = match edge.authority() {
        RelationAuthority::Entity(id) => ("entity", id.to_string()),
        RelationAuthority::Rule(id) => ("rule", id.to_string()),
        RelationAuthority::RulePack(id) => ("rule_pack", id.to_string()),
    };
    output.add_property_string("authority_kind", authority_kind);
    output.add_property_string("authority_id", authority_id);

    let (provenance_kind, provenance_id) = match edge.provenance() {
        FactProvenance::Source(id) => ("source", id.to_string()),
        FactProvenance::Derivation(id) => ("derivation", id.to_string()),
    };
    output.add_property_string("provenance_kind", provenance_kind);
    output.add_property_string("provenance_id", provenance_id);

    let completeness = match edge.completeness() {
        EvidenceCompleteness::Complete => "complete",
        EvidenceCompleteness::Partial => "partial",
        EvidenceCompleteness::Unknown => "unknown",
    };
    output.add_property_string("completeness", completeness);

    match edge.validity() {
        FactValidity::Valid => output.add_property_string("validity_kind", "valid"),
        FactValidity::InvalidatedBy(id) => {
            output.add_property_string("validity_kind", "invalidated_by");
            output.add_property_string("invalidated_by", id.to_string());
        }
    }
}

fn upstream(error: graphar_rs::Error) -> GraphArWriteError {
    GraphArWriteError::Native(error)
}
