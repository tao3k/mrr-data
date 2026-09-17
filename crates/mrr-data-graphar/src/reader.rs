use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
    path::{Path, PathBuf},
    str::FromStr,
};

use graphar_rs::{
    info::{AdjListType, GraphInfo},
    reader::{read_edge_strings, read_vertex_strings},
};
use meta_relational_reasoning::{
    DerivationId, EntityId, EvidenceCompleteness, Fact, FactId, FactProvenance, FactValidity,
    RelationAuthority, RelationContext, RelationContextError, RuleId, RulePackId, Value,
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
    facts: Vec<Fact>,
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
    let root = root.as_ref();
    let graph_info = GraphInfo::load(root.join(GRAPH_INFO_FILE))?;
    let vertex_properties = property_names(&VERTEX_PROPERTIES);
    let vertices = read_vertex_strings(
        &graph_info,
        ENTITY_TYPE,
        &vertex_properties,
        limits.max_vertices,
    )?;
    let mut physical_entities = BTreeMap::new();
    let mut semantic_entities = BTreeSet::new();
    for vertex in &vertices {
        let entity = parse_identity("entity_id", required(vertex.values(), 0, "entity_id")?)?;
        if physical_entities.insert(vertex.id(), entity).is_some() {
            return Err(GraphArReadError::DuplicatePhysicalVertex(vertex.id()));
        }
        if !semantic_entities.insert(entity) {
            return Err(GraphArReadError::DuplicateEntity(entity));
        }
    }

    let edge_properties = property_names(&EDGE_PROPERTIES);
    let edges = read_edge_strings(
        &graph_info,
        ENTITY_TYPE,
        EDGE_TYPE,
        ENTITY_TYPE,
        AdjListType::UnorderedBySource,
        &edge_properties,
        limits.max_edges,
    )?;
    let mut facts = Vec::with_capacity(edges.len());
    let mut fact_ids = BTreeSet::new();
    for edge in &edges {
        let source = endpoint(&physical_entities, "source", edge.source())?;
        let destination = endpoint(&physical_entities, "destination", edge.destination())?;
        let values = edge.values();
        let fact_id = parse_identity("fact_id", required(values, 0, "fact_id")?)?;
        if !fact_ids.insert(fact_id) {
            return Err(GraphArReadError::DuplicateFact(fact_id));
        }
        let relation_id = parse_identity("relation_id", required(values, 1, "relation_id")?)?;
        let predicate = required(values, 2, "predicate")?;
        if predicate != projection.predicate() {
            return Err(GraphArReadError::PredicateMismatch {
                expected: projection.predicate().to_owned(),
                actual: predicate.to_owned(),
            });
        }
        let generation = parse_identity("generation_id", required(values, 3, "generation_id")?)?;
        let authority = parse_authority(
            required(values, 4, "authority_kind")?,
            required(values, 5, "authority_id")?,
        )?;
        let provenance = parse_provenance(
            required(values, 6, "provenance_kind")?,
            required(values, 7, "provenance_id")?,
        )?;
        let completeness = parse_completeness(required(values, 8, "completeness")?)?;
        let validity = parse_validity(values, fact_id)?;
        let context =
            RelationContext::new(generation, authority, provenance, completeness, validity)
                .map_err(GraphArReadError::Context)?;
        let fact = Fact::new(
            fact_id,
            relation_id,
            vec![Value::Entity(source), Value::Entity(destination)],
            context,
        );
        projection.project(&fact)?;
        facts.push(fact);
    }

    Ok(GraphArDataset {
        root: root.to_path_buf(),
        vertex_count: vertices.len(),
        facts,
    })
}

fn property_names<const N: usize>(properties: &[&str; N]) -> Vec<String> {
    properties
        .iter()
        .map(|property| (*property).to_owned())
        .collect()
}

fn required<'a>(
    values: &'a [Option<String>],
    index: usize,
    property: &'static str,
) -> Result<&'a str, GraphArReadError> {
    values
        .get(index)
        .and_then(Option::as_deref)
        .ok_or(GraphArReadError::MissingProperty(property))
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

fn endpoint(
    entities: &BTreeMap<i64, EntityId>,
    role: &'static str,
    id: i64,
) -> Result<EntityId, GraphArReadError> {
    entities
        .get(&id)
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
    values: &[Option<String>],
    fact_id: FactId,
) -> Result<FactValidity, GraphArReadError> {
    let invalidated_by = values
        .get(10)
        .and_then(Option::as_deref)
        .filter(|value| !value.is_empty());
    match required(values, 9, "validity_kind")? {
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
