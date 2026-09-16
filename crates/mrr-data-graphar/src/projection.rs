use std::{collections::BTreeSet, error::Error, fmt};

use meta_relational_reasoning::{
    EntityId, EvidenceCompleteness, Fact, FactId, FactProvenance, FactValidity, GenerationId,
    RelationAuthority, RelationError, RelationId, RelationSchema, Value, ValueSchema,
};

/// A validated binary-Entity relation that can be projected as `GraphAr` edges.
///
/// Field order is semantic: field zero is the source endpoint and field one is
/// the destination endpoint. No endpoint role is guessed from field names.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinaryEntityProjection {
    relation: RelationSchema,
}

impl BinaryEntityProjection {
    /// Admits exactly two non-null Entity fields.
    ///
    /// # Errors
    ///
    /// Returns a typed error for invalid relation schemas, non-binary arity,
    /// nullable endpoints, or endpoint types other than `Entity`.
    pub fn admit(relation: &RelationSchema) -> Result<Self, GraphProjectionError> {
        relation
            .validate()
            .map_err(GraphProjectionError::InvalidRelation)?;
        if relation.fields().len() != 2 {
            return Err(GraphProjectionError::UnsupportedArity {
                actual: relation.fields().len(),
            });
        }
        for field in relation.fields() {
            if field.nullable() {
                return Err(GraphProjectionError::NullableEndpoint {
                    field: field.name().to_owned(),
                });
            }
            if field.schema() != &ValueSchema::Entity {
                return Err(GraphProjectionError::UnsupportedEndpoint {
                    field: field.name().to_owned(),
                    schema: field.schema().clone(),
                });
            }
        }
        Ok(Self {
            relation: relation.clone(),
        })
    }

    /// The admitted source endpoint field name.
    #[must_use]
    pub fn source_field(&self) -> &str {
        self.relation.fields()[0].name()
    }

    /// The admitted destination endpoint field name.
    #[must_use]
    pub fn destination_field(&self) -> &str {
        self.relation.fields()[1].name()
    }

    /// The MRR relation identity that owns every projected edge.
    #[must_use]
    pub const fn relation_id(&self) -> RelationId {
        self.relation.id()
    }

    /// The relation predicate; an upstream adapter may use it as an edge label.
    #[must_use]
    pub fn predicate(&self) -> &str {
        self.relation.predicate()
    }

    /// Validates and projects an MRR fact without assigning physical IDs.
    ///
    /// # Errors
    ///
    /// Returns a typed error when the fact violates the admitted relation.
    pub fn project(&self, fact: &Fact) -> Result<GraphEdgeRecord, GraphProjectionError> {
        self.relation
            .validate_fact(fact)
            .map_err(|error| GraphProjectionError::InvalidFact {
                fact: fact.id(),
                error,
            })?;
        let [Value::Entity(source), Value::Entity(destination)] = fact.values() else {
            return Err(GraphProjectionError::InvariantViolation);
        };
        let context = fact.context();
        Ok(GraphEdgeRecord {
            source: *source,
            destination: *destination,
            fact_id: fact.id(),
            relation_id: fact.relation(),
            generation_id: context.generation(),
            authority: context.authority(),
            provenance: context.provenance(),
            completeness: context.completeness(),
            validity: context.validity(),
        })
    }
}

/// One semantic edge record ready for an upstream `GraphAr` adapter.
///
/// `GraphAr` row indices, chunk positions, and adjacency offsets are deliberately
/// absent: repartitioning must not change any field in this record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GraphEdgeRecord {
    source: EntityId,
    destination: EntityId,
    fact_id: FactId,
    relation_id: RelationId,
    generation_id: GenerationId,
    authority: RelationAuthority,
    provenance: FactProvenance,
    completeness: EvidenceCompleteness,
    validity: FactValidity,
}

impl GraphEdgeRecord {
    #[must_use]
    pub const fn source(&self) -> EntityId {
        self.source
    }

    #[must_use]
    pub const fn destination(&self) -> EntityId {
        self.destination
    }

    #[must_use]
    pub const fn fact_id(&self) -> FactId {
        self.fact_id
    }

    #[must_use]
    pub const fn relation_id(&self) -> RelationId {
        self.relation_id
    }

    #[must_use]
    pub const fn generation_id(&self) -> GenerationId {
        self.generation_id
    }

    #[must_use]
    pub const fn authority(&self) -> RelationAuthority {
        self.authority
    }

    #[must_use]
    pub const fn provenance(&self) -> FactProvenance {
        self.provenance
    }

    #[must_use]
    pub const fn completeness(&self) -> EvidenceCompleteness {
        self.completeness
    }

    #[must_use]
    pub const fn validity(&self) -> FactValidity {
        self.validity
    }
}

/// A deterministic dense mapping between semantic entities and `GraphAr` vertex IDs.
///
/// `GraphAr` internal IDs are physical row identifiers. They are derived from the
/// sorted set of edge endpoints and never replace the semantic [`EntityId`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalVertexIndex {
    entities: Vec<EntityId>,
}

impl PhysicalVertexIndex {
    /// Builds an order- and partition-independent index for an edge set.
    #[must_use]
    pub fn from_edges(edges: &[GraphEdgeRecord]) -> Self {
        let entities = edges
            .iter()
            .flat_map(|edge| [edge.source(), edge.destination()])
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        Self { entities }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.entities.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entities.is_empty()
    }

    /// Resolves a semantic entity to its dense physical vertex ID.
    #[must_use]
    pub fn internal_id(&self, entity: EntityId) -> Option<i64> {
        self.entities
            .binary_search(&entity)
            .ok()
            .and_then(|index| i64::try_from(index).ok())
    }

    /// Resolves a physical vertex ID back to its semantic entity.
    #[must_use]
    pub fn entity_id(&self, internal_id: i64) -> Option<EntityId> {
        usize::try_from(internal_id)
            .ok()
            .and_then(|index| self.entities.get(index).copied())
    }

    /// Assigns physical endpoints while retaining the complete semantic edge.
    ///
    /// # Errors
    ///
    /// Returns a typed error if the edge was not part of the indexed endpoint set.
    pub fn index_edge(
        &self,
        edge: GraphEdgeRecord,
    ) -> Result<IndexedGraphEdge, GraphProjectionError> {
        let source = self
            .internal_id(edge.source())
            .ok_or(GraphProjectionError::UnknownEntity(edge.source()))?;
        let destination = self
            .internal_id(edge.destination())
            .ok_or(GraphProjectionError::UnknownEntity(edge.destination()))?;
        Ok(IndexedGraphEdge {
            source,
            destination,
            semantic: edge,
        })
    }
}

/// A GraphAr-ready pair of physical endpoints plus its semantic source record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IndexedGraphEdge {
    source: i64,
    destination: i64,
    semantic: GraphEdgeRecord,
}

impl IndexedGraphEdge {
    #[must_use]
    pub const fn source(&self) -> i64 {
        self.source
    }

    #[must_use]
    pub const fn destination(&self) -> i64 {
        self.destination
    }

    #[must_use]
    pub const fn semantic(&self) -> GraphEdgeRecord {
        self.semantic
    }
}

/// Fail-closed binary-Entity projection errors.
#[derive(Debug, Eq, PartialEq)]
pub enum GraphProjectionError {
    InvalidRelation(RelationError),
    UnsupportedArity { actual: usize },
    NullableEndpoint { field: String },
    UnsupportedEndpoint { field: String, schema: ValueSchema },
    InvalidFact { fact: FactId, error: RelationError },
    UnknownEntity(EntityId),
    InvariantViolation,
}

impl fmt::Display for GraphProjectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRelation(error) => write!(formatter, "invalid relation: {error:?}"),
            Self::UnsupportedArity { actual } => {
                write!(
                    formatter,
                    "binary-Entity projection requires arity 2, got {actual}"
                )
            }
            Self::NullableEndpoint { field } => {
                write!(formatter, "GraphAr endpoint `{field}` cannot be nullable")
            }
            Self::UnsupportedEndpoint { field, schema } => {
                write!(
                    formatter,
                    "GraphAr endpoint `{field}` must be Entity, got {schema:?}"
                )
            }
            Self::InvalidFact { fact, error } => {
                write!(
                    formatter,
                    "fact {fact} violates the admitted relation: {error:?}"
                )
            }
            Self::UnknownEntity(entity) => {
                write!(
                    formatter,
                    "entity {entity} is absent from the physical index"
                )
            }
            Self::InvariantViolation => formatter
                .write_str("admitted binary-Entity relation produced a non-Entity endpoint"),
        }
    }
}

impl Error for GraphProjectionError {}
