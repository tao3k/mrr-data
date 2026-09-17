//! Prepared `GraphAr` source lifecycle and projection-owned re-admission.

use std::{
    collections::{HashMap, hash_map::Entry},
    hash::Hash,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use arrow_array::RecordBatch;
use graphar_rs::info::GraphInfo;
use meta_relational_reasoning::{EntityId, Fact, RelationId, Value};

use super::{
    BinaryEntityProjection, ContextKey, EdgeArrowColumns, GRAPH_INFO_FILE, GraphArDataset,
    GraphArReadError, GraphArReadLimits, endpoint, parse_context, parse_identity,
    read_and_admit_vertices, read_edge_batches, required_value,
};

/// Native `GraphAr` storage decoded into reusable semantic facts.
///
/// Preparing a source performs all metadata, Parquet, and Arrow C Stream work
/// plus projection-independent MRR decoding once. Calling [`Self::admit`]
/// never re-enters the native import closure or reparses persisted identities;
/// it only validates the retained immutable facts against the supplied MRR
/// projection and returns an owned dataset.
#[derive(Clone, Debug)]
pub struct PreparedGraphArSource {
    root: PathBuf,
    vertex_count: usize,
    edge_count: usize,
    facts: Arc<[Fact]>,
    predicates: Arc<[Arc<str>]>,
    timings: GraphArPrepareTimings,
}

#[derive(Clone, Debug)]
struct PreparedFact {
    predicate: Arc<str>,
    value: Fact,
}

/// Measured phases of preparing native `GraphAr` storage for reuse.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GraphArPrepareTimings {
    graph_info: Duration,
    native_vertex_read: Duration,
    vertex_admission: Duration,
    edge_storage_read: Duration,
    arrow_c_stream_import: Duration,
    fact_identity_decode: Duration,
    fact_materialization: Duration,
    fact_ordering: Duration,
    fact_preparation: Duration,
}

impl GraphArPrepareTimings {
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
}

impl PreparedGraphArSource {
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
    pub const fn timings(&self) -> GraphArPrepareTimings {
        self.timings
    }

    /// Re-admits MRR facts exclusively from prepared immutable semantic values.
    ///
    /// # Errors
    ///
    /// Returns a typed error for facts rejected by `projection`.
    pub fn admit(
        &self,
        projection: &BinaryEntityProjection,
    ) -> Result<GraphArDataset, GraphArReadError> {
        self.admit_observed(projection)
            .map(|(dataset, _fact_admission)| dataset)
    }

    /// Admits prepared facts and reports only projection-owned admission time.
    ///
    /// # Errors
    ///
    /// Returns the same fail-closed errors as [`Self::admit`].
    pub fn admit_observed(
        &self,
        projection: &BinaryEntityProjection,
    ) -> Result<(GraphArDataset, Duration), GraphArReadError> {
        let fact_admission = admit_prepared_facts(&self.facts, &self.predicates, projection)?;
        Ok((
            GraphArDataset {
                root: self.root.clone(),
                vertex_count: self.vertex_count,
                facts: Arc::clone(&self.facts),
            },
            fact_admission,
        ))
    }
}

/// Decodes bounded native `GraphAr` storage into reusable immutable MRR facts.
///
/// This is the only public operation that enters the native storage reader.
/// Repeated semantic consumption should call [`PreparedGraphArSource::admit`]
/// instead of preparing the same source again.
///
/// # Errors
///
/// Returns a typed error for resource-budget rejection, malformed identities or
/// contexts, physical/semantic identity ambiguity, Arrow stream failures, or
/// native `GraphAr` errors.
pub fn prepare_graphar_source(
    root: impl AsRef<Path>,
    limits: GraphArReadLimits,
) -> Result<PreparedGraphArSource, GraphArReadError> {
    let root = root.as_ref();
    let graph_info_started = Instant::now();
    let graph_info = GraphInfo::load(root.join(GRAPH_INFO_FILE))?;
    let graph_info_elapsed = graph_info_started.elapsed();
    let vertices = read_and_admit_vertices(&graph_info, limits.max_vertices)?;
    let edges = read_edge_batches(&graph_info, limits.max_edges)?;
    let facts = prepare_facts(&edges.values, edges.count, &vertices.physical_entities)?;

    Ok(PreparedGraphArSource {
        root: root.to_path_buf(),
        vertex_count: vertices.count,
        edge_count: edges.count,
        facts: Arc::from(facts.values),
        predicates: Arc::from(facts.predicates),
        timings: GraphArPrepareTimings {
            graph_info: graph_info_elapsed,
            native_vertex_read: vertices.native_read,
            vertex_admission: vertices.semantic_admission,
            edge_storage_read: edges.storage_read,
            arrow_c_stream_import: edges.c_stream_import,
            fact_identity_decode: facts.identity_decode,
            fact_materialization: facts.materialization,
            fact_ordering: facts.ordering,
            fact_preparation: facts.semantic_preparation,
        },
    })
}

struct PreparedFacts {
    values: Vec<Fact>,
    predicates: Vec<Arc<str>>,
    identity_decode: Duration,
    materialization: Duration,
    ordering: Duration,
    semantic_preparation: Duration,
}

struct RunCache<K, V> {
    last: Option<(K, V)>,
    values: HashMap<K, V>,
}

impl<K, V> RunCache<K, V>
where
    K: Copy + Eq + Hash,
    V: Clone,
{
    fn with_capacity(capacity: usize) -> Self {
        Self {
            last: None,
            values: HashMap::with_capacity(capacity),
        }
    }

    fn get_or_try_insert_with<E>(
        &mut self,
        key: K,
        decode: impl FnOnce(K) -> Result<V, E>,
    ) -> Result<V, E> {
        if let Some((last_key, last_value)) = &self.last
            && *last_key == key
        {
            return Ok(last_value.clone());
        }
        let value = match self.values.entry(key) {
            Entry::Occupied(entry) => entry.get().clone(),
            Entry::Vacant(entry) => entry.insert(decode(key)?).clone(),
        };
        self.last = Some((key, value.clone()));
        Ok(value)
    }
}

fn prepare_facts(
    edge_batches: &[RecordBatch],
    edge_count: usize,
    physical_entities: &[EntityId],
) -> Result<PreparedFacts, GraphArReadError> {
    let fact_preparation_started = Instant::now();
    let columns = edge_batches
        .iter()
        .map(EdgeArrowColumns::try_new)
        .collect::<Result<Vec<_>, _>>()?;
    let identity_decode_started = Instant::now();
    let fact_ids = columns
        .iter()
        .flat_map(|columns| {
            (0..columns.source.len()).map(|row| {
                parse_identity(
                    "fact_id",
                    required_value(columns.properties[0].value(row), "fact_id")?,
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let identity_decode = identity_decode_started.elapsed();
    debug_assert_eq!(fact_ids.len(), edge_count);

    let materialization_started = Instant::now();
    let mut facts = Vec::with_capacity(edge_count);
    let mut relation_ids = RunCache::<&str, RelationId>::with_capacity(1);
    let mut contexts = RunCache::with_capacity(1);
    let mut predicates = RunCache::<&str, Arc<str>>::with_capacity(1);
    let mut fact_ids = fact_ids.into_iter();
    for columns in &columns {
        for row in 0..columns.source.len() {
            let source = endpoint(physical_entities, "source", columns.source(row)?)?;
            let destination =
                endpoint(physical_entities, "destination", columns.destination(row)?)?;
            let fact_id = fact_ids
                .next()
                .expect("decoded fact count matches validated Arrow rows");
            let relation_value = required_value(columns.properties[1].value(row), "relation_id")?;
            let relation_id = relation_ids.get_or_try_insert_with(relation_value, |value| {
                parse_identity("relation_id", value)
            })?;
            let predicate = required_value(columns.properties[2].value(row), "predicate")?;
            let predicate = predicates.get_or_try_insert_with(predicate, |value| {
                Ok::<_, GraphArReadError>(Arc::from(value))
            })?;
            let context_key = ContextKey {
                generation: required_value(columns.properties[3].value(row), "generation_id")?,
                authority_kind: required_value(columns.properties[4].value(row), "authority_kind")?,
                authority_id: required_value(columns.properties[5].value(row), "authority_id")?,
                provenance_kind: required_value(
                    columns.properties[6].value(row),
                    "provenance_kind",
                )?,
                provenance_id: required_value(columns.properties[7].value(row), "provenance_id")?,
                completeness: required_value(columns.properties[8].value(row), "completeness")?,
                validity_kind: required_value(columns.properties[9].value(row), "validity_kind")?,
                invalidated_by: columns.properties[10].value(row),
            };
            let context =
                contexts.get_or_try_insert_with(context_key, |key| parse_context(key, fact_id))?;
            facts.push(PreparedFact {
                predicate,
                value: Fact::new(
                    fact_id,
                    relation_id,
                    vec![Value::Entity(source), Value::Entity(destination)],
                    context,
                ),
            });
        }
    }
    let materialization = materialization_started.elapsed();

    let ordering_started = Instant::now();
    facts.sort_unstable_by_key(|fact| fact.value.id());
    if let Some(duplicate) = facts
        .windows(2)
        .find(|pair| pair[0].value.id() == pair[1].value.id())
    {
        return Err(GraphArReadError::DuplicateFact(duplicate[0].value.id()));
    }
    let (predicates, values) = facts
        .into_iter()
        .map(|fact| (fact.predicate, fact.value))
        .unzip();
    let ordering = ordering_started.elapsed();
    Ok(PreparedFacts {
        values,
        predicates,
        identity_decode,
        materialization,
        ordering,
        semantic_preparation: fact_preparation_started.elapsed(),
    })
}

fn admit_prepared_facts(
    facts: &[Fact],
    predicates: &[Arc<str>],
    projection: &BinaryEntityProjection,
) -> Result<Duration, GraphArReadError> {
    let fact_admission_started = Instant::now();
    for (fact, predicate) in facts.iter().zip(predicates) {
        if predicate.as_ref() != projection.predicate() {
            return Err(GraphArReadError::PredicateMismatch {
                expected: projection.predicate().to_owned(),
                actual: predicate.to_string(),
            });
        }
        projection.project(fact)?;
    }
    Ok(fact_admission_started.elapsed())
}
