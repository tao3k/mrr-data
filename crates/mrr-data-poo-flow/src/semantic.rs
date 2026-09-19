//! MRR-owned catalogs, identities and the fixed static-edge query.
use crate::protocol::{MAX_EDGES, PROFILE, text};
use anyhow::{Result, ensure};
use meta_relational_reasoning as mrr;
use mrr::{EntityId, GenerationId, RelationId, RelationSchema, ValueSchema};

pub(crate) struct Context {
    pub(crate) relation: RelationSchema,
    pub(crate) relations: mrr::RelationCatalog,
    pub(crate) entities: mrr::EntityCatalog,
    pub(crate) semantic: mrr::SemanticSnapshot,
    pub(crate) query: mrr::CatalogBoundQuery,
}
impl Context {
    pub(crate) fn new(source: &str, revision: &str) -> Result<Self> {
        ensure!(text(source) && text(revision), "invalid source or revision");
        let identity = serde_json::to_vec(&(PROFILE, source, revision)).map_err(semantic_error)?;
        let generation = GenerationId::from_canonical_bytes(&identity).map_err(semantic_error)?;
        let semantic = mrr::SemanticSnapshot::admit(
            generation,
            vec![
                mrr::RevisionBinding::admit(
                    mrr::ExternalRevisionIdentity::new(PROFILE, source, revision)
                        .map_err(semantic_error)?,
                    generation,
                )
                .map_err(semantic_error)?,
            ],
        )
        .map_err(semantic_error)?;
        let relation = RelationSchema::new(
            RelationId::from_canonical_bytes(PROFILE).map_err(semantic_error)?,
            "StaticEdge",
            vec![
                mrr::RelationField::new("source", ValueSchema::Entity, false)
                    .map_err(semantic_error)?,
                mrr::RelationField::new("target", ValueSchema::Entity, false)
                    .map_err(semantic_error)?,
            ],
            vec![],
        )
        .map_err(semantic_error)?;
        let entity =
            EntityId::from_canonical_bytes("poo-flow.runtime-node.v1").map_err(semantic_error)?;
        let entities = mrr::EntityCatalog::admit(vec![
            mrr::EntitySchema::new(entity, "RuntimeNode", vec![]).map_err(semantic_error)?,
        ])
        .map_err(semantic_error)?;
        let relations =
            mrr::RelationCatalog::admit(vec![relation.clone()]).map_err(semantic_error)?;
        let a = mrr::Binding::new("source").map_err(semantic_error)?;
        let b = mrr::Binding::new("target").map_err(semantic_error)?;
        let query_id = mrr::QueryId::from_canonical_bytes("poo-flow.static-edge-endpoints.v1")
            .map_err(semantic_error)?;
        let query = mrr::MetaQueryIr::new(
            query_id,
            mrr::GraphPattern::new(
                mrr::QueryOperatorId::from_canonical_bytes("graph").map_err(semantic_error)?,
                vec![mrr::PathPattern::new(
                    mrr::NodePattern::new(a.clone(), vec![entity]),
                    vec![mrr::PathSegment::new(
                        mrr::RelationPattern::new(
                            None,
                            vec![relation.id()],
                            mrr::Direction::Outgoing,
                            1,
                            Some(1),
                        )
                        .map_err(semantic_error)?,
                        mrr::NodePattern::new(b.clone(), vec![entity]),
                    )],
                )],
            )
            .map_err(semantic_error)?,
            vec![],
            mrr::QueryResult::returning(mrr::SetQuantifier::All).with_projections(vec![
                mrr::Projection::new(
                    mrr::QueryOperatorId::from_canonical_bytes("source").map_err(semantic_error)?,
                    mrr::Expression::Binding(a.clone()),
                    mrr::Binding::new("source_entity").map_err(semantic_error)?,
                ),
                mrr::Projection::new(
                    mrr::QueryOperatorId::from_canonical_bytes("target").map_err(semantic_error)?,
                    mrr::Expression::Binding(b.clone()),
                    mrr::Binding::new("target_entity").map_err(semantic_error)?,
                ),
            ]),
        )
        .map_err(semantic_error)?;
        let bundle = mrr::ReasoningBundle::admit(mrr::ReasoningBundleDeclaration {
            relations: vec![relation.clone()],
            entities: vec![
                mrr::EntitySchema::new(entity, "RuntimeNode", vec![]).map_err(semantic_error)?,
            ],
            query_templates: vec![mrr::QueryTemplate::new(query, vec![])],
            ..mrr::ReasoningBundleDeclaration::default()
        })
        .map_err(semantic_error)?;
        let query =
            mrr::bind_query_to_catalog(&bundle, query_id, &semantic).map_err(semantic_error)?;
        Ok(Self {
            relation,
            relations,
            entities,
            semantic,
            query,
        })
    }
    fn fact_context(&self, source: &str) -> Result<mrr::RelationContext> {
        let owner = EntityId::from_canonical_bytes(source).map_err(semantic_error)?;
        let context = mrr::RelationContext::new(
            self.semantic.generation(),
            mrr::RelationAuthority::Entity(owner),
            mrr::FactProvenance::Source(owner),
            mrr::EvidenceCompleteness::Complete,
            mrr::FactValidity::Valid,
        )
        .map_err(semantic_error)?;
        Ok(context)
    }
    pub(crate) fn validate_facts(&self, source: &str, facts: &[mrr::Fact]) -> Result<()> {
        use std::collections::BTreeSet;
        let expected = self.fact_context(source)?;
        let mut identities = BTreeSet::new();
        let mut edges = BTreeSet::new();
        for fact in facts {
            ensure!(
                fact.context() == &expected,
                "fact authority, generation or validity mismatch"
            );
            self.relation.validate_fact(fact).map_err(semantic_error)?;
            ensure!(identities.insert(fact.id()), "duplicate fact identity");
            let [mrr::Value::Entity(a), mrr::Value::Entity(b)] = fact.values() else {
                anyhow::bail!("invalid edge values")
            };
            ensure!(edges.insert((*a, *b)), "duplicate static edge");
        }
        Ok(())
    }
    pub(crate) fn facts(&self, source: &str, edges: &[[String; 2]]) -> Result<Vec<mrr::Fact>> {
        ensure!(edges.len() <= MAX_EDGES, "edge budget exceeded");
        let context = self.fact_context(source)?;
        let mut sorted = edges.to_vec();
        sorted.sort();
        sorted.dedup();
        ensure!(sorted.len() == edges.len(), "duplicate static edge");
        sorted
            .iter()
            .map(|edge| {
                ensure!(edge.iter().all(|v| text(v)), "invalid node identity");
                Ok(mrr::Fact::new(
                    mrr::FactId::from_canonical_bytes(
                        serde_json::to_vec(&(PROFILE, source, edge)).map_err(semantic_error)?,
                    )
                    .map_err(semantic_error)?,
                    self.relation.id(),
                    vec![
                        mrr::Value::Entity(
                            EntityId::from_canonical_bytes(
                                serde_json::to_vec(&(source, &edge[0])).map_err(semantic_error)?,
                            )
                            .map_err(semantic_error)?,
                        ),
                        mrr::Value::Entity(
                            EntityId::from_canonical_bytes(
                                serde_json::to_vec(&(source, &edge[1])).map_err(semantic_error)?,
                            )
                            .map_err(semantic_error)?,
                        ),
                    ],
                    context,
                ))
            })
            .collect()
    }
}

fn semantic_error(error: impl std::fmt::Debug) -> anyhow::Error {
    anyhow::anyhow!("MRR admission: {error:?}")
}
