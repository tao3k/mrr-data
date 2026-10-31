//! Shared checked shape for physical binary-Entity graph adapters.
use crate::BinaryEntityProjection;
use meta_relational_reasoning::{
    Binding, Direction, EntityId, Expression, RelationId, ResultMode, SetQuantifier,
};
use mrr_data_core::{BoundDataQuery, DataEngineProfile};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EntityHopError {
    EngineProfileMismatch,
    SourceMismatch,
    UnsupportedShape(&'static str),
}
impl std::fmt::Display for EntityHopError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Entity hop: {self:?}")
    }
}
impl std::error::Error for EntityHopError {}

/// A physical endpoint projection with its already admitted semantic type.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EntityEndpoint {
    Source(EntityId),
    Target(EntityId),
}
impl EntityEndpoint {
    #[must_use]
    pub const fn entity_type(self) -> EntityId {
        match self {
            Self::Source(id) | Self::Target(id) => id,
        }
    }
}

/// No source parser or semantic rebinding; only the supported physical shape.
#[derive(Clone, Debug)]
pub struct BinaryEntityHop {
    outputs: Vec<Binding>,
    columns: Vec<EntityEndpoint>,
    relation: RelationId,
}
impl BinaryEntityHop {
    /// Check an admitted outgoing single-hop RETURN ALL query against a projection.
    /// # Errors
    /// Refuses profile/catalog drift or every unsupported query shape.
    pub fn admit(
        query: &BoundDataQuery,
        projection: &BinaryEntityProjection,
        engine: &DataEngineProfile,
    ) -> Result<Self, EntityHopError> {
        if query.engine() != engine {
            return Err(EntityHopError::EngineProfileMismatch);
        }
        if query.graph_projection_manifest().is_none()
            || projection.catalog_digest() != Some(query.query().catalog_digest())
        {
            return Err(EntityHopError::SourceMismatch);
        }
        let ir = query.query().query();
        if !ir.filters().is_empty()
            || !ir.aggregations().is_empty()
            || !ir.grouping().is_empty()
            || !ir.ordering().is_empty()
            || ir.offset().is_some()
            || ir.limit().is_some()
            || ir.result().mode() != ResultMode::Return(SetQuantifier::All)
        {
            return Err(EntityHopError::UnsupportedShape(
                "only unordered RETURN ALL without filters or paging",
            ));
        }
        let [path] = ir.graph().paths() else {
            return Err(EntityHopError::UnsupportedShape("exactly one path"));
        };
        let [segment] = path.segments() else {
            return Err(EntityHopError::UnsupportedShape("exactly one edge"));
        };
        let edge = segment.relation();
        if edge.binding().is_some() {
            return Err(EntityHopError::UnsupportedShape(
                "edge bindings are unsupported",
            ));
        }
        if edge.direction() != Direction::Outgoing
            || edge.min_hops() != 1
            || edge.max_hops() != Some(1)
            || edge.types() != [projection.relation_id()]
        {
            return Err(EntityHopError::UnsupportedShape(
                "one outgoing edge of the projected relation",
            ));
        }
        let [source_type] = path.start().types() else {
            return Err(EntityHopError::UnsupportedShape("one source Entity type"));
        };
        let [target_type] = segment.node().types() else {
            return Err(EntityHopError::UnsupportedShape("one target Entity type"));
        };
        if path.start().binding() == segment.node().binding() {
            return Err(EntityHopError::UnsupportedShape(
                "distinct endpoint bindings required",
            ));
        }
        let columns = ir
            .projections()
            .iter()
            .map(|p| match p.expression() {
                Expression::Binding(binding) if binding == path.start().binding() => {
                    Ok(EntityEndpoint::Source(*source_type))
                }
                Expression::Binding(binding) if binding == segment.node().binding() => {
                    Ok(EntityEndpoint::Target(*target_type))
                }
                _ => Err(EntityHopError::UnsupportedShape(
                    "direct endpoint projections only",
                )),
            })
            .collect::<Result<Vec<_>, _>>()?;
        if columns.is_empty() {
            return Err(EntityHopError::UnsupportedShape(
                "at least one endpoint projection",
            ));
        }

        Ok(Self {
            outputs: ir.projections().iter().map(|p| p.alias().clone()).collect(),
            columns,
            relation: projection.relation_id(),
        })
    }
    #[must_use]
    pub fn outputs(&self) -> &[Binding] {
        &self.outputs
    }
    #[must_use]
    pub fn columns(&self) -> &[EntityEndpoint] {
        &self.columns
    }
    #[must_use]
    pub const fn relation(&self) -> RelationId {
        self.relation
    }
}
