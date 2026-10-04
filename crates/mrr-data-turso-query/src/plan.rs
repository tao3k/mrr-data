//! Checked physical plan for the first bound binary-Entity query slice.

#[cfg(feature = "turso-graphar")]
use meta_relational_reasoning::QueryResultValue;
use meta_relational_reasoning::{
    Binding, Direction, EntityId, Expression, RelationId, ResultMode, SetQuantifier,
};
#[cfg(feature = "turso-graphar")]
use mrr_data_core::PhysicalQueryOutput;
use mrr_data_core::{BoundDataQuery, DataEngineProfile};
use mrr_data_graphar::BinaryEntityProjection;

/// A rejected physical query shape or execution boundary. No variant grants
/// semantic admission, even if the native statement completed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqlQueryError {
    EngineProfileMismatch,
    SourceMismatch,
    UnsupportedShape(&'static str),
    Limit(&'static str),
    Native,
    Cleanup,
    Cancelled,
    Deadline,
    CorruptOutput,
    #[cfg(feature = "backend-worker")]
    Backend(mrr_data_backend::BackendError),
}
impl std::fmt::Display for SqlQueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SQL query: {self:?}")
    }
}
impl std::error::Error for SqlQueryError {}

#[cfg(feature = "backend-worker")]
impl From<mrr_data_backend::ResourceStop> for SqlQueryError {
    fn from(stop: mrr_data_backend::ResourceStop) -> Self {
        match stop {
            mrr_data_backend::ResourceStop::Cancelled => Self::Cancelled,
            mrr_data_backend::ResourceStop::Deadline => Self::Deadline,
        }
    }
}

/// Exact physical profile for the qualified Turso binary-Entity slice.
/// # Errors
/// Refuses an invalid static profile declaration.
pub fn turso_graphar_engine_profile() -> Result<DataEngineProfile, SqlQueryError> {
    DataEngineProfile::new("turso-graphar-single-hop", true, [])
        .map_err(|_| SqlQueryError::EngineProfileMismatch)
}

/// A read-only Turso statement for the first admitted binary-Entity slice.
/// The statement itself does not authenticate a source or admit a result.
#[derive(Clone, Debug)]
pub struct TursoSingleHopSql {
    statement: String,
    outputs: Vec<Binding>,
    columns: Vec<EndpointColumn>,
    relation: RelationId,
    relation_binding: String,
    generation_binding: String,
}
#[derive(Clone, Copy, Debug)]
enum EndpointColumn {
    Source(EntityId),
    Target(EntityId),
}
impl EndpointColumn {
    fn sql(self) -> &'static str {
        match self {
            Self::Source(_) => "\"source_entity\"",
            Self::Target(_) => "\"target_entity\"",
        }
    }
    fn entity_type(self) -> EntityId {
        match self {
            Self::Source(id) | Self::Target(id) => id,
        }
    }
}
impl TursoSingleHopSql {
    /// Compile only a one-hop outgoing RETURN ALL query over an admitted
    /// binary-Entity `GraphAr` projection. Source authentication is repeated by
    /// the executor against a captured snapshot before physical work.
    /// # Errors
    /// Refuses catalog/projection drift and every unsupported MRR query shape.
    pub fn compile(
        query: &BoundDataQuery,
        projection: &BinaryEntityProjection,
    ) -> Result<Self, SqlQueryError> {
        if query.engine() != &turso_graphar_engine_profile()? {
            return Err(SqlQueryError::EngineProfileMismatch);
        }
        if query.graph_projection_manifest().is_none()
            || projection.catalog_digest() != Some(query.query().catalog_digest())
        {
            return Err(SqlQueryError::SourceMismatch);
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
            return Err(SqlQueryError::UnsupportedShape(
                "only unordered RETURN ALL without filters or paging",
            ));
        }
        let [path] = ir.graph().paths() else {
            return Err(SqlQueryError::UnsupportedShape("exactly one path"));
        };
        let [segment] = path.segments() else {
            return Err(SqlQueryError::UnsupportedShape("exactly one edge"));
        };
        let edge = segment.relation();
        if edge.binding().is_some() {
            return Err(SqlQueryError::UnsupportedShape(
                "edge bindings are unsupported",
            ));
        }
        if edge.direction() != Direction::Outgoing
            || edge.min_hops() != 1
            || edge.max_hops() != Some(1)
            || edge.types() != [projection.relation_id()]
        {
            return Err(SqlQueryError::UnsupportedShape(
                "one outgoing edge of the projected relation",
            ));
        }
        let [source_type] = path.start().types() else {
            return Err(SqlQueryError::UnsupportedShape("one source Entity type"));
        };
        let [target_type] = segment.node().types() else {
            return Err(SqlQueryError::UnsupportedShape("one target Entity type"));
        };
        if path.start().binding() == segment.node().binding() {
            return Err(SqlQueryError::UnsupportedShape(
                "distinct endpoint bindings required",
            ));
        }
        let columns = ir
            .projections()
            .iter()
            .map(|p| match p.expression() {
                Expression::Binding(binding) if binding == path.start().binding() => {
                    Ok(EndpointColumn::Source(*source_type))
                }
                Expression::Binding(binding) if binding == segment.node().binding() => {
                    Ok(EndpointColumn::Target(*target_type))
                }
                _ => Err(SqlQueryError::UnsupportedShape(
                    "direct endpoint projections only",
                )),
            })
            .collect::<Result<Vec<_>, _>>()?;
        if columns.is_empty() {
            return Err(SqlQueryError::UnsupportedShape(
                "at least one endpoint projection",
            ));
        }
        // Every emitted identifier is a compiler constant. No GQL binding,
        // catalog name or runtime value enters the SQL text.
        let statement = format!(
            "SELECT {} FROM \"mrr_query_edges\" WHERE \"relation_id\"=?1 AND \"generation_id\"=?2 ORDER BY \"fact_id\"",
            columns
                .iter()
                .map(|column| column.sql())
                .collect::<Vec<_>>()
                .join(", ")
        );
        Ok(Self {
            statement,
            outputs: ir.projections().iter().map(|p| p.alias().clone()).collect(),
            columns,
            relation: projection.relation_id(),
            relation_binding: projection.relation_id().to_string(),
            generation_binding: query.query().generation().to_string(),
        })
    }
    #[must_use]
    pub fn statement(&self) -> &str {
        &self.statement
    }
    #[must_use]
    pub fn bindings(&self) -> [&str; 2] {
        [&self.relation_binding, &self.generation_binding]
    }
    #[must_use]
    pub fn outputs(&self) -> &[Binding] {
        &self.outputs
    }
    pub fn output_entity_types(&self) -> impl Iterator<Item = EntityId> + '_ {
        self.columns.iter().map(|column| column.entity_type())
    }
    #[must_use]
    pub const fn relation(&self) -> RelationId {
        self.relation
    }
    #[cfg(feature = "turso-graphar")]
    pub(crate) fn decode(&self, values: &[String]) -> Result<Vec<QueryResultValue>, SqlQueryError> {
        if values.len() != self.columns.len() {
            return Err(SqlQueryError::CorruptOutput);
        }
        values
            .iter()
            .zip(&self.columns)
            .map(|(value, column)| {
                value
                    .parse::<EntityId>()
                    .map(|entity| QueryResultValue::node(entity, column.entity_type()))
                    .map_err(|_| SqlQueryError::CorruptOutput)
            })
            .collect()
    }
    #[cfg(feature = "turso-graphar")]
    pub(crate) fn output(&self, rows: Vec<Vec<QueryResultValue>>) -> PhysicalQueryOutput {
        PhysicalQueryOutput::new(self.outputs.clone(), rows)
    }
}
