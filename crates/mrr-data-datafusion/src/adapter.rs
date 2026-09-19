//! `DataFusion` execution for the admitted single-hop binary Entity query slice.

use std::fmt;
use std::sync::Arc;

use arrow_array::{Array, RecordBatch, StringArray};
use datafusion::datasource::MemTable;
use datafusion::error::DataFusionError;
use datafusion::prelude::{SessionContext, col};
use meta_relational_reasoning::{
    Binding, CatalogBoundQuery, Direction, EntityId, Expression, QueryResultValue, RelationSchema,
    ResultMode, SetQuantifier, ValueSchema,
};
use mrr_data_core::{DataEngineProfile, PhysicalQueryOutput};

/// Fail-closed reasons the initial `DataFusion` adapter cannot execute a query.
#[derive(Debug)]
pub enum DataFusionQueryError {
    InvalidEngineProfile,
    UnsupportedShape(&'static str),
    RelationMismatch,
    InvalidRelationSchema,
    InvalidArrowBatch(&'static str),
    InvalidEntityIdentity(String),
    Engine(DataFusionError),
}

impl fmt::Display for DataFusionQueryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidEngineProfile => formatter.write_str("invalid DataFusion engine profile"),
            Self::UnsupportedShape(reason) => {
                write!(formatter, "unsupported DataFusion query shape: {reason}")
            }
            Self::RelationMismatch => {
                formatter.write_str("query relation does not match the physical relation")
            }
            Self::InvalidRelationSchema => formatter.write_str(
                "DataFusion adapter requires exactly two non-null Entity relation fields",
            ),
            Self::InvalidArrowBatch(reason) => write!(formatter, "invalid Arrow batch: {reason}"),
            Self::InvalidEntityIdentity(value) => {
                write!(formatter, "invalid canonical Entity identity `{value}`")
            }
            Self::Engine(error) => write!(formatter, "DataFusion execution failed: {error}"),
        }
    }
}

impl std::error::Error for DataFusionQueryError {}

impl From<DataFusionError> for DataFusionQueryError {
    fn from(error: DataFusionError) -> Self {
        Self::Engine(error)
    }
}

/// Stable physical profile selected by this adapter.
///
/// # Errors
///
/// Returns [`DataFusionQueryError::InvalidEngineProfile`] if the static profile
/// declaration ever stops satisfying the core profile contract.
pub fn datafusion_engine_profile() -> Result<DataEngineProfile, DataFusionQueryError> {
    DataEngineProfile::new("datafusion-arrow", false, [])
        .map_err(|_| DataFusionQueryError::InvalidEngineProfile)
}

/// Executes one admitted single-hop binary Entity query through `DataFusion`.
///
/// The adapter intentionally owns no runtime. Callers await `DataFusion` through
/// their existing async lifecycle, then project this storage-neutral output at
/// the `mrr-data-core` identity boundary.
///
/// # Errors
///
/// Returns [`DataFusionQueryError`] when the query exceeds the initial exact
/// slice, the relation or Arrow batch is incompatible, or `DataFusion` fails.
pub async fn execute_binary_entity_query(
    query: &CatalogBoundQuery,
    relation: &RelationSchema,
    batch: RecordBatch,
) -> Result<PhysicalQueryOutput, DataFusionQueryError> {
    let plan = admit_plan(query, relation)?;
    let table = MemTable::try_new(batch.schema(), vec![vec![batch]])?;
    let context = SessionContext::new();
    context.register_table("mrr_relation", Arc::new(table))?;
    let frame = context.table("mrr_relation").await?.select(
        plan.projections
            .iter()
            .map(|projection| col(&projection.field).alias(projection.output.as_str()))
            .collect::<Vec<_>>(),
    )?;
    let batches = frame.collect().await?;
    decode_output(&plan, &batches)
}

struct AdmittedPlan {
    source_type: EntityId,
    target_type: EntityId,
    projections: Vec<AdmittedProjection>,
}

struct AdmittedProjection {
    output: Binding,
    field: String,
    entity_type: EntityId,
}

fn admit_plan(
    query: &CatalogBoundQuery,
    relation: &RelationSchema,
) -> Result<AdmittedPlan, DataFusionQueryError> {
    let query = query.query();
    let [source_field, target_field] = relation.fields() else {
        return Err(DataFusionQueryError::InvalidRelationSchema);
    };
    if source_field.nullable()
        || target_field.nullable()
        || source_field.schema() != &ValueSchema::Entity
        || target_field.schema() != &ValueSchema::Entity
    {
        return Err(DataFusionQueryError::InvalidRelationSchema);
    }
    if !query.filters().is_empty()
        || !query.aggregations().is_empty()
        || !query.grouping().is_empty()
        || !query.ordering().is_empty()
        || query.offset().is_some()
        || query.limit().is_some()
    {
        return Err(DataFusionQueryError::UnsupportedShape(
            "filters, aggregation, grouping, ordering, and pagination are not admitted",
        ));
    }
    if query.result().mode() != ResultMode::Return(SetQuantifier::All) {
        return Err(DataFusionQueryError::UnsupportedShape(
            "only RETURN ALL is admitted",
        ));
    }
    let [path] = query.graph().paths() else {
        return Err(DataFusionQueryError::UnsupportedShape(
            "exactly one path is required",
        ));
    };
    let [segment] = path.segments() else {
        return Err(DataFusionQueryError::UnsupportedShape(
            "exactly one path segment is required",
        ));
    };
    let edge = segment.relation();
    if edge.direction() != Direction::Outgoing || edge.min_hops() != 1 || edge.max_hops() != Some(1)
    {
        return Err(DataFusionQueryError::UnsupportedShape(
            "only one outgoing hop is admitted",
        ));
    }
    if edge.types() != [relation.id()] {
        return Err(DataFusionQueryError::RelationMismatch);
    }
    let [source_type] = path.start().types() else {
        return Err(DataFusionQueryError::UnsupportedShape(
            "source node requires exactly one Entity type",
        ));
    };
    let [target_type] = segment.node().types() else {
        return Err(DataFusionQueryError::UnsupportedShape(
            "target node requires exactly one Entity type",
        ));
    };
    let projections = query
        .projections()
        .iter()
        .map(|projection| match projection.expression() {
            Expression::Binding(binding) if binding == path.start().binding() => {
                Ok(AdmittedProjection {
                    output: projection.alias().clone(),
                    field: source_field.name().to_owned(),
                    entity_type: *source_type,
                })
            }
            Expression::Binding(binding) if binding == segment.node().binding() => {
                Ok(AdmittedProjection {
                    output: projection.alias().clone(),
                    field: target_field.name().to_owned(),
                    entity_type: *target_type,
                })
            }
            _ => Err(DataFusionQueryError::UnsupportedShape(
                "projections must directly select a path endpoint",
            )),
        })
        .collect::<Result<Vec<_>, _>>()?;
    if projections.is_empty() {
        return Err(DataFusionQueryError::UnsupportedShape(
            "at least one endpoint projection is required",
        ));
    }
    Ok(AdmittedPlan {
        source_type: *source_type,
        target_type: *target_type,
        projections,
    })
}

fn decode_output(
    plan: &AdmittedPlan,
    batches: &[RecordBatch],
) -> Result<PhysicalQueryOutput, DataFusionQueryError> {
    let rows = batches
        .iter()
        .map(|batch| decode_batch(plan, batch))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .collect();
    debug_assert!(plan.projections.iter().all(|projection| {
        projection.entity_type == plan.source_type || projection.entity_type == plan.target_type
    }));
    Ok(PhysicalQueryOutput::new(
        plan.projections
            .iter()
            .map(|projection| projection.output.clone())
            .collect(),
        rows,
    ))
}

fn decode_batch(
    plan: &AdmittedPlan,
    batch: &RecordBatch,
) -> Result<Vec<Vec<QueryResultValue>>, DataFusionQueryError> {
    if batch.num_columns() != plan.projections.len() {
        return Err(DataFusionQueryError::InvalidArrowBatch(
            "projection column count changed during execution",
        ));
    }
    let columns = batch
        .columns()
        .iter()
        .map(|column| {
            column.as_any().downcast_ref::<StringArray>().ok_or(
                DataFusionQueryError::InvalidArrowBatch("endpoint projection is not Utf8"),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    (0..batch.num_rows())
        .map(|row| decode_row(plan, &columns, row))
        .collect()
}

fn decode_row(
    plan: &AdmittedPlan,
    columns: &[&StringArray],
    row: usize,
) -> Result<Vec<QueryResultValue>, DataFusionQueryError> {
    columns
        .iter()
        .zip(&plan.projections)
        .map(|(column, projection)| decode_entity(column, row, projection.entity_type))
        .collect()
}

fn decode_entity(
    column: &StringArray,
    row: usize,
    entity_type: EntityId,
) -> Result<QueryResultValue, DataFusionQueryError> {
    if column.is_null(row) {
        return Err(DataFusionQueryError::InvalidArrowBatch(
            "endpoint projection contains null",
        ));
    }
    let identity = column.value(row);
    let entity = identity
        .parse::<EntityId>()
        .map_err(|_| DataFusionQueryError::InvalidEntityIdentity(identity.to_owned()))?;
    Ok(QueryResultValue::node(entity, entity_type))
}

#[cfg(test)]
#[path = "../tests/unit/identity.rs"]
mod identity_tests;
