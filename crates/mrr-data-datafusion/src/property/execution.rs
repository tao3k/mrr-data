//! Bounded catalog-backed path joins and string-property projections.
use std::collections::BTreeMap;

use arrow_array::{Array, RecordBatch, StringArray};
use datafusion::{
    common::Column,
    execution::runtime_env::RuntimeEnvBuilder,
    logical_expr::{Expr, JoinType},
    prelude::{SessionConfig, SessionContext, lit},
};
use meta_relational_reasoning::{
    BinaryOperator, CatalogBoundQuery, Direction, EntitySchema, Expression, NodePattern,
    PathPattern, QueryResultValue, RelationSchema, ResultMode, SetQuantifier, Value, ValueSchema,
};
use mrr_data_core::PhysicalQueryOutput;

use super::validation::validate_tables;
use crate::DataFusionQueryError;

pub(super) type Result<T> = std::result::Result<T, DataFusionQueryError>;

/// An entity table: canonical entity ID first, then catalog properties in order.
/// All columns in this bounded slice are Utf8; nullable properties retain nulls.
pub struct EntityPropertyTable {
    pub schema: EntitySchema,
    pub batch: RecordBatch,
}

/// A binary relation table, with source and target columns in schema order.
pub struct BinaryRelationTable {
    pub schema: RelationSchema,
    pub batch: RecordBatch,
}

/// Hard preflight bounds. The join bound is deliberately conservative: the
/// product of edge cardinalities, before filters, must fit `max_join_rows`.
/// A caller must additionally bound the worker lifetime, including cancellation.
#[derive(Clone, Copy)]
pub struct PropertyQueryLimits {
    pub max_input_rows: usize,
    pub max_input_bytes: usize,
    pub max_join_rows: usize,
    pub max_output_cells: usize,
    pub execution_memory_bytes: usize,
}

/// Execute one outgoing path of one or two binary edges with string property
/// equality filters and string property projections. Catalogs must exactly match
/// the bound MRR query. This function neither parses GQL nor admits results.
///
/// # Errors
/// Rejects unsupported shapes, catalog drift, malformed or duplicate entities,
/// dangling edges, and resource limits before returning physical results.
pub async fn execute_property_path_query(
    query: &CatalogBoundQuery,
    entities: &[EntityPropertyTable],
    relations: &[BinaryRelationTable],
    limits: PropertyQueryLimits,
) -> Result<PhysicalQueryOutput> {
    validate_tables(query, entities, relations, limits)?;
    let plan = admit_property_plan(query, entities, relations, limits)?;
    let path = plan.path;
    let nodes = &plan.nodes;
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_limit(limits.execution_memory_bytes, 1.0)
        .build_arc()?;
    let context =
        SessionContext::new_with_config_rt(SessionConfig::new().with_target_partitions(1), runtime);
    let relation_index: BTreeMap<_, _> = relations
        .iter()
        .map(|table| (table.schema.id(), table))
        .collect();
    let mut frame = entity_frame(&context, nodes[0], 0, entities)?;
    for (index, segment) in path.segments().iter().enumerate() {
        let table = relation_index
            .get(&segment.relation().types()[0])
            .ok_or(DataFusionQueryError::RelationMismatch)?;
        let from = format!("e{index}_from");
        let to = format!("e{index}_to");
        let edges = context.read_batch(table.batch.clone())?.select(vec![
            named(table.schema.fields()[0].name()).alias(&from),
            named(table.schema.fields()[1].name()).alias(&to),
        ])?;
        let left = format!("n{index}_id");
        let right = format!("n{}_id", index + 1);
        frame = frame
            .join(edges, JoinType::Inner, &[&left], &[&from], None)?
            .join(
                entity_frame(&context, nodes[index + 1], index + 1, entities)?,
                JoinType::Inner,
                &[&to],
                &[&right],
                None,
            )?;
    }
    for filter in plan.filters {
        frame = frame.filter(filter)?;
    }
    // RETURN ALL has no semantic ordering. Use a deterministic physical order
    // for reproducible receipts while preserving repeated rows and nulls.
    let ordering = query
        .query()
        .projections()
        .iter()
        .map(|projection| named(projection.alias().as_str()).sort(true, true))
        .collect();
    let batches = frame
        .select(plan.projections)?
        .sort(ordering)?
        .collect()
        .await?;
    decode_properties(query, batches)
}

fn decode_properties(
    query: &CatalogBoundQuery,
    batches: Vec<RecordBatch>,
) -> Result<PhysicalQueryOutput> {
    let mut rows = Vec::new();
    for batch in batches {
        let columns = batch
            .columns()
            .iter()
            .map(|a| strings(a.as_ref()))
            .collect::<Result<Vec<_>>>()?;
        for row in 0..batch.num_rows() {
            rows.push(
                columns
                    .iter()
                    .map(|c| {
                        if c.is_null(row) {
                            QueryResultValue::Null
                        } else {
                            QueryResultValue::Scalar {
                                schema: ValueSchema::String,
                                value: Value::String(c.value(row).to_owned()),
                            }
                        }
                    })
                    .collect(),
            );
        }
    }
    Ok(PhysicalQueryOutput::new(
        query
            .query()
            .projections()
            .iter()
            .map(|p| p.alias().clone())
            .collect(),
        rows,
    ))
}

struct PropertyPlan<'a> {
    path: &'a PathPattern,
    nodes: Vec<&'a NodePattern>,
    projections: Vec<Expr>,
    filters: Vec<Expr>,
}
fn admit_property_plan<'a>(
    query: &'a CatalogBoundQuery,
    entities: &[EntityPropertyTable],
    relations: &[BinaryRelationTable],
    limits: PropertyQueryLimits,
) -> Result<PropertyPlan<'a>> {
    let ir = query.query();
    if !ir.aggregations().is_empty()
        || !ir.grouping().is_empty()
        || !ir.ordering().is_empty()
        || ir.limit().is_some()
        || ir.offset().is_some()
        || ir.result().mode() != ResultMode::Return(SetQuantifier::All)
    {
        return unsupported(
            "only unordered RETURN ALL without aggregation or pagination is supported",
        );
    }
    let [path] = ir.graph().paths() else {
        return unsupported("one path required");
    };
    if !(1..=2).contains(&path.segments().len()) {
        return unsupported("one or two edges required");
    }
    let nodes: Vec<_> = std::iter::once(path.start())
        .chain(
            path.segments()
                .iter()
                .map(meta_relational_reasoning::PathSegment::node),
        )
        .collect();
    let mut bindings = BTreeMap::new();
    for (index, node) in nodes.iter().enumerate() {
        if bindings.insert(node.binding().as_str(), index).is_some() {
            return unsupported("repeated node bindings are unsupported");
        }
    }
    validate_join_bound(query, path, relations, limits)?;
    // Validate all expressions before submitting any work to DataFusion.
    let projections = ir
        .projections()
        .iter()
        .map(|p| {
            property_column(p.expression(), &bindings, &nodes, entities)
                .map(|name| named(&name).alias(p.alias().as_str()))
        })
        .collect::<Result<Vec<_>>>()?;
    let filters = ir
        .filters()
        .iter()
        .map(|f| {
            let Expression::Binary {
                left,
                operator: BinaryOperator::Equal,
                right,
            } = f.predicate()
            else {
                return unsupported("only property = string filters are supported");
            };
            let Expression::Literal(Value::String(value)) = right.as_ref() else {
                return unsupported("string literal required");
            };
            Ok(named(&property_column(left, &bindings, &nodes, entities)?).eq(lit(value.clone())))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(PropertyPlan {
        path,
        nodes,
        projections,
        filters,
    })
}
fn validate_join_bound(
    query: &CatalogBoundQuery,
    path: &PathPattern,
    relations: &[BinaryRelationTable],
    limits: PropertyQueryLimits,
) -> Result<()> {
    let ir = query.query();
    let relation_index: BTreeMap<_, _> = relations
        .iter()
        .map(|table| (table.schema.id(), table))
        .collect();
    let mut bound = 1usize;
    for segment in path.segments() {
        let edge = segment.relation();
        if edge.direction() != Direction::Outgoing
            || edge.min_hops() != 1
            || edge.max_hops() != Some(1)
            || edge.binding().is_some()
        {
            return unsupported("only anonymous outgoing single-hop edges are supported");
        }
        let [id] = edge.types() else {
            return unsupported("exactly one relation type required");
        };
        let table = relation_index
            .get(id)
            .ok_or(DataFusionQueryError::RelationMismatch)?;
        bound = bound
            .checked_mul(table.batch.num_rows())
            .ok_or(DataFusionQueryError::ResourceLimit("join rows"))?;
        if bound > limits.max_join_rows {
            return Err(DataFusionQueryError::ResourceLimit("join rows"));
        }
    }
    if ir.projections().is_empty() {
        return unsupported("property projections required");
    }
    if bound
        .checked_mul(ir.projections().len())
        .is_none_or(|cells| cells > limits.max_output_cells)
    {
        return Err(DataFusionQueryError::ResourceLimit("output cells"));
    }
    Ok(())
}

fn named(name: &str) -> Expr {
    Expr::Column(Column::from_name(name))
}
pub(super) fn unsupported<T>(reason: &'static str) -> Result<T> {
    Err(DataFusionQueryError::UnsupportedShape(reason))
}
pub(super) fn invalid<T>(reason: &'static str) -> Result<T> {
    Err(DataFusionQueryError::InvalidArrowBatch(reason))
}
pub(super) fn strings(array: &dyn Array) -> Result<&StringArray> {
    array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or(DataFusionQueryError::InvalidArrowBatch(
            "Utf8 column required",
        ))
}
fn entity_table<'a>(
    node: &NodePattern,
    tables: &'a [EntityPropertyTable],
) -> Result<&'a EntityPropertyTable> {
    let [id] = node.types() else {
        return unsupported("one node type required");
    };
    tables
        .iter()
        .find(|t| t.schema.id() == *id)
        .ok_or(DataFusionQueryError::InvalidArrowBatch(
            "missing entity type",
        ))
}
fn entity_frame(
    context: &SessionContext,
    node: &NodePattern,
    index: usize,
    tables: &[EntityPropertyTable],
) -> Result<datafusion::dataframe::DataFrame> {
    let table = entity_table(node, tables)?;
    let mut columns =
        vec![named(table.batch.schema().field(0).name()).alias(format!("n{index}_id"))];
    columns.extend(
        table
            .schema
            .properties()
            .iter()
            .enumerate()
            .map(|(p, field)| named(field.name()).alias(format!("n{index}_p{p}"))),
    );
    Ok(context.read_batch(table.batch.clone())?.select(columns)?)
}
fn property_column(
    expression: &Expression,
    bindings: &BTreeMap<&str, usize>,
    nodes: &[&NodePattern],
    tables: &[EntityPropertyTable],
) -> Result<String> {
    let Expression::Property { binding, key } = expression else {
        return unsupported("node property required");
    };
    let index = *bindings
        .get(binding.as_str())
        .ok_or(DataFusionQueryError::UnsupportedShape(
            "unknown node binding",
        ))?;
    let table = entity_table(nodes[index], tables)?;
    let property = table
        .schema
        .properties()
        .iter()
        .position(|f| f.name() == key.as_str())
        .ok_or(DataFusionQueryError::UnsupportedShape("unknown property"))?;
    Ok(format!("n{index}_p{property}"))
}
