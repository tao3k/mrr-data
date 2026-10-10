//! Independent, bounded relational interpretation of the supported property IR.
//! No `DataFusion` plan, SQL translation or transformation extractor is evaluated.
use super::execution::{invalid, strings, unsupported, validate_property_plan};
use super::validation::validate_tables;
use super::{BinaryRelationTable, EntityPropertyTable, PropertyQueryLimits};
use crate::DataFusionQueryError;
use arrow_array::{Array, Int64Array};
use meta_relational_reasoning::{
    CatalogBoundQuery, Expression, NodePattern, QueryResultValue, Value, ValueSchema,
};
use mrr_data_core::PhysicalQueryOutput;
use std::collections::BTreeMap;

type Result<T> = std::result::Result<T, DataFusionQueryError>;

/// Interpret path joins as row assignments, then evaluate original IR filters
/// and projections. Duplicate edges produce duplicate answers; null equality
/// does not satisfy a string predicate. Bounds are checked before enumeration.
/// # Errors
/// Rejects unsupported IR, invalid tables and exceeded input or join limits.
pub fn reference_property_path_query(
    query: &CatalogBoundQuery,
    entities: &[EntityPropertyTable],
    relations: &[BinaryRelationTable],
    limits: PropertyQueryLimits,
) -> Result<PhysicalQueryOutput> {
    validate_tables(query, entities, relations, limits)?;
    validate_property_plan(query, entities, relations, limits)?;
    let path = &query.query().graph().paths()[0];
    let nodes: Vec<_> = std::iter::once(path.start())
        .chain(
            path.segments()
                .iter()
                .map(meta_relational_reasoning::PathSegment::node),
        )
        .collect();
    let tables = nodes
        .iter()
        .map(|node| entity_table(node, entities))
        .collect::<Result<Vec<_>>>()?;
    let mut assignments: Vec<Vec<usize>> = (0..tables[0].batch.num_rows())
        .map(|row| vec![row])
        .collect();
    for (index, segment) in path.segments().iter().enumerate() {
        let relation = relations
            .iter()
            .find(|table| table.schema.id() == segment.relation().types()[0])
            .ok_or(DataFusionQueryError::RelationMismatch)?;
        let from = strings(relation.batch.column(0).as_ref())?;
        let to = strings(relation.batch.column(1).as_ref())?;
        let current = strings(tables[index].batch.column(0).as_ref())?;
        let next = strings(tables[index + 1].batch.column(0).as_ref())?;
        let next_rows: BTreeMap<_, _> = (0..next.len()).map(|row| (next.value(row), row)).collect();
        let mut neighbors: BTreeMap<&str, Vec<usize>> = BTreeMap::new();
        for edge in 0..relation.batch.num_rows() {
            let row =
                *next_rows
                    .get(to.value(edge))
                    .ok_or(DataFusionQueryError::InvalidArrowBatch(
                        "reference dangling edge",
                    ))?;
            neighbors.entry(from.value(edge)).or_default().push(row);
        }
        let mut expanded = Vec::new();
        for assignment in &assignments {
            for row in neighbors
                .get(current.value(assignment[index]))
                .into_iter()
                .flatten()
            {
                if expanded.len() >= limits.max_join_rows {
                    return Err(DataFusionQueryError::ResourceLimit("reference join rows"));
                }
                let mut joined = assignment.clone();
                joined.push(*row);
                expanded.push(joined);
            }
        }
        assignments = expanded;
    }
    let mut rows = Vec::new();
    for assignment in assignments {
        let mut accepted = true;
        for filter in query.query().filters() {
            let Expression::Binary { left, right, .. } = filter.predicate() else {
                return unsupported("reference equality predicate required");
            };
            let Expression::Literal(Value::String(expected)) = right.as_ref() else {
                return unsupported("reference string literal required");
            };
            if property(left, &nodes, &tables, &assignment)?
                != (QueryResultValue::Scalar {
                    schema: ValueSchema::String,
                    value: Value::String(expected.clone()),
                })
            {
                accepted = false;
                break;
            }
        }
        if accepted {
            let row = query
                .query()
                .projections()
                .iter()
                .map(|projection| property(projection.expression(), &nodes, &tables, &assignment))
                .collect::<Result<Vec<_>>>()?;
            rows.push(row);
        }
    }
    Ok(PhysicalQueryOutput::new(
        query
            .query()
            .projections()
            .iter()
            .map(|projection| projection.alias().clone())
            .collect(),
        rows,
    ))
}

/// Compare exact answer bags, preserving multiplicity without assuming order
/// or digest injectivity. This is independent execution evidence, not a proof
/// of `DataFusion`'s implementation or machine-level resource complexity.
/// # Errors
/// Rejects invalid inputs and any column, value or multiplicity discrepancy.
pub fn verify_property_path_output(
    query: &CatalogBoundQuery,
    entities: &[EntityPropertyTable],
    relations: &[BinaryRelationTable],
    limits: PropertyQueryLimits,
    output: &PhysicalQueryOutput,
) -> Result<()> {
    let expected = reference_property_path_query(query, entities, relations, limits)?;
    if expected.columns() != output.columns() || expected.rows().len() != output.rows().len() {
        return invalid("property answer differs from independent relation specification");
    }
    if bag(expected.rows())? != bag(output.rows())? {
        return invalid("property answer multiplicity or values differ");
    }
    Ok(())
}

#[derive(Eq, PartialEq, Ord, PartialOrd)]
enum AnswerCell<'a> {
    Null,
    String(&'a str),
    Integer(i64),
}
fn bag(rows: &[Vec<QueryResultValue>]) -> Result<BTreeMap<Vec<AnswerCell<'_>>, usize>> {
    let mut bag = BTreeMap::new();
    for row in rows {
        let key = row
            .iter()
            .map(|cell| match cell {
                QueryResultValue::Null => Ok(AnswerCell::Null),
                QueryResultValue::Scalar {
                    schema: ValueSchema::String,
                    value: Value::String(text),
                } => Ok(AnswerCell::String(text)),
                QueryResultValue::Scalar {
                    schema: ValueSchema::Integer,
                    value: Value::Integer(value),
                } => Ok(AnswerCell::Integer(*value)),
                _ => invalid("unsupported reference answer cell"),
            })
            .collect::<Result<Vec<_>>>()?;
        *bag.entry(key).or_insert(0) += 1;
    }
    Ok(bag)
}

fn entity_table<'a>(
    node: &NodePattern,
    entities: &'a [EntityPropertyTable],
) -> Result<&'a EntityPropertyTable> {
    let [id] = node.types() else {
        return unsupported("one reference node type required");
    };
    entities
        .iter()
        .find(|table| table.schema.id() == *id)
        .ok_or(DataFusionQueryError::RelationMismatch)
}
fn property(
    expression: &Expression,
    nodes: &[&NodePattern],
    tables: &[&EntityPropertyTable],
    assignment: &[usize],
) -> Result<QueryResultValue> {
    let Expression::Property { binding, key } = expression else {
        return unsupported("reference property expression required");
    };
    let node = nodes
        .iter()
        .position(|node| node.binding() == binding)
        .ok_or(DataFusionQueryError::UnsupportedShape(
            "unknown reference node",
        ))?;
    let table = tables[node];
    let column = table
        .schema
        .properties()
        .iter()
        .position(|field| field.name() == key.as_str())
        .ok_or(DataFusionQueryError::UnsupportedShape(
            "unknown reference property",
        ))?
        + 1;
    let array = table.batch.column(column);
    let row = assignment[node];
    if array.is_null(row) {
        return Ok(QueryResultValue::Null);
    }
    let (schema, value) = match table.schema.properties()[column - 1].schema() {
        ValueSchema::String => (
            ValueSchema::String,
            Value::String(strings(array.as_ref())?.value(row).to_owned()),
        ),
        ValueSchema::Integer => (
            ValueSchema::Integer,
            Value::Integer(
                array
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or(DataFusionQueryError::InvalidArrowBatch(
                        "reference Int64 property required",
                    ))?
                    .value(row),
            ),
        ),
        _ => return unsupported("reference string or integer property required"),
    };
    Ok(QueryResultValue::Scalar { schema, value })
}
