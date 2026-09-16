//! Lossless, relation-specific Arrow interchange for MRR V1 values.
#![forbid(unsafe_code)]

use std::{collections::HashMap, str::FromStr, sync::Arc};

use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Int64Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema};
use meta_relational_reasoning::{
    EntityId, FloatWidth, RelationField, RelationSchema, TemporalUnit, TimezonePolicy, Value,
    ValueSchema,
};

/// Stable metadata identity for the first relation-row interchange profile.
pub const ARROW_RELATION_PROFILE_V1: &str = "mrr.data.arrow.relation-row.v1";

/// Fail-closed errors from schema projection or row reconstruction.
#[derive(Debug)]
pub enum ArrowRelationError {
    /// The schema is valid MRR but has no admitted lossless V1 Arrow mapping.
    UnsupportedSchema { field: String, schema: ValueSchema },
    /// One row does not match the relation arity.
    ArityMismatch { expected: usize, actual: usize },
    /// A value does not satisfy its declared field shape.
    ValueMismatch { field: String },
    /// The Arrow batch does not carry the expected MRR profile or relation.
    SchemaMismatch(&'static str),
    /// Arrow rejected a structurally invalid batch.
    Arrow(arrow_schema::ArrowError),
}

impl PartialEq for ArrowRelationError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::UnsupportedSchema {
                    field: left,
                    schema: left_schema,
                },
                Self::UnsupportedSchema {
                    field: right,
                    schema: right_schema,
                },
            ) => left == right && left_schema == right_schema,
            (
                Self::ArityMismatch {
                    expected: left_expected,
                    actual: left_actual,
                },
                Self::ArityMismatch {
                    expected: right_expected,
                    actual: right_actual,
                },
            ) => left_expected == right_expected && left_actual == right_actual,
            (Self::ValueMismatch { field: left }, Self::ValueMismatch { field: right }) => {
                left == right
            }
            (Self::SchemaMismatch(left), Self::SchemaMismatch(right)) => left == right,
            (Self::Arrow(left), Self::Arrow(right)) => left.to_string() == right.to_string(),
            _ => false,
        }
    }
}

impl From<arrow_schema::ArrowError> for ArrowRelationError {
    fn from(error: arrow_schema::ArrowError) -> Self {
        Self::Arrow(error)
    }
}

fn temporal_unit_name(unit: TemporalUnit) -> &'static str {
    match unit {
        TemporalUnit::Second => "second",
        TemporalUnit::Millisecond => "millisecond",
        TemporalUnit::Microsecond => "microsecond",
        TemporalUnit::Nanosecond => "nanosecond",
    }
}

fn timezone_name(timezone: TimezonePolicy) -> &'static str {
    match timezone {
        TimezonePolicy::Naive => "naive",
        TimezonePolicy::Utc => "utc",
    }
}

fn value_schema_identity(schema: &ValueSchema) -> Result<String, ()> {
    match schema {
        ValueSchema::Entity => Ok("entity:typed-text-v1".to_owned()),
        ValueSchema::Boolean => Ok("boolean".to_owned()),
        ValueSchema::Integer => Ok("integer".to_owned()),
        ValueSchema::Decimal { precision, scale } => {
            Ok(format!("decimal-lexical:{precision}:{scale}"))
        }
        ValueSchema::Float { width } => Ok(match width {
            FloatWidth::Binary32 => "float-lexical:binary32",
            FloatWidth::Binary64 => "float-lexical:binary64",
        }
        .to_owned()),
        ValueSchema::String => Ok("string".to_owned()),
        ValueSchema::ByteString => Ok("byte-string".to_owned()),
        ValueSchema::Date => Ok("date-lexical".to_owned()),
        ValueSchema::Time { unit, timezone } => Ok(format!(
            "time-lexical:{}:{}",
            temporal_unit_name(*unit),
            timezone_name(*timezone)
        )),
        ValueSchema::Timestamp { unit, timezone } => Ok(format!(
            "timestamp-lexical:{}:{}",
            temporal_unit_name(*unit),
            timezone_name(*timezone)
        )),
        ValueSchema::Duration => Ok("duration-lexical:v1".to_owned()),
        ValueSchema::List { .. } | ValueSchema::Record { .. } => Err(()),
    }
}

fn arrow_data_type(schema: &ValueSchema) -> Result<DataType, ()> {
    match schema {
        ValueSchema::Boolean => Ok(DataType::Boolean),
        ValueSchema::Integer => Ok(DataType::Int64),
        ValueSchema::ByteString => Ok(DataType::Binary),
        ValueSchema::Entity
        | ValueSchema::Decimal { .. }
        | ValueSchema::Float { .. }
        | ValueSchema::String
        | ValueSchema::Date
        | ValueSchema::Time { .. }
        | ValueSchema::Timestamp { .. }
        | ValueSchema::Duration => Ok(DataType::Utf8),
        ValueSchema::List { .. } | ValueSchema::Record { .. } => Err(()),
    }
}

fn project_field(field: &RelationField) -> Result<Field, ArrowRelationError> {
    let data_type =
        arrow_data_type(field.schema()).map_err(|()| ArrowRelationError::UnsupportedSchema {
            field: field.name().to_owned(),
            schema: field.schema().clone(),
        })?;
    let value_schema =
        value_schema_identity(field.schema()).expect("supported schema has metadata");
    Ok(
        Field::new(field.name(), data_type, field.nullable()).with_metadata(HashMap::from([
            ("mrr.value-schema".to_owned(), value_schema),
            ("mrr.value-schema-version".to_owned(), "v1".to_owned()),
        ])),
    )
}

/// Projects one semantic relation schema into a relation-specific Arrow schema.
///
/// # Errors
///
/// Returns [`ArrowRelationError::UnsupportedSchema`] when a field has no
/// admitted lossless mapping in this profile.
pub fn project_relation_schema(relation: &RelationSchema) -> Result<Schema, ArrowRelationError> {
    let fields = relation
        .fields()
        .iter()
        .map(project_field)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Schema::new_with_metadata(
        fields,
        HashMap::from([
            (
                "mrr.profile".to_owned(),
                ARROW_RELATION_PROFILE_V1.to_owned(),
            ),
            ("mrr.relation-id".to_owned(), relation.id().to_string()),
            ("mrr.predicate".to_owned(), relation.predicate().to_owned()),
        ]),
    ))
}

fn validate_rows(relation: &RelationSchema, rows: &[Vec<Value>]) -> Result<(), ArrowRelationError> {
    for row in rows {
        if row.len() != relation.fields().len() {
            return Err(ArrowRelationError::ArityMismatch {
                expected: relation.fields().len(),
                actual: row.len(),
            });
        }
    }
    Ok(())
}

fn string_value(
    field: &RelationField,
    value: &Value,
) -> Result<Option<String>, ArrowRelationError> {
    let value = match (field.schema(), value) {
        (_, Value::Null) if field.nullable() => None,
        (ValueSchema::Entity, Value::Entity(value)) => Some(value.to_string()),
        (ValueSchema::Decimal { .. }, Value::Decimal(value))
        | (ValueSchema::Float { .. }, Value::Float(value))
        | (ValueSchema::String, Value::String(value))
        | (ValueSchema::Date, Value::Date(value))
        | (ValueSchema::Time { .. }, Value::Time(value))
        | (ValueSchema::Timestamp { .. }, Value::Timestamp(value))
        | (ValueSchema::Duration, Value::Duration(value)) => Some(value.clone()),
        _ => {
            return Err(ArrowRelationError::ValueMismatch {
                field: field.name().to_owned(),
            });
        }
    };
    Ok(value)
}

fn project_column(
    field: &RelationField,
    values: impl Iterator<Item = Value>,
) -> Result<ArrayRef, ArrowRelationError> {
    let values = values.collect::<Vec<_>>();
    let array: ArrayRef = match field.schema() {
        ValueSchema::Boolean => Arc::new(BooleanArray::from_iter(
            values
                .iter()
                .map(|value| match value {
                    Value::Boolean(value) => Ok(Some(*value)),
                    Value::Null if field.nullable() => Ok(None),
                    _ => Err(ArrowRelationError::ValueMismatch {
                        field: field.name().to_owned(),
                    }),
                })
                .collect::<Result<Vec<_>, _>>()?,
        )),
        ValueSchema::Integer => Arc::new(Int64Array::from_iter(
            values
                .iter()
                .map(|value| match value {
                    Value::Integer(value) => Ok(Some(*value)),
                    Value::Null if field.nullable() => Ok(None),
                    _ => Err(ArrowRelationError::ValueMismatch {
                        field: field.name().to_owned(),
                    }),
                })
                .collect::<Result<Vec<_>, _>>()?,
        )),
        ValueSchema::ByteString => Arc::new(BinaryArray::from_iter(
            values
                .iter()
                .map(|value| match value {
                    Value::ByteString(value) => Ok(Some(value.as_slice())),
                    Value::Null if field.nullable() => Ok(None),
                    _ => Err(ArrowRelationError::ValueMismatch {
                        field: field.name().to_owned(),
                    }),
                })
                .collect::<Result<Vec<_>, _>>()?,
        )),
        ValueSchema::List { .. } | ValueSchema::Record { .. } => {
            return Err(ArrowRelationError::UnsupportedSchema {
                field: field.name().to_owned(),
                schema: field.schema().clone(),
            });
        }
        _ => Arc::new(StringArray::from_iter(
            values
                .iter()
                .map(|value| string_value(field, value))
                .collect::<Result<Vec<_>, _>>()?,
        )),
    };
    Ok(array)
}

/// Encodes validated semantic rows without inventing a universal triple layout.
///
/// # Errors
///
/// Returns an error when the schema is unsupported, a row has the wrong arity,
/// a value disagrees with its field, or Arrow rejects the projected columns.
pub fn rows_to_record_batch(
    relation: &RelationSchema,
    rows: &[Vec<Value>],
) -> Result<RecordBatch, ArrowRelationError> {
    validate_rows(relation, rows)?;
    let schema = Arc::new(project_relation_schema(relation)?);
    let columns = relation
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| project_column(field, rows.iter().map(|row| row[index].clone())))
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new(schema, columns).map_err(Into::into)
}

fn decode_string(field: &RelationField, value: &str) -> Result<Value, ArrowRelationError> {
    match field.schema() {
        ValueSchema::Entity => EntityId::from_str(value).map(Value::Entity).map_err(|_| {
            ArrowRelationError::ValueMismatch {
                field: field.name().to_owned(),
            }
        }),
        ValueSchema::Decimal { .. } => Ok(Value::Decimal(value.to_owned())),
        ValueSchema::Float { .. } => Ok(Value::Float(value.to_owned())),
        ValueSchema::String => Ok(Value::String(value.to_owned())),
        ValueSchema::Date => Ok(Value::Date(value.to_owned())),
        ValueSchema::Time { .. } => Ok(Value::Time(value.to_owned())),
        ValueSchema::Timestamp { .. } => Ok(Value::Timestamp(value.to_owned())),
        ValueSchema::Duration => Ok(Value::Duration(value.to_owned())),
        _ => Err(ArrowRelationError::ValueMismatch {
            field: field.name().to_owned(),
        }),
    }
}

fn decode_column(
    field: &RelationField,
    array: &dyn Array,
    row: usize,
) -> Result<Value, ArrowRelationError> {
    if array.is_null(row) {
        return field.nullable().then_some(Value::Null).ok_or_else(|| {
            ArrowRelationError::ValueMismatch {
                field: field.name().to_owned(),
            }
        });
    }
    match field.schema() {
        ValueSchema::Boolean => array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .map(|array| Value::Boolean(array.value(row))),
        ValueSchema::Integer => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|array| Value::Integer(array.value(row))),
        ValueSchema::ByteString => array
            .as_any()
            .downcast_ref::<BinaryArray>()
            .map(|array| Value::ByteString(array.value(row).to_vec())),
        ValueSchema::List { .. } | ValueSchema::Record { .. } => None,
        _ => array
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|array| decode_string(field, array.value(row)))
            .transpose()?,
    }
    .ok_or_else(|| ArrowRelationError::ValueMismatch {
        field: field.name().to_owned(),
    })
}

/// Reconstructs semantic rows only when profile, relation, fields, and arrays agree.
///
/// # Errors
///
/// Returns an error when metadata or arrays drift from the supplied semantic
/// relation schema, or a stored value cannot reconstruct its MRR type.
pub fn record_batch_to_rows(
    relation: &RelationSchema,
    batch: &RecordBatch,
) -> Result<Vec<Vec<Value>>, ArrowRelationError> {
    let expected = project_relation_schema(relation)?;
    if batch.schema().as_ref() != &expected {
        return Err(ArrowRelationError::SchemaMismatch(
            "Arrow schema does not match relation",
        ));
    }
    (0..batch.num_rows())
        .map(|row| {
            relation
                .fields()
                .iter()
                .zip(batch.columns())
                .map(|(field, column)| decode_column(field, column.as_ref(), row))
                .collect()
        })
        .collect()
}

#[cfg(test)]
#[path = "../tests/unit/mod.rs"]
mod tests;
