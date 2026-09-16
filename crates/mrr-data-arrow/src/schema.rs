//! Recursive MRR value-schema projection into native Arrow fields.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Fields};
use meta_relational_reasoning::{
    FloatWidth, RelationField, TemporalUnit, TimezonePolicy, ValueSchema,
};

use crate::error::ArrowRelationError;

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

fn value_schema_identity(schema: &ValueSchema) -> String {
    match schema {
        ValueSchema::Entity => "entity:typed-text-v1".to_owned(),
        ValueSchema::Boolean => "boolean".to_owned(),
        ValueSchema::Integer => "integer".to_owned(),
        ValueSchema::Decimal { precision, scale } => format!("decimal-lexical:{precision}:{scale}"),
        ValueSchema::Float { width } => match width {
            FloatWidth::Binary32 => "float-lexical:binary32",
            FloatWidth::Binary64 => "float-lexical:binary64",
        }
        .to_owned(),
        ValueSchema::String => "string".to_owned(),
        ValueSchema::ByteString => "byte-string".to_owned(),
        ValueSchema::Date => "date-lexical".to_owned(),
        ValueSchema::Time { unit, timezone } => format!(
            "time-lexical:{}:{}",
            temporal_unit_name(*unit),
            timezone_name(*timezone)
        ),
        ValueSchema::Timestamp { unit, timezone } => format!(
            "timestamp-lexical:{}:{}",
            temporal_unit_name(*unit),
            timezone_name(*timezone)
        ),
        ValueSchema::Duration => "duration-lexical:v1".to_owned(),
        ValueSchema::List { .. } => "list:v1".to_owned(),
        ValueSchema::Record { .. } => "record:v1".to_owned(),
    }
}

fn arrow_data_type(schema: &ValueSchema) -> Result<DataType, ArrowRelationError> {
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
        ValueSchema::List {
            element,
            element_nullable,
        } => Ok(DataType::List(Arc::new(project_schema_field(
            "item",
            element,
            *element_nullable,
        )?))),
        ValueSchema::Record { fields } => Ok(DataType::Struct(Fields::from(
            fields
                .iter()
                .map(project_value_field)
                .collect::<Result<Vec<_>, _>>()?,
        ))),
    }
}

pub(super) fn project_schema_field(
    name: &str,
    schema: &ValueSchema,
    nullable: bool,
) -> Result<Field, ArrowRelationError> {
    Ok(
        Field::new(name, arrow_data_type(schema)?, nullable).with_metadata(
            [
                ("mrr.value-schema".to_owned(), value_schema_identity(schema)),
                ("mrr.value-schema-version".to_owned(), "v1".to_owned()),
            ]
            .into_iter()
            .collect(),
        ),
    )
}

pub(super) fn project_value_field(field: &RelationField) -> Result<Field, ArrowRelationError> {
    if field.name().starts_with("__mrr_") {
        return Err(ArrowRelationError::ReservedFieldName(
            field.name().to_owned(),
        ));
    }
    project_schema_field(field.name(), field.schema(), field.nullable())
}
