//! Arrow schema, complete-Fact projection, and bounded IPC implementation.

use std::{
    collections::HashMap,
    io::Cursor,
    panic::{AssertUnwindSafe, catch_unwind},
    str::FromStr,
    sync::Arc,
};

use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Int64Array, ListArray, RecordBatch, StringArray,
    StructArray,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_ipc::{reader::FileReaderBuilder, writer::FileWriter};
use arrow_schema::{DataType, Field, Fields, Schema};
use meta_relational_reasoning::{
    DerivationId, EntityId, EvidenceCompleteness, Fact, FactId, FactProvenance, FactValidity,
    GenerationId, RelationAuthority, RelationContext, RelationField, RelationSchema, RuleId,
    RulePackId, Value, ValueSchema,
};
use mrr_data_core::{ARROW_FACT_SCHEMA_NAMESPACE, ARROW_FACT_SCHEMA_VERSION};

use crate::error::ArrowRelationError;
use crate::ipc::{IpcImportLimits, check_limit, preflight_ipc};
use crate::schema::{project_schema_field, project_value_field};

const FACT_ID_COLUMN: &str = "__mrr_fact_id";
const GENERATION_ID_COLUMN: &str = "__mrr_generation_id";
const AUTHORITY_COLUMN: &str = "__mrr_authority";
const PROVENANCE_COLUMN: &str = "__mrr_provenance";
const COMPLETENESS_COLUMN: &str = "__mrr_completeness";
const INVALIDATED_BY_COLUMN: &str = "__mrr_invalidated_by";
const SEMANTIC_COLUMN_COUNT: usize = 6;

fn semantic_field(name: &'static str, semantic_type: &'static str, nullable: bool) -> Field {
    Field::new(name, DataType::Utf8, nullable).with_metadata(HashMap::from([(
        "mrr.semantic-column".to_owned(),
        semantic_type.to_owned(),
    )]))
}

/// Projects one semantic relation into its complete fact-batch Arrow schema.
///
/// # Errors
///
/// Returns [`ArrowRelationError::UnsupportedSchema`] when a field has no
/// admitted lossless mapping in this profile.
pub fn project_fact_schema(relation: &RelationSchema) -> Result<Schema, ArrowRelationError> {
    let mut fields = vec![
        semantic_field(FACT_ID_COLUMN, "fact-id", false),
        semantic_field(GENERATION_ID_COLUMN, "generation-id", false),
        semantic_field(AUTHORITY_COLUMN, "relation-authority", false),
        semantic_field(PROVENANCE_COLUMN, "fact-provenance", false),
        semantic_field(COMPLETENESS_COLUMN, "evidence-completeness", false),
        semantic_field(INVALIDATED_BY_COLUMN, "invalidated-by-fact-id", true),
    ];
    fields.extend(
        relation
            .fields()
            .iter()
            .map(project_value_field)
            .collect::<Result<Vec<_>, _>>()?,
    );
    Ok(Schema::new_with_metadata(
        fields,
        HashMap::from([
            (
                "mrr.schema.namespace".to_owned(),
                ARROW_FACT_SCHEMA_NAMESPACE.to_owned(),
            ),
            (
                "mrr.schema.version".to_owned(),
                ARROW_FACT_SCHEMA_VERSION.to_string(),
            ),
            ("mrr.relation-id".to_owned(), relation.id().to_string()),
            ("mrr.predicate".to_owned(), relation.predicate().to_owned()),
        ]),
    ))
}

fn validate_facts(relation: &RelationSchema, facts: &[Fact]) -> Result<(), ArrowRelationError> {
    for fact in facts {
        relation
            .validate_fact(fact)
            .map_err(|error| ArrowRelationError::InvalidFact {
                fact: fact.id(),
                error,
            })?;
    }
    Ok(())
}

fn authority_identity(authority: RelationAuthority) -> String {
    match authority {
        RelationAuthority::Entity(identity) => identity.to_string(),
        RelationAuthority::Rule(identity) => identity.to_string(),
        RelationAuthority::RulePack(identity) => identity.to_string(),
    }
}

fn provenance_identity(provenance: FactProvenance) -> String {
    match provenance {
        FactProvenance::Source(identity) => identity.to_string(),
        FactProvenance::Derivation(identity) => identity.to_string(),
    }
}

const fn completeness_name(completeness: EvidenceCompleteness) -> &'static str {
    match completeness {
        EvidenceCompleteness::Complete => "complete",
        EvidenceCompleteness::Partial => "partial",
        EvidenceCompleteness::Unknown => "unknown",
    }
}

fn semantic_columns(facts: &[Fact]) -> Vec<ArrayRef> {
    let fact_ids = facts
        .iter()
        .map(|fact| fact.id().to_string())
        .collect::<Vec<_>>();
    let generation_ids = facts
        .iter()
        .map(|fact| fact.context().generation().to_string())
        .collect::<Vec<_>>();
    let authorities = facts
        .iter()
        .map(|fact| authority_identity(fact.context().authority()))
        .collect::<Vec<_>>();
    let provenances = facts
        .iter()
        .map(|fact| provenance_identity(fact.context().provenance()))
        .collect::<Vec<_>>();
    let completeness = facts
        .iter()
        .map(|fact| completeness_name(fact.context().completeness()))
        .collect::<Vec<_>>();
    let invalidated_by = facts
        .iter()
        .map(|fact| match fact.context().validity() {
            FactValidity::Valid => None,
            FactValidity::InvalidatedBy(identity) => Some(identity.to_string()),
        })
        .collect::<Vec<_>>();
    vec![
        Arc::new(StringArray::from(fact_ids)),
        Arc::new(StringArray::from(generation_ids)),
        Arc::new(StringArray::from(authorities)),
        Arc::new(StringArray::from(provenances)),
        Arc::new(StringArray::from(completeness)),
        Arc::new(StringArray::from(invalidated_by)),
    ]
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

fn validity(values: &[Value]) -> Option<NullBuffer> {
    let valid = values
        .iter()
        .map(|value| !matches!(value, Value::Null))
        .collect::<Vec<_>>();
    (!valid.iter().all(|value| *value)).then(|| NullBuffer::from(valid))
}

fn project_list(
    field_name: &str,
    element: &ValueSchema,
    element_nullable: bool,
    nullable: bool,
    values: &[Value],
) -> Result<ArrayRef, ArrowRelationError> {
    let mismatch = || ArrowRelationError::ValueMismatch {
        field: field_name.to_owned(),
    };
    let lengths = values
        .iter()
        .map(|value| match value {
            Value::List(items) => Ok(items.len()),
            Value::Null if nullable => Ok(0),
            _ => Err(mismatch()),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let element_count = lengths.iter().try_fold(0_usize, |total, length| {
        total
            .checked_add(*length)
            .filter(|total| i32::try_from(*total).is_ok())
    });
    if element_count.is_none() {
        return Err(mismatch());
    }
    let elements = values
        .iter()
        .filter_map(|value| match value {
            Value::List(items) => Some(items.as_slice()),
            Value::Null => None,
            _ => unreachable!("list shape checked above"),
        })
        .flatten()
        .cloned()
        .collect::<Vec<_>>();
    let child = project_values(
        &format!("{field_name}[]"),
        element,
        element_nullable,
        &elements,
    )?;
    Ok(Arc::new(ListArray::new(
        Arc::new(project_schema_field("item", element, element_nullable)?),
        OffsetBuffer::from_lengths(lengths),
        child,
        validity(values),
    )))
}

fn project_record(
    field_name: &str,
    fields: &[RelationField],
    nullable: bool,
    values: &[Value],
) -> Result<ArrayRef, ArrowRelationError> {
    let records = values
        .iter()
        .map(|value| match value {
            Value::Record(items) => Ok(Some(items)),
            Value::Null if nullable => Ok(None),
            _ => Err(ArrowRelationError::ValueMismatch {
                field: field_name.to_owned(),
            }),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let columns = fields
        .iter()
        .enumerate()
        .map(|(index, child_field)| {
            let child_values = records
                .iter()
                .map(|record| record.map_or(Value::Null, |items| items[index].1.clone()))
                .collect::<Vec<_>>();
            project_values(
                &format!("{field_name}.{}", child_field.name()),
                child_field.schema(),
                child_field.nullable() || nullable,
                &child_values,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Arc::new(StructArray::new(
        Fields::from(
            fields
                .iter()
                .map(project_value_field)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        columns,
        validity(values),
    )))
}

fn project_values(
    field_name: &str,
    schema: &ValueSchema,
    nullable: bool,
    values: &[Value],
) -> Result<ArrayRef, ArrowRelationError> {
    let mismatch = || ArrowRelationError::ValueMismatch {
        field: field_name.to_owned(),
    };
    let array: ArrayRef = match schema {
        ValueSchema::Boolean => Arc::new(BooleanArray::from_iter(
            values
                .iter()
                .map(|value| match value {
                    Value::Boolean(value) => Ok(Some(*value)),
                    Value::Null if nullable => Ok(None),
                    _ => Err(mismatch()),
                })
                .collect::<Result<Vec<_>, _>>()?,
        )),
        ValueSchema::Integer => Arc::new(Int64Array::from_iter(
            values
                .iter()
                .map(|value| match value {
                    Value::Integer(value) => Ok(Some(*value)),
                    Value::Null if nullable => Ok(None),
                    _ => Err(mismatch()),
                })
                .collect::<Result<Vec<_>, _>>()?,
        )),
        ValueSchema::ByteString => Arc::new(BinaryArray::from_iter(
            values
                .iter()
                .map(|value| match value {
                    Value::ByteString(value) => Ok(Some(value.as_slice())),
                    Value::Null if nullable => Ok(None),
                    _ => Err(mismatch()),
                })
                .collect::<Result<Vec<_>, _>>()?,
        )),
        ValueSchema::List {
            element,
            element_nullable,
        } => project_list(field_name, element, *element_nullable, nullable, values)?,
        ValueSchema::Record { fields } => project_record(field_name, fields, nullable, values)?,
        _ => Arc::new(StringArray::from_iter(
            values
                .iter()
                .map(|value| {
                    let synthetic = RelationField::new(field_name, schema.clone(), nullable)
                        .expect("validated schema and field name");
                    string_value(&synthetic, value)
                })
                .collect::<Result<Vec<_>, _>>()?,
        )),
    };
    Ok(array)
}

fn project_column(
    field: &RelationField,
    values: impl Iterator<Item = Value>,
) -> Result<ArrayRef, ArrowRelationError> {
    project_values(
        field.name(),
        field.schema(),
        field.nullable(),
        &values.collect::<Vec<_>>(),
    )
}

/// Encodes validated complete facts without inventing a universal triple layout.
///
/// # Errors
///
/// Returns an error when the schema is unsupported, a row has the wrong arity,
/// a value disagrees with its field, or Arrow rejects the projected columns.
pub fn facts_to_record_batch(
    relation: &RelationSchema,
    facts: &[Fact],
) -> Result<RecordBatch, ArrowRelationError> {
    validate_facts(relation, facts)?;
    let schema = Arc::new(project_fact_schema(relation)?);
    let mut columns = semantic_columns(facts);
    columns.extend(
        relation
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                project_column(field, facts.iter().map(|fact| fact.values()[index].clone()))
            })
            .collect::<Result<Vec<_>, _>>()?,
    );
    RecordBatch::try_new(schema, columns).map_err(Into::into)
}

/// Serializes one complete fact batch with Arrow's file IPC profile.
///
/// # Errors
///
/// Returns a typed projection or Arrow IPC error. The resulting bytes identify
/// a physical artifact, not an MRR semantic generation.
pub fn facts_to_ipc(
    relation: &RelationSchema,
    facts: &[Fact],
) -> Result<Vec<u8>, ArrowRelationError> {
    let batch = facts_to_record_batch(relation, facts)?;
    let mut writer = FileWriter::try_new(Vec::new(), batch.schema().as_ref())?;
    writer.write(&batch)?;
    writer.into_inner().map_err(Into::into)
}

fn required_string<'a>(
    batch: &'a RecordBatch,
    column: usize,
    row: usize,
    name: &'static str,
) -> Result<&'a str, ArrowRelationError> {
    let array = batch
        .column(column)
        .as_any()
        .downcast_ref::<StringArray>()
        .filter(|array| !array.is_null(row))
        .ok_or(ArrowRelationError::InvalidSemanticValue { column: name, row })?;
    Ok(array.value(row))
}

fn optional_string<'a>(
    batch: &'a RecordBatch,
    column: usize,
    row: usize,
    name: &'static str,
) -> Result<Option<&'a str>, ArrowRelationError> {
    let array = batch
        .column(column)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or(ArrowRelationError::InvalidSemanticValue { column: name, row })?;
    Ok((!array.is_null(row)).then(|| array.value(row)))
}

fn parse_identity<T: FromStr>(
    value: &str,
    column: &'static str,
    row: usize,
) -> Result<T, ArrowRelationError> {
    value
        .parse()
        .map_err(|_| ArrowRelationError::InvalidSemanticValue { column, row })
}

fn decode_authority(value: &str, row: usize) -> Result<RelationAuthority, ArrowRelationError> {
    if let Ok(identity) = value.parse::<EntityId>() {
        return Ok(RelationAuthority::Entity(identity));
    }
    if let Ok(identity) = value.parse::<RuleId>() {
        return Ok(RelationAuthority::Rule(identity));
    }
    value
        .parse::<RulePackId>()
        .map(RelationAuthority::RulePack)
        .map_err(|_| ArrowRelationError::InvalidSemanticValue {
            column: AUTHORITY_COLUMN,
            row,
        })
}

fn decode_provenance(value: &str, row: usize) -> Result<FactProvenance, ArrowRelationError> {
    if let Ok(identity) = value.parse::<EntityId>() {
        return Ok(FactProvenance::Source(identity));
    }
    value
        .parse::<DerivationId>()
        .map(FactProvenance::Derivation)
        .map_err(|_| ArrowRelationError::InvalidSemanticValue {
            column: PROVENANCE_COLUMN,
            row,
        })
}

fn decode_completeness(
    value: &str,
    row: usize,
) -> Result<EvidenceCompleteness, ArrowRelationError> {
    match value {
        "complete" => Ok(EvidenceCompleteness::Complete),
        "partial" => Ok(EvidenceCompleteness::Partial),
        "unknown" => Ok(EvidenceCompleteness::Unknown),
        _ => Err(ArrowRelationError::InvalidSemanticValue {
            column: COMPLETENESS_COLUMN,
            row,
        }),
    }
}

fn decode_context(batch: &RecordBatch, row: usize) -> Result<RelationContext, ArrowRelationError> {
    let generation = parse_identity::<GenerationId>(
        required_string(batch, 1, row, GENERATION_ID_COLUMN)?,
        GENERATION_ID_COLUMN,
        row,
    )?;
    let authority = decode_authority(required_string(batch, 2, row, AUTHORITY_COLUMN)?, row)?;
    let provenance = decode_provenance(required_string(batch, 3, row, PROVENANCE_COLUMN)?, row)?;
    let completeness =
        decode_completeness(required_string(batch, 4, row, COMPLETENESS_COLUMN)?, row)?;
    let validity = optional_string(batch, 5, row, INVALIDATED_BY_COLUMN)?
        .map(|value| {
            parse_identity::<FactId>(value, INVALIDATED_BY_COLUMN, row)
                .map(FactValidity::InvalidatedBy)
        })
        .transpose()?
        .unwrap_or(FactValidity::Valid);
    RelationContext::new(generation, authority, provenance, completeness, validity).map_err(|_| {
        ArrowRelationError::InvalidSemanticValue {
            column: AUTHORITY_COLUMN,
            row,
        }
    })
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
        ValueSchema::List {
            element,
            element_nullable,
        } => array
            .as_any()
            .downcast_ref::<ListArray>()
            .map(|array| {
                let values = array.value(row);
                let child = RelationField::new("item", (**element).clone(), *element_nullable)
                    .expect("validated list element schema");
                (0..values.len())
                    .map(|index| decode_column(&child, values.as_ref(), index))
                    .collect::<Result<Vec<_>, _>>()
                    .map(Value::List)
            })
            .transpose()?,
        ValueSchema::Record { fields } => array
            .as_any()
            .downcast_ref::<StructArray>()
            .map(|array| {
                fields
                    .iter()
                    .zip(array.columns())
                    .map(|(child, column)| {
                        decode_column(child, column.as_ref(), row)
                            .map(|value| (child.name().to_owned(), value))
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .map(Value::Record)
            })
            .transpose()?,
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

/// Reconstructs complete semantic facts only when every identity and field agrees.
///
/// # Errors
///
/// Returns an error when metadata or arrays drift from the supplied semantic
/// relation schema, or a stored value cannot reconstruct its MRR type.
pub fn record_batch_to_facts(
    relation: &RelationSchema,
    batch: &RecordBatch,
) -> Result<Vec<Fact>, ArrowRelationError> {
    let expected = project_fact_schema(relation)?;
    if batch.schema().as_ref() != &expected {
        return Err(ArrowRelationError::SchemaMismatch(
            "Arrow schema does not match relation",
        ));
    }
    (0..batch.num_rows())
        .map(|row| {
            let fact_id = parse_identity::<FactId>(
                required_string(batch, 0, row, FACT_ID_COLUMN)?,
                FACT_ID_COLUMN,
                row,
            )?;
            let context = decode_context(batch, row)?;
            let values = relation
                .fields()
                .iter()
                .zip(&batch.columns()[SEMANTIC_COLUMN_COUNT..])
                .map(|(field, column)| decode_column(field, column.as_ref(), row))
                .collect::<Result<Vec<_>, _>>()?;
            let fact = Fact::new(fact_id, relation.id(), values, context);
            relation
                .validate_fact(&fact)
                .map_err(|error| ArrowRelationError::InvalidFact {
                    fact: fact_id,
                    error,
                })?;
            Ok(fact)
        })
        .collect()
}

fn data_type_shape(data_type: &DataType) -> (usize, usize) {
    match data_type {
        DataType::List(field) => {
            let (fields, depth) = data_type_shape(field.data_type());
            (fields.saturating_add(1), depth.saturating_add(1))
        }
        DataType::Struct(fields) => fields.iter().fold((1_usize, 1_usize), |shape, field| {
            let child = data_type_shape(field.data_type());
            (
                shape.0.saturating_add(child.0),
                shape.1.max(child.1.saturating_add(1)),
            )
        }),
        _ => (1, 1),
    }
}

fn schema_shape(schema: &Schema) -> (usize, usize) {
    schema
        .fields()
        .iter()
        .fold((0_usize, 0_usize), |shape, field| {
            let child = data_type_shape(field.data_type());
            (shape.0.saturating_add(child.0), shape.1.max(child.1))
        })
}

fn array_value_count(array: &dyn Array) -> usize {
    if let Some(list) = array.as_any().downcast_ref::<ListArray>() {
        return array
            .len()
            .saturating_add(array_value_count(list.values().as_ref()));
    }
    if let Some(record) = array.as_any().downcast_ref::<StructArray>() {
        return record.columns().iter().fold(array.len(), |count, child| {
            count.saturating_add(array_value_count(child.as_ref()))
        });
    }
    array.len()
}

fn decode_ipc_to_facts(
    relation: &RelationSchema,
    bytes: &[u8],
    limits: IpcImportLimits,
) -> Result<Vec<Fact>, ArrowRelationError> {
    preflight_ipc(bytes, limits)?;
    let reader = FileReaderBuilder::new()
        .with_max_footer_fb_tables(limits.columns.saturating_mul(4).saturating_add(32))
        .with_max_footer_fb_depth(limits.nesting_depth.saturating_add(8))
        .build(Cursor::new(bytes))?;
    let expected = project_fact_schema(relation)?;
    if reader.schema().as_ref() != &expected {
        return Err(ArrowRelationError::SchemaMismatch(
            "Arrow schema does not match relation",
        ));
    }
    let (columns, nesting_depth) = schema_shape(reader.schema().as_ref());
    check_limit("columns", limits.columns, columns)?;
    check_limit("nesting-depth", limits.nesting_depth, nesting_depth)?;
    let batch = reader
        .into_iter()
        .next()
        .expect("preflight requires one batch")?;
    check_limit("rows", limits.rows, batch.num_rows())?;
    check_limit(
        "decoded-bytes",
        limits.decoded_bytes,
        batch.get_array_memory_size(),
    )?;
    let values = batch.columns().iter().fold(0_usize, |count, array| {
        count.saturating_add(array_value_count(array.as_ref()))
    });
    check_limit("values", limits.values, values)?;
    record_batch_to_facts(relation, &batch)
}

/// Decodes exactly one complete fact batch under explicit resource limits.
///
/// # Errors
///
/// Rejects oversized inputs before Arrow decoding, oversized decoded shapes,
/// multiple batches, schema drift, malformed semantic identities, invalid
/// reconstructed facts, and upstream decoder panics caused by malformed IPC.
pub fn ipc_to_facts(
    relation: &RelationSchema,
    bytes: &[u8],
    limits: IpcImportLimits,
) -> Result<Vec<Fact>, ArrowRelationError> {
    catch_unwind(AssertUnwindSafe(|| {
        decode_ipc_to_facts(relation, bytes, limits)
    }))
    .unwrap_or(Err(ArrowRelationError::MalformedIpc))
}
