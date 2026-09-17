//! Arrow schema, complete-Fact projection, and bounded IPC implementation.

use std::{
    collections::HashMap,
    fmt::Write as _,
    io::Cursor,
    panic::{AssertUnwindSafe, catch_unwind},
    str::FromStr,
    sync::Arc,
};

use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Int64Array, ListArray, RecordBatch, StringArray,
    StructArray,
    builder::{BinaryBuilder, BooleanBuilder, Int64Builder, StringBuilder},
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_ipc::{reader::FileReaderBuilder, writer::FileWriter};
use arrow_schema::{DataType, Fields, Schema};
use meta_relational_reasoning::{
    EntityId, Fact, RelationField, RelationSchema, Value, ValueSchema,
};
use mrr_data_profile::{ARROW_FACT_SCHEMA_NAMESPACE, ARROW_FACT_SCHEMA_VERSION};

use crate::error::ArrowRelationError;
use crate::ipc::{IpcImportLimits, check_limit, preflight_ipc};
use crate::schema::{project_schema_field, project_value_field};
use crate::semantic::{self, Decoder as SemanticDecoder};

/// Projects one semantic relation into its complete fact-batch Arrow schema.
///
/// # Errors
///
/// Returns [`ArrowRelationError::UnsupportedSchema`] when a field has no
/// admitted lossless mapping in this profile.
pub fn project_fact_schema(relation: &RelationSchema) -> Result<Schema, ArrowRelationError> {
    let mut fields = semantic::fields();
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

#[derive(Clone, Copy)]
enum ValueRef<'a> {
    Null,
    Value(&'a Value),
}

impl<'a> From<&'a Value> for ValueRef<'a> {
    fn from(value: &'a Value) -> Self {
        match value {
            Value::Null => Self::Null,
            value => Self::Value(value),
        }
    }
}

fn append_string_value(
    builder: &mut StringBuilder,
    field_name: &str,
    schema: &ValueSchema,
    nullable: bool,
    value: ValueRef<'_>,
) -> Result<(), ArrowRelationError> {
    match (schema, value) {
        (_, ValueRef::Null) if nullable => builder.append_null(),
        (ValueSchema::Entity, ValueRef::Value(Value::Entity(value))) => {
            write!(builder, "{value}").expect("writing to an Arrow string builder is infallible");
            builder.append_value("");
        }
        (ValueSchema::Decimal { .. }, ValueRef::Value(Value::Decimal(value)))
        | (ValueSchema::Float { .. }, ValueRef::Value(Value::Float(value)))
        | (ValueSchema::String, ValueRef::Value(Value::String(value)))
        | (ValueSchema::Date, ValueRef::Value(Value::Date(value)))
        | (ValueSchema::Time { .. }, ValueRef::Value(Value::Time(value)))
        | (ValueSchema::Timestamp { .. }, ValueRef::Value(Value::Timestamp(value)))
        | (ValueSchema::Duration, ValueRef::Value(Value::Duration(value))) => {
            builder.append_value(value);
        }
        _ => {
            return Err(ArrowRelationError::ValueMismatch {
                field: field_name.to_owned(),
            });
        }
    }
    Ok(())
}

fn validity(values: &[ValueRef<'_>]) -> Option<NullBuffer> {
    let valid = values
        .iter()
        .map(|value| !matches!(value, ValueRef::Null))
        .collect::<Vec<_>>();
    (!valid.iter().all(|value| *value)).then(|| NullBuffer::from(valid))
}

fn project_list(
    field_name: &str,
    element: &ValueSchema,
    element_nullable: bool,
    nullable: bool,
    values: &[ValueRef<'_>],
) -> Result<ArrayRef, ArrowRelationError> {
    let mismatch = || ArrowRelationError::ValueMismatch {
        field: field_name.to_owned(),
    };
    let lengths = values
        .iter()
        .map(|value| match value {
            ValueRef::Value(Value::List(items)) => Ok(items.len()),
            ValueRef::Null if nullable => Ok(0),
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
            ValueRef::Value(Value::List(items)) => Some(items.as_slice()),
            ValueRef::Null => None,
            ValueRef::Value(_) => unreachable!("list shape checked above"),
        })
        .flatten()
        .map(ValueRef::from)
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
    values: &[ValueRef<'_>],
) -> Result<ArrayRef, ArrowRelationError> {
    let columns = fields
        .iter()
        .enumerate()
        .map(|(index, child_field)| {
            let child_values = values
                .iter()
                .map(|value| match value {
                    ValueRef::Value(Value::Record(items)) => items
                        .get(index)
                        .map(|item| ValueRef::from(&item.1))
                        .ok_or_else(|| ArrowRelationError::ValueMismatch {
                            field: field_name.to_owned(),
                        }),
                    ValueRef::Null if nullable => Ok(ValueRef::Null),
                    _ => Err(ArrowRelationError::ValueMismatch {
                        field: field_name.to_owned(),
                    }),
                })
                .collect::<Result<Vec<_>, _>>()?;
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
    values: &[ValueRef<'_>],
) -> Result<ArrayRef, ArrowRelationError> {
    let mismatch = || ArrowRelationError::ValueMismatch {
        field: field_name.to_owned(),
    };
    let array: ArrayRef = match schema {
        ValueSchema::Boolean => {
            let mut builder = BooleanBuilder::with_capacity(values.len());
            for value in values {
                match value {
                    ValueRef::Value(Value::Boolean(value)) => builder.append_value(*value),
                    ValueRef::Null if nullable => builder.append_null(),
                    _ => return Err(mismatch()),
                }
            }
            Arc::new(builder.finish())
        }
        ValueSchema::Integer => {
            let mut builder = Int64Builder::with_capacity(values.len());
            for value in values {
                match value {
                    ValueRef::Value(Value::Integer(value)) => builder.append_value(*value),
                    ValueRef::Null if nullable => builder.append_null(),
                    _ => return Err(mismatch()),
                }
            }
            Arc::new(builder.finish())
        }
        ValueSchema::ByteString => {
            let bytes = values.iter().fold(0_usize, |bytes, value| match value {
                ValueRef::Value(Value::ByteString(value)) => bytes.saturating_add(value.len()),
                _ => bytes,
            });
            let mut builder = BinaryBuilder::with_capacity(values.len(), bytes);
            for value in values {
                match value {
                    ValueRef::Value(Value::ByteString(value)) => builder.append_value(value),
                    ValueRef::Null if nullable => builder.append_null(),
                    _ => return Err(mismatch()),
                }
            }
            Arc::new(builder.finish())
        }
        ValueSchema::List {
            element,
            element_nullable,
        } => project_list(field_name, element, *element_nullable, nullable, values)?,
        ValueSchema::Record { fields } => project_record(field_name, fields, nullable, values)?,
        _ => {
            let mut builder =
                StringBuilder::with_capacity(values.len(), values.len().saturating_mul(32));
            for value in values {
                append_string_value(&mut builder, field_name, schema, nullable, *value)?;
            }
            Arc::new(builder.finish())
        }
    };
    Ok(array)
}

fn project_column<'a>(
    field: &RelationField,
    values: impl Iterator<Item = ValueRef<'a>>,
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
    let mut columns = semantic::encode_columns(facts);
    columns.extend(
        relation
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                project_column(
                    field,
                    facts
                        .iter()
                        .map(|fact| ValueRef::from(&fact.values()[index])),
                )
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

fn decode_string(
    field_name: &str,
    schema: &ValueSchema,
    value: &str,
) -> Result<Value, ArrowRelationError> {
    match schema {
        ValueSchema::Entity => EntityId::from_str(value).map(Value::Entity).map_err(|_| {
            ArrowRelationError::ValueMismatch {
                field: field_name.to_owned(),
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
            field: field_name.to_owned(),
        }),
    }
}

enum ColumnDecoder<'a> {
    Boolean(&'a BooleanArray),
    Integer(&'a Int64Array),
    Binary(&'a BinaryArray),
    String(&'a StringArray),
    List {
        array: &'a ListArray,
        child: Box<Self>,
        element: &'a ValueSchema,
        element_nullable: bool,
    },
    Record {
        array: &'a StructArray,
        children: Vec<(&'a RelationField, Self)>,
    },
}

impl<'a> ColumnDecoder<'a> {
    fn try_new(
        field_name: &str,
        schema: &'a ValueSchema,
        array: &'a dyn Array,
    ) -> Result<Self, ArrowRelationError> {
        let mismatch = || ArrowRelationError::ValueMismatch {
            field: field_name.to_owned(),
        };
        match schema {
            ValueSchema::Boolean => array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .map(Self::Boolean)
                .ok_or_else(mismatch),
            ValueSchema::Integer => array
                .as_any()
                .downcast_ref::<Int64Array>()
                .map(Self::Integer)
                .ok_or_else(mismatch),
            ValueSchema::ByteString => array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .map(Self::Binary)
                .ok_or_else(mismatch),
            ValueSchema::List {
                element,
                element_nullable,
            } => {
                let array = array
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .ok_or_else(mismatch)?;
                let child = Self::try_new(field_name, element, array.values().as_ref())?;
                Ok(Self::List {
                    array,
                    child: Box::new(child),
                    element,
                    element_nullable: *element_nullable,
                })
            }
            ValueSchema::Record { fields } => {
                let array = array
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(mismatch)?;
                let children = fields
                    .iter()
                    .zip(array.columns())
                    .map(|(field, column)| {
                        Self::try_new(field.name(), field.schema(), column.as_ref())
                            .map(|decoder| (field, decoder))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Self::Record { array, children })
            }
            _ => array
                .as_any()
                .downcast_ref::<StringArray>()
                .map(Self::String)
                .ok_or_else(mismatch),
        }
    }

    fn is_null(&self, row: usize) -> bool {
        match self {
            Self::Boolean(array) => array.is_null(row),
            Self::Integer(array) => array.is_null(row),
            Self::Binary(array) => array.is_null(row),
            Self::String(array) => array.is_null(row),
            Self::List { array, .. } => array.is_null(row),
            Self::Record { array, .. } => array.is_null(row),
        }
    }

    fn decode(
        &self,
        field_name: &str,
        schema: &ValueSchema,
        nullable: bool,
        row: usize,
    ) -> Result<Value, ArrowRelationError> {
        if self.is_null(row) {
            return nullable.then_some(Value::Null).ok_or_else(|| {
                ArrowRelationError::ValueMismatch {
                    field: field_name.to_owned(),
                }
            });
        }
        match (self, schema) {
            (Self::Boolean(array), ValueSchema::Boolean) => Ok(Value::Boolean(array.value(row))),
            (Self::Integer(array), ValueSchema::Integer) => Ok(Value::Integer(array.value(row))),
            (Self::Binary(array), ValueSchema::ByteString) => {
                Ok(Value::ByteString(array.value(row).to_vec()))
            }
            (
                Self::List {
                    array,
                    child,
                    element,
                    element_nullable,
                },
                ValueSchema::List { .. },
            ) => {
                let offsets = array.value_offsets();
                let start = usize::try_from(offsets[row]).map_err(|_| {
                    ArrowRelationError::ValueMismatch {
                        field: field_name.to_owned(),
                    }
                })?;
                let end = usize::try_from(offsets[row + 1]).map_err(|_| {
                    ArrowRelationError::ValueMismatch {
                        field: field_name.to_owned(),
                    }
                })?;
                (start..end)
                    .map(|index| child.decode(field_name, element, *element_nullable, index))
                    .collect::<Result<Vec<_>, _>>()
                    .map(Value::List)
            }
            (Self::Record { children, .. }, ValueSchema::Record { .. }) => children
                .iter()
                .map(|(field, decoder)| {
                    decoder
                        .decode(field.name(), field.schema(), field.nullable(), row)
                        .map(|value| (field.name().to_owned(), value))
                })
                .collect::<Result<Vec<_>, _>>()
                .map(Value::Record),
            (Self::String(array), _) => decode_string(field_name, schema, array.value(row)),
            _ => Err(ArrowRelationError::ValueMismatch {
                field: field_name.to_owned(),
            }),
        }
    }
}

fn decode_column(
    field: &RelationField,
    decoder: &ColumnDecoder<'_>,
    row: usize,
) -> Result<Value, ArrowRelationError> {
    decoder.decode(field.name(), field.schema(), field.nullable(), row)
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
    let mut semantic = SemanticDecoder::try_new(batch)?;
    let value_decoders = relation
        .fields()
        .iter()
        .zip(&batch.columns()[semantic::COLUMN_COUNT..])
        .map(|(field, column)| {
            ColumnDecoder::try_new(field.name(), field.schema(), column.as_ref())
                .map(|decoder| (field, decoder))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut facts = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let fact_id = semantic.fact_id(row)?;
        let context = semantic.context(row)?;
        let values = value_decoders
            .iter()
            .map(|(field, decoder)| decode_column(field, decoder, row))
            .collect::<Result<Vec<_>, _>>()?;
        let fact = Fact::new(fact_id, relation.id(), values, context);
        relation
            .validate_fact(&fact)
            .map_err(|error| ArrowRelationError::InvalidFact {
                fact: fact_id,
                error,
            })?;
        facts.push(fact);
    }
    Ok(facts)
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
