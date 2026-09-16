//! Arrow schema, complete-Fact projection, and bounded IPC implementation.

use std::{
    collections::HashMap,
    io::Cursor,
    panic::{AssertUnwindSafe, catch_unwind},
    str::FromStr,
    sync::Arc,
};

use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Int64Array, RecordBatch, StringArray,
};
use arrow_ipc::{reader::FileReaderBuilder, writer::FileWriter};
use arrow_schema::{DataType, Field, Schema};
use meta_relational_reasoning::{
    DerivationId, EntityId, EvidenceCompleteness, Fact, FactId, FactProvenance, FactValidity,
    FloatWidth, GenerationId, RelationAuthority, RelationContext, RelationError, RelationField,
    RelationSchema, RuleId, RulePackId, TemporalUnit, TimezonePolicy, Value, ValueSchema,
};

/// Stable metadata identity for complete relation-specific fact batches.
pub const ARROW_FACT_PROFILE_V1: &str = "mrr.data.arrow.fact-batch.v1";

const FACT_ID_COLUMN: &str = "__mrr_fact_id";
const GENERATION_ID_COLUMN: &str = "__mrr_generation_id";
const AUTHORITY_COLUMN: &str = "__mrr_authority";
const PROVENANCE_COLUMN: &str = "__mrr_provenance";
const COMPLETENESS_COLUMN: &str = "__mrr_completeness";
const INVALIDATED_BY_COLUMN: &str = "__mrr_invalidated_by";
const SEMANTIC_COLUMN_COUNT: usize = 6;

/// Fail-closed errors from schema projection or row reconstruction.
#[derive(Debug)]
pub enum ArrowRelationError {
    /// The schema is valid MRR but has no admitted lossless V1 Arrow mapping.
    UnsupportedSchema { field: String, schema: ValueSchema },
    /// A user field collides with the reserved semantic namespace.
    ReservedFieldName(String),
    /// A fact does not satisfy the owning relation contract.
    InvalidFact { fact: FactId, error: RelationError },
    /// A value does not satisfy its declared field shape.
    ValueMismatch { field: String },
    /// A reserved semantic column contains an invalid value.
    InvalidSemanticValue { column: &'static str, row: usize },
    /// An untrusted IPC resource exceeds its caller-owned import budget.
    ImportLimitExceeded {
        resource: &'static str,
        limit: usize,
        actual: usize,
    },
    /// The V1 IPC profile contains anything other than one complete fact batch.
    UnexpectedBatchCount(usize),
    /// The upstream Arrow decoder panicked while inspecting malformed IPC.
    MalformedIpc,
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
            (Self::ReservedFieldName(left), Self::ReservedFieldName(right)) => left == right,
            (
                Self::InvalidFact {
                    fact: left_fact,
                    error: left_error,
                },
                Self::InvalidFact {
                    fact: right_fact,
                    error: right_error,
                },
            ) => left_fact == right_fact && left_error == right_error,
            (Self::ValueMismatch { field: left }, Self::ValueMismatch { field: right }) => {
                left == right
            }
            (
                Self::InvalidSemanticValue {
                    column: left_column,
                    row: left_row,
                },
                Self::InvalidSemanticValue {
                    column: right_column,
                    row: right_row,
                },
            ) => left_column == right_column && left_row == right_row,
            (
                Self::ImportLimitExceeded {
                    resource: left_resource,
                    limit: left_limit,
                    actual: left_actual,
                },
                Self::ImportLimitExceeded {
                    resource: right_resource,
                    limit: right_limit,
                    actual: right_actual,
                },
            ) => {
                left_resource == right_resource
                    && left_limit == right_limit
                    && left_actual == right_actual
            }
            (Self::UnexpectedBatchCount(left), Self::UnexpectedBatchCount(right)) => left == right,
            (Self::MalformedIpc, Self::MalformedIpc) => true,
            (Self::SchemaMismatch(left), Self::SchemaMismatch(right)) => left == right,
            (Self::Arrow(left), Self::Arrow(right)) => left.to_string() == right.to_string(),
            _ => false,
        }
    }
}

/// Caller-owned limits for decoding untrusted Arrow IPC fact batches.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IpcImportLimits {
    bytes: usize,
    rows: usize,
    columns: usize,
}

impl IpcImportLimits {
    #[must_use]
    pub const fn new(max_bytes: usize, max_rows: usize, max_columns: usize) -> Self {
        Self {
            bytes: max_bytes,
            rows: max_rows,
            columns: max_columns,
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

fn project_value_field(field: &RelationField) -> Result<Field, ArrowRelationError> {
    if field.name().starts_with("__mrr_") {
        return Err(ArrowRelationError::ReservedFieldName(
            field.name().to_owned(),
        ));
    }
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
            ("mrr.profile".to_owned(), ARROW_FACT_PROFILE_V1.to_owned()),
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

fn check_limit(
    resource: &'static str,
    limit: usize,
    actual: usize,
) -> Result<(), ArrowRelationError> {
    if actual > limit {
        Err(ArrowRelationError::ImportLimitExceeded {
            resource,
            limit,
            actual,
        })
    } else {
        Ok(())
    }
}

fn decode_ipc_to_facts(
    relation: &RelationSchema,
    bytes: &[u8],
    limits: IpcImportLimits,
) -> Result<Vec<Fact>, ArrowRelationError> {
    check_limit("bytes", limits.bytes, bytes.len())?;
    let reader = FileReaderBuilder::new()
        .with_max_footer_fb_tables(limits.columns.saturating_mul(4).saturating_add(32))
        .with_max_footer_fb_depth(16)
        .build(Cursor::new(bytes))?;
    check_limit("columns", limits.columns, reader.schema().fields().len())?;
    if reader.num_batches() != 1 {
        return Err(ArrowRelationError::UnexpectedBatchCount(
            reader.num_batches(),
        ));
    }
    let mut batches = reader.collect::<Result<Vec<_>, _>>()?;
    let batch = batches.pop().expect("batch count checked");
    check_limit("rows", limits.rows, batch.num_rows())?;
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
