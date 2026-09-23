//! Complete-Fact semantic columns and batch-local context decoding.

use std::{collections::HashMap, fmt::Write as _, str::FromStr, sync::Arc};

use arrow_array::{Array, ArrayRef, RecordBatch, StringArray, builder::StringBuilder};
use arrow_schema::{DataType, Field};
use meta_relational_reasoning::{
    DerivationId, EntityId, EvidenceCompleteness, Fact, FactId, FactProvenance, FactValidity,
    GenerationId, RelationAuthority, RelationContext, RuleId, RulePackId,
};

use crate::error::ArrowRelationError;

const FACT_ID_COLUMN: &str = "__mrr_fact_id";
const GENERATION_ID_COLUMN: &str = "__mrr_generation_id";
const AUTHORITY_COLUMN: &str = "__mrr_authority";
const PROVENANCE_COLUMN: &str = "__mrr_provenance";
const COMPLETENESS_COLUMN: &str = "__mrr_completeness";
const INVALIDATED_BY_COLUMN: &str = "__mrr_invalidated_by";
pub(crate) const COLUMN_COUNT: usize = 6;

fn semantic_field(name: &'static str, semantic_type: &'static str, nullable: bool) -> Field {
    Field::new(name, DataType::Utf8, nullable).with_metadata(HashMap::from([(
        "mrr.semantic-column".to_owned(),
        semantic_type.to_owned(),
    )]))
}

pub(crate) fn fields() -> Vec<Field> {
    vec![
        semantic_field(FACT_ID_COLUMN, "fact-id", false),
        semantic_field(GENERATION_ID_COLUMN, "generation-id", false),
        semantic_field(AUTHORITY_COLUMN, "relation-authority", false),
        semantic_field(PROVENANCE_COLUMN, "fact-provenance", false),
        semantic_field(COMPLETENESS_COLUMN, "evidence-completeness", false),
        semantic_field(INVALIDATED_BY_COLUMN, "invalidated-by-fact-id", true),
    ]
}

fn write_authority(builder: &mut StringBuilder, authority: RelationAuthority) {
    match authority {
        RelationAuthority::Entity(identity) => write!(builder, "{identity}"),
        RelationAuthority::Rule(identity) => write!(builder, "{identity}"),
        RelationAuthority::RulePack(identity) => write!(builder, "{identity}"),
    }
    .expect("writing to an Arrow string builder is infallible");
    builder.append_value("");
}

fn write_provenance(builder: &mut StringBuilder, provenance: FactProvenance) {
    match provenance {
        FactProvenance::Source(identity) => write!(builder, "{identity}"),
        FactProvenance::Derivation(identity) => write!(builder, "{identity}"),
    }
    .expect("writing to an Arrow string builder is infallible");
    builder.append_value("");
}

const fn completeness_name(completeness: EvidenceCompleteness) -> &'static str {
    match completeness {
        EvidenceCompleteness::Complete => "complete",
        EvidenceCompleteness::Partial => "partial",
        EvidenceCompleteness::Unknown => "unknown",
    }
}

pub(crate) fn encode_columns(facts: &[Fact]) -> Vec<ArrayRef> {
    let rows = facts.len();
    let identity_bytes = rows.saturating_mul(80);
    let mut fact_ids = StringBuilder::with_capacity(rows, identity_bytes);
    let mut generation_ids = StringBuilder::with_capacity(rows, identity_bytes);
    let mut authorities = StringBuilder::with_capacity(rows, identity_bytes);
    let mut provenances = StringBuilder::with_capacity(rows, identity_bytes);
    let mut completeness = StringBuilder::with_capacity(rows, rows.saturating_mul(8));
    let mut invalidated_by = StringBuilder::with_capacity(rows, identity_bytes);

    for fact in facts {
        write!(&mut fact_ids, "{}", fact.id())
            .expect("writing to an Arrow string builder is infallible");
        fact_ids.append_value("");
        write!(&mut generation_ids, "{}", fact.context().generation())
            .expect("writing to an Arrow string builder is infallible");
        generation_ids.append_value("");
        write_authority(&mut authorities, fact.context().authority());
        write_provenance(&mut provenances, fact.context().provenance());
        completeness.append_value(completeness_name(fact.context().completeness()));
        match fact.context().validity() {
            FactValidity::Valid => invalidated_by.append_null(),
            FactValidity::InvalidatedBy(identity) => {
                write!(&mut invalidated_by, "{identity}")
                    .expect("writing to an Arrow string builder is infallible");
                invalidated_by.append_value("");
            }
        }
    }

    vec![
        Arc::new(fact_ids.finish()),
        Arc::new(generation_ids.finish()),
        Arc::new(authorities.finish()),
        Arc::new(provenances.finish()),
        Arc::new(completeness.finish()),
        Arc::new(invalidated_by.finish()),
    ]
}

fn required_string<'a>(
    array: &'a StringArray,
    row: usize,
    name: &'static str,
) -> Result<&'a str, ArrowRelationError> {
    let array = (!array.is_null(row))
        .then_some(array)
        .ok_or(ArrowRelationError::InvalidSemanticValue { column: name, row })?;
    Ok(array.value(row))
}

fn optional_string(array: &StringArray, row: usize) -> Option<&str> {
    (!array.is_null(row)).then(|| array.value(row))
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

fn decode_cached<'a, T: Copy>(
    cache: &mut HashMap<&'a str, T>,
    value: &'a str,
    decode: impl FnOnce(&str) -> Result<T, ArrowRelationError>,
) -> Result<T, ArrowRelationError> {
    if let Some(decoded) = cache.get(value) {
        return Ok(*decoded);
    }
    let decoded = decode(value)?;
    cache.insert(value, decoded);
    Ok(decoded)
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

struct Columns<'a> {
    fact_ids: &'a StringArray,
    generation_ids: &'a StringArray,
    authorities: &'a StringArray,
    provenances: &'a StringArray,
    completeness: &'a StringArray,
    invalidated_by: &'a StringArray,
}

impl<'a> Columns<'a> {
    fn try_new(batch: &'a RecordBatch) -> Result<Self, ArrowRelationError> {
        let column = |index: usize, name: &'static str| {
            batch
                .column(index)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or(ArrowRelationError::InvalidSemanticValue {
                    column: name,
                    row: 0,
                })
        };
        Ok(Self {
            fact_ids: column(0, FACT_ID_COLUMN)?,
            generation_ids: column(1, GENERATION_ID_COLUMN)?,
            authorities: column(2, AUTHORITY_COLUMN)?,
            provenances: column(3, PROVENANCE_COLUMN)?,
            completeness: column(4, COMPLETENESS_COLUMN)?,
            invalidated_by: column(5, INVALIDATED_BY_COLUMN)?,
        })
    }
}

pub(crate) struct Decoder<'a> {
    columns: Columns<'a>,
    generations: HashMap<&'a str, GenerationId>,
    authorities: HashMap<&'a str, RelationAuthority>,
    provenances: HashMap<&'a str, FactProvenance>,
}

impl<'a> Decoder<'a> {
    pub(crate) fn try_new(batch: &'a RecordBatch) -> Result<Self, ArrowRelationError> {
        Ok(Self {
            columns: Columns::try_new(batch)?,
            generations: HashMap::new(),
            authorities: HashMap::new(),
            provenances: HashMap::new(),
        })
    }

    pub(crate) fn fact_id(&self, row: usize) -> Result<FactId, ArrowRelationError> {
        parse_identity(
            required_string(self.columns.fact_ids, row, FACT_ID_COLUMN)?,
            FACT_ID_COLUMN,
            row,
        )
    }

    pub(crate) fn context(&mut self, row: usize) -> Result<RelationContext, ArrowRelationError> {
        let generation_value =
            required_string(self.columns.generation_ids, row, GENERATION_ID_COLUMN)?;
        let generation = decode_cached(&mut self.generations, generation_value, |value| {
            parse_identity::<GenerationId>(value, GENERATION_ID_COLUMN, row)
        })?;
        let authority_value = required_string(self.columns.authorities, row, AUTHORITY_COLUMN)?;
        let authority = decode_cached(&mut self.authorities, authority_value, |value| {
            decode_authority(value, row)
        })?;
        let provenance_value = required_string(self.columns.provenances, row, PROVENANCE_COLUMN)?;
        let provenance = decode_cached(&mut self.provenances, provenance_value, |value| {
            decode_provenance(value, row)
        })?;
        let completeness = decode_completeness(
            required_string(self.columns.completeness, row, COMPLETENESS_COLUMN)?,
            row,
        )?;
        let validity = optional_string(self.columns.invalidated_by, row)
            .map(|value| {
                parse_identity::<FactId>(value, INVALIDATED_BY_COLUMN, row)
                    .map(FactValidity::InvalidatedBy)
            })
            .transpose()?
            .unwrap_or(FactValidity::Valid);
        RelationContext::new(generation, authority, provenance, completeness, validity).map_err(
            |_| ArrowRelationError::InvalidSemanticValue {
                column: AUTHORITY_COLUMN,
                row,
            },
        )
    }
}
