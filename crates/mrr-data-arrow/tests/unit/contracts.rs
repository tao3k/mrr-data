//! Complete Fact and bounded IPC contracts.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use meta_relational_reasoning::{
    DerivationId, EntityId, EvidenceCompleteness, Fact, FactId, FactProvenance, FactValidity,
    FloatWidth, GenerationId, RelationAuthority, RelationContext, RelationError, RelationField,
    RelationId, RelationSchema, RuleId, RulePackId, TemporalUnit, TimezonePolicy, Value,
    ValueSchema,
};

use crate::{
    ARROW_FACT_SCHEMA_NAMESPACE, ARROW_FACT_SCHEMA_VERSION, ArrowRelationError, IpcImportLimits,
    facts_to_ipc, facts_to_record_batch, ipc_to_facts, record_batch_to_facts,
};

fn id<T>(name: &str) -> T
where
    T: TryFromCanonical,
{
    T::from_name(name)
}

trait TryFromCanonical {
    fn from_name(name: &str) -> Self;
}

macro_rules! canonical_id {
    ($type:ty) => {
        impl TryFromCanonical for $type {
            fn from_name(name: &str) -> Self {
                Self::from_canonical_bytes(name).expect("valid identity")
            }
        }
    };
}

canonical_id!(DerivationId);
canonical_id!(EntityId);
canonical_id!(FactId);
canonical_id!(GenerationId);
canonical_id!(RelationId);
canonical_id!(RuleId);
canonical_id!(RulePackId);

fn field(name: &str, schema: ValueSchema, nullable: bool) -> RelationField {
    RelationField::new(name, schema, nullable).expect("valid field")
}

fn scalar_relation() -> RelationSchema {
    RelationSchema::new(
        id("observation"),
        "observation",
        vec![
            field("entity", ValueSchema::Entity, false),
            field("active", ValueSchema::Boolean, false),
            field("count", ValueSchema::Integer, false),
            field(
                "ratio",
                ValueSchema::Float {
                    width: FloatWidth::Binary64,
                },
                false,
            ),
            field("note", ValueSchema::String, true),
            field("payload", ValueSchema::ByteString, false),
            field(
                "observed",
                ValueSchema::Timestamp {
                    unit: TemporalUnit::Microsecond,
                    timezone: TimezonePolicy::Utc,
                },
                false,
            ),
            field("duration", ValueSchema::Duration, false),
        ],
        vec![],
    )
    .expect("valid relation")
}

fn values(entity: &str, count: i64) -> Vec<Value> {
    vec![
        Value::Entity(id(entity)),
        Value::Boolean(true),
        Value::Integer(count),
        Value::Float("0.125".into()),
        Value::Null,
        Value::ByteString(vec![0, 1, 255]),
        Value::Timestamp("2026-09-16T10:00:00.123456Z".into()),
        Value::Duration("P1DT2H3M4.5S".into()),
    ]
}

fn nested_relation() -> RelationSchema {
    let profile_schema = ValueSchema::Record {
        fields: vec![
            field("name", ValueSchema::String, false),
            field(
                "scores",
                ValueSchema::List {
                    element: Box::new(ValueSchema::Integer),
                    element_nullable: true,
                },
                false,
            ),
            field(
                "metadata",
                ValueSchema::Record {
                    fields: vec![
                        field("active", ValueSchema::Boolean, false),
                        field("note", ValueSchema::String, true),
                    ],
                },
                false,
            ),
        ],
    };
    RelationSchema::new(
        id("nested"),
        "nested",
        vec![field("profile", profile_schema, true)],
        vec![],
    )
    .expect("valid nested relation")
}

fn nested_values(index: usize) -> Vec<Value> {
    if index.is_multiple_of(4) {
        return vec![Value::Null];
    }
    vec![Value::Record(vec![
        ("name".into(), Value::String(format!("profile-{index}"))),
        (
            "scores".into(),
            Value::List(vec![
                Value::Integer(i64::try_from(index).expect("index fits i64")),
                Value::Null,
                Value::Integer(i64::try_from(index + 1).expect("index fits i64")),
            ]),
        ),
        (
            "metadata".into(),
            Value::Record(vec![
                ("active".into(), Value::Boolean(index.is_multiple_of(2))),
                (
                    "note".into(),
                    if index.is_multiple_of(3) {
                        Value::Null
                    } else {
                        Value::String(format!("note-{index}"))
                    },
                ),
            ]),
        ),
    ])]
}

#[test]
fn complete_facts_round_trip_losslessly() {
    let relation = scalar_relation();
    let source = id::<EntityId>("source-owner");
    let first_id = id::<FactId>("source-fact");
    let facts = vec![
        Fact::new(
            first_id,
            relation.id(),
            values("entity-1", 7),
            RelationContext::new(
                id("generation-1"),
                RelationAuthority::Entity(source),
                FactProvenance::Source(source),
                EvidenceCompleteness::Complete,
                FactValidity::Valid,
            )
            .unwrap(),
        ),
        Fact::new(
            id("derived-fact"),
            relation.id(),
            values("entity-2", 8),
            RelationContext::new(
                id("generation-2"),
                RelationAuthority::RulePack(id("rule-pack")),
                FactProvenance::Derivation(id("derivation")),
                EvidenceCompleteness::Partial,
                FactValidity::InvalidatedBy(first_id),
            )
            .unwrap(),
        ),
        Fact::new(
            id("rule-fact"),
            relation.id(),
            values("entity-3", 9),
            RelationContext::new(
                id("generation-3"),
                RelationAuthority::Rule(id("rule")),
                FactProvenance::Derivation(id("rule-derivation")),
                EvidenceCompleteness::Unknown,
                FactValidity::Valid,
            )
            .unwrap(),
        ),
    ];

    let batch = facts_to_record_batch(&relation, &facts).expect("encode facts");
    assert_eq!(record_batch_to_facts(&relation, &batch).unwrap(), facts);
    assert_eq!(
        batch.schema().metadata()["mrr.schema.namespace"],
        ARROW_FACT_SCHEMA_NAMESPACE
    );
    assert_eq!(
        batch.schema().metadata()["mrr.schema.version"],
        ARROW_FACT_SCHEMA_VERSION.to_string()
    );
    assert_eq!(
        batch.schema().field(9).metadata()["mrr.value-schema"],
        "float-lexical:binary64"
    );
}

#[test]
fn fact_matrix_round_trips_without_losing_identity_or_context() {
    let relation = scalar_relation();
    let source = id::<EntityId>("matrix-source");
    let facts = (0..64)
        .map(|index| {
            Fact::new(
                id(&format!("matrix-fact-{index}")),
                relation.id(),
                values(&format!("matrix-entity-{index}"), index),
                RelationContext::new(
                    id(&format!("matrix-generation-{}", index % 4)),
                    RelationAuthority::Entity(source),
                    FactProvenance::Source(source),
                    match index % 3 {
                        0 => EvidenceCompleteness::Complete,
                        1 => EvidenceCompleteness::Partial,
                        _ => EvidenceCompleteness::Unknown,
                    },
                    FactValidity::Valid,
                )
                .unwrap(),
            )
        })
        .collect::<Vec<_>>();

    let batch = facts_to_record_batch(&relation, &facts).unwrap();
    assert_eq!(record_batch_to_facts(&relation, &batch).unwrap(), facts);
}

#[test]
fn scenario_native_arrow_round_trips_ten_thousand_complete_facts() {
    const FACT_COUNT: usize = 10_000;
    const SAMPLE_COUNT: usize = 11;

    let relation = scalar_relation();
    let source = id::<EntityId>("native-scenario-source");
    let facts = (0..FACT_COUNT)
        .map(|index| {
            Fact::new(
                id(&format!("native-scenario-fact-{index}")),
                relation.id(),
                values(
                    &format!("native-scenario-entity-{index}"),
                    i64::try_from(index).expect("scenario index fits i64"),
                ),
                RelationContext::new(
                    id(&format!("native-scenario-generation-{}", index % 16)),
                    RelationAuthority::Entity(source),
                    FactProvenance::Source(source),
                    EvidenceCompleteness::Complete,
                    FactValidity::Valid,
                )
                .unwrap(),
            )
        })
        .collect::<Vec<_>>();

    let warm_batch = facts_to_record_batch(&relation, &facts).expect("warm native Arrow encoder");
    assert_eq!(
        record_batch_to_facts(&relation, &warm_batch).expect("warm native Arrow decoder"),
        facts
    );

    let mut encode_samples = Vec::with_capacity(SAMPLE_COUNT);
    let mut decode_samples = Vec::with_capacity(SAMPLE_COUNT);
    let mut total_samples = Vec::with_capacity(SAMPLE_COUNT);
    for _ in 0..SAMPLE_COUNT {
        let encode_started = Instant::now();
        let batch = facts_to_record_batch(&relation, &facts).expect("encode native Arrow batch");
        let encode_elapsed = encode_started.elapsed();
        let decode_started = Instant::now();
        let decoded = record_batch_to_facts(&relation, &batch).expect("decode native Arrow batch");
        let decode_elapsed = decode_started.elapsed();

        assert_eq!(batch.num_rows(), FACT_COUNT);
        assert_eq!(decoded, facts);
        encode_samples.push(encode_elapsed);
        decode_samples.push(decode_elapsed);
        total_samples.push(encode_elapsed + decode_elapsed);
    }
    encode_samples.sort_unstable();
    decode_samples.sort_unstable();
    total_samples.sort_unstable();
    let encode_p50 = encode_samples[SAMPLE_COUNT / 2];
    let decode_p50 = decode_samples[SAMPLE_COUNT / 2];
    let total_p50 = total_samples[SAMPLE_COUNT / 2];

    assert!(
        total_p50 < Duration::from_secs(2),
        "10,000-row native Arrow round-trip P50 exceeded two seconds: encode={encode_p50:?}, decode={decode_p50:?}, total={total_p50:?}"
    );
    eprintln!(
        "mrr-arrow-native-scenario rows={FACT_COUNT} samples={SAMPLE_COUNT} encode_p50_us={} decode_p50_us={} total_p50_us={}",
        encode_p50.as_micros(),
        decode_p50.as_micros(),
        total_p50.as_micros()
    );
}

#[test]
fn ipc_is_deterministic_and_round_trips_complete_facts() {
    let relation = scalar_relation();
    let source = id::<EntityId>("ipc-source");
    let facts = (0..40)
        .map(|index| {
            Fact::new(
                id(&format!("ipc-fact-{index}")),
                relation.id(),
                values(&format!("ipc-entity-{index}"), index),
                RelationContext::new(
                    id("ipc-generation"),
                    RelationAuthority::Entity(source),
                    FactProvenance::Source(source),
                    EvidenceCompleteness::Complete,
                    FactValidity::Valid,
                )
                .unwrap(),
            )
        })
        .collect::<Vec<_>>();

    let first = facts_to_ipc(&relation, &facts).unwrap();
    let second = facts_to_ipc(&relation, &facts).unwrap();
    assert_eq!(first, second);
    assert_eq!(
        ipc_to_facts(
            &relation,
            &first,
            IpcImportLimits::new(first.len(), facts.len(), 14)
        )
        .unwrap(),
        facts
    );
}

#[test]
fn ipc_import_limits_fail_closed() {
    let relation = scalar_relation();
    let source = id::<EntityId>("limit-source");
    let fact = Fact::new(
        id("limit-fact"),
        relation.id(),
        values("limit-entity", 1),
        RelationContext::new(
            id("limit-generation"),
            RelationAuthority::Entity(source),
            FactProvenance::Source(source),
            EvidenceCompleteness::Complete,
            FactValidity::Valid,
        )
        .unwrap(),
    );
    let batch = facts_to_record_batch(&relation, std::slice::from_ref(&fact)).unwrap();
    let decoded_bytes = batch.get_array_memory_size();
    let bytes = facts_to_ipc(&relation, &[fact]).unwrap();

    assert_eq!(
        ipc_to_facts(
            &relation,
            &bytes,
            IpcImportLimits::new(bytes.len() - 1, 1, 14)
        ),
        Err(ArrowRelationError::ImportLimitExceeded {
            resource: "bytes",
            limit: bytes.len() - 1,
            actual: bytes.len()
        })
    );
    assert_eq!(
        ipc_to_facts(&relation, &bytes, IpcImportLimits::new(bytes.len(), 0, 14)),
        Err(ArrowRelationError::ImportLimitExceeded {
            resource: "rows",
            limit: 0,
            actual: 1
        })
    );
    assert_eq!(
        ipc_to_facts(&relation, &bytes, IpcImportLimits::new(bytes.len(), 1, 13)),
        Err(ArrowRelationError::ImportLimitExceeded {
            resource: "columns",
            limit: 13,
            actual: 14
        })
    );
    match ipc_to_facts(
        &relation,
        &bytes,
        IpcImportLimits::new(bytes.len(), 1, 14).with_decoded_bytes(0),
    ) {
        Err(ArrowRelationError::ImportLimitExceeded {
            resource: "decoded-bytes",
            limit: 0,
            actual,
        }) => assert!(actual > 0, "IPC body must be admitted before decoding"),
        result => panic!("expected preflight decoded-byte failure, got {result:?}"),
    }
    match ipc_to_facts(
        &relation,
        &bytes,
        IpcImportLimits::new(bytes.len(), 1, 14).with_decoded_bytes(decoded_bytes - 1),
    ) {
        Err(ArrowRelationError::ImportLimitExceeded {
            resource: "decoded-bytes",
            limit,
            actual,
        }) => {
            assert_eq!(limit, decoded_bytes - 1);
            assert!(actual > limit);
        }
        result => panic!("expected decoded-byte limit failure, got {result:?}"),
    }
    assert_eq!(
        ipc_to_facts(
            &relation,
            &bytes,
            IpcImportLimits::new(bytes.len(), 1, 14).with_values(13)
        ),
        Err(ArrowRelationError::ImportLimitExceeded {
            resource: "values",
            limit: 13,
            actual: 14
        })
    );
    assert_eq!(
        ipc_to_facts(
            &relation,
            &bytes,
            IpcImportLimits::new(bytes.len(), 1, 14).with_nesting_depth(0)
        ),
        Err(ArrowRelationError::ImportLimitExceeded {
            resource: "nesting-depth",
            limit: 0,
            actual: 1
        })
    );
}

#[test]
fn malformed_ipc_corpus_is_bounded_and_never_bypasses_fact_validation() {
    let relation = scalar_relation();
    let source = id::<EntityId>("corpus-source");
    let fact = Fact::new(
        id("corpus-fact"),
        relation.id(),
        values("corpus-entity", 1),
        RelationContext::new(
            id("corpus-generation"),
            RelationAuthority::Entity(source),
            FactProvenance::Source(source),
            EvidenceCompleteness::Complete,
            FactValidity::Valid,
        )
        .unwrap(),
    );
    let bytes = facts_to_ipc(&relation, &[fact]).unwrap();
    let limits = IpcImportLimits::new(bytes.len(), 1, 14);

    for boundary in (0..bytes.len()).step_by((bytes.len() / 31).max(1)) {
        assert!(ipc_to_facts(&relation, &bytes[..boundary], limits).is_err());
    }
    for index in (0..bytes.len()).step_by((bytes.len() / 31).max(1)) {
        let mut corrupted = bytes.clone();
        corrupted[index] ^= 0xff;
        if let Ok(decoded) = ipc_to_facts(&relation, &corrupted, limits) {
            assert!(
                decoded
                    .iter()
                    .all(|fact| relation.validate_fact(fact).is_ok())
            );
            assert!(decoded.len() <= 1);
        }
    }
}

#[test]
fn parameterized_schema_drift_is_not_hidden_by_the_arrow_primitive() {
    let relation_id = id("measurement");
    let binary64 = RelationSchema::new(
        relation_id,
        "measurement",
        vec![field(
            "value",
            ValueSchema::Float {
                width: FloatWidth::Binary64,
            },
            false,
        )],
        vec![],
    )
    .unwrap();
    let binary32 = RelationSchema::new(
        relation_id,
        "measurement",
        vec![field(
            "value",
            ValueSchema::Float {
                width: FloatWidth::Binary32,
            },
            false,
        )],
        vec![],
    )
    .unwrap();
    let source = id::<EntityId>("measurement-source");
    let fact = Fact::new(
        id("measurement-fact"),
        relation_id,
        vec![Value::Float("1.25".into())],
        RelationContext::new(
            id("measurement-generation"),
            RelationAuthority::Entity(source),
            FactProvenance::Source(source),
            EvidenceCompleteness::Complete,
            FactValidity::Valid,
        )
        .unwrap(),
    );
    let batch = facts_to_record_batch(&binary64, &[fact]).unwrap();

    assert_eq!(
        record_batch_to_facts(&binary32, &batch),
        Err(ArrowRelationError::SchemaMismatch(
            "Arrow schema does not match relation"
        ))
    );
}

#[test]
fn malformed_semantic_identity_fails_closed() {
    let relation = scalar_relation();
    let source = id::<EntityId>("tamper-source");
    let fact = Fact::new(
        id("tamper-fact"),
        relation.id(),
        values("tamper-entity", 1),
        RelationContext::new(
            id("tamper-generation"),
            RelationAuthority::Entity(source),
            FactProvenance::Source(source),
            EvidenceCompleteness::Complete,
            FactValidity::Valid,
        )
        .unwrap(),
    );
    let batch = facts_to_record_batch(&relation, &[fact]).unwrap();
    let mut columns = batch.columns().to_vec();
    columns[0] = Arc::new(StringArray::from(vec!["not-a-fact-id"])) as ArrayRef;
    let tampered = RecordBatch::try_new(batch.schema(), columns).unwrap();

    assert_eq!(
        record_batch_to_facts(&relation, &tampered),
        Err(ArrowRelationError::InvalidSemanticValue {
            column: "__mrr_fact_id",
            row: 0
        })
    );
}

#[test]
fn invalid_facts_are_rejected_before_arrow_projection() {
    let relation = scalar_relation();
    let source = id::<EntityId>("wrong-relation-source");
    let fact = Fact::new(
        id("wrong-relation-fact"),
        id("other-relation"),
        values("wrong-relation-entity", 1),
        RelationContext::new(
            id("wrong-relation-generation"),
            RelationAuthority::Entity(source),
            FactProvenance::Source(source),
            EvidenceCompleteness::Complete,
            FactValidity::Valid,
        )
        .unwrap(),
    );

    assert_eq!(
        facts_to_record_batch(&relation, std::slice::from_ref(&fact)),
        Err(ArrowRelationError::InvalidFact {
            fact: fact.id(),
            error: RelationError::WrongRelation
        })
    );
}

#[test]
fn nested_list_and_record_shapes_round_trip_without_json() {
    let relation = nested_relation();
    let source = id::<EntityId>("nested-source");
    let context = RelationContext::new(
        id("nested-generation"),
        RelationAuthority::Entity(source),
        FactProvenance::Source(source),
        EvidenceCompleteness::Complete,
        FactValidity::Valid,
    )
    .unwrap();
    let facts = vec![
        Fact::new(id("nested-fact"), relation.id(), nested_values(3), context),
        Fact::new(
            id("null-nested-fact"),
            relation.id(),
            nested_values(0),
            context,
        ),
    ];

    let batch = facts_to_record_batch(&relation, &facts).expect("encode nested facts");
    assert_eq!(record_batch_to_facts(&relation, &batch).unwrap(), facts);

    let ipc = facts_to_ipc(&relation, &facts).expect("encode nested IPC");
    assert_eq!(
        ipc_to_facts(
            &relation,
            &ipc,
            IpcImportLimits::new(ipc.len(), facts.len(), 16)
        )
        .unwrap(),
        facts
    );
}

#[test]
fn scenario_native_arrow_round_trips_ten_thousand_nested_null_heavy_facts() {
    const FACT_COUNT: usize = 10_000;

    let relation = nested_relation();
    let source = id::<EntityId>("nested-scenario-source");
    let context = RelationContext::new(
        id("nested-scenario-generation"),
        RelationAuthority::Entity(source),
        FactProvenance::Source(source),
        EvidenceCompleteness::Complete,
        FactValidity::Valid,
    )
    .expect("valid repeated context");
    let facts = (0..FACT_COUNT)
        .map(|index| {
            Fact::new(
                id(&format!("nested-scenario-fact-{index}")),
                relation.id(),
                nested_values(index),
                context,
            )
        })
        .collect::<Vec<_>>();

    let encode_started = Instant::now();
    let batch = facts_to_record_batch(&relation, &facts).expect("encode nested Arrow batch");
    let encode_elapsed = encode_started.elapsed();
    let decode_started = Instant::now();
    let decoded = record_batch_to_facts(&relation, &batch).expect("decode nested Arrow batch");
    let decode_elapsed = decode_started.elapsed();

    assert_eq!(batch.num_rows(), FACT_COUNT);
    assert_eq!(decoded, facts);
    assert!(
        encode_elapsed + decode_elapsed < Duration::from_secs(3),
        "10,000-row nested Arrow round trip exceeded three seconds: encode={encode_elapsed:?}, decode={decode_elapsed:?}"
    );
    eprintln!(
        "mrr-arrow-nested-scenario rows={FACT_COUNT} null_rows={} encode_us={} decode_us={} total_us={}",
        FACT_COUNT / 4,
        encode_elapsed.as_micros(),
        decode_elapsed.as_micros(),
        (encode_elapsed + decode_elapsed).as_micros()
    );
}
