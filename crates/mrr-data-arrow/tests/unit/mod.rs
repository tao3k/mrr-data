use meta_relational_reasoning::{
    EntityId, FloatWidth, RelationField, RelationId, RelationSchema, TemporalUnit, TimezonePolicy,
    Value, ValueSchema,
};

use crate::{ArrowRelationError, record_batch_to_rows, rows_to_record_batch};

fn field(name: &str, schema: ValueSchema, nullable: bool) -> RelationField {
    RelationField::new(name, schema, nullable).expect("valid field")
}

#[test]
fn scalar_relation_rows_round_trip_losslessly() {
    let relation = RelationSchema::new(
        RelationId::from_canonical_bytes("observation").unwrap(),
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
    .unwrap();
    let rows = vec![vec![
        Value::Entity(EntityId::from_canonical_bytes("entity-1").unwrap()),
        Value::Boolean(true),
        Value::Integer(7),
        Value::Float("0.125".into()),
        Value::Null,
        Value::ByteString(vec![0, 1, 255]),
        Value::Timestamp("2026-09-16T10:00:00.123456Z".into()),
        Value::Duration("PT1.250S".into()),
    ]];

    let batch = rows_to_record_batch(&relation, &rows).expect("encode rows");
    assert_eq!(record_batch_to_rows(&relation, &batch).unwrap(), rows);
    assert_eq!(
        batch.schema().metadata()["mrr.profile"],
        "mrr.data.arrow.relation-row.v1"
    );
    assert_eq!(
        batch.schema().field(3).metadata()["mrr.value-schema"],
        "float-lexical:binary64"
    );
}

#[test]
fn parameterized_schema_drift_is_not_hidden_by_the_arrow_primitive() {
    let relation_id = RelationId::from_canonical_bytes("measurement").unwrap();
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
    let batch = rows_to_record_batch(&binary64, &[vec![Value::Float("1.25".into())]]).unwrap();

    assert_eq!(
        record_batch_to_rows(&binary32, &batch),
        Err(ArrowRelationError::SchemaMismatch(
            "Arrow schema does not match relation"
        ))
    );
}

#[test]
fn unsupported_nested_shapes_fail_closed() {
    let relation = RelationSchema::new(
        RelationId::from_canonical_bytes("nested").unwrap(),
        "nested",
        vec![field(
            "items",
            ValueSchema::List {
                element: Box::new(ValueSchema::Integer),
                element_nullable: false,
            },
            false,
        )],
        vec![],
    )
    .unwrap();
    assert_eq!(
        rows_to_record_batch(&relation, &[vec![Value::List(vec![Value::Integer(1)])]]),
        Err(ArrowRelationError::UnsupportedSchema {
            field: "items".into(),
            schema: ValueSchema::List {
                element: Box::new(ValueSchema::Integer),
                element_nullable: false
            }
        })
    );
}
