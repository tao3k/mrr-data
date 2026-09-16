//! Property-based contracts for recursively nested complete facts.

use meta_relational_reasoning::{
    EntityId, EvidenceCompleteness, Fact, FactId, FactProvenance, FactValidity, GenerationId,
    RelationAuthority, RelationContext, RelationField, RelationId, RelationSchema, Value,
    ValueSchema,
};
use proptest::prelude::{any, prop, prop_assert_eq, proptest};

use crate::{
    IpcImportLimits, facts_to_ipc, facts_to_record_batch, ipc_to_facts, record_batch_to_facts,
};

fn field(name: &str, schema: ValueSchema, nullable: bool) -> RelationField {
    RelationField::new(name, schema, nullable).expect("valid generated field")
}

fn nested_relation() -> RelationSchema {
    RelationSchema::new(
        RelationId::from_canonical_bytes("property-nested").expect("valid relation id"),
        "property-nested",
        vec![field(
            "profile",
            ValueSchema::Record {
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
            },
            true,
        )],
        vec![],
    )
    .expect("valid generated relation")
}

proptest! {
    #[test]
    fn nested_fact_and_ipc_round_trips_are_lossless(
        name in "[A-Za-z0-9 _-]{0,48}",
        scores in prop::collection::vec(prop::option::of(any::<i64>()), 0..48),
        active in any::<bool>(),
        note in prop::option::of("[A-Za-z0-9 _-]{0,48}"),
        null_profile in any::<bool>(),
    ) {
        let relation = nested_relation();
        let source = EntityId::from_canonical_bytes("property-source").expect("valid source");
        let context = RelationContext::new(
            GenerationId::from_canonical_bytes("property-generation").expect("valid generation"),
            RelationAuthority::Entity(source),
            FactProvenance::Source(source),
            EvidenceCompleteness::Complete,
            FactValidity::Valid,
        )
        .expect("valid context");
        let profile = if null_profile {
            Value::Null
        } else {
            Value::Record(vec![
                ("name".into(), Value::String(name)),
                (
                    "scores".into(),
                    Value::List(
                        scores
                            .into_iter()
                            .map(|score| score.map_or(Value::Null, Value::Integer))
                            .collect(),
                    ),
                ),
                (
                    "metadata".into(),
                    Value::Record(vec![
                        ("active".into(), Value::Boolean(active)),
                        ("note".into(), note.map_or(Value::Null, Value::String)),
                    ]),
                ),
            ])
        };
        let facts = vec![Fact::new(
            FactId::from_canonical_bytes("property-fact").expect("valid fact id"),
            relation.id(),
            vec![profile],
            context,
        )];

        let batch = facts_to_record_batch(&relation, &facts).expect("encode generated fact");
        prop_assert_eq!(record_batch_to_facts(&relation, &batch).unwrap(), facts.clone());

        let ipc = facts_to_ipc(&relation, &facts).expect("encode generated IPC");
        prop_assert_eq!(
            ipc_to_facts(&relation, &ipc, IpcImportLimits::new(ipc.len(), 1, 16)).unwrap(),
            facts
        );
    }
}
