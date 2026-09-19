#![no_main]

use std::sync::LazyLock;

use libfuzzer_sys::fuzz_target;
use meta_relational_reasoning::{
    EntityId, EvidenceCompleteness, Fact, FactId, FactProvenance, FactValidity, GenerationId,
    RelationAuthority, RelationContext, RelationField, RelationId, RelationSchema, Value,
    ValueSchema,
};
use mrr_data_arrow::{IpcImportLimits, facts_to_ipc, ipc_to_facts};

fn field(name: &str, schema: ValueSchema, nullable: bool) -> RelationField {
    RelationField::new(name, schema, nullable).expect("static fuzz field is valid")
}

fn nested_relation() -> RelationSchema {
    RelationSchema::new(
        RelationId::from_canonical_bytes("fuzz-nested").expect("static relation id is valid"),
        "fuzz-nested",
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
                ],
            },
            true,
        )],
        vec![],
    )
    .expect("static fuzz relation is valid")
}

fn valid_nested_ipc(relation: &RelationSchema) -> Vec<u8> {
    let source = EntityId::from_canonical_bytes("fuzz-source").expect("static entity id is valid");
    let fact = Fact::new(
        FactId::from_canonical_bytes("fuzz-fact").expect("static fact id is valid"),
        relation.id(),
        vec![Value::Record(vec![
            ("name".into(), Value::String("seed".into())),
            (
                "scores".into(),
                Value::List(vec![Value::Integer(1), Value::Null, Value::Integer(3)]),
            ),
        ])],
        RelationContext::new(
            GenerationId::from_canonical_bytes("fuzz-generation")
                .expect("static generation id is valid"),
            RelationAuthority::Entity(source),
            FactProvenance::Source(source),
            EvidenceCompleteness::Complete,
            FactValidity::Valid,
        )
        .expect("static relation context is valid"),
    );
    facts_to_ipc(relation, &[fact]).expect("static nested fixture encodes")
}

static FIXTURE: LazyLock<(RelationSchema, Vec<u8>)> = LazyLock::new(|| {
    let relation = nested_relation();
    let ipc = valid_nested_ipc(&relation);
    (relation, ipc)
});

fuzz_target!(|data: &[u8]| {
    let (relation, baseline) = &*FIXTURE;
    let candidate = match data.first().map(|byte| byte % 4) {
        None | Some(0) => data.to_vec(),
        Some(1) => baseline.clone(),
        Some(2) => {
            let mut mutated = baseline.clone();
            for (index, byte) in data.iter().skip(1).enumerate() {
                let target = index % mutated.len();
                mutated[target] ^= byte;
            }
            mutated
        }
        Some(3) => {
            baseline[..usize::from(data.get(1).copied().unwrap_or(0)) % baseline.len()].into()
        }
        Some(_) => unreachable!("modulo range is exhaustive"),
    };
    let limits = IpcImportLimits::new(candidate.len(), 256, 32);
    let _ = ipc_to_facts(relation, &candidate, limits);
});
