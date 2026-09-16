#![no_main]

use libfuzzer_sys::fuzz_target;
use meta_relational_reasoning::{RelationField, RelationId, RelationSchema, ValueSchema};
use mrr_data_arrow::{IpcImportLimits, ipc_to_facts};

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

fuzz_target!(|data: &[u8]| {
    let relation = nested_relation();
    let limits = IpcImportLimits::new(data.len(), 256, 32);
    let _ = ipc_to_facts(&relation, data, limits);
});
