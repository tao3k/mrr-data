//! Independent synthetic oracle and bounded native workload declaration.
use super::binary_entity::{fact, schema};
use meta_relational_reasoning::{EntityId, Fact, FactId, RelationCatalog, Value};
use mrr_data_core::{GraphDatasetInventory, GraphInventoryLimits};
use mrr_data_graphar::{
    BinaryEntityProjection, GraphArAdjacency, GraphArChunkLayout, GraphArReadLimits,
    GraphArWriteOptions, prepare_graphar_source_with_adjacency, write_graphar_dataset_with_options,
};
use serde::{Deserialize, Serialize};
use std::{io::Write, path::Path};

pub(super) const EDGES: usize = 65_536;
pub(super) const NODES: usize = 128;
pub(super) const CHUNK: usize = 512;

#[derive(Serialize, Deserialize)]
pub(super) struct Oracle {
    pub count: usize,
    pub digest: String,
}
#[derive(Serialize, Deserialize)]
pub(super) struct Input {
    pub inventory: GraphDatasetInventory,
    pub selected: [Oracle; 2],
    pub full: Oracle,
}
struct HashWriter(blake3::Hasher);
impl Write for HashWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
pub(super) fn digest(facts: &[Fact]) -> String {
    let mut writer = HashWriter(blake3::Hasher::new());
    serde_json::to_writer(&mut writer, facts).expect("encode independent fact oracle");
    writer.0.finalize().to_hex().to_string()
}
pub(super) fn source(index: usize) -> EntityId {
    EntityId::from_canonical_bytes(format!("node-{index}")).expect("canonical synthetic node")
}
pub(super) fn projection() -> BinaryEntityProjection {
    BinaryEntityProjection::admit_catalog(
        &RelationCatalog::admit(vec![schema()]).expect("admit fixture catalog"),
        schema().id(),
    )
    .expect("admit binary projection")
}
fn oracle(facts: &[Fact]) -> Oracle {
    Oracle {
        count: facts.len(),
        digest: digest(facts),
    }
}
fn facts(skewed: bool) -> Vec<Fact> {
    let context = *fact().context();
    let nodes = (0..NODES).map(source).collect::<Vec<_>>();
    let mut facts = (0..EDGES)
        .map(|index| {
            let from = if skewed {
                if index < EDGES / 2 {
                    0
                } else {
                    1 + index % (NODES - 1)
                }
            } else {
                index % NODES
            };
            Fact::new(
                FactId::from_canonical_bytes(format!("benchmark-{index}")).unwrap(),
                schema().id(),
                vec![
                    Value::Entity(nodes[from]),
                    Value::Entity(nodes[(index * 7 + 1) % NODES]),
                ],
                context,
            )
        })
        .collect::<Vec<_>>();
    facts.sort_by_key(Fact::id);
    facts
}
pub(super) fn prepare(root: &Path, skewed: bool) -> Input {
    let facts = facts(skewed);
    let projection = projection();
    let edges = facts
        .iter()
        .map(|f| projection.project(f).unwrap())
        .collect::<Vec<_>>();
    let receipt = write_graphar_dataset_with_options(
        root.join("source"),
        &projection,
        &edges,
        GraphArWriteOptions {
            inventory_limits: GraphInventoryLimits::default(),
            adjacency: GraphArAdjacency::OrderedBySource,
            layout: GraphArChunkLayout::new(NODES, CHUNK).unwrap(),
        },
    )
    .unwrap();
    // Native full-source parity runs only in the fixture parent. Worker RSS
    // never includes fixture Fact vectors or this decoded reference.
    let reference = prepare_graphar_source_with_adjacency(
        root.join("source"),
        GraphArReadLimits::new(NODES, EDGES),
        GraphArAdjacency::OrderedBySource,
    )
    .unwrap()
    .admit(&projection)
    .unwrap();
    assert_eq!(reference.facts(), facts);
    let selected = [0, 1].map(|index| {
        let values = facts
            .iter()
            .filter(|f| f.values()[0] == Value::Entity(source(index)))
            .cloned()
            .collect::<Vec<_>>();
        oracle(&values)
    });
    Input {
        inventory: receipt.inventory().clone(),
        selected,
        full: oracle(&facts),
    }
}
