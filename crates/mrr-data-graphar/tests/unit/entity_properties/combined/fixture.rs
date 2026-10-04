use crate::tests::entity_properties::{
    acceptance::{inputs, limits},
    fixture as properties,
};
use crate::{
    CombinedGraphArLimits, GraphArChunkLayout, GraphArEntityPropertyProjection, GraphArReadLimits,
    inventory_graphar_directory, write_graphar_entity_properties,
};
use meta_relational_reasoning as mrr;
use mrr_data_content::{
    CombinedGraphInputs, ContentBlock, ContentCodec, ContentStore, GraphTransferLimits,
    MemoryContentStore, PreparedCombinedGraph, prepare_combined_graph,
};
use mrr_data_core::{
    BatchDescriptor, BoundDataQuery, CoverageDescriptor, CoverageKind, EntityDescriptor,
    GraphDatasetDescriptor, GraphDatasetLimits, GraphEntityPropertyDescriptor, GraphRelationMember,
    RelationDescriptor, SnapshotBlock, SnapshotManifest, SnapshotManifestRequest, bind_data_query,
    raw_cid,
};

pub struct Fixture {
    pub original: properties::Fixture,
    pub relations: mrr::RelationCatalog,
    pub entities: mrr::EntityCatalog,
    pub projection: GraphArEntityPropertyProjection,
    pub query: BoundDataQuery,
    pub snapshot: SnapshotBlock,
    pub dataset: GraphDatasetDescriptor,
    pub local: MemoryContentStore,
}
pub fn capture_limits() -> CombinedGraphArLimits {
    CombinedGraphArLimits {
        dataset: GraphDatasetLimits {
            inventory: limits().inventory,
            max_relations: 8,
            max_property_rows: 100,
        },
        properties: limits(),
        topology: GraphArReadLimits::new(100, 100),
    }
}
pub fn transfer_limits() -> GraphTransferLimits {
    GraphTransferLimits {
        max_blocks: 512,
        max_block_bytes: 4 << 20,
        max_total_bytes: 32 << 20,
    }
}
impl Fixture {
    pub fn new() -> Self {
        Self::with_original(properties::fixture())
    }
    pub(super) fn with_original(original: properties::Fixture) -> Self {
        let (projection, tables) = inputs(&original);
        let relations = mrr::RelationCatalog::admit(
            original
                .relations
                .iter()
                .map(|t| t.schema.clone())
                .collect(),
        )
        .unwrap();
        let entities =
            mrr::EntityCatalog::admit(original.entities.iter().map(|t| t.schema.clone()).collect())
                .unwrap();
        let directory = tempfile::tempdir().unwrap();
        let receipt = write_graphar_entity_properties(
            &directory.path().join("properties"),
            &projection,
            &original.semantic,
            &tables,
            GraphArChunkLayout::new(2, 4).unwrap(),
            limits(),
        )
        .unwrap();
        let property = receipt.descriptor(limits()).unwrap();
        let property = GraphEntityPropertyDescriptor::decode_checked(
            property.bytes(),
            property.cid(),
            limits().inventory,
            limits().max_rows,
        )
        .unwrap();
        properties::native_relations(&original, directory.path());
        let local = MemoryContentStore::default();
        let mut members = Vec::new();
        store_inventory(&local, receipt.root(), receipt.inventory());
        for (index, table) in original.relations.iter().enumerate() {
            let source = directory.path().join(format!("relation-{index}"));
            let inventory = inventory_graphar_directory(&source, limits().inventory).unwrap();
            store_inventory(&local, &source, &inventory);
            members.push(GraphRelationMember {
                relation: table.schema.id(),
                inventory,
            });
        }
        let dataset = GraphDatasetDescriptor::admit(
            &original.query,
            &relations,
            property,
            members,
            capture_limits().dataset,
        )
        .unwrap();
        local
            .put(ContentBlock::new(
                ContentCodec::DagCbor,
                &dataset.canonical_bytes(capture_limits().dataset).unwrap(),
            ))
            .unwrap();
        let snapshot = snapshot(&original, &relations, &entities, &dataset, &local);
        let query = bind_data_query(
            &original.query,
            &snapshot,
            &mrr_data_datafusion::datafusion_engine_profile().unwrap(),
        )
        .unwrap();
        // Native source files are gone before preparation/publication or restore.
        directory.close().unwrap();
        Self {
            original,
            relations,
            entities,
            projection,
            query,
            snapshot,
            dataset,
            local,
        }
    }
    pub fn inputs(&self) -> CombinedGraphInputs<'_> {
        CombinedGraphInputs {
            query: &self.query,
            snapshot: &self.snapshot,
            relations: &self.relations,
            entities: &self.entities,
            dataset_limits: capture_limits().dataset,
            limits: transfer_limits(),
        }
    }
    pub fn prepare(&self) -> PreparedCombinedGraph {
        prepare_combined_graph(&self.local, self.inputs()).unwrap()
    }
}
fn store_inventory(
    local: &MemoryContentStore,
    source: &std::path::Path,
    inventory: &mrr_data_core::GraphDatasetInventory,
) {
    for file in inventory.files() {
        let bytes = std::fs::read(source.join(file.path())).unwrap();
        assert_eq!(
            local
                .put(ContentBlock::new(ContentCodec::Raw, &bytes))
                .unwrap(),
            *file.cid()
        );
    }
}
fn ipc(local: &MemoryContentStore, batch: &arrow_array::RecordBatch) -> BatchDescriptor {
    let mut bytes = Vec::new();
    {
        let mut writer =
            arrow_ipc::writer::StreamWriter::try_new(&mut bytes, &batch.schema()).unwrap();
        writer.write(batch).unwrap();
        writer.finish().unwrap();
    }
    local
        .put(ContentBlock::new(ContentCodec::Raw, &bytes))
        .unwrap();
    BatchDescriptor::new(raw_cid(&bytes), batch.num_rows() as u64, bytes.len() as u64).unwrap()
}

fn snapshot(
    original: &properties::Fixture,
    relations: &mrr::RelationCatalog,
    entities: &mrr::EntityCatalog,
    dataset: &GraphDatasetDescriptor,
    local: &MemoryContentStore,
) -> SnapshotBlock {
    let relation_batches = original
        .relations
        .iter()
        .map(|t| {
            RelationDescriptor::new(
                t.schema.id(),
                t.batch.num_rows() as u64,
                vec![ipc(local, &t.batch)],
            )
            .unwrap()
        })
        .collect();
    let property_batches = original
        .entities
        .iter()
        .map(|t| {
            EntityDescriptor::new(
                t.schema.clone(),
                t.batch.num_rows() as u64,
                vec![ipc(local, &t.batch)],
            )
            .unwrap()
        })
        .collect();
    let coverage = b"simulated complete fixture";
    local
        .put(ContentBlock::new(ContentCodec::Raw, coverage))
        .unwrap();
    SnapshotBlock::encode(
        SnapshotManifest::admit(
            SnapshotManifestRequest::new(
                original.semantic.clone(),
                relations,
                entities,
                relation_batches,
                CoverageDescriptor::new(CoverageKind::Complete, raw_cid(coverage)).unwrap(),
            )
            .with_entities(property_batches)
            .with_graph_projection(
                dataset
                    .snapshot_projection(capture_limits().dataset)
                    .unwrap(),
            ),
        )
        .unwrap(),
    )
    .unwrap()
}
