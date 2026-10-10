//! Opaque physical closure fixtures; no native Parquet/Arrow claim.
use meta_relational_reasoning::{
    Binding, Direction, EntityCatalog, EntityId, EntitySchema, Expression,
    ExternalRevisionIdentity, GenerationId, GraphPattern, MetaQueryIr, NodePattern, PathPattern,
    PathSegment, Projection, QueryId, QueryOperatorId, QueryResult, QueryTemplate, ReasoningBundle,
    ReasoningBundleDeclaration, RelationCatalog, RelationField, RelationId, RelationPattern,
    RelationSchema, RevisionBinding, SemanticSnapshot, SetQuantifier, ValueSchema,
    bind_query_to_catalog,
};
use mrr_data_core::{
    BatchDescriptor, BoundDataQuery, CoverageDescriptor, CoverageKind, DataEngineProfile,
    GraphDatasetBinding, GraphDatasetInventory, GraphFile, GraphFileKind, GraphInventoryLimits,
    GraphProjectionDescriptor, RelationDescriptor, SnapshotBlock, SnapshotManifest,
    SnapshotManifestRequest, bind_data_query, raw_cid,
};

use cid::Cid;
use mrr_data_content::{
    ContentBlock, ContentCodec, ContentStore, GraphPublishInputs, GraphTransferLimits,
    MemoryContentStore, PreparedGraphPublication, RemoteContentStore, RemoteError, RemoteFuture,
    prepare_graph_publication,
};
use std::{collections::BTreeMap, sync::Mutex};
pub fn schema() -> RelationSchema {
    RelationSchema::new(
        RelationId::from_canonical_bytes("knows").unwrap(),
        "knows",
        vec![
            RelationField::new("source", ValueSchema::Entity, false).unwrap(),
            RelationField::new("destination", ValueSchema::Entity, false).unwrap(),
        ],
        vec![],
    )
    .unwrap()
}
fn query(
    inventory: &GraphDatasetInventory,
    ipc: &[u8],
) -> (
    BoundDataQuery,
    SnapshotBlock,
    RelationCatalog,
    EntityCatalog,
) {
    let generation = "generation";
    let generation = GenerationId::from_canonical_bytes(generation).unwrap();
    let snapshot = SemanticSnapshot::admit(
        generation,
        vec![
            RevisionBinding::admit(
                ExternalRevisionIdentity::new("git", "fixture", "revision").unwrap(),
                generation,
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let node = EntityId::from_canonical_bytes("node").unwrap();
    let binding = |name: &str| Binding::new(name).unwrap();
    let op = |name: &str| QueryOperatorId::from_canonical_bytes(name).unwrap();
    let query_id = QueryId::from_canonical_bytes("query").unwrap();
    let query = MetaQueryIr::new(
        query_id,
        GraphPattern::new(
            op("graph"),
            vec![PathPattern::new(
                NodePattern::new(binding("source"), vec![node]),
                vec![PathSegment::new(
                    RelationPattern::new(
                        None,
                        vec![schema().id()],
                        Direction::Outgoing,
                        1,
                        Some(1),
                    )
                    .unwrap(),
                    NodePattern::new(binding("target"), vec![node]),
                )],
            )],
        )
        .unwrap(),
        vec![],
        QueryResult::returning(SetQuantifier::All).with_projections(vec![Projection::new(
            op("return"),
            Expression::Binding(binding("source")),
            binding("entity"),
        )]),
    )
    .unwrap();
    let bundle = ReasoningBundle::admit(ReasoningBundleDeclaration {
        relations: vec![schema()],
        entities: vec![EntitySchema::new(node, "Node", vec![]).unwrap()],
        query_templates: vec![QueryTemplate::new(query, vec![])],
        ..ReasoningBundleDeclaration::default()
    })
    .unwrap();
    let bound = bind_query_to_catalog(&bundle, query_id, &snapshot).unwrap();
    let relations = RelationCatalog::admit(vec![schema()]).unwrap();
    let entities =
        EntityCatalog::admit(vec![EntitySchema::new(node, "Node", vec![]).unwrap()]).unwrap();
    let metadata = inventory
        .files()
        .iter()
        .find(|f| f.path() == inventory.entry())
        .unwrap()
        .cid();
    let manifest = SnapshotManifest::admit(
        SnapshotManifestRequest::new(
            snapshot,
            &relations,
            &entities,
            vec![
                RelationDescriptor::new(
                    schema().id(),
                    1,
                    vec![
                        BatchDescriptor::new(raw_cid(ipc), 1, u64::try_from(ipc.len()).unwrap())
                            .unwrap(),
                    ],
                )
                .unwrap(),
            ],
            CoverageDescriptor::new(CoverageKind::Complete, raw_cid(b"coverage")).unwrap(),
        )
        .with_graph_projection(GraphProjectionDescriptor::new("0.12.0", *metadata).unwrap()),
    )
    .unwrap();
    let snapshot = SnapshotBlock::encode(manifest).unwrap();
    let query = bind_data_query(
        &bound,
        &snapshot,
        &DataEngineProfile::new("graphar-native", true, []).unwrap(),
    )
    .unwrap();
    (query, snapshot, relations, entities)
}

pub struct Fixture {
    pub query: BoundDataQuery,
    pub binding: GraphDatasetBinding,
    pub inventory: GraphDatasetInventory,
    pub snapshot: SnapshotBlock,
    pub relations: RelationCatalog,
    pub entities: EntityCatalog,
    pub local: MemoryContentStore,
}
impl Fixture {
    pub fn new() -> Self {
        let inventory = GraphDatasetInventory::admit(
            "mrr.graph.yaml".into(),
            vec![
                GraphFile::new(
                    "mrr.graph.yaml".into(),
                    b"metadata",
                    GraphFileKind::Metadata,
                ),
                GraphFile::new(
                    "chunk0".into(),
                    b"opaque-graph-payload",
                    GraphFileKind::Parquet,
                ),
            ],
            GraphInventoryLimits::default(),
        )
        .unwrap();
        let local = MemoryContentStore::default();
        for bytes in [b"metadata".as_slice(), b"opaque-graph-payload"] {
            local
                .put(ContentBlock::new(ContentCodec::Raw, bytes))
                .unwrap();
        }
        Self::with_dataset(inventory, local, b"ipc")
    }
    pub fn with_dataset(
        inventory: GraphDatasetInventory,
        local: MemoryContentStore,
        ipc: &[u8],
    ) -> Self {
        let (query, snapshot, relations, entities) = query(&inventory, ipc);
        let binding = GraphDatasetBinding::admit(
            &query,
            schema().id(),
            &inventory,
            GraphInventoryLimits::default(),
        )
        .unwrap();
        for bytes in [ipc, b"coverage"] {
            local
                .put(ContentBlock::new(ContentCodec::Raw, bytes))
                .unwrap();
        }
        Self {
            query,
            binding,
            inventory,
            snapshot,
            relations,
            entities,
            local,
        }
    }
    pub fn inputs(&self) -> GraphPublishInputs<'_> {
        GraphPublishInputs {
            query: &self.query,
            binding: &self.binding,
            inventory: &self.inventory,
            snapshot: &self.snapshot,
            relations: &self.relations,
            entities: &self.entities,
            inventory_limits: GraphInventoryLimits::default(),
            limits: limits(),
        }
    }
    pub fn prepare(&self) -> PreparedGraphPublication {
        prepare_graph_publication(&self.local, self.inputs()).unwrap()
    }
}
pub fn limits() -> GraphTransferLimits {
    GraphTransferLimits {
        max_blocks: 32,
        max_block_bytes: 100_000,
        max_total_bytes: 1_000_000,
    }
}
#[derive(Default)]
pub struct Remote {
    pub blocks: Mutex<BTreeMap<Cid, Vec<u8>>>,
    pub writes: Mutex<Vec<Cid>>,
    pub fail: Mutex<Option<Cid>>,
    pub lost_ack: Mutex<Option<Cid>>,
}
impl RemoteContentStore for Remote {
    fn get<'a>(&'a self, cid: &'a Cid, max_bytes: usize) -> RemoteFuture<'a, Option<Vec<u8>>> {
        Box::pin(async move {
            let blocks = self.blocks.lock().unwrap();
            if blocks.get(cid).is_some_and(|b| b.len() > max_bytes) {
                return Err(RemoteError::TooLarge);
            }
            Ok(blocks.get(cid).cloned())
        })
    }
    fn put<'a>(&'a self, block: ContentBlock<'a>) -> RemoteFuture<'a, ()> {
        Box::pin(async move {
            self.writes.lock().unwrap().push(block.cid());
            if *self.fail.lock().unwrap() == Some(block.cid()) {
                return Err(RemoteError::Unavailable);
            }
            self.blocks
                .lock()
                .unwrap()
                .insert(block.cid(), block.bytes().to_vec());
            if *self.lost_ack.lock().unwrap() == Some(block.cid()) {
                return Err(RemoteError::Unavailable);
            }
            Ok(())
        })
    }
}
