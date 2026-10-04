//! Each shape shares one immutable payload closure across isolated mode processes.
use crate::tests::entity_properties::combined::source_handoff::backend::selective::shaped_fixture;
use crate::tests::entity_properties::{
    acceptance::inputs,
    combined::{
        fixture::{Fixture, capture_limits},
        remote::Remote,
    },
};
use crate::{GraphArAdjacency, GraphArChunkLayout, GraphArWriteOptions};
use meta_relational_reasoning as mrr;
use mrr_data_content::{
    ContentBlock, ContentCodec, ContentStore, MemoryContentStore, publish_combined_graph,
};
use mrr_data_core::{GraphDatasetDescriptor, SnapshotBlock, SnapshotManifest, bind_data_query};
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Serialize, Deserialize)]
struct Payload {
    shape: String,
    snapshot: String,
    dataset: String,
    blocks: Vec<(String, Vec<u8>)>,
}
pub(super) async fn write(shape: &str, path: &Path) {
    println!("original-source shared fixture preparation started shape={shape}");
    let f = Fixture::with_original_options(
        shaped_fixture(shape == "skewed"),
        GraphArWriteOptions {
            adjacency: GraphArAdjacency::OrderedBySource,
            layout: GraphArChunkLayout::new(2, 2).unwrap(),
            ..GraphArWriteOptions::default()
        },
    );
    let prepared = f.prepare();
    let remote = Remote::default();
    publish_combined_graph(&prepared, &f.local, &remote, &remote, || async { Ok(()) })
        .await
        .unwrap();
    let payload = Payload {
        shape: shape.into(),
        snapshot: f.snapshot.cid().to_string(),
        dataset: prepared
            .dataset()
            .snapshot_projection(capture_limits().dataset)
            .unwrap()
            .manifest_cid()
            .to_string(),
        blocks: remote
            .blocks
            .lock()
            .unwrap()
            .iter()
            .map(|(cid, bytes)| (cid.to_string(), bytes.clone()))
            .collect(),
    };
    std::fs::write(path, serde_json::to_vec(&payload).unwrap()).unwrap();
    println!("original-source shared fixture ready shape={shape}");
}
pub(super) fn load(shape: &str) -> Fixture {
    let path = std::env::var("MRR_DATA_SOURCE_FIXTURE").unwrap();
    let payload: Payload = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    assert_eq!(payload.shape, shape);
    let local = MemoryContentStore::default();
    for (root, bytes) in payload.blocks {
        let cid = root.parse::<cid::Cid>().unwrap();
        let codec = match cid.codec() {
            mrr_data_core::RAW_CODEC => ContentCodec::Raw,
            mrr_data_core::DAG_CBOR_CODEC => ContentCodec::DagCbor,
            _ => panic!("unsupported fixture content codec"),
        };
        assert_eq!(local.put(ContentBlock::new(codec, &bytes)).unwrap(), cid);
    }
    let root = payload.snapshot.parse::<cid::Cid>().unwrap();
    let snapshot = SnapshotBlock::encode(
        SnapshotManifest::decode_checked(&local.get(&root).unwrap(), &root).unwrap(),
    )
    .unwrap();
    let root = payload.dataset.parse::<cid::Cid>().unwrap();
    let dataset = GraphDatasetDescriptor::decode_checked(
        &local.get(&root).unwrap(),
        &root,
        capture_limits().dataset,
    )
    .unwrap();
    let original = shaped_fixture(shape == "skewed");
    let (projection, _) = inputs(&original);
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
    let query = bind_data_query(
        &original.query,
        &snapshot,
        &mrr_data_datafusion::datafusion_engine_profile().unwrap(),
    )
    .unwrap();
    Fixture {
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
