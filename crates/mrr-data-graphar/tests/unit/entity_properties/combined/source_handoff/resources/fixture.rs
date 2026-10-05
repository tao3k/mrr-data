//! Each shape shares one immutable payload closure across isolated mode processes.
use crate::tests::entity_properties::combined::source_handoff::{
    backend::selective::{append, shaped_fixture},
    source_fixture,
};
use crate::tests::entity_properties::fixture as properties;
use crate::tests::entity_properties::{
    acceptance::inputs,
    combined::{fixture::Fixture, remote::Remote},
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
    scale_rows: usize,
    snapshot: String,
    dataset: String,
    blocks: Vec<(String, Vec<u8>)>,
}
#[tokio::test]
#[ignore = "isolated immutable fixture producer; use the Rust matrix"]
async fn original_source_resource_fixture() {
    let shape = std::env::var("MRR_DATA_SOURCE_SHAPE").unwrap();
    assert!(matches!(shape.as_str(), "uniform" | "skewed"));
    let path = std::env::var("MRR_DATA_SOURCE_FIXTURE").unwrap();
    write(&shape, Path::new(&path)).await;
}
async fn write(shape: &str, path: &Path) {
    println!("original-source shared fixture preparation started shape={shape}");
    let rows = super::scale::rows();
    let layout = super::scale::layout(rows);
    let f = Fixture::with_original_options_and_limits(
        original(shape, rows),
        GraphArWriteOptions {
            adjacency: GraphArAdjacency::OrderedBySource,
            layout,
            ..GraphArWriteOptions::default()
        },
        super::scale::capture(rows),
        super::scale::transfer(rows),
        if rows == 4 {
            GraphArChunkLayout::new(2, 4).unwrap()
        } else {
            layout
        },
    );
    let prepared = f.prepare();
    let remote = Remote::default();
    publish_combined_graph(&prepared, &f.local, &remote, &remote, || async { Ok(()) })
        .await
        .unwrap();
    let payload = Payload {
        shape: shape.into(),
        scale_rows: rows,
        snapshot: f.snapshot.cid().to_string(),
        dataset: prepared
            .dataset()
            .snapshot_projection(super::scale::capture(rows).dataset)
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
    let rows = super::scale::rows();
    assert_eq!(payload.shape, shape);
    assert_eq!(payload.scale_rows, rows);
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
        super::scale::capture(rows).dataset,
    )
    .unwrap();
    let original = original(shape, rows);
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
        capture_limits: super::scale::capture(rows),
        transfer_limits: super::scale::transfer(rows),
    }
}

fn original(shape: &str, rows: usize) -> properties::Fixture {
    if rows == 4 {
        return shaped_fixture(shape == "skewed");
    }
    let mut original = source_fixture();
    let ids = |kind: &str| {
        (0..rows)
            .map(|index| properties::entity(&format!("resource-{kind}-{index}")).to_string())
            .collect::<Vec<_>>()
    };
    let scenarios = ids("scenario");
    let cases = ids("case");
    let profiles = ids("profile");
    // Each added Case has one Profile; skew changes the source degree without
    // multiplying the physical join or changing the admitted healthcare result.
    for (index, entity_ids) in [scenarios.clone(), cases.clone(), profiles.clone()]
        .into_iter()
        .enumerate()
    {
        append(
            &mut original.entities[index].batch,
            &[entity_ids, vec!["other".into(); rows]],
        );
    }
    let sources = if shape == "skewed" {
        vec![properties::entity("s2").to_string(); rows]
    } else {
        scenarios
    };
    append(&mut original.relations[0].batch, &[sources, cases.clone()]);
    append(&mut original.relations[1].batch, &[cases, profiles]);
    original
}
