//! Portable descriptor integrity and process-independent property recovery.
use super::{
    acceptance::{inputs, limits},
    fixture,
};
use crate::{
    GraphArChunkLayout, GraphArEntityPropertyError as Error, GraphArEntityPropertyReceipt,
    write_graphar_entity_properties,
};
use std::{
    process::Command,
    time::{Duration, Instant},
};

#[test]
fn descriptor_authenticates_scope_bytes_and_bounds() {
    let f = fixture::fixture();
    let (projection, tables) = inputs(&f);
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    let receipt = write_graphar_entity_properties(
        &source,
        &projection,
        &f.semantic,
        &tables,
        GraphArChunkLayout::new(2, 4).unwrap(),
        limits(),
    )
    .unwrap();
    let block = receipt.descriptor(limits()).unwrap();
    let restored = GraphArEntityPropertyReceipt::decode_descriptor_checked(
        source.clone(),
        block.cid(),
        block.bytes(),
        &projection,
        limits(),
    )
    .unwrap();
    assert_eq!(restored.snapshot_digest(), receipt.snapshot_digest());
    assert_eq!(restored.inventory(), receipt.inventory());
    assert_eq!(
        restored.descriptor(limits()).unwrap().bytes(),
        block.bytes()
    );
    let mut corrupt = block.bytes().to_vec();
    corrupt[0] ^= 1;
    assert!(matches!(
        GraphArEntityPropertyReceipt::decode_descriptor_checked(
            source.clone(),
            block.cid(),
            &corrupt,
            &projection,
            limits()
        ),
        Err(Error::Integrity)
    ));
    let mut bounded = limits();
    bounded.inventory.max_manifest_bytes = block.bytes().len() - 1;
    assert!(matches!(
        GraphArEntityPropertyReceipt::decode_descriptor_checked(
            source.clone(),
            block.cid(),
            block.bytes(),
            &projection,
            bounded
        ),
        Err(Error::Budget(_))
    ));
    let smaller =
        meta_relational_reasoning::EntityCatalog::admit(vec![tables[0].schema.clone()]).unwrap();
    let different = crate::GraphArEntityPropertyProjection::admit(&smaller).unwrap();
    assert!(matches!(
        GraphArEntityPropertyReceipt::decode_descriptor_checked(
            source,
            block.cid(),
            block.bytes(),
            &different,
            limits()
        ),
        Err(Error::Scope)
    ));
}

#[test]
fn descriptor_refuses_unknown_fields_versions_and_layout() {
    let f = fixture::fixture();
    let (projection, tables) = inputs(&f);
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    let receipt = write_graphar_entity_properties(
        &source,
        &projection,
        &f.semantic,
        &tables,
        GraphArChunkLayout::new(2, 4).unwrap(),
        limits(),
    )
    .unwrap();
    let block = receipt.descriptor(limits()).unwrap();
    let original: ipld_core::ipld::Ipld = serde_ipld_dagcbor::from_slice(block.bytes()).unwrap();
    for (field, value) in [
        ("version", ipld_core::ipld::Ipld::Integer(2)),
        ("unexpected", ipld_core::ipld::Ipld::Bool(true)),
        ("vertex_chunk", ipld_core::ipld::Ipld::Integer(0)),
        ("rows", ipld_core::ipld::Ipld::Integer(101)),
    ] {
        let mut modified = original.clone();
        let ipld_core::ipld::Ipld::Map(ref mut fields) = modified else {
            panic!("descriptor must be a map")
        };
        fields.insert(field.into(), value);
        let bytes = serde_ipld_dagcbor::to_vec(&modified).unwrap();
        let cid = mrr_data_core::dag_cbor_cid(&bytes);
        assert!(
            GraphArEntityPropertyReceipt::decode_descriptor_checked(
                source.clone(),
                &cid,
                &bytes,
                &projection,
                limits()
            )
            .is_err(),
            "accepted invalid {field}"
        );
    }
}

#[test]
fn property_descriptor_reopens_in_new_process() {
    let f = fixture::fixture();
    let (projection, tables) = inputs(&f);
    let dir = tempfile::tempdir().unwrap();
    let receipt = write_graphar_entity_properties(
        &dir.path().join("source"),
        &projection,
        &f.semantic,
        &tables,
        GraphArChunkLayout::new(2, 4).unwrap(),
        limits(),
    )
    .unwrap();
    let block = receipt.descriptor(limits()).unwrap();
    let root = super::registered::snapshot(&f, &block);
    std::fs::write(dir.path().join("snapshot.cbor"), root.bytes()).unwrap();
    std::fs::write(dir.path().join("snapshot.cid"), root.cid().to_string()).unwrap();
    std::fs::write(dir.path().join("manifest.cbor"), block.bytes()).unwrap();
    std::fs::write(dir.path().join("trusted-root.cid"), block.cid().to_string()).unwrap();
    drop((receipt, block, projection, tables, f));
    let mut child = Command::new(std::env::current_exe().unwrap())
        .current_dir(dir.path())
        .args([
            "--exact",
            "tests::entity_properties::descriptor::property_descriptor_child_reopens",
            "--ignored",
            "--nocapture",
        ])
        .spawn()
        .unwrap();
    let started = Instant::now();
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success(), "property restore child failed: {status}");
            assert_eq!(
                std::fs::read(dir.path().join("restore-complete")).unwrap(),
                b"8 complete rows"
            );
            break;
        }
        // This short native slice must finish inside the strict 5-second gate.
        if started.elapsed() >= Duration::from_secs(5) {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("property restore child exceeded 5 seconds");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
#[ignore = "fresh-process worker, invoked by its bounded parent acceptance test"]
fn property_descriptor_child_reopens() {
    println!("property restore: authenticating portable descriptor");
    let f = fixture::fixture();
    let (projection, expected) = inputs(&f);
    let source = std::env::current_dir().unwrap().join("source");
    let bytes = std::fs::read("manifest.cbor").unwrap();
    let root: cid::Cid = std::fs::read_to_string("trusted-root.cid")
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(mrr_data_core::dag_cbor_cid(&bytes), root);
    let root: cid::Cid = std::fs::read_to_string("snapshot.cid")
        .unwrap()
        .parse()
        .unwrap();
    let snapshot_bytes = std::fs::read("snapshot.cbor").unwrap();
    let manifest = mrr_data_core::SnapshotManifest::decode_checked(&snapshot_bytes, &root).unwrap();
    let snapshot = mrr_data_core::SnapshotBlock::encode(manifest).unwrap();
    let query = super::registered::bound(&f, &snapshot);
    let captured = crate::capture_registered_graphar_entity_properties(
        &source,
        &query,
        &projection,
        &bytes,
        limits(),
    )
    .unwrap();
    let actual = captured.tables(&query).unwrap();
    assert_eq!(actual.len(), expected.len());
    for table in actual {
        let expected = expected.iter().find(|t| t.schema == table.schema).unwrap();
        // Native physical indices sort canonical IDs; compare complete logical rows.
        assert_eq!(rows(&table.batch), rows(&expected.batch));
    }
    assert_eq!(actual.iter().map(|t| t.batch.num_rows()).sum::<usize>(), 8);
    std::fs::write("restore-complete", b"8 complete rows").unwrap();
    println!("property restore: 8 complete rows admitted in fresh process");
}

fn rows(batch: &arrow_array::RecordBatch) -> Vec<Vec<Option<String>>> {
    let mut rows = (0..batch.num_rows())
        .map(|i| {
            batch
                .columns()
                .iter()
                .map(|col| {
                    use arrow_array::Array;
                    let strings = col
                        .as_any()
                        .downcast_ref::<arrow_array::StringArray>()
                        .unwrap();
                    (!strings.is_null(i)).then(|| strings.value(i).to_owned())
                })
                .collect()
        })
        .collect::<Vec<Vec<Option<String>>>>();
    rows.sort();
    rows
}
