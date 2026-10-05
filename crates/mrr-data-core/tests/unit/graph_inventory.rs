use crate::{
    GraphDatasetInventory, GraphFile, GraphFileKind, GraphInventoryError, GraphInventoryLimits,
    dag_cbor_cid,
};
fn files() -> Vec<GraphFile> {
    vec![
        GraphFile::new(
            "vertex/chunk0.parquet".into(),
            b"parquet",
            GraphFileKind::Parquet,
        ),
        GraphFile::new(
            "mrr.graph.yaml".into(),
            b"metadata",
            GraphFileKind::Metadata,
        ),
    ]
}
#[test]
fn canonical_inventory_is_order_independent_and_verifies_each_payload() {
    let limits = GraphInventoryLimits::default();
    let a = GraphDatasetInventory::admit("mrr.graph.yaml".into(), files(), limits).unwrap();
    let mut reversed = files();
    reversed.reverse();
    let b = GraphDatasetInventory::admit("mrr.graph.yaml".into(), reversed, limits).unwrap();
    assert_eq!(a, b);
    let bytes = a.canonical_bytes(limits).unwrap();
    assert_eq!(
        GraphDatasetInventory::decode_checked(&bytes, &dag_cbor_cid(&bytes), limits).unwrap(),
        a
    );
    assert!(a.files()[0].verify(b"metadata").is_ok());
    assert_eq!(
        a.files()[0].verify(b"metadatz"),
        Err(GraphInventoryError::Integrity)
    );
    assert_eq!(
        a.files()[0].verify(b"meta"),
        Err(GraphInventoryError::Integrity)
    );
}
#[test]
fn portable_paths_duplicates_and_absent_entry_refuse() {
    for path in [
        "../escape",
        "/abs",
        "a//b",
        "a/./b",
        "a/../b",
        "a\\b",
        "c:foo",
        "con.yaml",
        "lpt1",
        "Foo",
        "name.",
    ] {
        let mut input = files();
        input.push(GraphFile::new(path.into(), b"bad", GraphFileKind::Count));
        assert_eq!(
            GraphDatasetInventory::admit(
                "mrr.graph.yaml".into(),
                input,
                GraphInventoryLimits::default()
            ),
            Err(GraphInventoryError::InvalidPath)
        );
    }
    let mut input = files();
    input.push(input[0].clone());
    assert_eq!(
        GraphDatasetInventory::admit(
            "mrr.graph.yaml".into(),
            input,
            GraphInventoryLimits::default()
        ),
        Err(GraphInventoryError::DuplicatePath)
    );
    assert_eq!(
        GraphDatasetInventory::admit(
            "absent.yaml".into(),
            files(),
            GraphInventoryLimits::default()
        ),
        Err(GraphInventoryError::MissingEntry)
    );
}
#[test]
fn decode_checks_root_before_parsing_and_enforces_byte_budgets() {
    let limits = GraphInventoryLimits::default();
    let a = GraphDatasetInventory::admit("mrr.graph.yaml".into(), files(), limits).unwrap();
    let bytes = a.canonical_bytes(limits).unwrap();
    assert_eq!(
        GraphDatasetInventory::decode_checked(&bytes, &dag_cbor_cid(b"wrong"), limits),
        Err(GraphInventoryError::Integrity)
    );
    let tiny = GraphInventoryLimits {
        max_manifest_bytes: 1,
        ..limits
    };
    assert_eq!(
        GraphDatasetInventory::decode_checked(&bytes, &dag_cbor_cid(&bytes), tiny),
        Err(GraphInventoryError::Limit)
    );
    for limited in [
        GraphInventoryLimits {
            max_files: 1,
            ..limits
        },
        GraphInventoryLimits {
            max_total_bytes: 1,
            ..limits
        },
        GraphInventoryLimits {
            max_files: 0,
            ..limits
        },
    ] {
        assert!(GraphDatasetInventory::admit("mrr.graph.yaml".into(), files(), limited).is_err());
    }
}
#[test]
fn forged_wire_order_unknown_fields_and_version_do_not_bypass_admission() {
    use ipld_core::ipld::Ipld;
    let limits = GraphInventoryLimits::default();
    let a = GraphDatasetInventory::admit("mrr.graph.yaml".into(), files(), limits).unwrap();
    let bytes = a.canonical_bytes(limits).unwrap();
    let original: Ipld = serde_ipld_dagcbor::from_slice(&bytes).unwrap();
    for (field, replacement, expected) in [
        ("extra", Ipld::Bool(true), GraphInventoryError::Decode),
        (
            "namespace",
            Ipld::String("mrr.graphar.file-inventory.v1".into()),
            GraphInventoryError::UnsupportedVersion,
        ),
        (
            "version",
            Ipld::Integer(2),
            GraphInventoryError::UnsupportedVersion,
        ),
    ] {
        let Ipld::Map(mut map) = original.clone() else {
            panic!("map")
        };
        map.insert(field.into(), replacement);
        let forged = serde_ipld_dagcbor::to_vec(&Ipld::Map(map)).unwrap();
        assert_eq!(
            GraphDatasetInventory::decode_checked(&forged, &dag_cbor_cid(&forged), limits),
            Err(expected)
        );
    }
    let Ipld::Map(mut map) = original.clone() else {
        panic!("map")
    };
    let Some(Ipld::List(children)) = map.get_mut("files") else {
        panic!("files")
    };
    children.reverse();
    let forged = serde_ipld_dagcbor::to_vec(&Ipld::Map(map)).unwrap();
    assert_eq!(
        GraphDatasetInventory::decode_checked(&forged, &dag_cbor_cid(&forged), limits),
        Err(GraphInventoryError::NonCanonical)
    );

    for (field, replacement, expected) in [
        (
            "cid",
            Ipld::Link(dag_cbor_cid(b"wrong codec")),
            GraphInventoryError::InvalidCid,
        ),
        (
            "kind",
            Ipld::String("parquet".into()),
            GraphInventoryError::MissingEntry,
        ),
        (
            "byte_length",
            Ipld::Integer(i128::from(u64::MAX)),
            GraphInventoryError::Limit,
        ),
    ] {
        let Ipld::Map(mut map) = original.clone() else {
            panic!("map")
        };
        let Some(Ipld::List(children)) = map.get_mut("files") else {
            panic!("files")
        };
        let Ipld::Map(child) = &mut children[0] else {
            panic!("child")
        };
        child.insert(field.into(), replacement);
        let forged = serde_ipld_dagcbor::to_vec(&Ipld::Map(map)).unwrap();
        let limits = GraphInventoryLimits {
            max_total_bytes: u64::MAX,
            ..limits
        };
        assert_eq!(
            GraphDatasetInventory::decode_checked(&forged, &dag_cbor_cid(&forged), limits),
            Err(expected)
        );
    }
}
#[test]
fn distinct_paths_can_share_content_without_undercounting_declared_bytes() {
    let mut input = files();
    input.push(GraphFile::new(
        "vertex/chunk1.parquet".into(),
        b"parquet",
        GraphFileKind::Parquet,
    ));
    let limits = GraphInventoryLimits {
        max_total_bytes: 22,
        ..GraphInventoryLimits::default()
    };
    assert!(GraphDatasetInventory::admit("mrr.graph.yaml".into(), input.clone(), limits).is_ok());
    assert_eq!(
        GraphDatasetInventory::admit(
            "mrr.graph.yaml".into(),
            input,
            GraphInventoryLimits {
                max_total_bytes: 21,
                ..limits
            }
        ),
        Err(GraphInventoryError::Limit)
    );
}

#[test]
fn streamed_identity_matches_borrowed_identity_and_refuses_read_overflow() {
    let bytes = vec![7u8; 100_000];
    let limits = GraphInventoryLimits::default();
    let streamed = GraphFile::from_reader(
        "chunk0".into(),
        bytes.as_slice(),
        GraphFileKind::Parquet,
        limits,
    )
    .unwrap();
    assert_eq!(
        streamed,
        GraphFile::new("chunk0".into(), &bytes, GraphFileKind::Parquet)
    );
    assert_eq!(
        GraphFile::from_reader(
            "chunk0".into(),
            bytes.as_slice(),
            GraphFileKind::Parquet,
            GraphInventoryLimits {
                max_total_bytes: 99_999,
                ..limits
            }
        ),
        Err(GraphInventoryError::Limit)
    );
    let broken = std::io::Read::take(std::io::repeat(1), 100);
    assert_eq!(
        GraphFile::from_reader(
            "chunk0".into(),
            broken,
            GraphFileKind::Parquet,
            GraphInventoryLimits {
                max_total_bytes: 10,
                ..limits
            }
        ),
        Err(GraphInventoryError::Limit)
    );
}

#[test]
fn stream_budget_reads_only_one_probe_byte_and_io_failure_refuses() {
    struct Broken;
    impl std::io::Read for Broken {
        fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
            Err(std::io::Error::other("read failed"))
        }
    }
    let bytes = vec![1u8; 100_000];
    let mut cursor = std::io::Cursor::new(&bytes);
    let limits = GraphInventoryLimits {
        max_total_bytes: 10,
        ..GraphInventoryLimits::default()
    };
    assert_eq!(
        GraphFile::from_reader("chunk0".into(), &mut cursor, GraphFileKind::Parquet, limits),
        Err(GraphInventoryError::Limit)
    );
    assert_eq!(cursor.position(), 11);
    assert_eq!(
        GraphFile::from_reader("chunk0".into(), Broken, GraphFileKind::Parquet, limits),
        Err(GraphInventoryError::Io)
    );
}
