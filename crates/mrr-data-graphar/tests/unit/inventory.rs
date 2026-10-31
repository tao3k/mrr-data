use crate::{GraphArInventoryError, inventory_graphar_directory, verify_graphar_directory};
use mrr_data_core::{GraphInventoryError, GraphInventoryLimits};
use std::fs;
fn fixture(root: &std::path::Path) {
    fs::create_dir_all(root.join("vertex/entity/properties")).unwrap();
    fs::write(root.join("mrr.graph.yaml"), b"metadata").unwrap();
    fs::write(root.join("vertex/entity/vertex_count"), 1i64.to_le_bytes()).unwrap();
    fs::write(
        root.join("vertex/entity/properties/chunk0"),
        b"PAR1fixture\x07\x00\x00\x00PAR1",
    )
    .unwrap();
}
#[test]
fn directory_receipt_detects_changed_missing_and_extra_files() {
    let directory = tempfile::tempdir().unwrap();
    fixture(directory.path());
    let limits = GraphInventoryLimits::default();
    let expected = inventory_graphar_directory(directory.path(), limits).unwrap();
    assert_eq!(expected.files().len(), 3);
    verify_graphar_directory(directory.path(), &expected, limits).unwrap();
    let chunk = directory.path().join("vertex/entity/properties/chunk0");
    fs::write(&chunk, b"PAR1changed\x07\x00\x00\x00PAR1").unwrap();
    assert_eq!(
        verify_graphar_directory(directory.path(), &expected, limits),
        Err(GraphArInventoryError::Integrity)
    );
    fs::remove_file(chunk).unwrap();
    assert_eq!(
        verify_graphar_directory(directory.path(), &expected, limits),
        Err(GraphArInventoryError::Integrity)
    );
    fixture(directory.path());
    fs::write(
        directory.path().join("vertex/entity/properties/chunk1"),
        b"PAR1extra\x05\x00\x00\x00PAR1",
    )
    .unwrap();
    assert_eq!(
        verify_graphar_directory(directory.path(), &expected, limits),
        Err(GraphArInventoryError::Integrity)
    );
}
#[test]
fn directory_inventory_refuses_budgets_unknown_files_and_path_aliases() {
    let directory = tempfile::tempdir().unwrap();
    fixture(directory.path());
    for limits in [
        GraphInventoryLimits {
            max_total_bytes: 1,
            ..GraphInventoryLimits::default()
        },
        GraphInventoryLimits {
            max_files: 1,
            ..GraphInventoryLimits::default()
        },
    ] {
        assert_eq!(
            inventory_graphar_directory(directory.path(), limits),
            Err(GraphArInventoryError::Inventory(GraphInventoryError::Limit))
        );
    }
    fs::write(directory.path().join("unknown.bin"), b"unqualified").unwrap();
    assert_eq!(
        inventory_graphar_directory(directory.path(), GraphInventoryLimits::default()),
        Err(GraphArInventoryError::UnsupportedFile)
    );
    fs::remove_file(directory.path().join("unknown.bin")).unwrap();
    fs::write(directory.path().join("CON.yaml"), b"alias").unwrap();
    assert_eq!(
        inventory_graphar_directory(directory.path(), GraphInventoryLimits::default()),
        Err(GraphArInventoryError::Inventory(
            GraphInventoryError::InvalidPath
        ))
    );
}
#[cfg(unix)]
#[test]
fn directory_inventory_refuses_symlinks_and_hard_links() {
    use std::os::unix::fs::symlink;
    let directory = tempfile::tempdir().unwrap();
    fixture(directory.path());
    symlink(
        directory.path().join("vertex"),
        directory.path().join("alias"),
    )
    .unwrap();
    assert_eq!(
        inventory_graphar_directory(directory.path(), GraphInventoryLimits::default()),
        Err(GraphArInventoryError::UnsupportedEntry)
    );
    fs::remove_file(directory.path().join("alias")).unwrap();
    fs::hard_link(
        directory.path().join("mrr.graph.yaml"),
        directory.path().join("alias.yaml"),
    )
    .unwrap();
    assert_eq!(
        inventory_graphar_directory(directory.path(), GraphInventoryLimits::default()),
        Err(GraphArInventoryError::UnsupportedEntry)
    );
}

#[test]
fn parquet_framing_and_count_width_are_checked_before_receipting() {
    let directory = tempfile::tempdir().unwrap();
    fixture(directory.path());
    fs::write(
        directory.path().join("vertex/entity/properties/chunk0"),
        b"not parquet",
    )
    .unwrap();
    assert_eq!(
        inventory_graphar_directory(directory.path(), GraphInventoryLimits::default()),
        Err(GraphArInventoryError::UnsupportedFile)
    );
    fixture(directory.path());
    fs::write(
        directory.path().join("vertex/entity/vertex_count"),
        b"short",
    )
    .unwrap();
    assert_eq!(
        inventory_graphar_directory(directory.path(), GraphInventoryLimits::default()),
        Err(GraphArInventoryError::UnsupportedFile)
    );
}
