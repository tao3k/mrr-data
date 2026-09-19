use crate::{
    ContentBlock, ContentCodec, ContentError, ContentStore, FilesystemContentStore,
    MemoryContentStore,
};
use std::fs;
use tempfile::tempdir;

#[test]
fn memory_and_filesystem_stores_round_trip_and_verify_blocks() {
    let raw = ContentBlock::new(ContentCodec::Raw, b"payload");
    let memory = MemoryContentStore::default();
    let cid = memory.put(raw).unwrap();
    assert_eq!(memory.get(&cid).unwrap(), b"payload");
    assert_eq!(memory.put(raw).unwrap(), cid);

    let directory = tempdir().unwrap();
    let filesystem = FilesystemContentStore::open(directory.path()).unwrap();
    assert_eq!(filesystem.put(raw).unwrap(), cid);
    assert_eq!(filesystem.get(&cid).unwrap(), b"payload");
    assert_eq!(filesystem.put(raw).unwrap(), cid);

    fs::write(filesystem.root().join(cid.to_string()), b"tampered").unwrap();
    assert!(matches!(
        filesystem.get(&cid),
        Err(ContentError::CidMismatch { .. })
    ));
}

#[test]
fn oversized_sparse_file_is_rejected_before_materialization() {
    let directory = tempdir().unwrap();
    let store = FilesystemContentStore::open(directory.path()).unwrap();
    let cid = store
        .put(ContentBlock::new(ContentCodec::Raw, b"small"))
        .unwrap();
    let file = fs::OpenOptions::new()
        .write(true)
        .open(store.root().join(cid.to_string()))
        .unwrap();
    file.set_len(64 * 1024 * 1024).unwrap();
    assert_eq!(
        store.get_bounded(&cid, 16),
        Err(ContentError::BlockTooLarge {
            limit: 16,
            actual: 64 * 1024 * 1024,
        })
    );
}
