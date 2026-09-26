use super::{BoundedQueryCache, Guard, Record, State, write_record};
use crate::protocol::PROFILE;
use cid::Cid;
use mrr_data_cache::BlockingContentStore;
use mrr_data_content::{
    CacheAdmission, ContentBlock, ContentSource, FilesystemContentStore, RemoteContentStore,
    RemoteFuture, read_through,
};
use mrr_data_core::raw_cid;

#[test]
fn maintenance_keeps_pending_and_shared_blocks() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path();
    let shared = raw_cid(b"shared").to_string();
    let pending_root = raw_cid(b"pending").to_string();
    let expired_root = raw_cid(b"expired").to_string();
    for (cid, bytes) in [
        (&shared, b"shared".as_slice()),
        (&pending_root, b"pending".as_slice()),
        (&expired_root, b"expired".as_slice()),
    ] {
        std::fs::write(path.join(cid), bytes).unwrap();
    }
    let make = |root: String, state: State| Record {
        profile: PROFILE.to_owned(),
        source: "source".into(),
        revision: "revision".into(),
        generation: "generation".into(),
        protected_until: 1,
        state,
        blocks: vec![root.clone(), shared.clone()],
        root,
    };
    write_record(path, &make(pending_root.clone(), State::Pending)).unwrap();
    write_record(path, &make(expired_root.clone(), State::Synced)).unwrap();
    let guard = Guard::acquire(path).unwrap();
    guard.maintain(2).unwrap();
    assert!(path.join(&shared).exists());
    assert!(path.join(&pending_root).exists());
    assert!(!path.join(&expired_root).exists());
    assert!(guard.read(&pending_root).is_ok());
    assert!(guard.read(&expired_root).is_err());
}

struct RemoteBlock {
    cid: Cid,
    bytes: Vec<u8>,
}

impl RemoteContentStore for RemoteBlock {
    fn get<'a>(&'a self, cid: &'a Cid, max_bytes: usize) -> RemoteFuture<'a, Option<Vec<u8>>> {
        Box::pin(async move {
            Ok((*cid == self.cid && self.bytes.len() <= max_bytes).then(|| self.bytes.clone()))
        })
    }

    fn put<'a>(&'a self, _block: ContentBlock<'a>) -> RemoteFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test]
async fn remote_read_survives_rejected_cache_fill_without_exceeding_capacity() {
    let directory = tempfile::tempdir().unwrap();
    let held = b"held";
    let held_cid = raw_cid(held);
    std::fs::write(directory.path().join(held_cid.to_string()), held).unwrap();
    let remote = RemoteBlock {
        cid: raw_cid(b"remote"),
        bytes: b"remote".to_vec(),
    };
    let local = BlockingContentStore::new(FilesystemContentStore::open(directory.path()).unwrap());
    let bounded = BoundedQueryCache {
        local: &local,
        path: directory.path(),
        max_bytes: held.len() as u64,
    };
    let read = read_through(&bounded, &remote, &remote.cid, 32)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read.bytes, remote.bytes);
    assert!(matches!(
        read.source,
        ContentSource::Remote(CacheAdmission::Failed(_))
    ));
    assert!(!directory.path().join(remote.cid.to_string()).exists());
    assert_eq!(
        std::fs::read(directory.path().join(held_cid.to_string())).unwrap(),
        held
    );
}

#[tokio::test]
async fn fitting_remote_fill_is_reused_locally() {
    let directory = tempfile::tempdir().unwrap();
    let remote = RemoteBlock {
        cid: raw_cid(b"remote"),
        bytes: b"remote".to_vec(),
    };
    let local = BlockingContentStore::new(FilesystemContentStore::open(directory.path()).unwrap());
    let bounded = BoundedQueryCache {
        local: &local,
        path: directory.path(),
        max_bytes: remote.bytes.len() as u64,
    };
    let first = read_through(&bounded, &remote, &remote.cid, 32)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.source, ContentSource::Remote(CacheAdmission::Stored));
    let second = read_through(&bounded, &remote, &remote.cid, 32)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second.source, ContentSource::Local);
    assert_eq!(second.bytes, remote.bytes);
}

#[tokio::test]
async fn concurrent_remote_fills_admit_at_most_one_block() {
    let directory = tempfile::tempdir().unwrap();
    let first_remote = RemoteBlock {
        cid: raw_cid(b"first!"),
        bytes: b"first!".to_vec(),
    };
    let second_remote = RemoteBlock {
        cid: raw_cid(b"other!"),
        bytes: b"other!".to_vec(),
    };
    let first_local =
        BlockingContentStore::new(FilesystemContentStore::open(directory.path()).unwrap());
    let second_local =
        BlockingContentStore::new(FilesystemContentStore::open(directory.path()).unwrap());
    let first_cache = BoundedQueryCache {
        local: &first_local,
        path: directory.path(),
        max_bytes: 6,
    };
    let second_cache = BoundedQueryCache {
        local: &second_local,
        path: directory.path(),
        max_bytes: 6,
    };
    let (first, second) = tokio::join!(
        read_through(&first_cache, &first_remote, &first_remote.cid, 32),
        read_through(&second_cache, &second_remote, &second_remote.cid, 32),
    );
    assert_eq!(first.unwrap().unwrap().bytes, first_remote.bytes);
    assert_eq!(second.unwrap().unwrap().bytes, second_remote.bytes);
    let persisted = [first_remote.cid, second_remote.cid]
        .iter()
        .filter(|cid| directory.path().join(cid.to_string()).exists())
        .count();
    assert_eq!(persisted, 1);
}
