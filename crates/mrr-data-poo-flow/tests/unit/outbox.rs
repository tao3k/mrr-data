use super::{Guard, Record, State, write_record};
use crate::protocol::PROFILE;
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
