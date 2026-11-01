//! Certificates are bounded to one exact immutable source/projection identity.
use super::{GraphArReadError, GraphArReadLimits, PreparedGraphArSource, prepare_graphar_source};
use crate::tests::{binary_fact as fact, binary_schema as schema};
use crate::{BinaryEntityProjection, write_graphar_dataset};
use meta_relational_reasoning::{RelationCatalog, RelationId, RelationSchema};

fn fixture() -> (
    tempfile::TempDir,
    PreparedGraphArSource,
    BinaryEntityProjection,
) {
    let root = tempfile::tempdir().unwrap();
    let projection = BinaryEntityProjection::admit_catalog(
        &RelationCatalog::admit(vec![schema()]).unwrap(),
        schema().id(),
    )
    .unwrap();
    write_graphar_dataset(
        root.path().join("source"),
        &projection,
        &[projection.project(&fact()).unwrap()],
    )
    .unwrap();
    let prepared =
        prepare_graphar_source(root.path().join("source"), GraphArReadLimits::new(2, 1)).unwrap();
    (root, prepared, projection)
}
fn other() -> RelationSchema {
    RelationSchema::new(
        RelationId::from_canonical_bytes("foreign").unwrap(),
        "foreign",
        schema().fields().to_vec(),
        vec![],
    )
    .unwrap()
}
#[test]
fn failed_projection_never_certifies_and_changed_catalog_never_reuses_the_key() {
    let (_root, prepared, projection) = fixture();
    let foreign = BinaryEntityProjection::admit(&other()).unwrap();
    assert!(matches!(
        prepared.admit(&foreign),
        Err(GraphArReadError::PredicateMismatch { .. })
    ));
    assert!(prepared.admitted_projection.get().is_none());
    assert_eq!(prepared.admit(&projection).unwrap().facts(), &[fact()]);
    assert_eq!(prepared.admitted_projection.get(), Some(&projection));
    let changed_catalog = RelationCatalog::admit(vec![schema(), other()]).unwrap();
    let changed = BinaryEntityProjection::admit_catalog(&changed_catalog, schema().id()).unwrap();
    assert_ne!(changed, projection);
    assert_eq!(prepared.admit(&changed).unwrap().facts(), &[fact()]);
    // One entry only: a successfully validated different catalog cannot grow
    // a source cache or overwrite its first certificate.
    assert_eq!(prepared.admitted_projection.get(), Some(&projection));
    assert!(prepared.admit(&foreign).is_err());
    assert_eq!(prepared.admitted_projection.get(), Some(&projection));
}
#[test]
fn source_clones_share_one_certificate_and_reuse_after_directory_deletion() {
    let (root, prepared, projection) = fixture();
    let clone = prepared.clone();
    std::fs::remove_dir_all(root.path().join("source")).unwrap();
    std::thread::scope(|scope| {
        let workers = (0..8)
            .map(|_| {
                let prepared = &clone;
                let projection = &projection;
                scope.spawn(move || {
                    assert_eq!(prepared.admit(projection).unwrap().facts(), &[fact()]);
                })
            })
            .collect::<Vec<_>>();
        for worker in workers {
            worker.join().unwrap();
        }
    });
    assert_eq!(prepared.admitted_projection.get(), Some(&projection));
    assert_eq!(clone.admitted_projection.get(), Some(&projection));
    assert!(std::sync::Arc::ptr_eq(
        &clone.admitted_projection,
        &prepared.admitted_projection
    ));
    assert!(
        prepared
            .admit(&BinaryEntityProjection::admit(&other()).unwrap())
            .is_err()
    );
}
