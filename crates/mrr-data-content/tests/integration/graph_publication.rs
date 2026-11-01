#![cfg(feature = "graph-snapshot")]
#[path = "../support/graph_publication_fixture.rs"]
mod fixture;
use fixture::{Fixture, Remote, limits};
use mrr_data_content::{
    ContentBlock, ContentCodec, ContentStore, GraphTransferError, MemoryContentStore,
    prepare_graph_publication, publish_graph_dataset, restore_graph_dataset,
};
use mrr_data_core::GraphInventoryLimits;

#[tokio::test]
async fn complete_graph_closure_acks_binding_last_and_restores_from_cold_cache() {
    let f = Fixture::new();
    let prepared = f.prepare();
    let remote = Remote::default();
    let publication =
        publish_graph_dataset(&prepared, &f.local, &remote, &remote, || async { Ok(()) })
            .await
            .unwrap();
    assert_eq!(publication.receipt().cid, *prepared.root());
    let writes = remote.writes.lock().unwrap().clone();
    assert_eq!(writes.last(), Some(prepared.root()));
    assert_eq!(writes.len(), prepared.block_count());
    let cold = MemoryContentStore::default();
    let restored = restore_graph_dataset(
        &cold,
        &remote,
        prepared.root(),
        &f.query,
        &f.relations,
        &f.entities,
        (limits(), GraphInventoryLimits::default()),
    )
    .await
    .unwrap();
    assert_eq!(restored.root(), prepared.root());
    assert_eq!(restored.total_bytes(), prepared.total_bytes());
}
#[tokio::test]
async fn every_failed_ack_or_root_gate_refuses_a_complete_receipt() {
    let f = Fixture::new();
    let prepared = f.prepare();
    let successful = Remote::default();
    publish_graph_dataset(&prepared, &f.local, &successful, &successful, || async {
        Ok(())
    })
    .await
    .unwrap();
    let acknowledged = successful.writes.lock().unwrap().clone();
    for cid in acknowledged {
        let remote = Remote::default();
        *remote.fail.lock().unwrap() = Some(cid);
        assert!(
            publish_graph_dataset(&prepared, &f.local, &remote, &remote, || async { Ok(()) })
                .await
                .is_err()
        );
        if cid != *prepared.root() {
            assert!(!remote.writes.lock().unwrap().contains(prepared.root()));
        }
    }
    let remote = Remote::default();
    assert!(
        publish_graph_dataset(&prepared, &f.local, &remote, &remote, || async {
            Err(GraphTransferError::RootDenied)
        })
        .await
        .is_err()
    );
    assert!(!remote.writes.lock().unwrap().contains(prepared.root()));
}
#[tokio::test]
async fn lost_root_ack_can_leave_orphan_content_but_same_cid_retry_recovers() {
    let f = Fixture::new();
    let prepared = f.prepare();
    let remote = Remote::default();
    *remote.lost_ack.lock().unwrap() = Some(*prepared.root());
    assert!(
        publish_graph_dataset(&prepared, &f.local, &remote, &remote, || async { Ok(()) })
            .await
            .is_err()
    );
    assert!(remote.blocks.lock().unwrap().contains_key(prepared.root()));
    *remote.lost_ack.lock().unwrap() = None;
    let receipt = publish_graph_dataset(&prepared, &f.local, &remote, &remote, || async { Ok(()) })
        .await
        .unwrap();
    assert_eq!(receipt.receipt().cid, *prepared.root());
}
#[test]
fn missing_children_and_limits_refuse_before_any_publication() {
    let f = Fixture::new();
    assert!(prepare_graph_publication(&MemoryContentStore::default(), f.inputs()).is_err());
    let mut inputs = f.inputs();
    inputs.limits.max_blocks = 3;
    assert!(prepare_graph_publication(&f.local, inputs).is_err());
    let mut inputs = f.inputs();
    inputs.limits.max_total_bytes = 1;
    assert!(prepare_graph_publication(&f.local, inputs).is_err());
    assert!(
        f.local
            .get_bounded(&ContentBlock::new(ContentCodec::Raw, b"ipc").cid(), 2)
            .is_err()
    );
}

#[tokio::test]
async fn cold_restore_refuses_tampered_children_and_aggregate_budget() {
    let f = Fixture::new();
    let prepared = f.prepare();
    let remote = Remote::default();
    publish_graph_dataset(&prepared, &f.local, &remote, &remote, || async { Ok(()) })
        .await
        .unwrap();
    let child = *f.inventory.files()[0].cid();
    remote
        .blocks
        .lock()
        .unwrap()
        .insert(child, b"tampered".to_vec());
    assert!(
        restore_graph_dataset(
            &MemoryContentStore::default(),
            &remote,
            prepared.root(),
            &f.query,
            &f.relations,
            &f.entities,
            (limits(), GraphInventoryLimits::default())
        )
        .await
        .is_err()
    );
    let remote = Remote::default();
    publish_graph_dataset(&prepared, &f.local, &remote, &remote, || async { Ok(()) })
        .await
        .unwrap();
    let bounded = mrr_data_content::GraphTransferLimits {
        max_total_bytes: prepared.total_bytes() - 1,
        ..limits()
    };
    assert!(
        restore_graph_dataset(
            &MemoryContentStore::default(),
            &remote,
            prepared.root(),
            &f.query,
            &f.relations,
            &f.entities,
            (bounded, GraphInventoryLimits::default())
        )
        .await
        .is_err()
    );
}
#[tokio::test]
async fn cancelling_before_root_gate_leaves_no_binding_ack() {
    let f = Fixture::new();
    let prepared = f.prepare();
    let remote = Remote::default();
    let (entered, wait) = tokio::sync::oneshot::channel();
    let mut publication = Box::pin(publish_graph_dataset(
        &prepared,
        &f.local,
        &remote,
        &remote,
        || async {
            entered.send(()).unwrap();
            std::future::pending::<Result<(), GraphTransferError>>().await
        },
    ));
    tokio::select! {
        result = &mut publication => panic!("unexpected completion: {result:?}"),
        _ = wait => (),
    }
    drop(publication);
    assert!(!remote.writes.lock().unwrap().contains(prepared.root()));
}
