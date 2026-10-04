//! Simulated enrolled policy guards the selected snapshot and disclosure.
use crate::tests::entity_properties::combined::{fixture::Fixture, remote::Remote};
use mrr_data_backend::{
    AuthorityExpectation, AuthorityProposal, AuthorityState, AuthorityStatus, Backend, ProfilePort,
};
use mrr_data_content::{
    ConditionalContentCommitPort, ConditionalContentWrite, publish_combined_graph,
};
const SCOPE: &str = "dataset";
fn operation(root: cid::Cid) -> ConditionalContentWrite<'static> {
    ConditionalContentWrite {
        scope: SCOPE,
        operation_id: "original-source",
        expected: None,
        replacement: root,
    }
}
pub(super) async fn publish(
    f: &Fixture,
    backend: &Backend,
    remote: &Remote,
) -> (
    ProfilePort,
    AuthorityState,
    mrr_data_content::PublishReceipt,
) {
    let base = backend.profile("healthcare", "simulation").unwrap();
    let policy = base
        .advance_authority(
            SCOPE,
            AuthorityProposal {
                authority_id: "policy".into(),
                expected: None,
                replacement: mrr_data_core::raw_cid(b"simulated active policy"),
                status: AuthorityStatus::Active,
            },
        )
        .await
        .unwrap();
    let guarded = base
        .with_authorities(&[AuthorityExpectation {
            authority_id: "policy".into(),
            state: policy,
        }])
        .unwrap();
    let write = operation(*f.query.snapshot_root());
    let prepared = f.prepare();
    *remote.lost_ack.lock().unwrap() = Some(write.replacement);
    assert!(
        publish_combined_graph(&prepared, &f.local, remote, remote, || async { Ok(()) })
            .await
            .is_err()
    );
    assert!(guarded.recover(write).await.unwrap().is_none());
    *remote.lost_ack.lock().unwrap() = None;
    let receipt = publish_combined_graph(&prepared, &f.local, remote, remote, || async { Ok(()) })
        .await
        .unwrap();
    guarded
        .commit(write, Some(&receipt), |_| Ok::<_, ()>(()))
        .await
        .unwrap();
    assert_eq!(
        guarded
            .recover(write)
            .await
            .unwrap()
            .unwrap()
            .committed
            .root,
        write.replacement
    );
    (base, policy, receipt)
}
pub(super) async fn disclose(base: &ProfilePort, policy: AuthorityState) -> bool {
    base.authority(SCOPE, "policy").await.unwrap() == Some(policy)
}
pub(super) async fn retire_and_recover(
    f: &Fixture,
    base: &ProfilePort,
    policy: AuthorityState,
    receipt: &mrr_data_content::PublishReceipt,
) {
    base.advance_authority(
        SCOPE,
        AuthorityProposal {
            authority_id: "policy".into(),
            expected: Some(policy),
            replacement: policy.commitment,
            status: AuthorityStatus::Retired,
        },
    )
    .await
    .unwrap();
    let guarded = base
        .with_authorities(&[AuthorityExpectation {
            authority_id: "policy".into(),
            state: policy,
        }])
        .unwrap();
    assert!(
        guarded
            .recover(operation(*f.query.snapshot_root()))
            .await
            .unwrap()
            .is_some()
    );
    assert!(!disclose(base, policy).await);
    let fresh = ConditionalContentWrite {
        operation_id: "after-retirement",
        expected: Some(
            guarded
                .recover(operation(*f.query.snapshot_root()))
                .await
                .unwrap()
                .unwrap()
                .committed,
        ),
        ..operation(*f.query.snapshot_root())
    };
    assert!(matches!(
        guarded
            .commit(fresh, Some(receipt), |_| Ok::<_, ()>(()))
            .await,
        Err(mrr_data_content::ConditionalCommitPortError::BeforeCommit(
            mrr_data_backend::BackendError::AuthorityRetired
        ))
    ));
    assert!(guarded.recover(fresh).await.unwrap().is_none());
}

pub(super) async fn verify_reopened_history(
    f: &Fixture,
    backend: &Backend,
    policy: AuthorityState,
) {
    let base = backend.profile("healthcare", "simulation").unwrap();
    let guarded = base
        .with_authorities(&[AuthorityExpectation {
            authority_id: "policy".into(),
            state: policy,
        }])
        .unwrap();
    let write = operation(*f.query.snapshot_root());
    assert_eq!(
        guarded
            .recover(write)
            .await
            .unwrap()
            .unwrap()
            .committed
            .root,
        write.replacement
    );
    assert!(matches!(
        guarded
            .commit(write, None, |_| -> Result<(), ()> {
                panic!("historical replay grants no fresh validation")
            })
            .await
            .unwrap(),
        mrr_data_content::ConditionalContentCommitOutcome::Replayed(_)
    ));
    assert!(!disclose(&base, policy).await);
}
