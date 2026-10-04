//! Distinct original-source generations share admission, never source authority.
use super::*;

#[tokio::test]
async fn original_source_handoff_cross_profile_reuse_refuses_foreign_generation() {
    let first = Fixture::with_original(source_fixture());
    let second = Fixture::with_original(super::super::source_fixture_with_semantic(Some(
        properties::semantic(
            mrr::GenerationId::from_canonical_bytes("research-generation").unwrap(),
            "research-revision",
        ),
    )));
    assert_eq!(
        first.query.query().catalog_digest(),
        second.query.query().catalog_digest()
    );
    assert_ne!(first.query.snapshot_root(), second.query.snapshot_root());
    let storage = metadata::SimulatedMetadata::default();
    let backend = Backend::open(
        BackendConfig {
            max_resource_bytes: 4 * RESERVED,
            ..BackendConfig::default()
        },
        storage.clone(),
        tokio::runtime::Handle::current(),
    )
    .await
    .unwrap();
    let remote = Arc::new(Remote::default());
    let cache = Arc::new(MemoryContentStore::default());
    let (first_home, first_policy, publication) =
        authority::publish_for(&first, &backend, &remote, "healthcare").await;
    let (second_home, second_policy, _) =
        authority::publish_for(&second, &backend, &remote, "research").await;
    let first_source = capture(
        &first,
        &backend,
        restore(&first, &backend, remote.clone(), cache.clone()).await,
    )
    .await;
    let second_source = capture(
        &second,
        &backend,
        restore(&second, &backend, remote.clone(), cache.clone()).await,
    )
    .await;
    assert_eq!(backend.status().resource_bytes, 2 * RESERVED);
    for (source, foreign) in [
        (&first_source, &second.query),
        (&second_source, &first.query),
    ] {
        assert!(source.get().tables(foreign).is_err());
        assert!(source.get().relations(foreign).is_err());
    }
    // Both captures reuse the original MRR query; every execution/admission is fresh.
    let first_result = captured_transport(&first, &backend, first_source.clone()).await;
    let second_result = captured_transport(&second, &backend, second_source.clone()).await;
    let cap = NonZeroUsize::new(1 << 20).unwrap();
    assert!(
        first_result
            .get()
            .verify(&second.query, result_limits(), cap)
            .is_err()
    );
    assert!(
        second_result
            .get()
            .verify(&first.query, result_limits(), cap)
            .is_err()
    );
    assert_eq!(backend.status().resource_bytes, 4 * RESERVED);
    refuse_over_budget_driver(&backend).await;
    authority::retire_and_recover(&first, &first_home, first_policy, &publication).await;
    assert!(!authority::disclose(&first_home, first_policy).await);
    assert!(authority::disclose(&second_home, second_policy).await);
    drop(first_result);
    drop(second_result);
    drop(first_source);
    assert_eq!(backend.status().resource_bytes, RESERVED);
    // Removing remote storage cannot turn the shared immutable cache into a
    // foreign-generation hit. The second profile still restores its own root.
    remote.blocks.lock().unwrap().clear();
    let warm = restore(&second, &backend, remote, cache).await;
    assert_eq!(warm.get().root(), second.query.snapshot_root());
    drop(warm);
    let retained = captured_transport(&second, &backend, second_source.clone()).await;
    assert!(authority::disclose(&second_home, second_policy).await);
    drop(second_source);
    drain(&backend, retained, &second.query, result_limits(), cap).await;
    let reopened = Backend::open(
        BackendConfig::default(),
        storage,
        tokio::runtime::Handle::current(),
    )
    .await
    .unwrap();
    authority::verify_reopened_history_for(&first, &reopened, first_policy, "healthcare").await;
    let research = reopened.profile("research", "simulation").unwrap();
    assert!(authority::disclose(&research, second_policy).await);
    reopened.shutdown().await.unwrap();
}

// Actual original-query source/result consumers exhaust the common byte budget.
// Refusal must precede the driver; authority recovery above remains independent.
async fn refuse_over_budget_driver(backend: &Backend) {
    let refusal = backend
        .prepare_resource::<()>(1, || panic!("over-budget graph driver ran"))
        .await;
    assert!(matches!(
        refusal,
        Err(mrr_data_backend::BackendError::Saturated)
    ));
    assert_eq!(backend.status().resource_bytes, 4 * RESERVED);
    assert_eq!(backend.status().blocking_resources, 0);
}
