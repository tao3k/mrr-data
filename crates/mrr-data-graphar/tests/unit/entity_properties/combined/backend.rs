use super::fixture::{Fixture, capture_limits};
use crate::{
    CombinedGraphArRequest, GraphArEntityPropertyError as Error, prepare_combined_graphar,
};
use mrr_data_backend::{
    Backend, BackendConfig, BackendError, Lifecycle, ResourceControl, ResourcePreparationError,
    ResourceStop,
};
#[tokio::test]
async fn combined_native_preparation_uses_one_lease_and_drains_last_clone() {
    const RESERVED: usize = 8 << 20;
    let f = Fixture::new();
    let request = || CombinedGraphArRequest {
        closure: f.prepare(),
        query: f.query.clone(),
        relations: f.relations.clone(),
        properties: f.projection.clone(),
        limits: capture_limits(),
    };
    let backend = Backend::open(
        BackendConfig {
            max_resource_bytes: RESERVED,
            ..BackendConfig::default()
        },
        crate::tests::snapshot::backend_qualification::MetadataStub,
        tokio::runtime::Handle::current(),
    )
    .await
    .unwrap();
    assert!(matches!(
        prepare_combined_graphar(&backend, request(), 1, ResourceControl::new(None)).await,
        Err(ResourcePreparationError::Backend(BackendError::Limit))
    ));
    let canceled = ResourceControl::new(None);
    canceled.cancel();
    assert!(matches!(
        prepare_combined_graphar(&backend, request(), RESERVED, canceled).await,
        Err(ResourcePreparationError::Preparation(Error::Stop(
            ResourceStop::Cancelled
        )))
    ));
    assert_eq!(backend.status().resource_bytes, 0);
    let captured =
        prepare_combined_graphar(&backend, request(), RESERVED, ResourceControl::new(None))
            .await
            .unwrap();
    let pointer = captured.get().tables(&f.query).unwrap().as_ptr();
    let parts = captured
        .try_transform(|value| value.into_parts(&f.query))
        .unwrap_or_else(|_| panic!("unique source handoff"));
    assert_eq!(parts.get().tables.as_ptr(), pointer);
    assert_eq!(parts.get().relations.len(), f.relations.relations().len());
    let clone = parts.clone();
    let closing = backend.clone();
    let shutdown = tokio::spawn(async move { closing.shutdown().await });
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while backend.status().lifecycle != Lifecycle::Draining {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    drop(parts);
    assert_eq!(backend.status().resource_bytes, RESERVED);
    drop(clone);
    tokio::time::timeout(std::time::Duration::from_secs(3), shutdown)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(backend.status().resource_bytes, 0);
}
