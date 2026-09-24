#[test]
#[cfg(feature = "arrow")]
fn arrow_is_available_from_the_default_facade() {
    assert_eq!(
        crate::arrow::ARROW_FACT_SCHEMA_NAMESPACE,
        crate::profile::ARROW_FACT_SCHEMA_NAMESPACE
    );
}

#[test]
fn profile_is_available_without_optional_features() {
    assert_eq!(crate::profile::ARROW_FACT_SCHEMA_VERSION, 1);
}

#[test]
#[cfg(feature = "content")]
fn content_surface_is_available_when_selected() {
    assert_eq!(
        crate::manifest::SNAPSHOT_SCHEMA_NAMESPACE,
        crate::profile::SNAPSHOT_SCHEMA_NAMESPACE
    );
    let _ = core::mem::size_of::<crate::content::ContentCodec>();
}

#[test]
#[cfg(feature = "graphar")]
fn graphar_surface_is_available_when_selected() {
    assert_eq!(
        crate::graphar::GRAPHAR_BINARY_ENTITY_NAMESPACE,
        crate::profile::GRAPHAR_BINARY_ENTITY_NAMESPACE
    );
    let _ = core::mem::size_of::<crate::graphar::GraphArQuerySource>();
}

#[test]
#[cfg(all(feature = "content-identity", feature = "graphar"))]
fn graphar_query_source_admission_is_available_from_the_composed_facade() {
    let _ = core::mem::size_of::<crate::GraphArQuerySourceBindingError>();
    let _ = crate::admit_graphar_query_source;
}

#[test]
#[cfg(feature = "datafusion")]
fn datafusion_surface_is_available_when_selected() {
    assert_eq!(
        crate::datafusion::datafusion_engine_profile()
            .unwrap()
            .name(),
        "datafusion-arrow"
    );
}

#[test]
#[cfg(feature = "content-identity")]
fn content_identity_surface_is_available_without_storage() {
    let _ = crate::manifest::raw_cid(b"content identity");
}

#[test]
#[cfg(feature = "car")]
fn car_surface_requires_its_feature() {
    let _ = crate::content::CarImportLimits::new(100, 1, 100, 100);
}

#[test]
#[cfg(feature = "filesystem")]
fn filesystem_surface_is_available_from_the_default_facade() {
    let _ = core::mem::size_of::<crate::content::FilesystemContentStore>();
}

#[test]
#[cfg(feature = "snapshot")]
fn snapshot_transfer_surface_is_available_without_car() {
    let _ = crate::content::SnapshotTransferLimits::new(100, 10, 100, 1000);
}

#[test]
#[cfg(feature = "transfer")]
fn runtime_transfer_surface_is_opt_in() {
    let _ = core::mem::size_of::<crate::content::TransferStats>();
    let _ = core::mem::size_of::<
        crate::cache::BlockingContentStore<crate::content::MemoryContentStore>,
    >();
}
