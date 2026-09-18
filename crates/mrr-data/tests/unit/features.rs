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
