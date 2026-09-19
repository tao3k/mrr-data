use crate::{
    ARROW_FACT_SCHEMA_NAMESPACE, ARROW_FACT_SCHEMA_VERSION, GRAPHAR_BINARY_ENTITY_NAMESPACE,
    GRAPHAR_BINARY_ENTITY_VERSION, SNAPSHOT_SCHEMA_NAMESPACE, SNAPSHOT_SCHEMA_VERSION,
};

#[test]
fn namespaces_never_embed_schema_versions() {
    for namespace in [
        SNAPSHOT_SCHEMA_NAMESPACE,
        ARROW_FACT_SCHEMA_NAMESPACE,
        GRAPHAR_BINARY_ENTITY_NAMESPACE,
    ] {
        assert!(!namespace.contains("v1"));
        assert!(!namespace.ends_with(".1"));
    }
    assert_eq!(SNAPSHOT_SCHEMA_VERSION, 1);
    assert_eq!(ARROW_FACT_SCHEMA_VERSION, 1);
    assert_eq!(GRAPHAR_BINARY_ENTITY_VERSION, 1);
}
